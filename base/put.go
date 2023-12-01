package base

import (
	"config"
	"constants"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

/* Get Replication count */
func GetReplicationCount(c *config.Config) int {
	replicationCount := 0

	if c.N >= 0 {
		replicationCount = c.NUM_NODES
		if c.NUM_TOKENS < c.NUM_NODES {
			replicationCount = c.NUM_TOKENS
		}
		if c.N < replicationCount {
			replicationCount = c.N
		}
	}

	return replicationCount
}

func getWCount(c *config.Config) int {
	wCount := 0
	if c.W > 0 {
		wCount = c.W
	}

	if wCount > c.NUM_NODES {
		wCount = c.NUM_NODES
	}

	if wCount > c.N {
		wCount = c.N
	}
	return wCount
}

/* Send message to update the node with object. Returns True if ACK receive within timeout, False otherwise */
func (n *Node) updateToken(token *Token, msg Message, c *config.Config) bool {
	n.mutex.Lock()
	if _, exists := n.awaitAck[token.phy_id]; !exists {
		n.awaitAck[token.phy_id] = new(atomic.Bool)
	}
	n.awaitAck[token.phy_id].Store(true)
	n.channels[token.phy_id] <- msg
	n.mutex.Unlock()
	reqTime := time.Now()

	for {
		n.mutex.Lock()
		if !(n.awaitAck[token.phy_id].Load()) {
			n.mutex.Unlock()
			return true
		} else {
			n.mutex.Unlock()
		}
		if time.Since(reqTime) > time.Duration(c.SET_DATA_TIMEOUT_MS)*time.Millisecond {
			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("updateToken: %d->%d timeout reached.\n", n.GetID(), token.phy_id)
			}
			return false
		}
	}
}

/* Issue replication request, update a queue if replication failed */
func (n *Node) replicate(repJob *ReplicationJob, c *config.Config, failedRepQueue *ReplicationQueue, wg *sync.WaitGroup) {
	defer wg.Done()
	updateSuccess := n.updateToken(repJob.dst.Token, repJob.msg, c)

	// if replication fails or handoff fails, add to queue for the next batch of replication
	failedRepQueue.Lock.Lock()
	if !updateSuccess {
		failedRepQueue.Add(repJob)
	}
	failedRepQueue.Lock.Unlock()
}

/*
 1. Populate initial batch requests
 2. Loop while replication jobs not done
    a. Issue concurrent batch replication requests, store failed jobs
    b. Check for sloppy quorum condition, ACK if success
    c. Populate next batch requests by traversing ring and updating last batch request
*/
func (n *Node) Put(msg Message, value string, c *config.Config) {
	replicationCount := GetReplicationCount(c)

	W := getWCount(c)
	ackSent := false

	hashKey := ComputeMD5(msg.Key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()

	initToken := n.tokenStruct.Search(hashKey, c).Token

	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("Put: Coordinator node = %d, token = %d, responsible for hashkey = %032X, replicationCount %d.\n", n.GetID(), initToken.id, hashKey, replicationCount)
	}

	// Retrieve preference list
	pref_list, ok := n.prefList[initToken]
	if !ok {
		pref_list = nil
		if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
			fmt.Println("Token is not found on the preference list")
		}
		return
	}

	visitedNodes := make(map[int]struct{}) // visited unique nodes. Use map as set, struct{} to occupy 0 space
	var curTreeNode *TreeNode              // enforce traversal order despite concurrent replicate requests
	var repJobs []*ReplicationJob          // replication jobs per batch iteration
	for i := 0; i < c.N; i++ {             // populate first batch request
		repObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
		repMsg := Message{JobId: msg.JobId, Command: constants.SET_DATA, Key: hashKey, ObjData: &repObj, SrcID: n.GetID(), HandoffToken: pref_list[i].Token}
		repJob := ReplicationJob{msg: repMsg, dst: pref_list[i]}
		repJobs = append(repJobs, &repJob)
	}
	repJobs[0].msg.ObjData.isReplica = false

	// main replication loop
	for {
		if len(repJobs) == 0 {
			break
		}

		// failed replication jobs this batch
		failedRepQueue := ReplicationQueue{
			Data: []*ReplicationJob{},
			Lock: sync.Mutex{},
		}
		waitRep := new(sync.WaitGroup)

		// concurrent batch request
		for _, repJob := range repJobs {
			curTreeNode = repJob.dst // enforce iteration order through batch iters
			if _, visited := visitedNodes[curTreeNode.Token.phy_id]; !visited {
				visitedNodes[curTreeNode.Token.phy_id] = struct{}{}
				waitRep.Add(1)
				go n.replicate(repJob, c, &failedRepQueue, waitRep)
			} else {
				failedRepQueue.Add(repJob)
			}
		}

		waitRep.Wait()

		// sloppy quorum: after W replications, sent ACK to client
		if replicationCount-len(failedRepQueue.Data) >= W && !ackSent {
			ackSent = true
			msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_WRITE, Key: msg.Key, Data: msg.Data, SrcID: n.id}
		}

		// populate next batch request
		cnt := 0
		repJobs = make([]*ReplicationJob, 0)
		for cnt < len(failedRepQueue.Data) {
			nxt := n.tokenStruct.getNext(curTreeNode)
			if nxt.Token.GetID() == initToken.GetID() {
				fmt.Printf("Put: ERROR! Only replicated %d/%d times!\n", replicationCount-len(failedRepQueue.Data), c.N)
				break
			}
			if _, visited := visitedNodes[nxt.Token.phy_id]; !visited {
				failedRepQueue.Data[cnt].dst = nxt
				failedRepQueue.Data[cnt].msg.Command = constants.BACK_DATA // subsequent replications are handoffs
				repJobs = append(repJobs, failedRepQueue.Data[cnt])
				cnt++
			}
			curTreeNode = nxt
		}
	}
}
