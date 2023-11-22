package base

import (
	"bytes"
	"config"
	"constants"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/* hijacks Start(), message commands processed is CLIENT_REQ_REVIVE only */
func (n *Node) busyWait(duration int, c *config.Config) {
	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("\nbusyWait: %d killed for %d ms...\n", n.GetID(), duration)
	}
	reviveTime := time.Now().Add(time.Millisecond * time.Duration(duration))

	for {
		select {
		case <-n.close_ch:
			return

		case msg := <-n.rcv_ch: // consume rcv_ch and throw
			if msg.Command == constants.CLIENT_REQ_REVIVE {
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("\nbusyWait: %d reviving...\n", n.GetID())
				}
				return
			}

		case <-time.After(10 * time.Millisecond): // check in intervals of 10ms
			if time.Since(reviveTime) >= 0 {
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("\nbusyWait: %d reviving...\n", n.GetID())
				}
				return
			}

		}
	}
}

/* Send message to update the node with object, Keeps retrying forever.*/
func (n *Node) restoreHandoff(token *Token, msg Message, c *config.Config) {
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
			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("restoreHandoff: %d->%d complete.\n", n.GetID(), token.phy_id)
			}
			delete(n.backup, token.phy_id)
			n.mutex.Unlock()
			return
		} else {
			n.mutex.Unlock()
		}
		if time.Since(reqTime) > time.Duration(config.SET_DATA_TIMEOUT_MS)*time.Millisecond {
			if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
				fmt.Printf("restoreHandoff: %d->%d timeout reached. Retrying...\n", n.GetID(), token.phy_id)
			}
			n.channels[token.phy_id] <- msg
			reqTime = time.Now()
		}
	}
}

func (n *Node) Start(wg *sync.WaitGroup, c *config.Config) {
	defer wg.Done()

	for {
		select {
		case <-n.close_ch:
			// fmt.Println("[", n.id, "]", "node is closing")
			return

		case msg := <-n.rcv_ch:
			var debugMsg bytes.Buffer // allow appending of messages
			debugMsg.WriteString(fmt.Sprintf("Start: %s ", msg.ToString(n.GetID())))

			switch msg.Command {
			case constants.CLIENT_REQ_READ:
				go n.Get(msg, c) // nicer debug message (CLIENT_REQ_READ before subsequent handling messages)

			case constants.CLIENT_REQ_WRITE:
				go n.Put(msg, msg.Data, c)

			case constants.CLIENT_REQ_KILL:
				duration, err := strconv.Atoi(strings.TrimSpace(msg.Data))
				if err != nil {
					fmt.Printf("CLIENT_REQ_KILL ERROR: %d->%d invalid duration %s, expect integer value denoting milliseconds to kill for.", msg.SrcID, n.GetID(), msg.Data)
				}
				n.busyWait(duration, c) // blocking

			case constants.SET_DATA:
				n.data[msg.Key] = msg.ObjData
				n.channels[msg.SrcID] <- Message{JobId: msg.JobId, Command: constants.ACK_SET_DATA, Key: msg.Key, SrcID: n.GetID(), ObjData: msg.ObjData}

			case constants.BACK_DATA:
				backupID := msg.HandoffToken.phy_id
				if _, exists := n.backup[backupID]; !exists {
					n.backup[backupID] = make(map[string]*Object)
				}
				n.backup[backupID][msg.Key] = msg.ObjData
				n.channels[msg.SrcID] <- Message{JobId: msg.JobId, Command: constants.ACK_BACK_DATA, Key: msg.Key, SrcID: n.GetID()}
				msg.Command = constants.SET_DATA
				msg.SrcID = n.GetID()
				go n.restoreHandoff(msg.HandoffToken, msg, c)

			case constants.READ_DATA: //coordinator requested to read data, so send it back
				//return data
				obj := n.data[msg.Key]
				fmt.Printf("[%d] send acknowledgement\n", n.id)
				n.channels[msg.SrcID] <- Message{JobId: msg.JobId, Command: constants.READ_DATA_ACK, Key: msg.Key, SrcID: n.GetID(), ObjData: obj, Client_Ch: msg.Client_Ch}

			case constants.READ_DATA_ACK:
				debugMsg.WriteString(fmt.Sprintf("numReads: %d", n.numReads))
				if n.numReads[msg.JobId] == c.R {
					n.reconcile(n.data[msg.Key], msg.ObjData)
					msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_READ, Key: msg.Key, Data: n.data[msg.Key].data, SrcID: n.GetID()}
				} else {
					n.reconcile(n.data[msg.Key], msg.ObjData)
				}
				n.numReads[msg.JobId]++

			case constants.ACK_SET_DATA:
				n.mutex.Lock()
				n.awaitAck[msg.SrcID].Store(false)
				n.mutex.Unlock()

			case constants.ACK_BACK_DATA:
				n.mutex.Lock()
				n.awaitAck[msg.SrcID].Store(false)
				n.mutex.Unlock()

			case constants.ALIVE_ACK:
				msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_ALIVE, Key: msg.Key, Data: msg.Data, SrcID: n.id}
			}

			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("%s\n", debugMsg.String())
			}

		case jobId := <-n.readTimeout:
			if n.numReads[jobId] < c.R {
				fmt.Println("Quorum not fulfilled for get(), get() failed")
				n.numReads[jobId] = -c.NUM_NODES //set to some negative number so it will not send
			}
		}
	}
}

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
			if c.DEBUG_LEVEL >= constants.INFO {
				fmt.Printf("updateToken: Replicated to token=%d, node=%d\n", token.GetID(), token.phy_id)
			}
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

/* Traverses token BST struc, assumed to be consistent across nodes 0 and other nodes */
func FindNode(key string, phy_nodes []*Node, c *config.Config) (*Token, *Node) {
	hashkey := ComputeMD5(key)
	ctc := rand.Intn(len(phy_nodes))
	root := phy_nodes[ctc]

	// bst_node satisfies hashInRange(value, bst_node.Token.GetStartRange(), bst_node.Token.GetEndRange())
	bst_node := root.tokenStruct.Search(hashkey, c)
	if bst_node == nil {
		panic("node not found due to key being out of range of all tokens")
	}
	return bst_node.Token, phy_nodes[bst_node.Token.phy_id]
}

func FindPrefList(token *Token, phy_nodes []*Node, cnt int) *Node {
	ctc := rand.Intn(len(phy_nodes))
	root := phy_nodes[ctc]
	pref := root.prefList[token]
	// Assuming at least one node in the preference list alive
	if cnt > len(pref) {
		return nil
	}
	return phy_nodes[pref[cnt].Token.phy_id]
}

/*
1. Updates coordinator value
2. Concurrent replicate to N
3. Store tokens for timeout replica
4. Concurrent handoff to K
5. Repeat 3 to 4 till done or run out
*/
func (n *Node) Put(msg Message, value string, c *config.Config) {
	replicationCount := GetReplicationCount(c)

	W := getWCount(c)
	ackSent := false

	hashKey := ComputeMD5(msg.Key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()

	initToken := n.tokenStruct.Search(hashKey, c).Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique ph\ysical nodes. Use map as set. Use struct{} to occupy 0 space

	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("Put: Coordinator node = %d, token = %d, responsible for hashkey = %032X, replicationCount %d. Stored to self\n", n.GetID(), initToken.id, hashKey, replicationCount)
	}

	// Retrieve preference list and start replication
	pref_list, ok := n.prefList[initToken]
	if !ok {
		pref_list = nil
		if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
			fmt.Println("Token is not found on the preference list")
		}
		return
	}

	// restoreToken is for the node we originally want to replicate to
	// curToken is the node that we want to replicate to (does not necessarily need to be the original one)
	var restoreToken *TreeNode
	var curToken *TreeNode
	var iterList []*TreeNode
	var repCoor bool

	// Wg for replication go routine
	waitRep := new(sync.WaitGroup)

	// To store list of nodes to replicate to
	queue := Queue{
		Data: []*TreeNode{},
		Rep:  []bool{},
		Lock: sync.Mutex{},
	}

	// For first batch replication, queue will be the same as iterList
	if len(pref_list) > 0 {
		queue.Add(pref_list[0], true)
		iterList = append(iterList, pref_list[0])
	}

	for i := 1; i < len(pref_list); i++ { // skip coordinator
		queue.Add(pref_list[i], false)
		iterList = append(iterList, pref_list[i])
	}

	// message template
	repObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
	repMsg := &Message{JobId: msg.JobId, Command: constants.SET_DATA, Key: hashKey, ObjData: &repObj, SrcID: n.GetID()}

	repObjCoor := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: false}
	repMsgCoor := &Message{JobId: msg.JobId, Command: constants.SET_DATA, Key: hashKey, ObjData: &repObjCoor, SrcID: n.GetID()}

	// In first batch replication, we do not do handOff
	isHandOff := false
	for {
		if queue.Empty() {
			break
		}

		failedTreeNodes := Queue{
			Data: []*TreeNode{},
			Rep:  []bool{},
			Lock: sync.Mutex{},
		}

		for i := 0; i < len(queue.Data); i++ {
			restoreToken = queue.Data[i]
			repCoor = queue.Rep[i]
			curToken = iterList[i]

			if _, visited := visitedNodes[curToken.Token.phy_id]; !visited {
				visitedNodes[restoreToken.Token.phy_id] = struct{}{}
				waitRep.Add(1)
				handMsg := repMsg.Copy()
				if repCoor {
					handMsg = repMsgCoor.Copy()
				}
				if isHandOff {
					handMsg.Command = constants.BACK_DATA
					handMsg.HandoffToken = restoreToken.Token

					fmt.Printf("Attempting to replicate to node %d from failing node %d\n", curToken.Token.phy_id, restoreToken.Token.phy_id)
					go n.replicate(handMsg, curToken, c, &failedTreeNodes, waitRep)
				} else {
					go n.replicate(handMsg, curToken, c, &failedTreeNodes, waitRep)
				}
			}
		}

		waitRep.Wait()

		// Update queue for next batch rep
		// failedTreeNodes is to store the node that fails to replicate
		if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
			fmt.Printf("Current replicated = %d\n", replicationCount-len(failedTreeNodes.Data))
			for _, failedTreeNode := range failedTreeNodes.Data {
				fmt.Printf("failed node=%d, token=%d\n", failedTreeNode.Token.phy_id, failedTreeNode.Token.phy_id)
			}
		}

		// sloppy quorum after W rep sent ack to client
		if replicationCount-len(failedTreeNodes.Data) >= W && !ackSent {
			ackSent = true
			msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_WRITE, Key: msg.Key, Data: msg.Data, SrcID: n.id}
		}

		queue.Data = failedTreeNodes.Data

		// For next batch replication cannot use preference list
		// Cause of how preference list is calculated there is a chance that
		// token6 preference list ends with token6 restoreToken which may ends in infinite loop
		iterList = []*TreeNode{}
		cnt := 0
		loop := 0
		for cnt < len(queue.Data) {
			nxt := n.tokenStruct.getNext(curToken)
			if loop > c.NUM_TOKENS {
				iterList = append(iterList, nxt)
				cnt++
			} else if _, visited := visitedNodes[nxt.Token.phy_id]; !visited {
				iterList = append(iterList, nxt)
				cnt++
			}
			curToken = nxt
			loop++
		}

		// The first iteration is replication not handOff
		if !isHandOff {
			isHandOff = true
		}
	}
}

func (n *Node) replicate(msg Message, curToken *TreeNode, c *config.Config, failedTreeNodes *Queue, wg *sync.WaitGroup) {
	defer wg.Done()
	updateSuccess := n.updateToken(curToken.Token, msg, c)

	// if replication fails or handoff fails, add to queue for the next batch of replication
	failedTreeNodes.Lock.Lock()
	if !updateSuccess {
		failedTreeNodes.Data = append(failedTreeNodes.Data, curToken)
		failedTreeNodes.Rep = append(failedTreeNodes.Rep, true)
	}
	failedTreeNodes.Lock.Unlock()
}

// helper func for GET
func (n *Node) requestTreeNodeData(curToken *Token, hashKey string, c *config.Config) *Object {
	if _, exists := n.awaitAck[curToken.phy_id]; !exists {
		n.awaitAck[curToken.phy_id] = new(atomic.Bool)
	}

	n.awaitAck[curToken.phy_id].Store(true)
	n.channels[curToken.phy_id] <- Message{Command: constants.CLIENT_REQ_READ, Key: hashKey, SrcID: n.GetID()}
	reqTime := time.Now()

	for {
		if !(n.awaitAck[curToken.phy_id].Load()) {
			//receive the ACK with data
			resp := <-n.rcv_ch
			return resp.ObjData
		}

		if time.Since(reqTime) > time.Duration(c.SET_DATA_TIMEOUT_MS)*time.Millisecond {
			fmt.Printf("node %d: request node %d timeout reached.\n", n.GetID(), curToken.phy_id)
			break
		}
	}
	return nil
}

// attempt to reconcile original with receiving
func (n *Node) reconcile(original *Object, replica *Object) {
	fmt.Println(replica)
	if replica == nil {
		return //don't reconcile if there is nothing at replica
	}

	//if 1, means the replica has a strictly greater clock, reconcile.
	if compareVC(replica.context.v_clk, original.context.v_clk) == 1 {
		original.data = replica.data
		original.context = replica.Copy().context
	}
	//if 0, means the replica and original have concurrent copies. original keeps its own copy
	//if -1, means latestObj=objects[0] alrd has the latest clock
}

// helper func for GET
// if A -> B, A strictly lesser than B
func compareVC(a, b []int) int {
	for i := range a {
		if a[i] > b[i] {
			return -1
		} else if a[i] < b[i] {
			return 1
		}
	}
	return 0
}

// internal function GET
func (n *Node) Get(msg Message, c *config.Config) {
	n.increment_vclk()
	hashKey := ComputeMD5(msg.Key)

	if _, exists := n.data[hashKey]; !exists {
		return
	}

	n.numReads[msg.JobId] = 1
	//set up timer for the particular jobId here

	curTreeNode := n.tokenStruct.Search(hashKey, c)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique physical nodes

	reqCounter := 0

	for reqCounter < c.N-1 {
		curTreeNode = n.tokenStruct.getNext(curTreeNode)
		curToken := curTreeNode.Token

		if curToken.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[curToken.phy_id]; !visited {
			n.channels[curToken.phy_id] <- Message{JobId: msg.JobId, Command: constants.READ_DATA, Key: hashKey, SrcID: n.GetID(), Client_Ch: msg.Client_Ch}
			visitedNodes[curToken.phy_id] = struct{}{}
			reqCounter++
		}
	}

	go n.startTimer(time.Duration(c.CLIENT_GET_TIMEOUT_MS)*time.Millisecond, msg.JobId)

}

func (n *Node) startTimer(duration time.Duration, jobId int) {
	timer := time.NewTimer(duration)
	<-timer.C
	n.readTimeout <- jobId
}

func CreateNodes(close_ch chan struct{}, c *config.Config) []*Node {
	fmt.Println("Constructing machines...")

	numNodes := c.NUM_NODES
	var nodeGroup []*Node
	for j := 0; j < numNodes; j++ {
		pl := make(map[*Token][]*TreeNode, c.NUM_TOKENS)
		for i := range pl {
			pl[i] = make([]*TreeNode, c.N)
		}

		//make j nodes
		node := Node{
			id:          j,
			v_clk:       make([]int, numNodes),
			channels:    make(map[int](chan Message)),
			rcv_ch:      make(chan Message, numNodes),
			data:        make(map[string]*Object),
			backup:      make(map[int](map[string]*Object)),
			tokenStruct: BST{},
			close_ch:    close_ch,
			awaitAck:    make(map[int](*atomic.Bool)),
			prefList:    pl,
			numReads:    make(map[int]int),
			readTimeout: make(chan int),
		}

		nodeGroup = append(nodeGroup, &node)
	}

	// TODO: improve ease of adding and removing node channels
	//assign send channels usig other node's rcv channels to current node
	for j := 0; j < numNodes; j++ {
		//get pointer of node
		target_machine := nodeGroup[j]
		for i := 0; i < numNodes; i++ {
			machine := nodeGroup[i]
			receive_channel := machine.rcv_ch
			(*target_machine).channels[machine.GetID()] = receive_channel
		}
	}

	return nodeGroup

}

func (n *Node) copy_vclk() []int {
	copy_clk := make([]int, len(n.v_clk)) //send time of election
	copy(copy_clk, n.v_clk)
	return copy_clk
}

func (n *Node) increment_vclk() {
	n.v_clk[n.id]++
}

func (n *Node) GetAllData() map[string]*Object {
	return n.data
}

func (n *Node) GetAllBackup() map[int]map[string]*Object {
	return n.backup
}
