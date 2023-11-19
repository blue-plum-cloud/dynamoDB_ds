package base

import (
	"bytes"
	"config"
	"constants"
	"fmt"
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
	if _, exists := n.awaitAck[token.phy_id]; !exists {
		n.awaitAck[token.phy_id] = new(atomic.Bool)
	}
	n.awaitAck[token.phy_id].Store(true)
	n.channels[token.phy_id] <- msg

	reqTime := time.Now()

	for {
		if !(n.awaitAck[token.phy_id].Load()) {
			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("restoreHandoff: %d->%d complete.\n", n.GetID(), token.phy_id)
			}
			delete(n.backup, token.phy_id)
			return
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

	if c.W > c.NUM_NODES {
		wCount = c.NUM_NODES
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
func FindNode(key string, phy_nodes []*Node, c *config.Config) *Node {
	hashkey := ComputeMD5(key)
	root := phy_nodes[0]

	// bst_node satisfies hashInRange(value, bst_node.Token.GetStartRange(), bst_node.Token.GetEndRange())
	bst_node := root.tokenStruct.Search(hashkey, c)
	if bst_node == nil {
		panic("node not found due to key being out of range of all tokens")
	}
	return phy_nodes[bst_node.Token.phy_id]
}

// internal function
func (n *Node) Put(msg Message, value string, c *config.Config) {
	replicationCount := GetReplicationCount(c)

	W := getWCount(c)
	ackSent := false

	hashKey := ComputeMD5(msg.Key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()

	// Replication process
	curTreeNode := n.tokenStruct.Search(hashKey, c)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique physical nodes. Use map as set. Use struct{} to occupy 0 space

	// Retrieve preference list and start replication
	pref_list, ok := n.prefList[initToken]
	if !ok {
		pref_list = nil
		if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
			fmt.Println("Token is not found on the preference list")
		}
		return
	}

	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("Put: Coordinator node = %d, token = %d, responsible for hashkey = %032X, replicationCount %d\n", n.GetID(), curTreeNode.Token.id, hashKey, replicationCount)
	}

	// Coordinator copy
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: false}
	n.data[hashKey] = &newObj // do not handle coordinator die
	visitedNodes[initToken.phy_id] = struct{}{}
	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("Put: Stored to (corodinator) token %d, node=%d\n", initToken.GetID(), initToken.phy_id)
	}

	if replicationCount <= 1 {
		ackSent = true
		msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_WRITE, Key: msg.Key, Data: msg.Data, SrcID: n.GetID(), ObjData: msg.ObjData}
	}

	// ptr is for the node we originally want to replicate to
	// curToken is the node that we want to replicate to (does not necessarily need to be the original one)
	var ptr *TreeNode
	var curToken *TreeNode
	var iterList []*TreeNode

	// Wg for replication go routine
	waitRep := new(sync.WaitGroup)

	// To store list of nodes to replicate to
	queue := Queue{
		Data: []*TreeNode{},
		Lock: sync.Mutex{},
	}

	// For first batch replication, queue will be the same as iterList
	for i := 0; i < len(pref_list); i++ {
		queue.Add(pref_list[i])
		iterList = append(iterList, pref_list[i])
	}

	// In first batch replication, we do not do handOff
	isHandOff := false

	successfulReplication := 1
	first := true
	for {
		if queue.Empty() {
			break
		}
		temp := Queue{
			Data: []*TreeNode{},
			Lock: sync.Mutex{},
		}
		cnt := 0
		// Only for the first batch
		// Example: prefList for token2 having pid = 2, [7,3,2].
		// the first in the preflist not necessarily the phy node for itself given
		// how the prefList is implemented rn. Since we want the original copy to be on
		// the phy node thats why we want to for cnt == len(queue) - 1
		for i := 0; i < len(queue.Data); i++ {
			if cnt == len(queue.Data)-1 && first {
				first = false
				break
			}
			ptr = queue.Data[i]
			curToken = iterList[i]
			if isHandOff {
				fmt.Printf("Attempting to replicate to node %d from failing node %d", curToken.Token.phy_id, ptr.Token.phy_id)
			}

			if _, visited := visitedNodes[curToken.Token.phy_id]; !visited {
				cnt++
				visitedNodes[ptr.Token.GetPID()] = struct{}{}
				waitRep.Add(1)
				go n.replicate(value, copy_vclk, msg, hashKey, isHandOff, curToken, ptr, c, &temp, waitRep)
			}
		}

		// The first iteration is replication not handOff
		if !isHandOff {
			isHandOff = true
		}
		waitRep.Wait()

		// Update queue for next batch rep

		// temp is to store the node that fails to replicate
		successfulReplication += cnt - len(temp.Data)

		if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
			fmt.Printf("Current replicated = %d\n", successfulReplication)
			fmt.Printf("W = %d\n", W)
			for i := 0; i < len(queue.Data); i++ {
				fmt.Println(queue.Data[i].Token.GetPID())
			}
		}

		// sloppy quorum after W rep sent ack to client
		if successfulReplication >= W && !ackSent {
			ackSent = true
			msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_WRITE, Key: msg.Key, Data: msg.Data, SrcID: n.GetID()}
		}

		if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
			fmt.Println("Temp data")
			fmt.Println(temp.Data)
		}

		queue.Data = temp.Data

		// For next batch replication cannot use preference list
		// Cause of how preference list is calculated there is a chance that
		// token6 preference list ends with token6 ptr which may ends in infinite loop
		iterList = []*TreeNode{}
		cnt = 0
		for cnt < len(queue.Data) {
			nxt := n.tokenStruct.getNext(curToken)
			if _, visited := visitedNodes[nxt.Token.phy_id]; !visited {
				iterList = append(iterList, nxt)
				cnt++
			}
			ptr = nxt
		}

		// is the flag only intended for first batch replication
		first = false
	}
}

func (n *Node) replicate(value string, copy_vclk []int, mssg Message, hashKey string, isHandoff bool, curToken *TreeNode, restoreToken *TreeNode, c *config.Config, queue *Queue, wg *sync.WaitGroup) {
	defer wg.Done()
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
	msg := &Message{JobId: mssg.JobId, Command: constants.SET_DATA, Key: hashKey, ObjData: &newObj, SrcID: n.GetID()}
	// If it is in handoff mode, change command to BACK_DATA
	if isHandoff {
		msg.Command = constants.BACK_DATA
		msg.HandoffToken = restoreToken.Token
	}
	updateSuccess := n.updateToken(curToken.Token, *msg, c)

	// if replication fails or handoff fails, add to queue for the next batch of replication
	queue.Lock.Lock()
	if !updateSuccess {
		fmt.Println("Update failed")
		queue.Data = append(queue.Data, curToken)
		fmt.Println(queue.Data)
	}
	queue.Lock.Unlock()
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
