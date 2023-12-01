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
				n.numReads[msg.JobId]++
				debugMsg.WriteString(fmt.Sprintf("numReads: %d", n.numReads))
				R := getRCount(c)
				if n.numReads[msg.JobId] == R {
					n.reconcile(n.data[msg.Key], msg.ObjData)
					msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_READ, Key: msg.Key, Data: n.data[msg.Key].data, SrcID: n.GetID()}
				} else {
					n.reconcile(n.data[msg.Key], msg.ObjData)
				}

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
			R := getRCount(c)
			if n.numReads[jobId] < R {
				fmt.Println("Quorum not fulfilled for get(), get() failed")
				n.numReads[jobId] = -c.NUM_NODES //set to some negative number so it will not send
			}
		}
	}
}

func getRCount(c *config.Config) int {
	rCount := 0
	if c.R > 0 {
		rCount = c.R
	}

	if rCount > c.NUM_NODES {
		rCount = c.NUM_NODES
	}

	if rCount > c.N {
		rCount = c.N
	}
	return rCount
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
	if cnt >= len(pref) {
		return nil
	}
	return phy_nodes[pref[cnt].Token.phy_id]
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

	R := getRCount(c)

	if _, exists := n.data[hashKey]; !exists {
		return
	}

	//consider trivial case where R = 1
	//function just passes its data to the client and returns
	if R == 1 {
		msg.Client_Ch <- Message{JobId: msg.JobId, Command: constants.CLIENT_ACK_READ, Key: hashKey, Data: n.data[hashKey].data, SrcID: n.GetID()}
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
