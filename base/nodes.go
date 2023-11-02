package base

import (
	"config"
	"constants"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/* hijacks Start(), message commands processed is REQ_REVIVE only */
func (n *Node) busyWait(duration int, c *config.Config) {
	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("\nbusyWait: %d killed for %d ms...\n", n.GetID(), duration)
	}
	reviveTime := time.Now().Add(time.Millisecond * time.Duration(duration))

	for {
		select {
		case msg := <-n.rcv_ch: // consume rcv_ch and throw
			if msg.Command == constants.REQ_REVIVE {
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
			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("restoreHandoff: %d->%d timeout reached. Retrying...\n", n.GetID(), token.phy_id)
			}
			n.channels[token.phy_id] <- msg
			reqTime = time.Now()
		}
	}
}

func (n *Node) Start(wg *sync.WaitGroup, c *config.Config) {
	defer wg.Done()

	//put timer
	getTimer := time.NewTimer(time.Duration(c.CLIENT_GET_TIMEOUT_MS) * time.Millisecond)
	getTimer.Stop()

	for {
		select {
		case <-n.close_ch:
			// fmt.Println("[", n.id, "]", "node is closing")
			return

		case msg := <-n.rcv_ch:
			switch msg.Command {
			case constants.REQ_READ:
				n.Get(msg.Key, c)
				// n.client_ch <- Message{Command: constants.ACK, Key: msg.Key, Data: obj.GetData(), SrcID: n.GetID()}

			case constants.REQ_WRITE:
				go n.Put(msg.Key, msg.Data, c)

			case constants.REQ_KILL:
				duration, err := strconv.Atoi(strings.TrimSpace(msg.Data))
				if err != nil {
					fmt.Printf("REQ_KILL ERROR: %d->%d invalid duration %s, expect integer value denoting milliseconds to kill for.", msg.SrcID, n.GetID(), msg.Data)
				}
				n.busyWait(duration, c) // blocking

			case constants.SET_DATA:
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("Start: %d->%d SET_DATA, message info: key=%s, object=(%s)\n", msg.SrcID, n.GetID(), msg.Key, msg.ObjData.ToString())
				}
				n.data[msg.Key] = msg.ObjData
				n.channels[msg.SrcID] <- Message{Command: constants.ACK, Key: msg.Key, SrcID: n.GetID()}

			case constants.BACK_DATA:
				if c.DEBUG_LEVEL >= 1 {
					fmt.Printf("Start: %d->%d BACK_DATA, message info: key=%s, object=(%s)\n", msg.SrcID, n.GetID(), msg.Key, msg.ObjData.ToString())
				}
				backupID := msg.HandoffToken.phy_id
				if _, exists := n.backup[backupID]; !exists {
					n.backup[backupID] = make(map[string]*Object)
				}
				n.backup[backupID][msg.Key] = msg.ObjData
				n.channels[msg.SrcID] <- Message{Command: constants.ACK, Key: msg.Key, SrcID: n.GetID()}
				msg.Command = constants.SET_DATA
				msg.SrcID = n.GetID()
				go n.restoreHandoff(msg.HandoffToken, msg, c)

			case constants.READ_DATA: //coordinator requested to read data, so send it back
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("Start: %d->%d READ_DATA, message info: key=%s\n", msg.SrcID, n.GetID(), msg.Key)
				}
				//return data
				obj := n.data[msg.Key]
				n.channels[msg.SrcID] <- Message{Command: constants.READ_DATA_ACK, Key: msg.Key, SrcID: n.GetID(), ObjData: obj}

			case constants.READ_DATA_ACK:
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("Start: %d->%d READ_DATA_ACK, message info: key=%s, object=(%s), numReads: %d\n", msg.SrcID, n.GetID(), msg.Key, msg.ObjData.ToString(), n.numReads)
				}
				if n.numReads == c.R {
					n.reconcile(n.data[msg.Key], msg.ObjData)
					fmt.Println(msg.Key)
					n.client_ch <- Message{Command: constants.ACK, Key: msg.Key, Data: n.data[msg.Key].data, SrcID: n.GetID()}
				} else {
					n.reconcile(n.data[msg.Key], msg.ObjData)
				}
				n.numReads++

			case constants.ACK:
				if c.DEBUG_LEVEL >= constants.INFO {
					fmt.Printf("Start: %d->%d ACK received\n", msg.SrcID, n.GetID())
				}
				n.awaitAck[msg.SrcID].Store(false)

			}

		case <-getTimer.C:
			if n.numReads < c.R {
				fmt.Println("Quorum not fulfilled for get(), get() is failed")
			}
			n.numReads = 0
			getTimer.Reset(time.Duration(c.CLIENT_GET_TIMEOUT_MS))
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
	if _, exists := n.awaitAck[token.phy_id]; !exists {
		n.awaitAck[token.phy_id] = new(atomic.Bool)
	}
	n.awaitAck[token.phy_id].Store(true)
	n.channels[token.phy_id] <- msg

	reqTime := time.Now()

	for {
		if !(n.awaitAck[token.phy_id].Load()) {
			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("updateToken: Replicated to token=%d, node=%d\n", token.GetID(), token.phy_id)
			}
			return true
		}
		if time.Since(reqTime) > time.Duration(c.SET_DATA_TIMEOUT_MS)*time.Millisecond {
			if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
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
// Can refactor in the future to use dict instead so we know what are the key and value
// rather than accessing it by index which we not sure which correspond to which
func (n *Node) Put(key string, value string, c *config.Config) {
	replicationCount := GetReplicationCount(c)

	W := getWCount(c)

	hashKey := ComputeMD5(key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()

	// Replication process
	curTreeNode := n.tokenStruct.Search(hashKey, c)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique physical nodes. Use map as set. Use struct{} to occupy 0 space
	handoffQueue := make([]*Token, 0)

	if c.DEBUG_LEVEL >= constants.INFO {
		fmt.Printf("Put: Coordinator node = %d, token = %d, responsible for hashkey = %032X, replicationCount %d\n", n.GetID(), curTreeNode.Token.id, hashKey, replicationCount)
	}

	// Coordinator copy
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: false}
	n.data[hashKey] = &newObj // do not handle coordinator die
	visitedNodes[initToken.phy_id] = struct{}{}

	if replicationCount == 0 {
		n.client_ch <- Message{Command: constants.ACK, Key: key, SrcID: n.GetID()}
	}

	successfulReplication := 1
	for successfulReplication < replicationCount {
		nextTreeNode := n.tokenStruct.getNext(curTreeNode)
		curTreeNode = nextTreeNode
		curToken := curTreeNode.Token

		// After one loop stop.
		if curToken.GetID() == initToken.GetID() {
			fmt.Printf("Put: ERROR! Only replicated %d/%d times!\n", successfulReplication, c.N)
			break
		}

		if _, visited := visitedNodes[curToken.phy_id]; !visited {
			isHandoff := len(visitedNodes) > replicationCount - 1 // counts coordinator node
			newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
			msg := &Message{Command: constants.SET_DATA, Key: hashKey, ObjData: &newObj, SrcID: n.GetID()}
			if isHandoff {
				msg.Command = constants.BACK_DATA
				msg.HandoffToken = handoffQueue[0]
			}
			updateSuccess := n.updateToken(curToken, *msg, c)

			// fail SET_DATA: add to list of tokens to restore to
			if !updateSuccess && !isHandoff {
				handoffQueue = append(handoffQueue, curToken)
			}
			if updateSuccess {
				// success BACK_DATA: remove from list of tokens to restore to
				if isHandoff {
					handoffQueue = handoffQueue[1:]
				}
				successfulReplication += 1
			}
			visitedNodes[curToken.phy_id] = struct{}{}
		}

		if successfulReplication == W {
			n.client_ch <- Message{Command: constants.ACK, Key: key, SrcID: n.GetID()}
		}
	}
}

// helper func for GET
func (n *Node) requestTreeNodeData(curToken *Token, hashKey string, c *config.Config) *Object {
	if _, exists := n.awaitAck[curToken.phy_id]; !exists {
		n.awaitAck[curToken.phy_id] = new(atomic.Bool)
	}

	n.awaitAck[curToken.phy_id].Store(true)
	n.channels[curToken.phy_id] <- Message{Command: constants.REQ_READ, Key: hashKey, SrcID: n.GetID()}
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
func (n *Node) Get(key string, c *config.Config) {
	n.increment_vclk()
	hashKey := ComputeMD5(key)

	n.numReads = 1

	curTreeNode := n.tokenStruct.Search(hashKey, c)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique physical nodes

	if _, exists := n.data[hashKey]; !exists {
		return
	}

	reqCounter := 0

	for reqCounter < c.N-1 {
		curTreeNode = n.tokenStruct.getNext(curTreeNode)
		curToken := curTreeNode.Token

		if curToken.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[curToken.phy_id]; !visited {
			n.channels[curToken.phy_id] <- Message{Command: constants.READ_DATA, Key: hashKey, SrcID: n.GetID()}
			visitedNodes[curToken.phy_id] = struct{}{}
			reqCounter++
		}
	}

}

func CreateNodes(client_ch chan Message, close_ch chan struct{}, c *config.Config) []*Node {
	fmt.Println("Constructing machines...")

	numNodes := c.NUM_NODES
	var nodeGroup []*Node
	for j := 0; j < numNodes; j++ {

		//make j nodes
		node := Node{
			id:          j,
			v_clk:       make([]int, numNodes),
			channels:    make(map[int](chan Message)),
			rcv_ch:      make(chan Message, numNodes),
			data:        make(map[string]*Object),
			backup:      make(map[int](map[string]*Object)),
			tokenStruct: BST{},
			client_ch:   client_ch,
			close_ch:    close_ch,
			awaitAck:    make(map[int](*atomic.Bool)),
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
