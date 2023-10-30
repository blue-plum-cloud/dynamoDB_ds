package base

import (
	"config"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func (n *Node) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	//put timer
	getTimer := time.NewTimer(config.CLIENT_GET_TIMEOUT_MS)
	getTimer.Stop()
	for {
		select {
		case <-n.close_ch:
			// fmt.Println("[", n.id, "]", "node is closing")
			return

		case msg := <-n.rcv_ch:
			switch msg.Command {
			case config.REQ_READ:
				n.Get(msg.Key)
				// n.client_ch <- Message{Command: config.ACK, Key: msg.Key, Data: obj.GetData(), SrcID: n.GetID()}

			case config.REQ_WRITE:
				args := []int{config.N, config.NUM_NODES, config.NUM_TOKENS}
				go n.Put(msg.Key, msg.Data, args)

			case config.SET_DATA:
				if config.DEBUG_LEVEL >= 1 {
					fmt.Printf("Start: %d->%d SET_DATA, message info: key=%s, object=(%s)\n", msg.SrcID, n.GetID(), msg.Key, msg.ObjData.ToString())
				}
				n.data[msg.Key] = msg.ObjData
				n.channels[msg.SrcID] <- Message{Command: config.ACK, Key: msg.Key, SrcID: n.GetID()}

			case config.READ_DATA: //coordinator requested to read data, so send it back
				if config.DEBUG_LEVEL >= 1 {
					fmt.Printf("Start: %d->%d READ_DATA, message info: key=%s\n", msg.SrcID, n.GetID(), msg.Key)
				}
				//return data
				obj := n.data[msg.Key]
				n.channels[msg.SrcID] <- Message{Command: config.READ_DATA_ACK, Key: msg.Key, SrcID: n.GetID(), ObjData: obj}

			case config.READ_DATA_ACK:
				if config.DEBUG_LEVEL >= 1 {
					fmt.Printf("Start: %d->%d READ_DATA_ACK, message info: key=%s, object=(%s), numReads: %d\n", msg.SrcID, n.GetID(), msg.Key, msg.ObjData.ToString(), n.numReads)
				}
				if n.numReads == config.R {
					n.reconcile(n.data[msg.Key], msg.ObjData)
					fmt.Println(msg.Key)
					n.client_ch <- Message{Command: config.ACK, Key: msg.Key, Data: n.data[msg.Key].data, SrcID: n.GetID()}
				} else {
					n.reconcile(n.data[msg.Key], msg.ObjData)
				}
				n.numReads++

			case config.ACK:
				if config.DEBUG_LEVEL >= 1 {
					fmt.Printf("Start: %d->%d ACK received\n", msg.SrcID, n.GetID())
				}
				n.awaitAck[msg.SrcID].Store(false)

			}

		case <-getTimer.C:
			if n.numReads < config.R {
				fmt.Println("Quorum not fulfilled for get(), get() is failed")
			}
			n.numReads = 0
			getTimer.Reset(config.CLIENT_GET_TIMEOUT_MS)
		}
	}
}

/* Get Replication count */
func getReplicationCount(nValue []int) int {
	replicationCount := 0
	if len(nValue) == 0 {
		replicationCount = config.N
	} else {
		if nValue[0] >= 0 {
			replicationCount = nValue[1]
			if nValue[2] < nValue[1] {
				replicationCount = nValue[2]
			}
			if nValue[0] < replicationCount {
				replicationCount = nValue[0]
			}
		} else {
			replicationCount = 0
		}
	}
	return replicationCount
}

func getWCount(nValue []int) int {
	wCount := 0
	if len(nValue) == 0 {
		wCount = config.W
	} else {
		if nValue[3] > 0 {
			wCount = nValue[3]
		}

		if nValue[3] > nValue[1] {
			wCount = nValue[1]
		}
	}
	return wCount
}

/* Finds N+1-th available physical node from current node */
func (n *Node) getHintedHandoffToken(initNode *TreeNode, visitedNodes map[int]struct{}, replicationCount int) *Token {
	i := replicationCount
	treeNode := initNode
	initToken := initNode.Token

	for i > 0 {
		treeNode := n.tokenStruct.getNext(treeNode)

		if treeNode.Token.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[treeNode.Token.phy_id]; !visited {
			i -= 1
		}
	}

	return treeNode.Token
}

/* Send message to update the node with object, on timeout, send message to update backup node with the object */
func (n *Node) updateToken(token *Token, visitedNodes map[int]struct{}, msg Message) {
	if _, exists := n.awaitAck[token.phy_id]; !exists {
		n.awaitAck[token.phy_id] = new(atomic.Bool)
	}
	n.awaitAck[token.phy_id].Store(true)
	n.channels[token.phy_id] <- msg

	reqTime := time.Now()

	for {
		if !(n.awaitAck[token.phy_id].Load()) {
			visitedNodes[token.phy_id] = struct{}{}
			if config.DEBUG_LEVEL >= 2 {
				fmt.Printf("Replicated to token=%d, node=%d\n", token.GetID(), token.phy_id)
			}
			break
		}
		if time.Since(reqTime) > config.SET_DATA_TIMEOUT_NS {
			fmt.Printf("node %d: update node %d timeout reached.\n", n.GetID(), token.phy_id)
			// TODO: hinted handoff to N+1-th physical node from current node
			break
		}
	}
}

/* Traverses token BST struc, assumed to be consistent across nodes 0 and other nodes */
func FindNode(key string, phy_nodes []*Node) *Node {
	hashkey := ComputeMD5(key)
	root := phy_nodes[0]

	// bst_node satisfies hashInRange(value, bst_node.Token.GetStartRange(), bst_node.Token.GetEndRange())
	bst_node := root.tokenStruct.Search(hashkey)
	if bst_node == nil {
		panic("node not found due to key being out of range of all tokens")
	}
	return phy_nodes[bst_node.Token.phy_id]
}

func (n *Node) GetChannel() chan Message {
	return n.rcv_ch
}

// internal function
// Pass in global config details (ReplicationNum, PhysicalNum, VirtualNum, W)
// Can refactor in the future to use dict instead so we know what are the key and value
// rather than accessing it by index which we not sure which correspond to which
func (n *Node) Put(key string, value string, nValue []int) {
	replicationCount := getReplicationCount(nValue)

	// TODO: use this when sir Ian adjust the test case for args in nValue
	// W := getWCount(nValue)

	hashKey := ComputeMD5(key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()

	if config.DEBUG_LEVEL >= 1 {
		fmt.Printf("Put: Coordinator node = %d, responsible for hashkey = %032X, replicationCount %d\n", n.GetID(), hashKey, replicationCount)
	}

	// Replication process
	curTreeNode := n.tokenStruct.Search(hashKey)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique physical nodes. Use map as set. Use struct{} to occupy 0 space

	// Coordinator copy
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: false}
	msg := Message{Command: config.SET_DATA, Key: hashKey, ObjData: &newObj, SrcID: n.GetID()}
	n.updateToken(initToken, visitedNodes, msg)

	if replicationCount == 0 {
		n.client_ch <- Message{Command: config.ACK, Key: key, SrcID: n.GetID()}
	}

	for len(visitedNodes) < replicationCount {
		nextTreeNode := n.tokenStruct.getNext(curTreeNode)
		curTreeNode = nextTreeNode
		curToken := curTreeNode.Token

		// After one loop stop.
		if curToken.GetID() == initToken.GetID() {
			fmt.Printf("Put: ERROR! Only replicated %d/%d times!\n", len(visitedNodes), config.N)
			break
		}

		if _, visited := visitedNodes[curToken.phy_id]; !visited {
			newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
			msg := Message{Command: config.SET_DATA, Key: hashKey, ObjData: &newObj, SrcID: n.GetID()}
			n.updateToken(curToken, visitedNodes, msg)
		}

		// Change the config.W with W when the test case is adjusted accordingly
		if len(visitedNodes) == config.W {
			n.client_ch <- Message{Command: config.ACK, Key: key, SrcID: n.GetID()}
		}
	}
}

// helper func for GET
func (n *Node) requestTreeNodeData(curToken *Token, hashKey string) *Object {
	if _, exists := n.awaitAck[curToken.phy_id]; !exists {
		n.awaitAck[curToken.phy_id] = new(atomic.Bool)
	}

	n.awaitAck[curToken.phy_id].Store(true)
	n.channels[curToken.phy_id] <- Message{Command: config.REQ_READ, Key: hashKey, SrcID: n.GetID()}
	reqTime := time.Now()

	for {
		if !(n.awaitAck[curToken.phy_id].Load()) {
			//receive the ACK with data
			resp := <-n.rcv_ch
			return resp.ObjData
		}

		if time.Since(reqTime) > config.SET_DATA_TIMEOUT_NS {
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
func (n *Node) Get(key string) {
	n.increment_vclk()
	hashKey := ComputeMD5(key)

	n.numReads = 1

	curTreeNode := n.tokenStruct.Search(hashKey)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{}) // To keep track of unique physical nodes

	if _, exists := n.data[hashKey]; !exists {
		return
	}

	reqCounter := 0

	for reqCounter < config.N {
		curTreeNode = n.tokenStruct.getNext(curTreeNode)
		curToken := curTreeNode.Token

		if curToken.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[curToken.phy_id]; !visited {
			n.channels[curToken.phy_id] <- Message{Command: config.READ_DATA, Key: hashKey, SrcID: n.GetID()}
			visitedNodes[curToken.phy_id] = struct{}{}
			reqCounter++
		}
	}

}

// func (n *Node) Get_Phase2(key string) *Object {
// 	reqCounter := 0 //keeps track of outstanding requests
// 	//wait for R replicas or timeout
// 	startTime := time.Now()
// 	retrievedObjects := []*Object{}
// 	for reqCounter > 0 && time.Since(startTime) < config.CLIENT_GET_TIMEOUT_MS*time.Millisecond {
// 		select {
// 		case msg := <-n.rcv_ch:
// 			if msg.Command == config.ACK && msg.Key == hashKey {
// 				retrievedObjects = append(retrievedObjects, &Object{data: msg.Data})
// 				reqCounter--
// 			}
// 		default:
// 			continue
// 		}
// 	}

// 	//3. version compare and reconcilation
// 	finalObject := n.reconcile(retrievedObjects)
// 	localObj, exists := n.data[hashKey]

// 	if exists && finalObject != localObj {
// 		fmt.Printf("Key %s found in Node%d \n", key, n.id)
// 		//if localObj differs from retrived data, compare Vclk
// 		if compareVC(localObj.context.v_clk, finalObject.context.v_clk) == -1 { //means localObj is newer, use localObj
// 			finalObject = localObj
// 			//sync with other nodes function here?
// 		} else {
// 			n.data[hashKey] = finalObject
// 		}
// 	} else if !exists {
// 		finalObject = &Object{}
// 	}
// 	return finalObject
// }

func CreateNodes(client_ch chan Message, close_ch chan struct{}, numNodes int) []*Node {
	fmt.Println("Constructing machines...")

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
