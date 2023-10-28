package base

import (
	"config"
	"fmt"
	"sync"
	"time"
)


func (n *Node) restoreHandoff() {
	// transfer back data if dead node recovers
	for tarNode, nodeBackup := range n.backup {
		if time.Since(tarNode.GetAliveSince()) > 0 {
			for k, v := range nodeBackup {
				tarNode.data[k] = v
			}
			delete(n.backup, tarNode)
		}
	}
}

func (n *Node) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-n.close_ch:
			// fmt.Println("[", n.id, "]", "node is closing")
			return

		case msg := <-n.rcv_ch:
			if msg.Command == config.REQ_READ {
				obj := n.Get(msg.Key)
				n.client_ch <- Message{Data: obj.GetData(), Command: config.ACK, Key: msg.Key}
			} else if msg.Command == config.REQ_WRITE {
				args := []int{0, 0, 0} //change this to global config
				n.Put(msg.Key, msg.Data, args)
				n.client_ch <- Message{Command: config.ACK, Key: msg.Key}
			}
		}

		n.restoreHandoff()
	}
}

/* Get Replication count for testing */
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

/* Finds N+1-th available physical node from current node */
func getHintedHandoffToken(tokenStruct BST, initNode *TreeNode, visitedNodes map[int]struct{}, replicationCount int) *Token {
	i := replicationCount
	treeNode := initNode
	initToken := initNode.Token

	for i > 0 {
		treeNode := tokenStruct.getNext(treeNode)
		
		if treeNode.Token.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[treeNode.Token.phy_node.GetID()]; !visited {
			i -= 1
		}
	}

	return treeNode.Token
}

/* Update the node with the object */
func updateTreeNode(tokenStruct BST, curTreeNode *TreeNode, visitedNodes map[int]struct{}, replicationCount int, hashKey string, obj *Object) {
	curToken := curTreeNode.Token
	if time.Since(curToken.phy_node.GetAliveSince()) < 0 {
		// Hinted handoff to N+1-th physical node from current node
		token := getHintedHandoffToken(tokenStruct, curTreeNode, visitedNodes, replicationCount)
		node := token.phy_node
		
		node.backup[curToken.phy_node][hashKey] = obj
		visitedNodes[node.GetID()] = struct{}{}
		
		if config.DEBUG_LEVEL >= 2 {
			fmt.Printf("Handoff to token=%d, node=%d\n", token.GetID(), node.GetID())
		}

	} else {
		// Replicate data to the physical node of this token
		curToken.phy_node.data[hashKey] = obj
		visitedNodes[curToken.phy_node.GetID()] = struct{}{}
		if config.DEBUG_LEVEL >= 2 {
			fmt.Printf("Replicated to token=%d, node=%d\n", curToken.GetID(), curToken.phy_node.GetID())
		}
	}
}

/* Traverses token BST struc, assumed to be consistent across all nodes */
func FindNode(key string, phy_nodes []*Node) *Node {
	hashkey := computeMD5(key)
	root := phy_nodes[0]
	
	// bst_node satisfies hashInRange(value, bst_node.Token.GetStartRange(), bst_node.Token.GetEndRange())
	bst_node := root.tokenStruct.Search(hashkey)
	if bst_node == nil {
		panic("node not found due to key being out of range of all tokens")
	}
	return bst_node.Token.phy_node
}

func (n *Node) GetChannel() chan Message {
	return n.rcv_ch
}

// internal function
// Pass in global config details (PhysicalNum, VirtualNum, ReplicationNum)
func (n *Node) Put(key string, value string, nValue []int) {
	replicationCount := getReplicationCount(nValue)

	hashKey := computeMD5(key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()

	fmt.Printf("Coordinator node = %d, responsible for hashkey = %032X\n", n.GetID(), hashKey)

	// Replication process
	curTreeNode := n.tokenStruct.Search(hashKey)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{})  // To keep track of unique physical nodes. Use map as set. Use struct{} to occupy 0 space
	
	// Coordinator copy
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: false}
	updateTreeNode(n.tokenStruct, curTreeNode, visitedNodes, replicationCount, hashKey, &newObj)

	for len(visitedNodes) < replicationCount {
		nextTreeNode := n.tokenStruct.getNext(curTreeNode)
		curTreeNode = nextTreeNode
		curToken := curTreeNode.Token

		// After one loop stop.
		if curToken.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[curToken.phy_node.GetID()]; !visited {
			newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
			updateTreeNode(n.tokenStruct, curTreeNode, visitedNodes, replicationCount, hashKey, &newObj)
		}
	}
}

// remember to return object
// assume MD5 has already been computed
func (n *Node) Get(key string) *Object {
	n.increment_vclk()
	hashKey := computeMD5(key)
	obj, exists := n.data[hashKey]
	//reconciliation function here
	if exists {
		return obj
	} 
	
	for _, nodeBackup := range n.backup {
		obj, exists = nodeBackup[hashKey]
		if exists {
			return obj
		}
	}
	
	newObj := Object{}
	return &newObj
}

func CreateNodes(client_ch chan Message, close_ch chan struct{}, numNodes int) []*Node {
	fmt.Println("Constructing machines...")

	var nodeGroup []*Node
	for j := 0; j < numNodes; j++ {

		//make j nodes
		node := Node{
			id:          j,
			v_clk:       make([]int, numNodes),
			rcv_ch:      make(chan Message, numNodes),
			data:        make(map[string]*Object),
			tokenStruct: BST{},
			client_ch:   client_ch,
			close_ch:    close_ch,
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
			(*target_machine).channels = append((*target_machine).channels, receive_channel)
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
