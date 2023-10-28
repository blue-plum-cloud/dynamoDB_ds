package base

import (
	"config"
	"fmt"
	"sync"
)

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

	}
}

func FindNode(key string, phy_nodes []*Node) *Node {
	hashkey := computeMD5(key)
	for _, node := range phy_nodes {
		for _, token := range (*node).tokens {
			fmt.Printf("tokenid = %d\n", token.GetID())
			if hashInRange(hashkey, token.range_start, token.range_end) {
				return node
			}
		}
	}
	panic("node not found due to key being out of range of all tokens")
}

func (n *Node) GetChannel() chan Message {
	return n.rcv_ch
}

// internal function
func (n *Node) Put(key string, value string, nValue []int) {
	hashKey := computeMD5(key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()
	//create object and context from current node's state
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: false}
	fmt.Println(hashKey)
	n.data[hashKey] = &newObj

	fmt.Printf("Coordinator node = %d, responsible for hashkey = %032X\n", n.GetID(), hashKey)

	// Replication process
	curTreeNode := n.tokenStruct.Search(hashKey)
	initToken := curTreeNode.Token
	visitedNodes := make(map[int]struct{})  // To keep track of unique physical nodes
	visitedTokens := make(map[int]struct{}) // To keep track of unique virtual nodes

	visitedNodes[initToken.phy_node.GetID()] = struct{}{}
	visitedTokens[initToken.GetID()] = struct{}{}

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

	for len(visitedNodes) < replicationCount {
		fmt.Printf("Cur node = %d\n", curTreeNode.Token.GetID())
		nextTreeNode := n.tokenStruct.getNext(curTreeNode)
		curTreeNode = nextTreeNode
		fmt.Printf("next node = %d\n", curTreeNode.Token.GetID())
		curToken := curTreeNode.Token

		// After one loop stop.
		if curToken.GetID() == initToken.GetID() {
			break
		}

		if _, visited := visitedNodes[curToken.phy_node.GetID()]; !visited {
			// Replicate data to the physical node of this token
			fmt.Printf("Replicated to node = %d, for hashkey = %s\n", curToken.phy_node.GetID(), hashKey)
			newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
			curToken.phy_node.data[hashKey] = &newObj
			visitedNodes[curToken.phy_node.GetID()] = struct{}{}
			visitedTokens[curToken.GetID()] = struct{}{}
		}

	}

	// Take the rest node to replicate to (cannot be in previously visited node)
	res := replicationCount - len(visitedNodes)
	for res > 0 {
		curTreeNode = n.tokenStruct.getNext(curTreeNode)
		curToken := curTreeNode.Token
		if _, visited := visitedTokens[curToken.GetID()]; !visited {
			newObj := Object{data: value, context: &Context{v_clk: copy_vclk}, isReplica: true}
			curToken.phy_node.data[hashKey] = &newObj
			visitedNodes[curToken.phy_node.GetID()] = struct{}{}
			visitedTokens[curToken.GetID()] = struct{}{}
			res--
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
	} else {
		newObj := Object{}
		return &newObj
	}

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
