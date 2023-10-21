package base

import (
	"fmt"
	"sync"
)

// might want to create a global constants file for this
const (
	REQ_READ  = 0
	REQ_WRITE = 1
	ACK       = 2
)

func (n *Node) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-n.close_ch:
			// fmt.Println("[", n.id, "]", "node is closing")
			return

		case msg := <-n.rcv_ch:
			if msg.Command == REQ_READ {
				obj := n.Get(msg.Key)
				n.client_ch <- Message{Data: obj.GetData(), Command: ACK, Key: msg.Key}
			} else if msg.Command == REQ_WRITE {
				n.Put(msg.Key, msg.Data)
				n.client_ch <- Message{Command: ACK, Key: msg.Key}
			}
		}

	}
}

func FindNode(key string, phy_nodes []*Node) *Node {
	hashkey := computeMD5(key)
	for _, node := range phy_nodes {
		for _, token := range (*node).tokens {
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
func (n *Node) Put(key string, value string) {
	hashKey := computeMD5(key)
	n.increment_vclk()
	copy_vclk := n.copy_vclk()
	//create object and context from current node's state
	newObj := Object{data: value, context: &Context{v_clk: copy_vclk}}
	fmt.Println(hashKey)
	n.data[hashKey] = &newObj
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
			id:        j,
			v_clk:     make([]int, numNodes),
			rcv_ch:    make(chan Message, numNodes),
			data:      make(map[string]*Object),
			client_ch: client_ch,
			close_ch:  close_ch}

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
