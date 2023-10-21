package base

import (
	"fmt"
	"sync"
)

func (n *Node) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-n.close_ch:
			fmt.Println("[", n.id, "]", "node is closing")
			return
		}
	}
}

func CreateNodes(close_ch chan struct{}, numNodes int) []*Node {
	fmt.Println("Constructing machines...")

	var nodeGroup []*Node
	for j := 0; j < numNodes; j++ {

		//make j nodes
		node := Node{
			id:       j,
			v_clk:    make([]int, numNodes),
			rcv_ch:   make(chan Message, numNodes),
			close_ch: close_ch}

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

// internal function
func (n *Node) put(hash string, value *Object, context *Context) {
	context.v_clk = n.copy_vclk()
	(*value).context = context
	n.data[hash] = value
}

// remember to return object
// assume MD5 has already been computed
func (n *Node) get(key string) *Object {
	hashKey := computeMD5(key)
	obj, exists := n.data[hashKey]
	if exists {
		return obj
	}
	return nil
}
