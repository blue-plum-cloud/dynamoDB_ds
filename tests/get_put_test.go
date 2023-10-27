package tests

import (
	"base"
	"fmt"
	"sync"
	"testing"
)

var wg sync.WaitGroup

func setupNodes() ([]*base.Node, chan struct{}) {

	numNodes := 5
	numTokens := 5
	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, numNodes)
	base.InitializeTokens(phy_nodes, numTokens)
	fmt.Println("Setup nodes completed..")
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg)
	}

	return phy_nodes, close_ch
}

func TestSimplePutGet(t *testing.T) {
	phy_nodes, close_ch := setupNodes()

	key := "key"
	expected := "value"

	node := base.FindNode(key, phy_nodes)

	node.Put(key, expected)

	actual := node.Get(key).GetData()

	if actual != expected {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}

	close(close_ch)
}

func TestMultiplePutSingleGet(t *testing.T) {
	phy_nodes, close_ch := setupNodes()

	key := "key"
	expected := "value"

	node := base.FindNode(key, phy_nodes)

	// Find the coordinator node responsible for storing the data
	node.Put(key, expected)
	node1 := base.FindNode("u bad", phy_nodes)
	node1.Put("u bad", "u good")
	node2 := base.FindNode("this is", phy_nodes)
	node2.Put("this is", "da bomb")

	actual := node.Get(key).GetData()

	if actual != expected {
		t.Errorf("Expected %s, but got %s", expected, actual)
	}

	close(close_ch)
}

func TestGetNoData(t *testing.T) {
	phy_nodes, close_ch := setupNodes()

	key := "key"

	node := base.FindNode(key, phy_nodes)

	actual := node.Get(key)

	if actual.GetData() != "" {
		t.Errorf("Expected 'nothing', but got an something")
	}

	close(close_ch)
}
