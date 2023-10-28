package tests

import (
	"base"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

/*
======= Helper functions
*/
func setUpNodes(numNodes int, numTokens int) ([]*base.Node, chan struct{}) {

	var wg sync.WaitGroup
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

/*
======= Simple tests
*/
func TestSinglePutSingleGet(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens int
	}{
		{5, 5},
		{10, 10},
		{15, 15},
		{5, 10},
		{5, 20},
		{5, 50},
		{10, 20},
		{10, 40},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			key := "key"
			expected := "value"

			node := base.FindNode(key, phy_nodes)

			node.Put(key, expected)
			node.Put("u bad", "u good")
			node.Put("this is", "da bomb")

			actual := node.Get(key).GetData()

			if actual != expected {
				t.Errorf("Expected %s, but got %s", expected, actual)
			}

			close(close_ch)
		})
	}
}

// generateRandomString generates a random string (min 1 char) with maximum
// length defined by maxN
func generateRandomString(maxN int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	n := 1 + rand.Intn(maxN)

	res := make([]rune, n)
	for i := range res {
		res[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(res)
}
