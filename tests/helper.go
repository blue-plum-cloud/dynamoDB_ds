package tests

import (
	"base"
	"fmt"
	"math/rand"
	"sync"
)

/*
======= Helper functions
*/

func setupNodes(sys_config *base.Config) ([]*base.Node, chan struct{}) {
	var wg sync.WaitGroup
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
		go phy_nodes[i].Start(&wg, sys_config)
	}

	return phy_nodes, close_ch
}

func setUpNodes(sys_config *base.Config) ([]*base.Node, chan struct{}) {

	var wg sync.WaitGroup
	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, sys_config.NUM_NODES)
	base.InitializeTokens(phy_nodes, sys_config.NUM_TOKENS)
	fmt.Println("Setup nodes completed..")
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, sys_config)
	}

	return phy_nodes, close_ch
}

// generateRandomKeyValuePairs will generate n key-value pairs
// where key length is 1 - maxKeyLength and value is of length
// 1 - maxValueLength
func generateRandomKeyValuePairs(maxKeyLength int, maxValueLength, n int) map[string]string {
	keyValueMap := map[string]string{}
	// Populate key value pairs
	for i := 0; i < n; i++ {
		for {
			key := generateRandomString(maxKeyLength)
			// if key does not exist, add to keySet and break
			_, err := keyValueMap[key]
			if !err {
				keyValueMap[key] = generateRandomString(maxValueLength)
				break
			}
		}
	}

	return keyValueMap
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

// putKeyValuePairs will go through all key-value pairs in keyValueMap,
// find the correct node to send put request and send put command
// to respective node
func putKeyValuePairs(nValue int, keyValueMap map[string]string, phy_nodes []*base.Node, c *base.Config) {
	// Put all key value pairs into system
	for key, value := range keyValueMap {
		node := base.FindNode(key, phy_nodes)
		args := []int{c.NUM_NODES, c.NUM_TOKENS, nValue}
		node.Put(key, value, args, c)
	}
}

// randomlyUpdateValues will take in a keyValueMap and randomly update
// the values, leaving the keys unchanged. Length of newly generated values
// range from 1 - maxValueLength
func randomlyUpdateValues(keyValueMap map[string]string, maxValueLength int) map[string]string {
	for key := range keyValueMap {
		newValue := generateRandomString(maxValueLength)
		keyValueMap[key] = newValue
	}

	return keyValueMap
}
