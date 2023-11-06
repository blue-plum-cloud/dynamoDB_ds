package tests

import (
	"base"
	"config"
	"fmt"
	"math/rand"
	"sync"
)

/*
======= Helper functions
*/

func setUpNodes(c *config.Config) ([]*base.Node, chan struct{}, chan base.Message) {

	var wg sync.WaitGroup
	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, c)
	base.InitializeTokens(phy_nodes, c)
	fmt.Println("Setup nodes completed..")
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, c)
	}

	return phy_nodes, close_ch, client_ch
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

// calculatedExpectedReplicaitons takes in number of nodes, tokens and N value
// to calculate the expected replications of the data
func calculateExpectedTotalReplications(numNodes int, numTokens int, nValue int) int {
	expectedReplications := nValue - 1
	minPhyVirt := numNodes
	if numTokens < numNodes {
		minPhyVirt = numTokens
	}
	if nValue > minPhyVirt {
		expectedReplications = minPhyVirt - 1
	}

	if expectedReplications < 0 {
		return 0
	} else {
		return expectedReplications
	}
}
