package tests

import (
	"base"
	"config"
	"fmt"
	"testing"
)

// Test NR1

// TestNoUpdateGetPut tests if PUT for unique keys
// with no replication will be successful
func TestNoUpdateGetPut(t *testing.T) {
	keyValuePairs := generateRandomKeyValuePairs(20, 100, 1000)

	var tests = []struct {
		numNodes, numTokens, N, W int
	}{
		{5, 5, 1, 1},
		{10, 10, 1, 1},
		{40, 40, 1, 1},
		{100, 100, 1, 1},
		{800, 800, 1, 1},

		{5, 10, 1, 1},
		{5, 12, 1, 1},
		{15, 10, 1, 1},
		{273, 927, 1, 1},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.N
			c.CLIENT_GET_TIMEOUT_MS = 10_000 // set long timeout as we are not interested in testing timeout here
			c.CLIENT_PUT_TIMEOUT_MS = 10_000
			c.SET_DATA_TIMEOUT_MS = 10_000

			phy_nodes, close_ch := setUpNodes(&c)

			putKeyValuePairs(1, keyValuePairs, phy_nodes, &c)

			for key, value := range keyValuePairs {
				node := base.FindNode(key, phy_nodes, &c)
				actual := node.GetData(base.ComputeMD5(key)).GetData()

				if actual != value {
					t.Errorf("got: %s, expected: %s", actual, value)
				}
			}

			close(close_ch)
		})
	}
}

// Test NR2

// TestMultipleGetPut tests if PUT for unique keys
// with updates will be successul
func TestMultipleGetPut(t *testing.T) {
	keyValuePairs := generateRandomKeyValuePairs(200, 1000, 1000)

	var tests = []struct {
		numNodes, numTokens, N, W int
	}{
		{5, 5, 1, 1},
		{10, 10, 1, 1},
		{40, 40, 1, 1},
		{100, 100, 1, 1},
		{800, 800, 1, 1},

		{5, 10, 1, 1},
		{5, 12, 1, 1},
		{15, 10, 1, 1},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.N
			c.CLIENT_GET_TIMEOUT_MS = 10_000 // set long timeout as we are not interested in testing timeout here
			c.CLIENT_PUT_TIMEOUT_MS = 10_000
			c.SET_DATA_TIMEOUT_MS = 10_000

			phy_nodes, close_ch := setUpNodes(&c)

			putKeyValuePairs(1, keyValuePairs, phy_nodes, &c)

			updateTimes := 5
			for i := 0; i < updateTimes; i++ {
				randomlyUpdateValues(keyValuePairs, 1000)
				putKeyValuePairs(1, keyValuePairs, phy_nodes, &c)
			}

			for key, value := range keyValuePairs {
				node := base.FindNode(key, phy_nodes, &c)
				actual := node.GetData(base.ComputeMD5(key)).GetData()

				if actual != value {
					t.Errorf("got: %s, expected: %s", actual, value)
				}
			}

			close(close_ch)
		})
	}
}
