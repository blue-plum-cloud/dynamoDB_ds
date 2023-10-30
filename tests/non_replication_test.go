package tests

import (
	"base"
	"fmt"
	"testing"
)

// Test NR1

// TestNoUpdateGetPut tests if PUT for unique keys
// with no replication will be successful
func TestNoUpdateGetPut(t *testing.T) {
	keyValuePairs := generateRandomKeyValuePairs(20, 100, 1000)

	var tests = []struct {
		numNodes, numTokens int
	}{
		{5, 5},
		{10, 10},
		{40, 40},
		{100, 100},
		{800, 800},

		{5, 10},
		{5, 12},
		{15, 10},
		{273, 927},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			sys_config := base.Config{NUM_NODES: tt.numNodes, NUM_TOKENS: tt.numTokens}
			phy_nodes, close_ch := setUpNodes(&sys_config)

			putKeyValuePairs(1, keyValuePairs, phy_nodes, &sys_config)

			for key, value := range keyValuePairs {
				node := base.FindNode(key, phy_nodes)
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
		numNodes, numTokens int
	}{
		{5, 5},
		{10, 10},
		{40, 40},
		{100, 100},

		{5, 10},
		{5, 12},
		{15, 10},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			sys_config := base.Config{NUM_NODES: tt.numNodes, NUM_TOKENS: tt.numTokens}
			phy_nodes, close_ch := setUpNodes(&sys_config)

			putKeyValuePairs(1, keyValuePairs, phy_nodes, &sys_config)

			updateTimes := 5
			for i := 0; i < updateTimes; i++ {
				randomlyUpdateValues(keyValuePairs, 1000)
				putKeyValuePairs(1, keyValuePairs, phy_nodes, &sys_config)
			}

			for key, value := range keyValuePairs {
				node := base.FindNode(key, phy_nodes)
				actual := node.GetData(base.ComputeMD5(key)).GetData()

				if actual != value {
					t.Errorf("got: %s, expected: %s", actual, value)
				}
			}

			close(close_ch)
		})
	}
}
