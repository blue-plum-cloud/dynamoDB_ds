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
		numNodes, numTokens, N, W, CLIENT_GET_TIMEOUT_MS, CLIENT_PUT_TIMEOUT_MS, SET_DATA_TIMEOUT_NS int
	}{
		{5, 5, 0, 0, 2000, 2000, 1000000000},
		{10, 10, 0, 0, 2000, 2000, 1000000000},
		{40, 40, 0, 0, 2000, 2000, 1000000000},
		{100, 100, 0, 0, 2000, 2000, 1000000000},
		{800, 800, 0, 0, 2000, 2000, 1000000000},

		{5, 10, 0, 0, 2000, 2000, 1000000000},
		{5, 12, 0, 0, 2000, 2000, 1000000000},
		{15, 10, 0, 0, 2000, 2000, 1000000000},
		{273, 927, 0, 0, 2000, 2000, 1000000000},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			sys_config := base.Config{NUM_NODES: tt.numNodes,
				NUM_TOKENS: tt.numTokens,
				N:          tt.N, W: tt.W,
				CLIENT_GET_TIMEOUT_MS: tt.CLIENT_GET_TIMEOUT_MS,
				CLIENT_PUT_TIMEOUT_MS: tt.CLIENT_PUT_TIMEOUT_MS,
				SET_DATA_TIMEOUT_NS:   tt.SET_DATA_TIMEOUT_NS}

			phy_nodes, close_ch := setUpNodes(&sys_config)

			putKeyValuePairs(1, keyValuePairs, phy_nodes, &sys_config)

			for key, value := range keyValuePairs {
				node := base.FindNode(key, phy_nodes)
				actual := node.Get(key).GetData()

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
		numNodes, numTokens, N, W, CLIENT_GET_TIMEOUT_MS, CLIENT_PUT_TIMEOUT_MS, SET_DATA_TIMEOUT_NS int
	}{
		{5, 5, 0, 0, 2000, 2000, 1000000000},
		{10, 10, 0, 0, 2000, 2000, 1000000000},
		{40, 40, 0, 0, 2000, 2000, 1000000000},
		{100, 100, 0, 0, 2000, 2000, 1000000000},
		{800, 800, 0, 0, 2000, 2000, 1000000000},

		{5, 10, 0, 0, 2000, 2000, 1000000000},
		{5, 12, 0, 0, 2000, 2000, 1000000000},
		{15, 10, 0, 0, 2000, 2000, 1000000000},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			sys_config := base.Config{NUM_NODES: tt.numNodes,
				NUM_TOKENS: tt.numTokens,
				N:          tt.N, W: tt.W,
				CLIENT_GET_TIMEOUT_MS: tt.CLIENT_GET_TIMEOUT_MS,
				CLIENT_PUT_TIMEOUT_MS: tt.CLIENT_PUT_TIMEOUT_MS,
				SET_DATA_TIMEOUT_NS:   tt.SET_DATA_TIMEOUT_NS}

			phy_nodes, close_ch := setUpNodes(&sys_config)

			putKeyValuePairs(1, keyValuePairs, phy_nodes, &sys_config)

			updateTimes := 5
			for i := 0; i < updateTimes; i++ {
				randomlyUpdateValues(keyValuePairs, 1000)
				putKeyValuePairs(1, keyValuePairs, phy_nodes, &sys_config)
			}

			for key, value := range keyValuePairs {
				node := base.FindNode(key, phy_nodes)
				actual := node.Get(key).GetData()

				if actual != value {
					t.Errorf("got: %s, expected: %s", actual, value)
				}
			}

			close(close_ch)
		})
	}
}
