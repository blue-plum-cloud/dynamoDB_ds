package tests

import (
	"base"
	"fmt"
	"testing"
)

// TestSinglePutReplicationNonZeroNonNegative checks if replicas are
// correctly created for a single put request with non-zero and
// non-negative N values
func TestSinglePutReplicationNonZeroNonNegative(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue int
	}{
		{5, 5, 1},
		{10, 10, 3},
		{40, 40, 40},
		{100, 100, 100},

		{5, 10, 6},
		{5, 12, 6},
		{15, 10, 20},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n", tt.numNodes, tt.numTokens, tt.nValue)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			defer close(close_ch)

			key := "Sudipta"
			value := "Best Prof"

			node := base.FindNode("Sudipta", phy_nodes)

			node.Put(key, value, tt.nValue)
			ori := 0
			repCnt := 0
			for _, n := range phy_nodes {
				if val := n.Get(key); val.GetData() == value && val.IsReplica() {
					repCnt++
				} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
					ori++
				}
			}
			expectedRepFactor := tt.nValue - 1
			if repCnt != expectedRepFactor {
				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
			}
			if ori != 1 {
				t.Errorf("Original data for key '%s' is missing", key)
			}
		})
	}
}

// TestSinglePutReplicationZeroNegative checks if replicas are
// correctly created for a single put request for zero or
// negative N values
func TestSinglePutReplicationZeroNegative(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue int
	}{
		{5, 5, 0},
		{10, 10, -1},
		{40, 40, 0},
		{100, 100, -20},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n", tt.numNodes, tt.numTokens, tt.nValue)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			defer close(close_ch)

			key := "Sudipta"
			value := "Best Prof"

			node := base.FindNode("Sudipta", phy_nodes)

			node.Put(key, value, tt.nValue)
			ori := 0
			repCnt := 0
			for _, n := range phy_nodes {
				if val := n.Get(key); val.GetData() == value && val.IsReplica() {
					repCnt++
				} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
					ori++
				}
			}
			expectedRepFactor := 0
			if repCnt != expectedRepFactor {
				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
			}
			if ori != 1 {
				t.Errorf("Original data for key '%s' is missing", key)
			}
		})
	}
}

func TestMultipleUniquePutReplication(t *testing.T) {
	keyValuePairs := make([][]string, 0)
	// Populate key value pairs
	// Assume key as good as unique due to large randomisation space
	for i := 0; i < 100; i++ {
		key := generateRandomString(80)
		value := generateRandomString(100)
		newKeyValue := []string{key, value}
		keyValuePairs = append(keyValuePairs, newKeyValue)
	}

	var tests = []struct {
		numNodes, numTokens, nValue, numKeyValuePairs int
	}{
		{5, 5, 3, 2},
		{10, 20, 3, 8},
		{100, 524, 10, 20},
		{78, 78, 78, 100},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_keyValuePairs", tt.numNodes, tt.numTokens, tt.nValue, tt.numKeyValuePairs)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			defer close(close_ch)

			// Put all key value pairs into system
			for i := 0; i < tt.numKeyValuePairs; i++ {
				key := keyValuePairs[i][0]
				value := keyValuePairs[i][1]
				node := base.FindNode(key, phy_nodes)
				node.Put(key, value, tt.nValue)
			}

			// Check replications of all key value pairs
			for i := 0; i < tt.numKeyValuePairs; i++ {
				key := keyValuePairs[i][0]
				value := keyValuePairs[i][1]

				fmt.Println(key)

				ori := 0
				repCnt := 0
				for _, n := range phy_nodes {
					if val := n.Get(key); val.GetData() == value && val.IsReplica() {
						repCnt++
					} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
						ori++
					}
				}
				expectedRepFactor := tt.nValue - 1
				if repCnt != expectedRepFactor {
					t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
				}
				if ori != 1 {
					t.Errorf("Original data for key '%s' is missing", key)
				}
			}
		})
	}
}
