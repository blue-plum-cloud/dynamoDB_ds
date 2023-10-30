package tests

//TODO: adjust the testcase accordingly
// import (
// 	"base"
// 	"config"
// 	"fmt"
// 	"testing"
// 	"time"
// )

// // TEST R1

// // TestSinglePutReplicationNonZeroNonNegative checks if replicas are
// // correctly created for a single put request with non-zero and
// // non-negative N values
// func TestSinglePutReplicationNonZeroNonNegative(t *testing.T) {
// 	var tests = []struct {
// 		numNodes, numTokens, nValue int
// 	}{
// 		{5, 5, 1},
// 		{10, 10, 3},
// 		{40, 40, 40},
// 		{100, 100, 100},

// 		{5, 10, 6},
// 		{5, 12, 6},
// 		{15, 10, 20},
// 	}
// 	for _, tt := range tests {
// 		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n", tt.numNodes, tt.numTokens, tt.nValue)
// 		t.Run(testname, func(t *testing.T) {
// 			// phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

// 			close_ch := make(chan struct{})
// 			client_ch := make(chan base.Message)

// 			//node and token initialization
// 			phy_nodes := base.CreateNodes(client_ch, close_ch, tt.numNodes)
// 			base.InitializeTokens(phy_nodes, tt.numTokens)
// 			// defer close(close_ch)

// 			for i := range phy_nodes {
// 				wg.Add(1)
// 				go phy_nodes[i].Start(&wg)
// 			}

// 			key := "Sudipta"
// 			value := "Best Prof"

// 			node := base.FindNode("Sudipta", phy_nodes)

// 			args := []int{tt.numNodes, tt.numTokens, tt.nValue}

// 			go node.Put(key, value, args)

// 			// time.Sleep(2 * time.Second)

// 			select {
// 			case ack := <-client_ch: // reply received in time
// 				if ack.Key != key {
// 					panic("wrong key!")
// 				}

// 				fmt.Println("Value stored: ", value, " with key: ", key)

// 			case <-time.After(config.CLIENT_PUT_TIMEOUT_MS * time.Millisecond): // timeout reached
// 				fmt.Println("Put Timeout reached")
// 			}

// 			ori := 0
// 			repCnt := 0
// 			for _, n := range phy_nodes {
// 				if val := n.Get(key); val.GetData() == value && val.IsReplica() {
// 					repCnt++
// 				} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
// 					ori++
// 				}
// 			}
// 			expectedRepFactor := tt.nValue - 1
// 			minPhyVirt := tt.numNodes
// 			if tt.numTokens < tt.numNodes {
// 				minPhyVirt = tt.numTokens
// 			}
// 			if tt.nValue > minPhyVirt {
// 				expectedRepFactor = minPhyVirt - 1
// 			}
// 			if repCnt != expectedRepFactor {
// 				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
// 			}
// 			if ori != 1 {
// 				t.Errorf("Original data for key '%s' is missing", key)
// 			}
// 		})
// 	}
// }

// // TEST R2

// // TestSinglePutReplicationZeroNegative checks if replicas are
// // correctly created for a single put request for zero or
// // negative N values
// func TestSinglePutReplicationZeroNegative(t *testing.T) {
// 	var tests = []struct {
// 		numNodes, numTokens, nValue int
// 	}{
// 		{5, 5, 0},
// 		{10, 10, -1},
// 		{40, 40, 0},
// 		{100, 100, -20},
// 	}
// 	for _, tt := range tests {
// 		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n", tt.numNodes, tt.numTokens, tt.nValue)
// 		t.Run(testname, func(t *testing.T) {
// 			// phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

// 			close_ch := make(chan struct{})
// 			client_ch := make(chan base.Message)

// 			//node and token initialization
// 			phy_nodes := base.CreateNodes(client_ch, close_ch, tt.numNodes)
// 			base.InitializeTokens(phy_nodes, tt.numTokens)
// 			// defer close(close_ch)

// 			for i := range phy_nodes {
// 				wg.Add(1)
// 				go phy_nodes[i].Start(&wg)
// 			}

// 			key := "Sudipta"
// 			value := "Best Prof"

// 			node := base.FindNode("Sudipta", phy_nodes)

// 			args := []int{tt.numNodes, tt.numTokens, tt.nValue}
// 			node.Put(key, value, args)
// 			ori := 0
// 			repCnt := 0
// 			for _, n := range phy_nodes {
// 				if val := n.Get(key); val.GetData() == value && val.IsReplica() {
// 					repCnt++
// 				} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
// 					ori++
// 				}
// 			}
// 			expectedRepFactor := 0
// 			if repCnt != expectedRepFactor {
// 				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
// 			}
// 			if ori != 1 {
// 				t.Errorf("Original data for key '%s' is missing", key)
// 			}
// 		})
// 	}
// }

// // TEST R3

// // TestMultipleUniquePutReplication tests if replications are properly handled
// // for multiple put requests
// func TestMultipleUniquePutReplication(t *testing.T) {
// 	var tests = []struct {
// 		numNodes, numTokens, nValue, numKeyValuePairs int
// 	}{
// 		{5, 5, 3, 2},
// 		{10, 20, 3, 8},
// 		{100, 524, 10, 20},
// 		{78, 78, 78, 100},
// 	}
// 	for _, tt := range tests {
// 		keyValuePairs := generateRandomKeyValuePairs(80, 100, tt.numKeyValuePairs)
// 		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_keyValuePairs", tt.numNodes, tt.numTokens, tt.nValue, tt.numKeyValuePairs)
// 		t.Run(testname, func(t *testing.T) {
// 			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

// 			defer close(close_ch)

// 			putKeyValuePairs(tt.numNodes, tt.numTokens, tt.nValue, keyValuePairs, phy_nodes)

// 			// Check replications of all key value pairs
// 			for key, value := range keyValuePairs {
// 				ori := 0
// 				repCnt := 0
// 				for _, n := range phy_nodes {
// 					if val := n.Get(key); val.GetData() == value && val.IsReplica() {
// 						repCnt++
// 					} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
// 						ori++
// 					}
// 				}
// 				expectedRepFactor := tt.nValue - 1
// 				if repCnt != expectedRepFactor {
// 					t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
// 				}
// 				if ori != 1 {
// 					t.Errorf("Original data for key '%s' is missing", key)
// 				}
// 			}
// 		})
// 	}
// }

// func TestMultipleOverwritePutReplication(t *testing.T) {

// 	var tests = []struct {
// 		numNodes, numTokens, nValue, numKeyValuePairs int
// 	}{
// 		{5, 5, 3, 2},
// 		{10, 20, 3, 8},
// 		{100, 524, 10, 20},
// 		{78, 78, 78, 100},
// 	}
// 	for _, tt := range tests {
// 		keyValuePairs := generateRandomKeyValuePairs(80, 100, tt.numKeyValuePairs)
// 		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_keyValuePairs", tt.numNodes, tt.numTokens, tt.nValue, tt.numKeyValuePairs)
// 		t.Run(testname, func(t *testing.T) {
// 			// phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

// 			close_ch := make(chan struct{})
// 			client_ch := make(chan base.Message)

// 			//node and token initialization
// 			phy_nodes := base.CreateNodes(client_ch, close_ch, tt.numNodes)
// 			base.InitializeTokens(phy_nodes, tt.numTokens)
// 			// defer close(close_ch)

// 			for i := range phy_nodes {
// 				wg.Add(1)
// 				go phy_nodes[i].Start(&wg)
// 			}

// 			putKeyValuePairs(tt.numNodes, tt.numTokens, tt.nValue, keyValuePairs, phy_nodes)

// 			randomlyUpdateValues(keyValuePairs, 100)

// 			putKeyValuePairs(tt.numNodes, tt.numTokens, tt.nValue, keyValuePairs, phy_nodes)

// 			// Check replications of all key value pairs
// 			for key, value := range keyValuePairs {
// 				ori := 0
// 				repCnt := 0
// 				for _, n := range phy_nodes {
// 					if val := n.Get(key); val.GetData() == value && val.IsReplica() {
// 						repCnt++
// 					} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
// 						ori++
// 					}
// 				}
// 				expectedRepFactor := tt.nValue - 1
// 				if repCnt != expectedRepFactor {
// 					t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
// 				}
// 				if ori != 1 {
// 					t.Errorf("Original data for key '%s' is missing", key)
// 				}
// 			}
// 		})
// 	}
// }
