package tests

import (
	"base"
	"config"
	"constants"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TEST R1

// TestSinglePutReplicationNonZeroNonNegative checks if replicas are
// correctly created for a single put request with non-zero and
// non-negative N values, and W == N
func TestSinglePutReplicationNonZeroNonNegative(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue int
	}{
		{5, 5, 1},
		{10, 10, 3},
		{30, 30, 20},
		{40, 40, 40},
		{100, 100, 100},

		{5, 10, 6},
		{5, 12, 6},
		{15, 10, 20},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n", tt.numNodes, tt.numTokens, tt.nValue)
		t.Run(testname, func(t *testing.T) {
			expectedTotalReplications := calculateExpectedTotalReplications(tt.numNodes, tt.numTokens, tt.nValue)
			expectedReplicas := expectedTotalReplications - 1
			if expectedReplicas < 0 {
				expectedReplicas = 0
			}

			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = expectedTotalReplications
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long timeout since we are testing replications not timeout

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := "Sudipta"
			value := "Best Prof"

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic("wrong key!")
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed.")
				return
			}

			ori := 0
			repCnt := 0
			hashedKey := base.ComputeMD5(key)
			for _, n := range phy_nodes {
				val, ok := n.GetAllData()[hashedKey]
				if ok {
					if val.GetData() == value && val.IsReplica() {
						repCnt++
					} else if val.GetData() == value && !val.IsReplica() {
						ori++
					}
				}
			}
			if repCnt != expectedReplicas {
				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedReplicas)
			}
			if ori != 1 {
				t.Errorf("Original data for key '%s' is missing", key)
			}

			close(close_ch)
		})
	}
}

// TEST R2

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
			expectedTotalReplications := calculateExpectedTotalReplications(tt.numNodes, tt.numTokens, tt.nValue)
			expectedReplicas := expectedTotalReplications - 1
			if expectedReplicas < 0 {
				expectedReplicas = 0
			}

			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = expectedTotalReplications
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long timeout since we are testing replications not timeout

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := "Sudipta"
			value := "Best Prof"

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic("wrong key!")
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed.")
				return
			}

			ori := 0
			repCnt := 0
			hashedKey := base.ComputeMD5(key)
			for _, n := range phy_nodes {
				val, ok := n.GetAllData()[hashedKey]
				if ok {
					if val.GetData() == value && val.IsReplica() {
						repCnt++
					} else if val.GetData() == value && !val.IsReplica() {
						ori++
					}
				}
			}
			if repCnt != expectedReplicas {
				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedReplicas)
			}
			if ori != 1 {
				t.Errorf("Original data for key '%s' is missing", key)
			}

			close(close_ch)
		})
	}
}

// TEST R3

// TestMultipleUniquePutReplication tests if replications are properly handled
// for multiple put requests
func TestMultipleUniquePutReplication(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, numKeyValuePairs int
	}{
		{5, 5, 3, 2},
		{10, 20, 3, 8},
		{100, 524, 10, 20},
		{78, 78, 78, 100},
	}
	for _, tt := range tests {
		keyValuePairs := generateRandomKeyValuePairs(80, 100, tt.numKeyValuePairs)
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_keyValuePairs", tt.numNodes, tt.numTokens, tt.nValue, tt.numKeyValuePairs)
		t.Run(testname, func(t *testing.T) {
			expectedTotalReplications := calculateExpectedTotalReplications(tt.numNodes, tt.numTokens, tt.nValue)
			expectedReplicas := expectedTotalReplications - 1
			if expectedReplicas < 0 {
				expectedReplicas = 0
			}

			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = expectedTotalReplications
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long timeout since we are testing replications not timeout

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			for key, value := range keyValuePairs {
				hashedKey := base.ComputeMD5(key)
				fmt.Printf("Key: %s ; Value: %s ; Hashed Key: %s", key, value, hashedKey)

				_, node := base.FindNode(key, phy_nodes, &c)
				channel := (*node).GetChannel()
				channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

				select {
				case ack := <-client_ch: // reply received in time
					if ack.Key != key {
						panic(fmt.Sprintf("wrong key! ack.key [%s] is not key [%s].\n", ack.Key, key))
					}

					fmt.Println("Value stored: ", value, " with key: ", key)
				case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
					fmt.Println("Put Timeout reached")
					t.Error("Put timeout reached. Test failed.")
					return
				}

				ori := 0
				repCnt := 0
				for _, n := range phy_nodes {
					val, ok := n.GetAllData()[hashedKey]
					if ok {
						if val.GetData() == value && val.IsReplica() {
							repCnt++
						} else if val.GetData() == value && !val.IsReplica() {
							ori++
						}
					}
				}
				if repCnt != expectedReplicas {
					t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedReplicas)
				}
				if ori != 1 {
					t.Errorf("Original data for key '%s' is missing", key)
				}
			}

			close(close_ch)
		})
	}
}

// TestMultipleOverwritePutReplication tests if replications are
// properly handled with overwrites
func TestMultipleOverwritePutReplication(t *testing.T) {

	var tests = []struct {
		numNodes, numTokens, nValue, numKeyValuePairs, updates int
	}{
		{5, 5, 3, 2, 2},
		{10, 20, 3, 8, 3},
		{100, 524, 10, 20, 5},
		{78, 78, 78, 100, 7},
	}
	for _, tt := range tests {
		keyValuePairs := generateRandomKeyValuePairs(80, 100, tt.numKeyValuePairs)
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_keyValuePairs", tt.numNodes, tt.numTokens, tt.nValue, tt.numKeyValuePairs)
		t.Run(testname, func(t *testing.T) {
			expectedTotalReplications := calculateExpectedTotalReplications(tt.numNodes, tt.numTokens, tt.nValue)
			expectedReplicas := expectedTotalReplications - 1
			if expectedReplicas < 0 {
				expectedReplicas = 0
			}

			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = expectedTotalReplications
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long timeout since we are testing replications not timeout

			close_ch := make(chan struct{})
			client_ch := make(chan base.Message)

			//node and token initialization
			phy_nodes := base.CreateNodes(close_ch, &c)
			base.InitializeTokens(phy_nodes, &c)
			// defer close(close_ch)

			var wg sync.WaitGroup
			for i := range phy_nodes {
				wg.Add(1)
				go phy_nodes[i].Start(&wg, &c)
			}

			for updateCnt := 0; updateCnt < tt.updates; updateCnt++ {
				fmt.Println("Update count:", updateCnt)
				for key := range keyValuePairs {
					value := generateRandomString(100)
					hashedKey := base.ComputeMD5(key)
					fmt.Printf("Key: %s ; Value: %s ; Hashed Key: %s\n", key, value, hashedKey)

					_, node := base.FindNode(key, phy_nodes, &c)
					channel := (*node).GetChannel()
					channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

					select {
					case ack := <-client_ch: // reply received in time
						fmt.Println("ack", ack)
						if ack.Key != key {
							panic(fmt.Sprintf("wrong key! ack.key [%s] is not key [%s].\n", ack.Key, key))
						}

						fmt.Println("Value stored: ", value, " with key: ", key)
					case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
						fmt.Println("Put Timeout reached")
						t.Error("Put timeout reached. Test failed.")
						return
					}

					ori := 0
					repCnt := 0
					for _, n := range phy_nodes {
						val, ok := n.GetAllData()[hashedKey]
						if ok {
							if val.GetData() == value && val.IsReplica() {
								repCnt++
							} else if val.GetData() == value && !val.IsReplica() {
								ori++
							}
						}
					}
					if repCnt != expectedReplicas {
						t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedReplicas)
					}
					if ori != 1 {
						t.Errorf("Original data for key '%s' is missing", key)
					}
				}
			}
		})
	}
}
