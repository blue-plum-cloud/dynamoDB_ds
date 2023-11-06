package tests

import (
	"base"
	"config"
	"constants"
	"fmt"
	"testing"
	"time"
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

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			for key, value := range keyValuePairs {
				hashedKey := base.ComputeMD5(key)
				fmt.Printf("Key: %s ; Value: %s ; Hashed Key: %s", key, value, hashedKey)

				node := base.FindNode(key, phy_nodes, &c)
				channel := (*node).GetChannel()
				channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value}

				select {
				case ack := <-client_ch:
					if ack.Key != key {
						panic(fmt.Sprintf("wrong key! ack.key [%s] is not key [%s].\n", ack.Key, key))
					}

					fmt.Println("Value stored: ", value, " with key: ", key)
				case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
					fmt.Println("Put Timeout reached")
					t.Error("Put timeout reached. Test failed.")
					return
				}

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
		numNodes, numTokens, N, W, updates int
	}{
		{5, 5, 1, 1, 5},
		{10, 10, 1, 1, 5},
		{40, 40, 1, 1, 8},
		{100, 100, 1, 1, 10},
		{800, 800, 1, 1, 10},

		{5, 10, 1, 1, 5},
		{5, 12, 1, 1, 8},
		{15, 10, 1, 1, 10},
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

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			for updateCnt := 0; updateCnt < tt.updates; updateCnt++ {

				for key := range keyValuePairs {
					value := generateRandomString(100)
					hashedKey := base.ComputeMD5(key)
					fmt.Printf("Key: %s ; Value: %s ; Hashed Key: %s", key, value, hashedKey)

					node := base.FindNode(key, phy_nodes, &c)
					channel := (*node).GetChannel()
					channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value}

					select {
					case ack := <-client_ch:
						if ack.Key != key {
							panic(fmt.Sprintf("wrong key! ack.key [%s] is not key [%s].\n", ack.Key, key))
						}

						fmt.Println("Value stored: ", value, " with key: ", key)
					case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
						fmt.Println("Put Timeout reached")
						t.Error("Put timeout reached. Test failed.")
						return
					}

					actual := node.GetData(base.ComputeMD5(key)).GetData()

					if actual != value {
						t.Errorf("got: %s, expected: %s", actual, value)
					}
				}
			}

			close(close_ch)
		})
	}
}
