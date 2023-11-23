package tests

import (
	"base"
	"config"
	"constants"
	"fmt"
	"testing"
	"time"
)

// TEST Q1

// TestValidQuorumWrite ensures that slopppy quorum writes are
// successful when no nodes are down
func TestValidQuorumWrite(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, wValue int
	}{
		{5, 5, 3, 1},
		{5, 15, 3, 1},

		{5, 5, 5, 3},
		{10, 20, 5, 4},

		{5, 5, 5, 5},
		{10, 20, 10, 10},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_w", tt.numNodes, tt.numTokens, tt.nValue, tt.wValue)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = tt.wValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long put timeout to ensure that quorum failure is not due to latency

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := "Sudipta"
			value := "Best Prof"

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed.")
				return
			}

			close(close_ch)
		})
	}
}

// Test Q2

// TestHighQuorumWrite ensures system can handle W values
// which are too high
func TestHighQuorumWrite(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, wValue int
	}{
		{5, 5, 3, 4},
		{5, 15, 3, 5},

		{5, 5, 5, 3000},
		{10, 20, 5, 4000},

		{5, 5, 5, 5000},
		{10, 20, 10, 10000},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_w", tt.numNodes, tt.numTokens, tt.nValue, tt.wValue)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = tt.wValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := "Sudipta"
			value := "Best Prof"

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed.")
				return
			}

			close(close_ch)
		})
	}
}

// Test Q3

// TestQuorumWriteWithReasonableFailures ensures that the sloppy
// quorum writes are still successful with small numbers of failures
func TestQuorumWriteWithReasonableFailures(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, wValue, numKill int
	}{
		{5, 5, 3, 3, 1},
		{10, 10, 6, 3, 3},

		{5, 5, 3, 3, 2},
		{10, 15, 5, 5, 5},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_w_%d_kill", tt.numNodes, tt.numTokens, tt.nValue, tt.wValue, tt.numKill)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = tt.wValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000
			c.SET_DATA_TIMEOUT_MS = 500

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := generateRandomString(10)
			value := generateRandomString(10)

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()

			// Kill required num of nodes but not the one we are sending write to
			killed := 0
			for i := range phy_nodes {
				if killed == tt.numKill {
					break
				}
				if i == node.GetID() {
					continue
				}
				channel := phy_nodes[i].GetChannel()
				channel <- base.Message{JobId: i, Command: constants.CLIENT_REQ_KILL, Data: "1000000000", SrcID: -1}
				killed++
			}

			// Sleep for 1 second to allow nodes to be killed
			time.Sleep(1 * time.Second)

			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed.")
				return
			}

			close(close_ch)
		})
	}
}

// Test Q4

// TestQuorumWriteWithHighFailures checks that sloppy quorum writes
// fail when there are too many node failures
func TestQuorumWriteWithHighFailures(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, wValue, numKill int
	}{
		{5, 5, 3, 3, 40},
		{10, 10, 6, 6, 5},

		{5, 5, 3, 3, 3},
		{10, 15, 5, 5, 7},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_w_%d_kill", tt.numNodes, tt.numTokens, tt.nValue, tt.wValue, tt.numKill)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = tt.wValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000
			c.SET_DATA_TIMEOUT_MS = 500

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := generateRandomString(10)
			value := generateRandomString(10)

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()

			// Kill required num of nodes but not the one we are sending write to
			killed := 0
			for i := range phy_nodes {
				if killed == tt.numKill {
					break
				}
				if i == node.GetID() {
					continue
				}
				channel := phy_nodes[i].GetChannel()
				channel <- base.Message{JobId: i, Command: constants.CLIENT_REQ_KILL, Data: "1000000000", SrcID: -1}
				killed++
			}

			// Sleep for 1 second to allow nodes to be killed
			time.Sleep(1 * time.Second)

			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
				t.Error("Test failed. Value stored when not supposed to.")
				return
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				fmt.Println("Expected result: Put timeout reached.")
			}

			close(close_ch)
		})
	}
}

// Test Q5

// TestValidQuorumRead ensures that sloppy quorum reads
// are successful with valid R values
func TestValidQuorumRead(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, rValue int
	}{
		{5, 5, 3, 1},
		{10, 10, 6, 3},

		{5, 5, 3, 3},
		{10, 15, 5, 5},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_r", tt.numNodes, tt.numTokens, tt.nValue, tt.rValue)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = calculateExpectedTotalReplications(tt.numNodes, tt.numTokens, tt.nValue)
			c.R = tt.rValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000
			c.CLIENT_GET_TIMEOUT_MS = 2_000
			c.SET_DATA_TIMEOUT_MS = 500

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := generateRandomString(10)
			value := generateRandomString(10)

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()

			// Write
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed prematurely.")
				return
			}

			// Read
			channel <- base.Message{JobId: 1, Key: key, Command: constants.CLIENT_REQ_READ, SrcID: -1, Client_Ch: client_ch}
			select {
			case msg := <-client_ch: // reply received in time

				if msg.Data != value {
					panic(fmt.Sprintf("wrong value! expected: %s got :%s", key, msg.Key))
				}

				fmt.Println("Read value:", msg.Data)
			case <-time.After(time.Duration(c.CLIENT_GET_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Get Timeout reached")
				t.Error("Get timeout reached. Test failed.")
				return
			}
			close(close_ch)
		})
	}
}

// Test Q6

// TestHighQuorumRead ensures system can handle R values
// which are too high
func TestHighQuorumRead(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, rValue int
	}{
		{5, 5, 3, 1000},
		{10, 10, 6, 30000},

		{5, 5, 3, 30000},
		{10, 15, 5, 50000},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_r", tt.numNodes, tt.numTokens, tt.nValue, tt.rValue)
		t.Run(testname, func(t *testing.T) {
			c := config.InstantiateConfig()
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			c.N = tt.nValue
			c.W = calculateExpectedTotalReplications(tt.numNodes, tt.numTokens, tt.nValue)
			c.R = tt.rValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000
			c.CLIENT_GET_TIMEOUT_MS = 2_000
			c.SET_DATA_TIMEOUT_MS = 500

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := generateRandomString(10)
			value := generateRandomString(10)

			_, node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()

			// Write
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached")
				t.Error("Put timeout reached. Test failed prematurely.")
				return
			}

			// Read
			channel <- base.Message{JobId: 1, Key: key, Command: constants.CLIENT_REQ_READ, SrcID: -1, Client_Ch: client_ch}
			select {
			case msg := <-client_ch: // reply received in time

				if msg.Data != value {
					panic(fmt.Sprintf("wrong value! expected: %s got :%s", key, msg.Key))
				}

				fmt.Println("Read value:", msg.Data)
			case <-time.After(time.Duration(c.CLIENT_GET_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Get Timeout reached")
				t.Error("Get timeout reached. Test failed.")
				return
			}
			close(close_ch)
		})
	}
}
