package tests

import (
	"base"
	"config"
	"constants"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// TEST H1

// TestHandoffWorksWithSingleNodeDown check if updates
// work correctly when non-coordinator node is down
func TestHandoffWorksWithSingleNodeDown(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, wValue int
	}{
		{5, 5, 1, 1},
		{40, 40, 40, 20},
		{100, 100, 30, 10},

		{3, 4, 3, 2},
		{30, 65, 25, 25},
		{5, 7, 4, 3},
		{50, 51, 1, 1},

		{70, 3, 1, 1},
		{20, 18, 18, 16},
		{60, 30, 10, 10},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_w", tt.numNodes, tt.numTokens, tt.nValue, tt.wValue)
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
			c.W = tt.wValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long timeout since we are testing replications not timeout

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := "hello"
			value := "world"

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

			// Kill node
			// Choose random node to kill (non-coordinator)

			randomNodeId := -1
			for randomNodeId == -1 || randomNodeId == node.GetID() {
				randomNodeId = rand.Intn(len(phy_nodes))
				fmt.Println(randomNodeId)
			}
			fmt.Printf("Killing node %d\n", randomNodeId)
			node_channel := (phy_nodes[randomNodeId]).GetChannel()
			node_channel <- base.Message{JobId: 0, Command: constants.CLIENT_REQ_KILL, Data: "999999999999999999", SrcID: -1}

			// Reduce 1 due to node failure if N == min(tokens,nodes)
			// Impossible to get all replicas if node is dead
			if tt.nValue == tt.numNodes || tt.nValue == tt.numTokens {
				expectedReplicas -= 1
			}
			if tt.wValue > 1 {
				c.W = tt.wValue - 1
			}

			// Put new data
			value = "sudipta"
			channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached on update")
				t.Error("Put timeout reached on update. Test failed.")
				return
			}

			time.Sleep(100 * time.Millisecond) // Wait for backup to be written
			ori := 0
			repCnt := 0
			backupCnt := 0
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
				newVal, newOk := n.GetAllBackup()[randomNodeId][hashedKey]
				if newOk {
					if newVal.GetData() == value {
						fmt.Println("Found backup!")
						backupCnt++
					}
				}
			}

			if repCnt+backupCnt != expectedReplicas {
				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedReplicas)
			}
			if ori != 1 {
				t.Errorf("Original data for key '%s' is missing", key)
			}

			close(close_ch)
		})
	}
}

// TEST H2

// TestHandoffWorksCoordinatorDown check if updates
// work correctly when coordinator node is down
func TestHandoffWorksCoordinatorDown(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens, nValue, wValue int
	}{
		{5, 5, 1, 1},
		{40, 40, 40, 20},
		{100, 100, 30, 10},

		{3, 4, 3, 2},
		{30, 65, 25, 25},
		{5, 7, 4, 3},
		{50, 51, 1, 1},

		{70, 3, 1, 1},
		{20, 18, 18, 16},
		{60, 30, 10, 10},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens_%d_n_%d_w", tt.numNodes, tt.numTokens, tt.nValue, tt.wValue)
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
			c.W = tt.wValue
			c.CLIENT_PUT_TIMEOUT_MS = 5_000 // long timeout since we are testing replications not timeout

			phy_nodes, close_ch, client_ch := setUpNodes(&c)

			key := "hello"
			value := "world"

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

			// Kill coordinator
			fmt.Println("Killing coordinator")
			channel <- base.Message{JobId: 0, Command: constants.CLIENT_REQ_KILL, Data: "999999999999999999", SrcID: -1}

			// Reduce 1 due to node failure if N == min(tokens,nodes)
			// Impossible to get all replicas if node is dead
			if tt.nValue == tt.numNodes || tt.nValue == tt.numTokens {
				expectedReplicas -= 1
				expectedTotalReplications -= 1
			}
			if tt.wValue > 1 {
				c.W = tt.wValue - 1
			}

			fmt.Println("New config:", c)

			// Find new node
			token, node := base.FindNode(key, phy_nodes, &c)
			new_node := base.FindPrefList(token, phy_nodes, 2)
			new_channel := (*new_node).GetChannel()
			// Put new data
			value = "sudipta"
			new_channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch}

			select {
			case ack := <-client_ch: // reply received in time
				if ack.Key != key {
					panic(fmt.Sprintf("wrong key! expected: %s got :%s", key, ack.Key))
				}

				fmt.Println("Value stored: ", value, " with key: ", key)
			case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
				fmt.Println("Put Timeout reached on update")
				t.Error("Put timeout reached on update. Test failed.")
				return
			}

			time.Sleep(100 * time.Millisecond) // Wait for backup to be written
			ori := 0
			repCnt := 0
			backupCnt := 0
			hashedKey := base.ComputeMD5(key)
			fmt.Println("value", value)
			for _, n := range phy_nodes {
				val, ok := n.GetAllData()[hashedKey]
				if ok {
					if val.GetData() == value && val.IsReplica() {
						repCnt++
					} else if val.GetData() == value && !val.IsReplica() {
						ori++
					}
				}
				newVal, newOk := n.GetAllBackup()[node.GetID()][hashedKey]
				if newOk {
					if newVal.GetData() == value {
						fmt.Println("Found backup!")
						backupCnt++
					}
				}
			}

			if repCnt+backupCnt != expectedTotalReplications {
				t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt+backupCnt, expectedTotalReplications)
			}

			close(close_ch)
		})
	}
}
