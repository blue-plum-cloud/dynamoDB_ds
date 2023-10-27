package tests

import (
	"base"
	"config"
	"testing"
)

func TestSinglePutReplication(t *testing.T) {
	phy_nodes, close_ch := setupNodes()
	defer close(close_ch)

	key := "Sudipta"
	value := "Best Prof"

	node := base.FindNode("Sudipta", phy_nodes)

	node.Put(key, value)
	ori := 0
	repCnt := 0
	for _, n := range phy_nodes {
		if val := n.Get(key); val.GetData() == value && val.IsReplica() {
			repCnt++
		} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
			ori++
		}
	}
	expectedRepFactor := config.N - 1
	if repCnt != expectedRepFactor {
		t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
	}
	if ori != 1 {
		t.Errorf("Original data for key '%s' is missing", key)
	}
}

func TestMultiplePutReplication(t *testing.T) {
	phy_nodes, close_ch := setupNodes()
	defer close(close_ch)

	keyValuePairs := map[string]string{
		"Sudipta": "Best",
		"Others":  "Ok",
		"JWC":     "Bad",
	}

	expectedRepFactor := config.N - 1

	for key, value := range keyValuePairs {
		node := base.FindNode(key, phy_nodes)
		node.Put(key, value)

		ori := 0
		repCnt := 0
		for _, n := range phy_nodes {
			if val := n.Get(key); val.GetData() == value && val.IsReplica() {
				repCnt++
			} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
				ori++
			}
		}
		if repCnt != expectedRepFactor {
			t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
		}
		if ori != 1 {
			t.Errorf("Original data for key '%s' is missing", key)
		}
	}
}

func TestMultiplePutReplicationManyNodes(t *testing.T) {
	phy_nodes, close_ch := setUpNodes(10, 100)
	defer close(close_ch)

	keyValuePairs := map[string]string{
		"Sudipta": "Best",
		"Others":  "Ok",
		"JWC":     "Bad",
	}

	expectedRepFactor := config.N - 1

	for key, value := range keyValuePairs {
		node := base.FindNode(key, phy_nodes)
		node.Put(key, value)

		ori := 0
		repCnt := 0
		for _, n := range phy_nodes {
			if val := n.Get(key); val.GetData() == value && val.IsReplica() {
				repCnt++
			} else if val := n.Get(key); val.GetData() == value && !val.IsReplica() {
				ori++
			}
		}
		if repCnt != expectedRepFactor {
			t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
		}
		if ori != 1 {
			t.Errorf("Original data for key '%s' is missing", key)
		}
	}
}
