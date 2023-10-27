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
	repCnt := 0
	for _, n := range phy_nodes {
		if val := n.Get(key); val.GetData() == value {
			repCnt++
		}
	}
	expectedRepFactor := config.N
	if repCnt != expectedRepFactor {
		t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
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

	expectedRepFactor := config.N

	for key, value := range keyValuePairs {
		node := base.FindNode(key, phy_nodes)
		node.Put(key, value)

		repCnt := 0
		for _, n := range phy_nodes {
			if val := n.Get(key); val.GetData() == value {
				repCnt++
			}
		}

		// Check if the replication count matches the expected replication factor
		if repCnt != expectedRepFactor {
			t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
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

	expectedRepFactor := config.N

	for key, value := range keyValuePairs {
		node := base.FindNode(key, phy_nodes)
		node.Put(key, value)

		repCnt := 0
		for _, n := range phy_nodes {
			if val := n.Get(key); val.GetData() == value {
				repCnt++
			}
		}

		// Check if the replication count matches the expected replication factor
		if repCnt != expectedRepFactor {
			t.Errorf("Replication count for key '%s' is %d; expected %d", key, repCnt, expectedRepFactor)
		}
	}
}
