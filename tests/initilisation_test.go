package tests

import (
	"fmt"
	"testing"
)

/*
======= Initilisation tests
*/

// TEST I1

// TestInitilisationEqual tests if each node is correctly assigned
// 1 token when the number of nodes == number of tokens
func TestInitilisationEqual(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens int
	}{
		{5, 5},
		{10, 10},
		{15, 15},
		{40, 40},
		{100, 100},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			// Check each node has 1 token
			for _, node := range phy_nodes {
				numTokens := len(node.GetTokens())
				if numTokens != 1 {
					t.Errorf("got: %d, expected: 1 token for node %d\n", numTokens, node.GetID())
				}
			}

			close(close_ch)
		})
	}
}

// TestInitilisationTokenGTNodes tests if nodes are properly assigned the
// correct number of tokens when there are more tokens than nodes.
// All nodes should receive the base number (tokens int_divide nodes) and
// the first i nodes should receive an extra token, where i is the remainder
func TestInitilisationTokenGTNodes(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens int
	}{
		{5, 10},
		{5, 20},
		{5, 22},
		{10, 20},
		{10, 40},
		{10, 77},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			// Check each node has correct number of tokens
			for i, node := range phy_nodes {
				numTokens := len(node.GetTokens())
				expectedTokens := tt.numTokens / tt.numNodes
				if i < (tt.numTokens % tt.numNodes) {
					expectedTokens += 1
				}
				if numTokens != expectedTokens {
					t.Errorf("got: %d, expected: %d tokens for node %d\n", numTokens, expectedTokens, node.GetID())
				}
			}

			close(close_ch)
		})
	}
}

// TestInitilisationInvalid tests if tokens are properly assigned if
// there are less nodes than tokens.
// First i nodes should be used if there are i tokens
func TestInitilisationTokensLTNodes(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens int
	}{
		{2, 1},
		{9, 4},
		{10, 5},
		{101, 33},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			// Check nodes < i get 1 token and nodes >= i get no tokens
			for i, node := range phy_nodes {
				numTokens := len(node.GetTokens())
				expectedTokens := 0
				if i < tt.numTokens {
					expectedTokens = 1
				}
				if numTokens != expectedTokens {
					t.Errorf("got: %d, expected %d tokens for node %d\n", numTokens, expectedTokens, node.GetID())
				}
			}

			close(close_ch)
		})
	}
}

// TestInitilisationZeroTokens tests if all nodes are assigned
// no tokens when total number of tokens is zero
func TestInitilisationZeroTokens(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens int
	}{
		{0, 0},
		{2, 0},
		{9, 0},
		{10, 0},
		{101, 0},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			// Check nodes < i get 1 token and nodes >= i get no tokens
			for _, node := range phy_nodes {
				numTokens := len(node.GetTokens())
				expectedTokens := 0
				if numTokens != expectedTokens {
					t.Errorf("got: %d, expected %d tokens for node %d\n", numTokens, expectedTokens, node.GetID())
				}
			}

			close(close_ch)
		})
	}
}

// TestInitilisationZeroNodes tests if no nodes are returned during creation
func TestInitilisationZeroNodes(t *testing.T) {
	var tests = []struct {
		numNodes, numTokens int
	}{
		{0, 2},
		{0, 9},
		{0, 10},
		{0, 101},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_nodes_%d_tokens", tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {
			phy_nodes, close_ch := setUpNodes(tt.numNodes, tt.numTokens)

			// Check nodes < i get 1 token and nodes >= i get no tokens
			for _, node := range phy_nodes {
				numTokens := len(node.GetTokens())
				expectedTokens := 0
				if numTokens != expectedTokens {
					t.Errorf("got: %d, expected %d tokens for node %d\n", numTokens, expectedTokens, node.GetID())
				}
			}

			close(close_ch)
		})
	}
}
