package tests

import (
	"base"
	"config"
	"fmt"
	"math/big"
	"sort"
	"testing"
)

func TestNodeCreation(t *testing.T) {
	close_ch := make(chan struct{})
	num := 5
	client_ch := make(chan base.Message)

	//node and token initialization
	nodes := base.CreateNodes(client_ch, close_ch, num)
	numNodes := len(nodes)
	expected := 5
	if numNodes != expected {
		t.Errorf("Expected %d, but got %d", expected, numNodes)
	}
}

func TestTokenCreation(t *testing.T) {
	close_ch := make(chan struct{})
	num := 5
	client_ch := make(chan base.Message)

	//node and token initialization
	nodes := base.CreateNodes(client_ch, close_ch, num)
	base.InitializeTokens(nodes, num)
	numTokens := 0
	for _, node := range nodes {
		numTokens += len(node.GetTokens())
	}
	expected := 5
	if numTokens != expected {
		t.Errorf("Expected %d, but got %d", expected, numTokens)
	}
}

func TestTokenRange(t *testing.T) {
	close_ch := make(chan struct{})
	num := 3
	numTokens := 3
	client_ch := make(chan base.Message)

	//node and token initialization
	nodes := base.CreateNodes(client_ch, close_ch, num)
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	tokenRangeSize := new(big.Int).Div(maxValue, big.NewInt(int64(num)))
	base.InitializeTokens(nodes, num)

	allTokens := make([]*base.Token, 0)

	for _, node := range nodes {
		allTokens = append(allTokens, node.GetTokens()...)
	}

	sort.Slice(allTokens, func(i int, j int) bool {
		return allTokens[i].GetID() < allTokens[j].GetID()
	})

	for i, token := range allTokens {
		startRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i)))
		endRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i+1)))
		if i != numTokens-1 {
			endRange.Sub(endRange, big.NewInt(1))
		}
		if token.GetStartRange() != fmt.Sprintf("%032X", startRange) {
			t.Errorf("Expected start range of %s, but got %s", fmt.Sprintf("%032X", startRange), token.GetStartRange())
		}
		if token.GetEndRange() != fmt.Sprintf("%032X", endRange) {
			t.Errorf("Expected end range of %s, but got %s", fmt.Sprintf("%032X", endRange), token.GetEndRange())
		}
	}
}

func TestTokenUnevenRange(t *testing.T) {
	close_ch := make(chan struct{})
	num := 3
	numTokens := 5
	client_ch := make(chan base.Message)

	//node and token initialization
	nodes := base.CreateNodes(client_ch, close_ch, num)
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	tokenRangeSize := new(big.Int).Div(maxValue, big.NewInt(int64(numTokens)))
	base.InitializeTokens(nodes, numTokens)

	allTokens := make([]*base.Token, 0)

	for _, node := range nodes {
		for _, token := range node.GetTokens() {
			allTokens = append(allTokens, token)
			fmt.Println(token)
		}
	}

	sort.Slice(allTokens, func(i int, j int) bool {
		return allTokens[i].GetID() < allTokens[j].GetID()
	})

	for i, token := range allTokens {
		startRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i)))
		endRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i+1)))
		if i != numTokens-1 {
			endRange.Sub(endRange, big.NewInt(1))
		}
		if token.GetStartRange() != fmt.Sprintf("%032X", startRange) {
			t.Errorf("Expected start range of %s, but got %s", fmt.Sprintf("%032X", startRange), token.GetStartRange())
		}
		if token.GetEndRange() != fmt.Sprintf("%032X", endRange) {
			t.Errorf("Expected end range of %s, but got %s", fmt.Sprintf("%032X", endRange), token.GetEndRange())
		}
	}
}

func TestTokenRandomDistribution(t *testing.T) { //extremely low chance numTokens will come out sorted after assignment
	num := 5
	numTokens := 15
	client_ch := make(chan base.Message)

	//node and token initialization
	nodes := base.CreateNodes(client_ch, make(chan struct{}), num)
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	base.InitializeTokens(nodes, numTokens)

	allTokens := make([]*base.Token, 0)

	for _, node := range nodes {
		for _, token := range node.GetTokens() {
			allTokens = append(allTokens, token)
			fmt.Println(token)
		}
	}
	result := sort.SliceIsSorted(allTokens, func(i int, j int) bool {
		return allTokens[i].GetID() < allTokens[j].GetID()
	})
	if result == true {
		t.Errorf("Expected random distribution of tokens, but hash ranges of tokens are still contiguous.\n")
	}

}

func TestReplicationCount(t *testing.T) {
	var tests = []struct {
		N, numNodes, numTokens, expected int
	}{
		{3, 3, 3, 3},
		{4, 3, 3, 3},
		{3, 4, 3, 3},
		{3, 3, 4, 3},

		{20, 5, 10, 5},
		{3, 10, 20, 3},

		{0, 3, 3, 0},
		{5, 10, 0, 0},
		{4, 0, 20, 0},
	}
	for _, tt := range tests {
		testname := fmt.Sprintf("%d_N_%d_nodes_%d_tokens", tt.N, tt.numNodes, tt.numTokens)
		t.Run(testname, func(t *testing.T) {

			c := config.InstantiateConfig()
			c.N = tt.N
			c.NUM_NODES = tt.numNodes
			c.NUM_TOKENS = tt.numTokens
			actual := base.GetReplicationCount(c)

			if actual != tt.expected {
				t.Errorf("expected: %d, got: %d", tt.expected, actual)
			}
		})
	}
}
