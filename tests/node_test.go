package main

import (
	"base"
	"fmt"
	"math/big"
	"testing"
)

func TestNodeCreation(t *testing.T) {
	close_ch := make(chan struct{})
	num := 5
	nodes := base.CreateNodes(close_ch, num)
	numNodes := len(nodes)
	expected := 5
	if numNodes != expected {
		t.Errorf("Expected %d, but got %d", expected, numNodes)
	}
}

func TestTokenCreation(t *testing.T) {
	close_ch := make(chan struct{})
	num := 5
	nodes := base.CreateNodes(close_ch, num)
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

func TestTokenBigRange(t *testing.T) {
	close_ch := make(chan struct{})
	num := 3
	numTokens := 3
	nodes := base.CreateNodes(close_ch, num)
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)
	tokenCounter := 0

	//fairly distribute tokens for now
	tokenRangeSize := new(big.Int).Div(maxValue, big.NewInt(int64(num)))
	base.InitializeTokens(nodes, num)
	for i, node := range nodes {
		for _, token := range node.GetTokens() {
			startRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i)))
			endRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i+1)))
			if tokenCounter != numTokens-1 {
				endRange.Sub(endRange, big.NewInt(1))
			}
			if token.GetStartRange() != fmt.Sprintf("%032X", startRange) {
				t.Errorf("Expected start range of %s, but got %s", fmt.Sprintf("%032X", startRange), token.GetStartRange())
			}
			if token.GetEndRange() != fmt.Sprintf("%032X", endRange) {
				t.Errorf("Expected end range of %s, but got %s", fmt.Sprintf("%032X", endRange), token.GetEndRange())
			}
			tokenCounter++
		}

	}
}

func TestTokenUnevenRange(t *testing.T) {
	close_ch := make(chan struct{})
	num := 3
	numTokens := 5
	nodes := base.CreateNodes(close_ch, num)
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)
	tokenCounter := 0

	//fairly distribute tokens for now
	tokenRangeSize := new(big.Int).Div(maxValue, big.NewInt(int64(numTokens)))
	base.InitializeTokens(nodes, numTokens)
	for _, node := range nodes {
		for _, token := range node.GetTokens() {
			startRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(tokenCounter)))
			endRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(tokenCounter+1)))

			if tokenCounter != numTokens-1 {
				endRange.Sub(endRange, big.NewInt(1))
			}
			if token.GetStartRange() != fmt.Sprintf("%032X", startRange) {
				t.Errorf("Expected start range of %s, but got %s", fmt.Sprintf("%032X", startRange), token.GetStartRange())
			}
			if token.GetEndRange() != fmt.Sprintf("%032X", endRange) {
				t.Errorf("Expected end range of %s, but got %s", fmt.Sprintf("%032X", endRange), token.GetEndRange())
			}
			tokenCounter++
		}

	}
}
