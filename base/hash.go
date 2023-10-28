package base

import (
	"config"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
)

// Function to compute the MD5 hash of a string
// use hash as a string mainly for convenience
func computeMD5(data string) string {
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

func hashInRange(hashStr string, lowerBound string, upperBound string) bool {
	hashInt := new(big.Int)
	hashInt.SetString(hashStr, 16)

	lower := new(big.Int)
	lower.SetString(lowerBound, 16)

	upper := new(big.Int)
	upper.SetString(upperBound, 16)

	// Check if hashInt is in the range [lower, upper]
	return hashInt.Cmp(lower) >= 0 && hashInt.Cmp(upper) <= 0
}

func InitializeTokens(phy_nodes []*Node, numTokens int) {
	fmt.Println("Initializing tokens...")
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	baseTokensPerNode := 0
	extraTokens := 0
	tokenRangeSize := new(big.Int)
	if len(phy_nodes) != 0 {
		//prevent divide by zero errors
		baseTokensPerNode = numTokens / len(phy_nodes)
		extraTokens = numTokens % len(phy_nodes)
	}
	if numTokens != 0 {
		//prevent divide by zero errors
		tokenRangeSize = new(big.Int).Div(maxValue, big.NewInt(int64(numTokens)))
	}

	allTokens := make([]*Token, 0)

	//token init phase
	for i := 0; i < numTokens; i++ {
		startRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i)))
		endRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(i+1)))

		//to prevent address overlap
		if i != numTokens-1 {
			endRange.Sub(endRange, big.NewInt(1))
		}
		// fmt.Printf("token %d, range start = %s, range end = %s\n", tokenCounter, startRange, endRange)
		token := &Token{
			id:          i,
			range_start: fmt.Sprintf("%032X", startRange),
			range_end:   fmt.Sprintf("%032X", endRange),
		}
		allTokens = append(allTokens, token)
	}

	if config.DEBUG_LEVEL >= 2 {
		rand.Seed(0)
	}
	rand.Shuffle(len(allTokens), func(i, j int) { allTokens[i], allTokens[j] = allTokens[j], allTokens[i] })

	tokenCounter := 0
	//assignment phase
	for i, node := range phy_nodes {
		tokensPerNode := baseTokensPerNode
		if i < extraTokens {
			tokensPerNode++
		}

		fmt.Println()
		for j := 0; j < tokensPerNode; j++ {
			token := allTokens[tokenCounter]
			token.phy_node = node
			node.tokens = append(node.tokens, token)
			tokenCounter++
		}

	}

	// Insert all tokens into each node's tokensStruct (BST)
	if config.DEBUG_LEVEL >= 3 {
		fmt.Printf("All tokens ==== \n")
		for _, token := range allTokens {
			fmt.Printf("token: %v\n", token)
		}
		fmt.Printf("\n")
	}

	for _, node := range phy_nodes {
		for _, token := range allTokens {
			node.tokenStruct.Insert(token)
		}
	}

	if config.DEBUG_LEVEL >= 3 {
		fmt.Printf("Inserted tokens ==== \n")
		for _, node := range phy_nodes {
			node.GetTokenStruct().PrintBST()
			fmt.Printf("\n")
		}
	}
}
