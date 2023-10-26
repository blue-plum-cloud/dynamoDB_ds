package base

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/big"
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
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	//fairly distribute tokens for now
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

	tokenCounter := 0
	for i, node := range phy_nodes {
		tokensPerNode := baseTokensPerNode
		if i < extraTokens {
			tokensPerNode++
		}

		for j := 0; j < tokensPerNode; j++ {
			startRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(tokenCounter)))
			endRange := new(big.Int).Mul(tokenRangeSize, big.NewInt(int64(tokenCounter+1)))

			//to prevent address overlap
			if tokenCounter != numTokens-1 {
				endRange.Sub(endRange, big.NewInt(1))
			}

			token := &Token{
				id:          tokenCounter,
				phy_node:    node,
				range_start: fmt.Sprintf("%032X", startRange),
				range_end:   fmt.Sprintf("%032X", endRange),
			}

			node.tokens = append(node.tokens, token)
			tokenCounter++
		}
	}
}
