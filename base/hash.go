package base

import (
	"config"
	"constants"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
)

// Function to compute the MD5 hash of a string
// use hash as a string mainly for convenience
func ComputeMD5(data string) string {
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

func InitializeTokens(phy_nodes []*Node, c *config.Config) {
	fmt.Println("Initializing tokens...")
	maxValue := new(big.Int)
	maxValue.SetString("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16)

	numTokens := c.NUM_TOKENS
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

	if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
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

		for j := 0; j < tokensPerNode; j++ {
			token := allTokens[tokenCounter]
			token.phy_id = node.GetID()
			node.tokens = append(node.tokens, token)
			if c.DEBUG_LEVEL >= constants.INFO {
				fmt.Printf("\nInsert token %d into node %d with start range %s and end range %s\n",
					token.id, token.phy_id, token.range_start, token.range_end)
			}
			tokenCounter++
		}

	}

	// Insert all tokens into each node's tokensStruct (BST)
	if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
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

	// Make preference list copy for each node
	rangeMap := make(map[*Token][]int)

	node := phy_nodes[0]
	startNode := node.tokenStruct.Root
	endNode := node.tokenStruct.Root
	windowMap := make(map[int]struct{})
	var pref []int

	// Initial population of the pref list
	for endNode != nil && len(pref) < c.N {
		pid := endNode.Token.phy_id
		if _, exists := windowMap[pid]; !exists {
			pref = append(pref, pid)
			windowMap[pid] = struct{}{}
		}
		endNode = node.tokenStruct.getNext(endNode)
	}

	rangeMap[startNode.Token] = make([]int, c.N)
	copy(rangeMap[startNode.Token], pref)
	if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
		fmt.Printf("Preference list for token %d: \n", startNode.Token.GetID())
		fmt.Println(rangeMap[startNode.Token])
	}

	cnt := 1
	st := 0
	for cnt < c.NUM_TOKENS {
		startNode = node.tokenStruct.getNext(startNode)
		st = pref[0]
		pref = pref[1:]

		delete(windowMap, st)

		// Keep moving nextNode to find new unique phyIDs to fill the window
		for len(pref) < c.N {
			pid := endNode.Token.phy_id
			if _, exists := windowMap[pid]; !exists {
				pref = append(pref, pid)
				windowMap[pid] = struct{}{}
			}
			endNode = node.tokenStruct.getNext(endNode)
		}

		rangeMap[startNode.Token] = make([]int, c.N)
		copy(rangeMap[startNode.Token], pref)
		if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
			fmt.Printf("Preference list for token %d: \n", startNode.Token.GetID())
			fmt.Println(rangeMap[startNode.Token])
		}
		cnt++
	}

	if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
		fmt.Println("Overall preference list: ")
		fmt.Println(rangeMap)
	}

	for _, node := range phy_nodes {
		newMap := make(map[*Token][]int)
		for key, value := range rangeMap {
			newMap[key] = append([]int(nil), value...)
		}
		node.prefList = rangeMap

	}

	if c.DEBUG_LEVEL >= constants.VERY_VERBOSE {
		fmt.Printf("Inserted tokens ==== \n")
		for _, node := range phy_nodes {
			node.tokenStruct.PrintBST()
			fmt.Printf("\n")
		}
	}
}
