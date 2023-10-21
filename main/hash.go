package main

import (
	"crypto/md5"
	"encoding/hex"
	"math/big"
)

// Function to compute the MD5 hash of a string
// use hash as a string mainly for convenience
func computeMD5(data string) string {
	hash := md5.Sum([]byte(data))
	return hex.EncodeToString(hash[:])
}

func isHashInRange(hashStr string, lowerBound string, upperBound string) bool {
	hashInt := new(big.Int)
	hashInt.SetString(hashStr, 16)

	lower := new(big.Int)
	lower.SetString(lowerBound, 16)

	upper := new(big.Int)
	upper.SetString(upperBound, 16)

	// Check if hashInt is in the range [lower, upper]
	return hashInt.Cmp(lower) >= 0 && hashInt.Cmp(upper) <= 0
}
