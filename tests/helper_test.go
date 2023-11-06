package tests

import (
	"math"
	"testing"
)

// -------- HELPER FUNCTION TESTS

// TestGenerateRandomString will generate high numbers of
// random strings to check if length fulfils the specification
func TestGenerateRandomString(t *testing.T) {
	strMaxLength := 1000
	numStrings := 100000

	minLength := math.MaxInt32
	maxLength := 0

	for i := 0; i < numStrings; i++ {
		generatedStr := generateRandomString(strMaxLength)
		length := len(generatedStr)

		if length < minLength {
			minLength = length
		}
		if length > maxLength {
			maxLength = length
		}
	}

	// Length should be within 1 to strMaxLength
	if minLength < 1 || maxLength > strMaxLength {
		t.Errorf("found minLength: %d, maxLength: %d. minLength should be >1 and maxLength should be <=%d", minLength, maxLength, strMaxLength)
	}
}
