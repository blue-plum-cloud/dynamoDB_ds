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

// TestUpdateValues will test if randomlyUpdateValues function
// updates values in key-value pairs. Allow error margin due to random
// nature of updates.
func TestUpdateValues(t *testing.T) {
	ERROR_MARGIN_ALLOWED := 0.01
	numKeyValuePairs := 100000
	errCount := 0
	keyValuePairs := generateRandomKeyValuePairs(100, 100, numKeyValuePairs)
	keyValuePairsCopy := map[string]string{}

	// Copy key value pairs
	for key, value := range keyValuePairs {
		keyValuePairsCopy[key] = value
	}

	keyValuePairs = randomlyUpdateValues(keyValuePairs, 100)

	// Check if values are the same
	for key, value := range keyValuePairs {
		if value == keyValuePairsCopy[key] {
			errCount += 1
			if float64(errCount) > ERROR_MARGIN_ALLOWED*float64(numKeyValuePairs) {
				t.Errorf("key: %s, value %s did not change. expected different values. exceeded allowed margin of %f", key, value, ERROR_MARGIN_ALLOWED)
			}
		}
	}
}
