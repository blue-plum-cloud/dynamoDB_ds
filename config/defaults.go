package config

const (
	//default values for config struct
	NUM_NODES  = 10
	NUM_TOKENS = 10
	N          = 8 // number of put replication, MUST BE LESS THAN NUM_NODES
	R          = 3
	W          = 3

	CLIENT_GET_TIMEOUT_MS = 2000
	CLIENT_PUT_TIMEOUT_MS = 2000
	SET_DATA_TIMEOUT_MS   = 1000 // 1 second

	// 1 - key info
	// 2 - verbose, fixed random seed
	// 3 - very verbose
	DEBUG_LEVEL = 2
)
