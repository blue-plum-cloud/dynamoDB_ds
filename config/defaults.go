package config

const (
	//default values for config struct
	NUM_NODES  = 10
	NUM_TOKENS = 10
	N          = 5 // number of put replication, MUST BE LESS THAN NUM_NODES (R+W>N, R,W < N)
	R          = 3
	W          = 3

	CLIENT_GET_TIMEOUT_MS = 2000
	CLIENT_PUT_TIMEOUT_MS = 2000
	SET_DATA_TIMEOUT_MS   = 1000 // 1 second

	// see constants.go for description
	DEBUG_LEVEL = 3
)
