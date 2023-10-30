package config

const (
	REQ_READ      = 0
	REQ_WRITE     = 1
	ACK           = 2
	REQ_KILL      = 3
	SET_DATA      = 4
	READ_DATA     = 5
	READ_DATA_ACK = 6

	//default values for config struct
	NUM_NODES  = 10
	NUM_TOKENS = 10
	N          = 10 // number of put replication
	R          = 3  //
	W          = 3  // just put here first

	CLIENT_GET_TIMEOUT_MS = 2000
	CLIENT_PUT_TIMEOUT_MS = 2000
	SET_DATA_TIMEOUT_MS   = 1000 // 1 second

	// 1 - key info
	// 2 - verbose, fixed random seed
	// 3 - very verbose
	DEBUG_LEVEL = 2
)
