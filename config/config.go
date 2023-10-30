package config

const (
	NUM_NODES  = 10
	NUM_TOKENS = 10

	REQ_READ  = 0
	REQ_WRITE = 1
	ACK       = 2
	REQ_KILL  = 3
	SET_DATA  = 4

	N = 10 // number of put replication
	R = 2 //
	W = 0 // just put here first

	CLIENT_GET_TIMEOUT_MS = 2000
	CLIENT_PUT_TIMEOUT_MS = 2000
	SET_DATA_TIMEOUT_NS   = 1000000000 // 1 second

	// 1 - key info
	// 2 - verbose, fixed random seed
	// 3 - very verbose
	DEBUG_LEVEL = 2
)
