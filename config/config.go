package config

const (
	REQ_READ  = 0
	REQ_WRITE = 1
	ACK       = 2
	REQ_KILL  = 3
	SET_DATA  = 4

	NUM_NODES             = 10
	NUM_TOKENS            = 10
	CLIENT_GET_TIMEOUT_MS = 2000
	CLIENT_PUT_TIMEOUT_MS = 2000
	SET_DATA_TIMEOUT_NS   = 1000000000 // 1 second

	N = 10
	R = 3
	W = 0
	// 1 - key info
	// 2 - verbose, fixed random seed
	// 3 - very verbose
	DEBUG_LEVEL = 2
)
