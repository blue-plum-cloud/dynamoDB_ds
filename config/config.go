package config

const (
	REQ_READ  = 0
	REQ_WRITE = 1
	ACK       = 2

	N = 3 // number of put replication

	CLIENT_GET_TIMEOUT_MS = 10
	CLIENT_PUT_TIMEOUT_MS = 10

	// 1 - key info
	// 2 - verbose, fixed random seed
	// 3 - very verbose
	DEBUG_LEVEL = 2
)
