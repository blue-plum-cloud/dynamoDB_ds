package config

const (
	REQ_READ  = 0
	REQ_WRITE = 1
	ACK       = 2
	REQ_KILL  = 3
	SET_DATA  = 4

	// 1 - key info
	// 2 - verbose, fixed random seed
	// 3 - very verbose
	DEBUG_LEVEL = 2
)
