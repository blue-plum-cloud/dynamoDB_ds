package config

type Config struct {
	NUM_NODES             int
	NUM_TOKENS            int
	CLIENT_GET_TIMEOUT_MS int
	CLIENT_PUT_TIMEOUT_MS int
	SET_DATA_TIMEOUT_MS   int
	W                     int
	R                     int
	N                     int
	DEBUG_LEVEL           int
}

// Instantiate config object with default values
func InstantiateConfig() Config {
	c := Config{
		NUM_NODES:             NUM_NODES,
		NUM_TOKENS:            NUM_TOKENS,
		N:                     N,
		R:                     R,
		W:                     W,
		CLIENT_GET_TIMEOUT_MS: CLIENT_GET_TIMEOUT_MS,
		CLIENT_PUT_TIMEOUT_MS: CLIENT_PUT_TIMEOUT_MS,
		SET_DATA_TIMEOUT_MS:   SET_DATA_TIMEOUT_MS,
		DEBUG_LEVEL:           DEBUG_LEVEL,
	}

	return c
}
