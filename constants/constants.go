package constants

const (
	NO_DEBUG      = 1
	INFO          = 2
	VERBOSE_FIXED = 3
	VERY_VERBOSE  = 4

	CLIENT_REQ_READ   = 100
	CLIENT_REQ_WRITE  = 101
	CLIENT_REQ_KILL   = 102
	CLIENT_REQ_REVIVE = 103

	CLIENT_ACK_READ  = 200
	CLIENT_ACK_WRITE = 201
	CLIENT_ACK_ALIVE = 202

	SET_DATA  = 300
	BACK_DATA = 301

	ACK_SET_DATA  = 400
	ACK_BACK_DATA = 401

	READ_DATA     = 500
	READ_DATA_ACK = 501

	ALIVE_ACK = 600
)

func GetConstantString(c int) string {
	switch c {
	case 1:
		return "NO_DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "VERBOSE_FIXED"
	case 4:
		return "VERY_VERBOSE"

	case 100:
		return "CLIENT_REQ_READ"
	case 101:
		return "CLIENT_REQ_WRITE"
	case 102:
		return "CLIENT_REQ_KILL"
	case 103:
		return "CLIENT_REQ_REVIVE"

	case 200:
		return "CLIENT_ACK_READ"
	case 201:
		return "CLIENT_ACK_WRITE"

	case 300:
		return "SET_DATA\t"
	case 301:
		return "BACK_DATA\t"

	case 400:
		return "ACK_SET_DATA"
	case 401:
		return "ACK_BACK_DATA"

	case 500:
		return "READ_DATA\t"
	case 501:
		return "READ_DATA_ACK"

	case 600:
		return "ALIVE_ACK"

	default:
		return "UNKNOWN_CONSTANT"
	}
}
