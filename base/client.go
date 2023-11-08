package base

import (
	"bytes"
	"config"
	"constants"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

func ParseOneArg(input string, commandName string) (string, error) {
	trimString := fmt.Sprintf("%s(", commandName)
	key := strings.TrimSuffix(strings.TrimPrefix(input, trimString), ")")
	parts := strings.SplitN(key, ",", 2)
	if len(parts) != 1 {
		return "", errors.New("Invalid input for get. Expect get(string)")
	}
	return key, nil
}

func ParseTwoArgs(input string, commandName string) (string, string, error) {
	trimString := fmt.Sprintf("%s(", commandName)
	remainder := strings.TrimSuffix(strings.TrimPrefix(input, trimString), ")")
	parts := strings.SplitN(remainder, ",", 2)
	if len(parts) != 2 {
		return "", "", errors.New("Invalid input for put. Expect put(string, string)")
	}
	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	return key, value, nil
}

// Separate routine from client CLI
// Single and only source of client channel consume
// Messages are tracked by JobId to handle multiple requests for same node / dropped requests
func StartListening(close_ch chan struct{}, client_ch chan Message, awaitUids map[int](*atomic.Bool), c *config.Config) {
	for {
		select {
		case <-close_ch:
			return
		case msg := <-client_ch:
			var debugMsg bytes.Buffer // allow appending of messages
			debugMsg.WriteString(fmt.Sprintf("Start: %s ", msg.ToString(-1)))

			if !awaitUids[msg.JobId].Load() { // timeout reached for job id
				delete(awaitUids, msg.JobId)

			} else {
				awaitUids[msg.JobId].Store(false)

				switch msg.Command {

				case constants.CLIENT_ACK_READ:
					// TODO: validity check
					fmt.Printf("COMPLETED Jobid=%d Command=%s: (%s, %s)\n",
						msg.JobId, constants.GetConstantString(msg.Command), msg.Key, msg.Data)

				case constants.CLIENT_ACK_WRITE:
					// TODO: validity check
					fmt.Printf("COMPLETED Jobid=%d Command=%s: (%s, %s)\n",
						msg.JobId, constants.GetConstantString(msg.Command), msg.Key, msg.Data)

				default:
					panic("Unexpected ACK received in client_ch.")
				}
			}

			if c.DEBUG_LEVEL >= constants.VERBOSE_FIXED {
				fmt.Printf("%s\n", debugMsg.String())
			}
		}
	}
}

// Separate routine from client CLI
// Constantly consume client_ch even after timeout
// !!!! Loops infinitely if server drops request and does not ACK it
func StartTimeout(awaitUids map[int](*atomic.Bool), jobId int, command int, timeout_ms int, close_ch chan struct{}) {
	if _, exists := awaitUids[jobId]; !exists {
		awaitUids[jobId] = new(atomic.Bool)
	}
	awaitUids[jobId].Store(true)

	reqTime := time.Now()

	for {
		select {
		case <-close_ch:
			return
		default:
			if !(awaitUids[jobId].Load()) {
				delete(awaitUids, jobId)
				return
			}
			if time.Since(reqTime) > time.Duration(timeout_ms)*time.Millisecond {
				fmt.Printf("TIMEOUT REACHED: Jobid=%d Command=%s\n", jobId, constants.GetConstantString(command))
				awaitUids[jobId].Store(false)
				return
			}
		}
	}
}
