package base

import (
	"bytes"
	"config"
	"constants"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func ParsePutArg(putRegex string, input string) (string, string, int, error) {
	re := regexp.MustCompile(putRegex)
	matches := re.FindStringSubmatch(input)

	if len(matches) != 4 {
		return "", "", 0, errors.New("invalid put command format, must be put(string,string) int;")
	}

	client, err := strconv.Atoi(matches[3])
	if err != nil {
		return "", "", 0, errors.New("invalid put command format, must be put(string,string) int;")
	}
	return matches[1], matches[2], client, nil
}
func ParseGetArg(getRegex string, input string) (string, int, error) {
	re := regexp.MustCompile(getRegex)
	matches := re.FindStringSubmatch(input)

	if len(matches) != 3 {
		return "", 0, errors.New("invalid get command format, must be get(string) int;")
	}

	client, err := strconv.Atoi(matches[2])
	if err != nil {
		return "", 0, errors.New("invalid put command format, must be put(string,string) int;")
	}
	return matches[1], client, nil
}

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
func (client *Client) StartListening(c *config.Config) {
	for {
		select {
		case <-client.Close:
			return
		case msg := <-client.Client_ch:
			var debugMsg bytes.Buffer // allow appending of messages
			debugMsg.WriteString(fmt.Sprintf("Start: %s ", msg.ToString(-1)))

			if client.AwaitUids[msg.JobId].Load() { // timeout reached for job id
				delete(client.AwaitUids, msg.JobId)

			} else {
				client.AwaitUids[msg.JobId].Store(false)

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
func (client *Client) StartTimeout(jobId int, command int, timeout_ms int) {
	if _, exists := client.AwaitUids[jobId]; !exists {
		client.AwaitUids[jobId] = new(atomic.Bool)
	}
	client.AwaitUids[jobId].Store(true)
	fmt.Println("stored jobId ", jobId)
	reqTime := time.Now()

	for {
		select {
		case <-client.Close:
			return
		default:
			if !(client.AwaitUids[jobId].Load()) {
				fmt.Println("here")
				delete(client.AwaitUids, jobId)
				return
			}
			if time.Since(reqTime) > time.Duration(timeout_ms)*time.Millisecond {
				fmt.Printf("TIMEOUT REACHED: Jobid=%d Command=%s\n", jobId, constants.GetConstantString(command))
				client.AwaitUids[jobId].Store(false)
				return
			}
		}
	}
}

// func (client *Client)
