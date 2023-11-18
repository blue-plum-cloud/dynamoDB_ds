package base

import (
	"bytes"
	"config"
	"constants"
	"errors"
	"fmt"
	"regexp"
	"strconv"
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
func ParseKillArg(killRegex string, input string) (int, string, error) {

	re := regexp.MustCompile(killRegex)
	matches := re.FindStringSubmatch(input)

	killmsg := "invalid kill command format, must be kill(int,int);"

	if len(matches) != 3 {
		fmt.Println("not enough arguments in kill")
		return 0, "", errors.New(killmsg)
	}

	node, err := strconv.Atoi(matches[1])
	if err != nil {
		fmt.Println(err)
		return 0, "", errors.New(killmsg)
	}

	return node, matches[2], nil
}

func ParseRevArg(revRegex string, input string) (int, error) {
	re := regexp.MustCompile(revRegex)
	matches := re.FindStringSubmatch(input)

	if len(matches) != 2 {
		return 0, errors.New("invalid revive command format, must be revive(int);")
	}

	node, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, errors.New("invalid revive command format, must be revive(int);")
	}
	return node, nil
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

			if !client.AwaitUids[msg.JobId].Load() { // timeout reached for job id
				delete(client.AwaitUids, msg.JobId)

			} else {
				client.AwaitUids[msg.JobId].Store(false)

				switch msg.Command {

				case constants.CLIENT_ACK_READ:
					// TODO: validity check
					fmt.Printf("COMPLETED Jobid=%d Command=%s: (%s, %s)\n",
						msg.JobId, constants.GetConstantString(msg.Command), msg.Key, msg.Data)
					client.NewestRead = msg.Data //for testing purposes
					// fmt.Printf("set client newest read to %s\n", client.NewestRead)

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
	reqTime := time.Now()

	for {
		select {
		case <-client.Close:
			return
		default:
			if !(client.AwaitUids[jobId].Load()) {
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
