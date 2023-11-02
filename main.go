package main

import (
	"base"
	"bufio"
	"config"
	"constants"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

func ParseOneArg(input string, commandName string) (string, error) {
	trimString := fmt.Sprintf("%s(", commandName)
	key := strings.TrimSuffix(strings.TrimPrefix(input, trimString), ")")
	parts := strings.SplitN(key, ",", 2)
	if len(parts) != 1 {
		return "", errors.New("Invalid input for get. Expect get(string)")
	}
	return key, nil
}

func ListenGetReply(key string, client_ch chan base.Message, c *config.Config) {
	select {
	case value := <-client_ch: // reply received in time
		hashkey := base.ComputeMD5(key)
		if value.Key != hashkey {
			panic(fmt.Sprintf("wrong key! expected: %s actual: %s", hashkey, value.Key))
		}

		if value.Data != "" {
			fmt.Println("Value is: ", value.Data)
			//clear remaining messages in the channel after processing the first msg
			clearChannel(client_ch)
		} else {
			fmt.Println("Data not found!")
		}

	case <-time.After(time.Duration(c.CLIENT_GET_TIMEOUT_MS) * time.Millisecond): // timeout reached
		fmt.Println("Get Timeout reached")
	}

}

func clearChannel(ch chan base.Message) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
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

func ListenPutReply(key string, value string, client_ch chan base.Message, c *config.Config) {
	select {
	case ack := <-client_ch: // reply received in time
		if ack.Key != key {
			panic("wrong key!")
		}

		fmt.Println("Value stored: ", value, " with key: ", key)

	case <-time.After(time.Duration(c.CLIENT_PUT_TIMEOUT_MS) * time.Millisecond): // timeout reached
		fmt.Println("Put Timeout reached")
	}
}

func SetConfigs(c *config.Config, reader *bufio.Reader) {
	fmt.Println("Start System Configuration")

	prompts := []struct {
		config_type  string
		message      string
		setter       func(int)
		defaultValue int
	}{
		{"NUM_NODES", fmt.Sprintf("Set number of physical nodes (default: %d): ", config.NUM_NODES), func(val int) { c.NUM_NODES = val }, config.NUM_NODES},
		{"NUM_TOKENS", fmt.Sprintf("Set number of tokens (default: %d): ", config.NUM_TOKENS), func(val int) { c.NUM_TOKENS = val }, config.NUM_TOKENS},
		{"GET_TIMEOUT", fmt.Sprintf("Set number of CLIENT_GET_TIMEOUT in milliseconds (default: %d): ", config.CLIENT_GET_TIMEOUT_MS), func(val int) { c.CLIENT_GET_TIMEOUT_MS = val }, config.CLIENT_GET_TIMEOUT_MS},
		{"PUT_TIMEOUT", fmt.Sprintf("Set number of CLIENT_PUT_TIMEOUT in milliseconds (default: %d): ", config.CLIENT_PUT_TIMEOUT_MS), func(val int) { c.CLIENT_PUT_TIMEOUT_MS = val }, config.CLIENT_PUT_TIMEOUT_MS},
		{"DATA_TIMEOUT", fmt.Sprintf("Set number of SET_DATA_TIMEOUT in ms (default: %d): ", config.SET_DATA_TIMEOUT_MS), func(val int) { c.SET_DATA_TIMEOUT_MS = val }, config.SET_DATA_TIMEOUT_MS},
		{"N", fmt.Sprintf("Set number of N (default: %d): ", config.N), func(val int) { c.N = val }, config.N},
		{"R", fmt.Sprintf("Set number of R (default: %d): ", config.R), func(val int) { c.R = val }, config.R},
		{"W", fmt.Sprintf("Set number of W (default: %d): ", config.W), func(val int) { c.W = val }, config.W},
		{"DEBUG_LEVEL", fmt.Sprintf("Set debug level (default: %d): ", config.DEBUG_LEVEL), func(val int) { c.DEBUG_LEVEL = val }, config.DEBUG_LEVEL},
	}

	for _, prompt := range prompts {
		for {
			fmt.Print(prompt.message)
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)

			// Empty input uses the default value
			if input == "" {
				prompt.setter(prompt.defaultValue)
				// fmt.Printf("set to default: %d.\n\n", prompt.defaultValue)
				break
			}

			// Convert the string input to int
			value, err := strconv.Atoi(input)

			if err == nil && value > 0 {
				if prompt.config_type == "N" && value > c.NUM_NODES {
					fmt.Printf("WARNING: You are trying to set a %s value of %d which is larger than NUM_NODES. Please enter a value where %s ≤ NUM_NODES.\n",
						prompt.config_type, value, prompt.config_type)
					continue
				}
				if (prompt.config_type == "R" || prompt.config_type == "W") && value > c.N {
					fmt.Printf("WARNING: You are trying to set a %s value of %d which is larger than N. Please enter a value where %s ≤ N.\n",
						prompt.config_type, value, prompt.config_type)
					continue
				}
				prompt.setter(value)
				break
			}

			fmt.Println("Invalid input. Please enter a positive number.")
		}
	}

	fmt.Println("Configuration complete!")
	printConfig(c)
	fmt.Println("Starting system...")
}

func printConfig(c *config.Config) {
	fmt.Println("----------------------------------------")
	fmt.Printf("Physical nodes: %d.\n\n", c.NUM_NODES)
	fmt.Printf("Tokens: %d.\n\n", c.NUM_TOKENS)
	fmt.Printf("CLIENT_GET_TIMEOUT_MS: %d.\n\n", c.CLIENT_GET_TIMEOUT_MS)
	fmt.Printf("CLIENT_PUT_TIMEOUT_MS: %d.\n\n", c.CLIENT_PUT_TIMEOUT_MS)
	fmt.Printf("SET_DATA_TIMEOUT_MS: %d.\n\n", c.SET_DATA_TIMEOUT_MS)
	fmt.Printf("N: %d, R: %d, W: %d\n\n", c.N, c.R, c.W)
	fmt.Println("----------------------------------------")
}

func main() {
	fmt.Println("Starting the application...")
	reader := bufio.NewReader(os.Stdin)

	c := config.InstantiateConfig()
	SetConfigs(&c, reader)

	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, &c)
	base.InitializeTokens(phy_nodes, &c)

	//run nodes
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, &c)
	}

	for {
		fmt.Print("\nEnter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input) // Remove trailing newline

		//user inputs get()
		if strings.HasPrefix(input, "get(") && strings.HasSuffix(input, ")") {
			key, err := ParseOneArg(input, "get")
			if err != nil {
				fmt.Println(err)
				continue
			}

			node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: constants.REQ_READ}

			ListenGetReply(key, client_ch, &c)

		} else if strings.HasPrefix(input, "put(") && strings.HasSuffix(input, ")") {
			key, value, err := ParseTwoArgs(input, "put")
			if err != nil {
				fmt.Println(err)
				continue
			}

			node := base.FindNode(key, phy_nodes, &c)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: constants.REQ_WRITE, Data: value}

			ListenPutReply(key, value, client_ch, &c)

		} else if strings.HasPrefix(input, "kill(") && strings.HasSuffix(input, ")") {
			nodeIdxString, duration, err := ParseTwoArgs(input, "kill")
			if err != nil {
				fmt.Println(err)
				continue
			}

			nodeIdx, _ := strconv.Atoi(nodeIdxString)
			node := phy_nodes[nodeIdx]
			channel := (*node).GetChannel()
			channel <- base.Message{Command: constants.REQ_KILL, Data: duration}

		} else if strings.HasPrefix(input, "revive(") && strings.HasSuffix(input, ")") {
			nodeIdxString, err := ParseOneArg(input, "revive")
			if err != nil {
				fmt.Println(err)
				continue
			}

			nodeIdx, _ := strconv.Atoi(nodeIdxString)
			node := phy_nodes[nodeIdx]
			channel := (*node).GetChannel()
			channel <- base.Message{Command: constants.REQ_REVIVE}

		} else if input == "exit" {
			close(close_ch)
			break
		} else if strings.HasPrefix(input, "status") {
			fmt.Println("====== STATUS ======")
			for _, node := range phy_nodes {
				tokens := []int{}
				for _, token := range node.GetTokens() {
					tokens = append(tokens, token.GetID())
				}
				fmt.Printf("[Node %d] | Token(s): %v\n", node.GetID(), tokens)
				fmt.Println("> DATA")
				for key, value := range node.GetAllData() {
					fmt.Printf("	[%s] %s\n", key, value.ToString())
				}
				fmt.Println("> BACKUPS")
				for nodeId, value := range node.GetAllBackup() {
					fmt.Printf("Backup for node %d", nodeId)
					for key, valueObj := range value {
						fmt.Printf("	[%s] %s\n", key, valueObj.ToString())
					}
				}
				fmt.Println("===============")
			}
		} else {
			fmt.Println("Invalid input. Expected get(string), put(string, string) or exit")
		}
	}
	wg.Wait()
	fmt.Println("exiting program...")

}
