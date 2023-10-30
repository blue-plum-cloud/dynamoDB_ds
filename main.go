package main

import (
	"base"
	"bufio"
	"config"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

func ParseGetCommand(input string) (string, error) {
	key := strings.TrimSuffix(strings.TrimPrefix(input, "get("), ")")
	parts := strings.SplitN(key, ",", 2)
	if len(parts) != 1 {
		return "", errors.New("Invalid input for get. Expect get(string)")
	}
	return key, nil
}

func ListenGetReply(key string, client_ch chan base.Message, c *base.Config) {
	select {
	case value := <-client_ch: // reply received in time
		if value.Key != key {
			panic("wrong key!")
		}

		//i'm sure there's a better way here
		if value.Data != "" {
			fmt.Println("value is: ", value.Data)
		} else {
			fmt.Println("data not found!")
		}

	case <-time.After(time.Duration(c.CLIENT_GET_TIMEOUT_MS) * time.Millisecond): // timeout reached
		fmt.Println("Get Timeout reached")
	}
}

func ParsePutCommand(input string) (string, string, error) {
	remainder := strings.TrimSuffix(strings.TrimPrefix(input, "put("), ")")
	parts := strings.SplitN(remainder, ",", 2)
	if len(parts) != 2 {
		return "", "", errors.New("Invalid input for put. Expect put(string, string)")
	}
	key := strings.TrimSpace(parts[0])
	value := strings.TrimSpace(parts[1])
	return key, value, nil
}

func ListenPutReply(key string, value string, client_ch chan base.Message, c *base.Config) {
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

func ParseKillCommand(input string) (int, int, error) {
	remainder := strings.TrimSuffix(strings.TrimPrefix(input, "kill("), ")")
	parts := strings.SplitN(remainder, ",", 2)
	if len(parts) != 2 {
		return 0, 0, errors.New("Invalid input for kill. Expect kill(int, int)")
	}
	nodeIdx, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
	duration, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
	return nodeIdx, duration, nil
}

func SetConfigs(c *base.Config, reader *bufio.Reader) {
	fmt.Println("Start System Configuration")

	prompts := []struct {
		message      string
		setter       func(int)
		defaultValue int
	}{
		{fmt.Sprintf("Set number of physical nodes (default: %d): ", config.NUM_NODES), func(val int) { c.NUM_NODES = val }, config.NUM_NODES},
		{fmt.Sprintf("Set number of tokens (default: %d): ", config.NUM_TOKENS), func(val int) { c.NUM_TOKENS = val }, config.NUM_TOKENS},
		{fmt.Sprintf("Set number of CLIENT_GET_TIMEOUT in milliseconds (default: %d): ", config.CLIENT_GET_TIMEOUT_MS), func(val int) { c.CLIENT_GET_TIMEOUT_MS = val }, config.CLIENT_GET_TIMEOUT_MS},
		{fmt.Sprintf("Set number of CLIENT_PUT_TIMEOUT in milliseconds (default: %d): ", config.CLIENT_PUT_TIMEOUT_MS), func(val int) { c.CLIENT_PUT_TIMEOUT_MS = val }, config.CLIENT_PUT_TIMEOUT_MS},
		{fmt.Sprintf("Set number of SET_DATA_TIMEOUT in nanoseconds (default: %d): ", config.SET_DATA_TIMEOUT_NS), func(val int) { c.SET_DATA_TIMEOUT_NS = val }, config.SET_DATA_TIMEOUT_NS},
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
				prompt.setter(value)
				// fmt.Printf("set to %d.\n\n", value)
				break
			}

			fmt.Println("Invalid input. Please enter a positive number.")
		}
	}

	fmt.Println("Configuration complete!")
	printConfig(c)
	fmt.Println("Starting system...")
}

func printConfig(c *base.Config) {
	fmt.Println("----------------------------------------")
	fmt.Printf("Physical nodes: %d.\n\n", c.NUM_NODES)
	fmt.Printf("Tokens: %d.\n\n", c.NUM_TOKENS)
	fmt.Printf("CLIENT_GET_TIMEOUT_MS: %d.\n\n", c.CLIENT_GET_TIMEOUT_MS)
	fmt.Printf("CLIENT_PUT_TIMEOUT_MS: %d.\n\n", c.CLIENT_PUT_TIMEOUT_MS)
	fmt.Printf("SET_DATA_TIMEOUT_NS: %d.\n\n", c.SET_DATA_TIMEOUT_NS)
	fmt.Println("----------------------------------------")
}

func main() {
	fmt.Println("Starting the application...")
	reader := bufio.NewReader(os.Stdin)

	sys_config := base.Config{}
	SetConfigs(&sys_config, reader)

	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, sys_config.NUM_NODES)
	base.InitializeTokens(phy_nodes, sys_config.NUM_TOKENS)

	//run nodes
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, &sys_config)
	}

	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input) // Remove trailing newline

		//user inputs get()
		if strings.HasPrefix(input, "get(") && strings.HasSuffix(input, ")") {
			key, err := ParseGetCommand(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			node := base.FindNode(key, phy_nodes)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: config.REQ_READ}

			ListenGetReply(key, client_ch, &sys_config)

		} else if strings.HasPrefix(input, "put(") && strings.HasSuffix(input, ")") {
			key, value, err := ParsePutCommand(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			node := base.FindNode(key, phy_nodes)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: config.REQ_WRITE, Data: value}

			ListenPutReply(key, value, client_ch, &sys_config)

		} else if strings.HasPrefix(input, "kill(") && strings.HasSuffix(input, ")") {
			nodeIdx, duration, err := ParseKillCommand(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			newAliveSince := time.Now().Add(time.Millisecond * time.Duration(duration))
			phy_nodes[nodeIdx].SetAliveSince(newAliveSince)
			if config.DEBUG_LEVEL >= 1 {
				fmt.Printf("%v\n", time.Now())
				fmt.Printf("%v\n", phy_nodes[nodeIdx].GetAliveSince())
			}

		} else if input == "exit" {
			close(close_ch)
			break

		} else {
			fmt.Println("Invalid input. Expected get(string), put(string, string) or exit")
		}
	}
	wg.Wait()
	fmt.Println("exiting program...")

}
