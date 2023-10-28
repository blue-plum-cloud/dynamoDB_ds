package main

import (
	"base"
	"bufio"
	"config"
	"errors"
	"fmt"
	"os"
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

func ListenGetReply(key string, client_ch chan base.Message) {
	select {
		case value := <- client_ch: // reply received in time
			if value.Key != key {
				panic("wrong key!")
			}

			//i'm sure there's a better way here
			if value.Data != "" {
				fmt.Println("value is: ", value.Data)
			} else {
				fmt.Println("data not found!")
			}
		
		case <- time.After(config.CLIENT_GET_TIMEOUT_MS * time.Millisecond): // timeout reached
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

func ListenPutReply(key string, value string, client_ch chan base.Message) {
	select {
		case ack := <- client_ch: // reply received in time
			if ack.Key != key {
				panic("wrong key!")
			}

			fmt.Println("Value stored: ", value, " with key: ", key)
		
		case <- time.After(config.CLIENT_PUT_TIMEOUT_MS * time.Millisecond): // timeout reached
			fmt.Println("Put Timeout reached")
	}
}

func main() {
	fmt.Println("Starting the application...")
	reader := bufio.NewReader(os.Stdin)

	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, config.NUM_NODES)
	base.InitializeTokens(phy_nodes, config.NUM_TOKENS)

	//run nodes
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg)
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

			ListenGetReply(key, client_ch)

		} else if strings.HasPrefix(input, "put(") && strings.HasSuffix(input, ")") {
			key, value, err := ParsePutCommand(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			node := base.FindNode(key, phy_nodes)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: config.REQ_WRITE, Data: value}

			ListenPutReply(key, value, client_ch)

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
