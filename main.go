package main

import (
	"base"
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

var wg sync.WaitGroup

const (
	numNodes  = 5
	numTokens = 5

	REQ_READ  = 0
	REQ_WRITE = 1
)

func main() {
	fmt.Println("Starting the application...")
	reader := bufio.NewReader(os.Stdin)

	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message)

	//node and token initialization
	phy_nodes := base.CreateNodes(client_ch, close_ch, numNodes)
	base.InitializeTokens(phy_nodes, numTokens)

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
			key := strings.TrimSuffix(strings.TrimPrefix(input, "get("), ")")
			parts := strings.SplitN(key, ",", 2)
			if len(parts) != 1 {
				fmt.Println("Invalid input")
				continue
			}
			node := base.FindNode(key, phy_nodes)
			// node := phy_nodes[3]

			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: REQ_READ}

			//wait for node to reply, might want to set timeout here in case node dies
			value := <-client_ch
			if value.Key != key {
				panic("wrong key!")
			}

			//i'm sure there's a better way here
			if value.Data != "" {
				fmt.Println("value is: ", value.Data)
			} else {
				fmt.Println("data not found!")
			}

		} else if strings.HasPrefix(input, "put(") && strings.HasSuffix(input, ")") {
			remainder := strings.TrimSuffix(strings.TrimPrefix(input, "put("), ")")
			parts := strings.SplitN(remainder, ",", 2)
			if len(parts) != 2 {
				fmt.Println("Invalid input")
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			node := base.FindNode(key, phy_nodes)
			channel := (*node).GetChannel()
			channel <- base.Message{Key: key, Command: REQ_WRITE, Data: value}

			//wait for node to reply, might want to set timeout here in case node dies
			ack := <-client_ch
			if ack.Key != key {
				panic("wrong key!")
			}

			fmt.Println("Value stored: ", value, " with key: ", key)

		} else if input == "exit" {
			close(close_ch)
			break

		} else {
			fmt.Println("Invalid input")
		}
	}
	wg.Wait()
	fmt.Println("exiting program...")

}
