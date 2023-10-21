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
)

func main() {
	fmt.Println("Starting the application...")
	close_ch := make(chan struct{})

	phy_nodes := base.CreateNodes(close_ch, numNodes)
	base.InitializeTokens(phy_nodes, numTokens)

	reader := bufio.NewReader(os.Stdin)

	//run nodes
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg)
	}

	for {
		fmt.Print("Enter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input) // Remove trailing newline

		if strings.HasPrefix(input, "get(") && strings.HasSuffix(input, ")") {
			key := strings.TrimSuffix(strings.TrimPrefix(input, "get("), ")")
			fmt.Println("Key is: ", key)

		} else if strings.HasPrefix(input, "put(") && strings.HasSuffix(input, ")") {
			remainder := strings.TrimSuffix(strings.TrimPrefix(input, "put("), ")")
			parts := strings.SplitN(remainder, ",", 2)
			if len(parts) != 2 {
				fmt.Println("Invalid input")
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
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
