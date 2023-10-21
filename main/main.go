package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	fmt.Println("Starting the application...")
	reader := bufio.NewReader(os.Stdin)

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
			break
		} else {
			fmt.Println("Invalid input")
		}
	}
	fmt.Println("exiting program...")

}
