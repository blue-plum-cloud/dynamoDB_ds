package main

import (
	"base"
	"config"
	"constants"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func generateRandomKeyValuePairs(maxKeyLength int, maxValueLength, n int) map[string]string {
	keyValueMap := map[string]string{}
	// Populate key value pairs
	for i := 0; i < n; i++ {
		for {
			key := generateRandomString(maxKeyLength)
			// if key does not exist, add to keySet and break
			_, err := keyValueMap[key]
			if !err {
				keyValueMap[key] = generateRandomString(maxValueLength)
				break
			}
		}
	}

	return keyValueMap
}

// generateRandomString generates a random string (min 1 char) with maximum
// length defined by maxN
func generateRandomString(maxN int) string {
	letterRunes := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	n := 1 + rand.Intn(maxN)

	res := make([]rune, n)
	for i := range res {
		res[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(res)
}

func setUpNodes(c *config.Config) ([]*base.Node, chan struct{}, chan base.Message) {

	var wg sync.WaitGroup
	//create close_ch for goroutines
	close_ch := make(chan struct{})
	client_ch := make(chan base.Message, 100)

	//node and token initialization
	phy_nodes := base.CreateNodes(close_ch, c)
	base.InitializeTokens(phy_nodes, c)
	fmt.Println("Setup nodes completed..")
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, c)
	}

	return phy_nodes, close_ch, client_ch
}

func main() {
	//CURRENT EXPERIMENT ONLY DOES PUT WITH VARYING NUMBER OF MESSAGES
	c := config.InstantiateConfig()
	//ALL OF THIS CAN BE CHANGED
	c.NUM_NODES = 10
	c.NUM_TOKENS = 10
	c.N = 5
	c.W = 3
	c.CLIENT_PUT_TIMEOUT_MS = 100_000
	c.DEBUG_LEVEL = 1
	NUM_MSG := 500

	phy_nodes, close_ch, client_ch := setUpNodes(&c)
	keyValuePairs := generateRandomKeyValuePairs(80, 100, NUM_MSG)

	//here is a put function
	for key, value := range keyValuePairs {
		hashedKey := base.ComputeMD5(key)
		fmt.Printf("Key: %s ; Value: %s ; Hashed Key: %s", key, value, hashedKey)

		_, node := base.FindNode(key, phy_nodes, &c)
		channel := (*node).GetChannel()
		channel <- base.Message{Key: key, Command: constants.CLIENT_REQ_WRITE, Data: value, Client_Ch: client_ch} //pass write to system via client channel

	}

	rcv_count := 0
Loop: // label to break out of
	for {
		select {
		case <-client_ch: // receives replies from the system that it has succeeded the put/get
			rcv_count++
			if rcv_count == NUM_MSG {
				break Loop // This breaks out of the for loop, not just the select
			}
		}
	}
	startTime := time.Now()
	jobId := 0
	for key, _ := range keyValuePairs {
		// hashedKey := base.ComputeMD5(key)
		// fmt.Printf("Key: %s ; Value: %s ; Hashed Key: %s", key, value, hashedKey)

		_, node := base.FindNode(key, phy_nodes, &c)
		channel := (*node).GetChannel()
		channel <- base.Message{JobId: jobId, Key: key, Command: constants.CLIENT_REQ_READ, Client_Ch: client_ch} //pass write to system via client channel
		jobId++
	}

	rcv_count = 0
Loop2: // label to break out of
	for {
		select {
		case <-client_ch: // receives replies from the system that it has succeeded the put/get
			rcv_count++
			if rcv_count >= NUM_MSG {
				break Loop2 // This breaks out of the for loop, not just the select
			}
		}
	}

	// Stop the timer
	endTime := time.Now()

	// Calculate the duration
	duration := endTime.Sub(startTime)

	fmt.Println("Time elapsed:", duration)
	close(close_ch)
}
