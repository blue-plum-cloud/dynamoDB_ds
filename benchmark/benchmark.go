package main

import (
	"base"
	"config"
	"constants"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"

	// "sync/atomic"
	"time"
)

// /////////////////  Helper funcs  ///////////////////////////
func setupClients(numClients int, close_ch chan struct{}, c *config.Config) map[int](*base.Client) {
	clients := make(map[int](*base.Client))

	for j := 0; j < numClients; j++ {
		clients[j] = &base.Client{
			Id:        j,
			Close:     close_ch,
			Client_ch: make(chan base.Message),
			AwaitUids: make(map[int]*atomic.Bool)}
		go clients[j].StartListening(c)
	}

	return clients
}

func generateOrderedKeys(keyValuePairs map[string]string) []string {
	keys := make([]string, 0, len(keyValuePairs))
	for key := range keyValuePairs {
		keys = append(keys, key)
	}
	return keys

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

// generateRandomKeyValuePairs will generate n key-value pairs
// where key length is 1 - maxKeyLength and value is of length
// 1 - maxValueLength
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

///////////////////  Benchmark funcs  ///////////////////////////

func BenchmarkSingleClientGet() {
	fmt.Println("~~ Running benchmark: Single client Get to Many ~~ ")
	// var tests = []struct {
	// 	numClients, numNodes, numTokens, nValue, rAndWValue int
	// }{
	// 	{1, 1, 1, 1, 1},

	// 	{1, 5, 5, 1, 1},
	// 	{1, 5, 10, 3, 2},

	// 	{1, 30, 30, 15, 10},
	// 	{1, 30, 40, 15, 10},
	// 	{1, 30, 20, 15, 10},
	// }

	numClients := 1
	numNodes := 20
	numTokens := 30
	nValue := 1
	rAndWValue := 10

	startTime := time.Now()

	fmt.Printf("clientNum: %d   |   nodeNum: %d   |   tokenNum: %d   |   nValue: %d   |   R_and_W_Value: %d\n", numClients, numNodes, numTokens, nValue, rAndWValue)

	c := config.InstantiateConfig()
	c.NUM_NODES = numNodes
	c.NUM_TOKENS = numTokens
	c.N = nValue
	c.R = rAndWValue
	c.W = rAndWValue
	// c.DEBUG_LEVEL = 1
	c.CLIENT_GET_TIMEOUT_MS = 5_000
	close_ch := make(chan struct{})
	key := "k"
	value := "val"

	//create 1 client
	clients := setupClients(1, close_ch, &c)

	phy_nodes := base.CreateNodes(close_ch, &c)
	base.InitializeTokens(phy_nodes, &c)
	// defer close(close_ch)

	var wg sync.WaitGroup
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, &c)
	}

	//NOTE: if this time.Sleep is excluded, data may not be fully replicated before the read
	time.Sleep(time.Millisecond)

	client := clients[0]

	_, node := base.FindNode(key, phy_nodes, &c)
	channel := (*node).GetChannel()

	channel <- base.Message{
		JobId:     0,
		Key:       key,
		Command:   constants.CLIENT_REQ_WRITE,
		Data:      value,
		SrcID:     client.Id,
		Client_Ch: client.Client_ch}

	client.StartTimeout(0, constants.CLIENT_REQ_WRITE, c.CLIENT_GET_TIMEOUT_MS)

	channel <- base.Message{
		JobId:     1,
		Key:       key,
		Command:   constants.CLIENT_REQ_READ,
		SrcID:     client.Id,
		Client_Ch: client.Client_ch}
	client.StartTimeout(1, constants.CLIENT_REQ_READ, c.CLIENT_GET_TIMEOUT_MS)

	close(close_ch)
	wg.Wait()
	//read only after execution is done since client's StartListening function is a goroutine
	if client.NewestRead != value {
		fmt.Printf("got: %s, expected: %s", client.NewestRead, value)

	}

	duration := time.Since(startTime)

	fmt.Printf("----------------------\n")
	fmt.Printf("|                     |\n")
	fmt.Printf("|                     |\n")
	fmt.Printf("|      %v    |\n", duration)
	fmt.Printf("|                     |\n")
	fmt.Printf("|                     |\n")
	fmt.Printf("----------------------")
}

func BenchmarkMultipleClientMultiplePutMultipleGet() {

	//adjust here for diff variables
	numClients := 300
	numNodes := 30
	numTokens := 60
	nValue := 10
	rAndWValue := 10

	// var tests = []struct {
	// 	numClients, numNodes, numTokens, nValue, rAndWValue int
	// }{
	// 	{2, 1, 1, 1, 1},

	// 	{10, 5, 5, 1, 1},
	// 	{10, 5, 10, 3, 2},

	// 	{20, 30, 30, 15, 10},
	// 	{40, 30, 40, 15, 10},
	// 	{20, 30, 20, 15, 10},
	// }

	// testname := fmt.Sprintf("%d_clients_%d_nodes_%d_tokens_%d_n_%d_rAndW", numClients, numNodes, numTokens, nValue, rAndWValue)

	startTime := time.Now()

	fmt.Printf("clientNum: %d   |   nodeNum: %d   |   tokenNum: %d   |   nValue: %d   |   R_and_W_Value: %d\n", numClients, numNodes, numTokens, nValue, rAndWValue)
	fmt.Println("Start test for numclients: ", numClients)
	keyValuePairs := generateRandomKeyValuePairs(20, 100, numClients)
	//key-value pairs are not in order
	keys := generateOrderedKeys(keyValuePairs)
	c := config.InstantiateConfig()
	c.NUM_NODES = numNodes
	c.NUM_TOKENS = numTokens
	c.N = nValue
	c.R = rAndWValue
	c.W = rAndWValue
	c.DEBUG_LEVEL = 1

	c.CLIENT_GET_TIMEOUT_MS = 5_000
	// c.DEBUG_LEVEL = 0
	jobId := 0

	close_ch := make(chan struct{})

	//create 1 client
	clients := setupClients(numClients, close_ch, &c)

	phy_nodes := base.CreateNodes(close_ch, &c)
	base.InitializeTokens(phy_nodes, &c)
	// defer close(close_ch)

	var wg sync.WaitGroup
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, &c)
	}

	//this put is sequential but technically it is how the client is working as well
	for i, key := range keys {
		value := keyValuePairs[key]
		client := clients[i]
		// fmt.Println("Client ", client.Id, "putting key ", key, " val: ", value)
		_, node := base.FindNode(key, phy_nodes, &c)
		channel := (*node).GetChannel()

		channel <- base.Message{
			JobId:     jobId,
			Key:       key,
			Command:   constants.CLIENT_REQ_WRITE,
			Data:      value,
			SrcID:     client.Id,
			Client_Ch: client.Client_ch}

		client.StartTimeout(jobId, constants.CLIENT_REQ_WRITE, c.CLIENT_GET_TIMEOUT_MS)
		jobId++
	}

	//NOTE: if this time.Sleep is excluded, data may not be fully replicated before the read
	// time.Sleep(time.Millisecond * time.Duration(numClients))

	for i, key := range keys {
		client := clients[i]
		// fmt.Println("Client ", client.Id, "getting key ", key, " val: ", keyValuePairs[key])
		_, node := base.FindNode(key, phy_nodes, &c)
		channel := (*node).GetChannel()
		channel <- base.Message{
			JobId:     jobId,
			Key:       key,
			Command:   constants.CLIENT_REQ_READ,
			SrcID:     client.Id,
			Client_Ch: client.Client_ch}
		client.StartTimeout(jobId, constants.CLIENT_REQ_READ, c.CLIENT_GET_TIMEOUT_MS)
		jobId++
	}
	close(close_ch)
	wg.Wait()
	//read only after execution is done since client's StartListening function is a goroutine
	for i, key := range keys {
		client := clients[i]
		if client.NewestRead != keyValuePairs[key] {
			fmt.Printf("got from client %d: %s, expected: %s", client.Id, client.NewestRead, keyValuePairs[key])
		}
	}

	duration := time.Since(startTime)

	fmt.Printf("----------------------\n")
	fmt.Printf("|                     |\n")
	fmt.Printf("|                     |\n")
	fmt.Printf("|      %v    |\n", duration)
	fmt.Printf("|                     |\n")
	fmt.Printf("|                     |\n")
	fmt.Printf("----------------------")
}

func main() {

	// BenchmarkSingleClientGet()
	BenchmarkMultipleClientMultiplePutMultipleGet()
}
