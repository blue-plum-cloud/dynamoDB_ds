package tests

import (
	"base"
	"config"
	"constants"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestSingleClientGet(t *testing.T) {
	c := config.InstantiateConfig()
	c.DEBUG_LEVEL = 1
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

	client := clients[0]

	node := base.FindNode(key, phy_nodes, &c)
	channel := (*node).GetChannel()

	channel <- base.Message{
		JobId:     0,
		Key:       key,
		Command:   constants.CLIENT_REQ_WRITE,
		Data:      value,
		SrcID:     client.Id,
		Client_Ch: client.Client_ch}

	client.StartTimeout(0, constants.CLIENT_REQ_WRITE, c.CLIENT_GET_TIMEOUT_MS)

	//NOTE: if this time.Sleep is excluded, data may not be fully replicated before the read is done
	time.Sleep(time.Millisecond)

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
		t.Errorf("got: %s, expected: %s", client.NewestRead, value)
	}

}

func TestMultipleClientMultiplePutSingleGet(t *testing.T) {
	keyValuePairs := generateRandomKeyValuePairs(20, 100, 3)
	c := config.InstantiateConfig()
	c.DEBUG_LEVEL = 1
	count := 0
	get_key := ""

	close_ch := make(chan struct{})

	//create 1 client
	clients := setupClients(3, close_ch, &c)

	phy_nodes := base.CreateNodes(close_ch, &c)
	base.InitializeTokens(phy_nodes, &c)
	// defer close(close_ch)

	var wg sync.WaitGroup
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, &c)
	}

	//this put is sequential but technically it is how the client is working as well
	for key, value := range keyValuePairs {
		client := clients[count]
		if count == 1 {
			get_key = key
		}
		node := base.FindNode(key, phy_nodes, &c)
		channel := (*node).GetChannel()

		channel <- base.Message{
			JobId:     0,
			Key:       key,
			Command:   constants.CLIENT_REQ_WRITE,
			Data:      value,
			SrcID:     client.Id,
			Client_Ch: client.Client_ch}

		client.StartTimeout(0, constants.CLIENT_REQ_WRITE, c.CLIENT_GET_TIMEOUT_MS)
		count++
	}

	//NOTE: if this time.Sleep is excluded, data may not be fully replicated before the read
	time.Sleep(time.Millisecond)

	client := clients[0]
	node := base.FindNode(get_key, phy_nodes, &c)
	channel := (*node).GetChannel()
	channel <- base.Message{
		JobId:     1,
		Key:       get_key,
		Command:   constants.CLIENT_REQ_READ,
		SrcID:     client.Id,
		Client_Ch: client.Client_ch}
	client.StartTimeout(1, constants.CLIENT_REQ_READ, c.CLIENT_GET_TIMEOUT_MS)

	close(close_ch)
	wg.Wait()
	//read only after execution is done since client's StartListening function is a goroutine
	if client.NewestRead != keyValuePairs[get_key] {
		t.Errorf("got: %s, expected: %s", client.NewestRead, keyValuePairs[get_key])
	}
}

// each of the clients will write something concurrently, then they will read concurrently from what they wrote
func TestMultipleClientMultiplePutMultipleGet(t *testing.T) {

	var tests = []struct {
		numClients int
	}{
		{2},
		{3},
		{5},
		{8},
	}
	for _, tt := range tests {
		fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
		fmt.Println("Start test for numclients: ", tt.numClients)
		keyValuePairs := generateRandomKeyValuePairs(20, 100, tt.numClients)
		//key-value pairs are not in order
		keys := generateOrderedKeys(keyValuePairs)
		c := config.InstantiateConfig()
		c.DEBUG_LEVEL = 1
		jobId := 0

		close_ch := make(chan struct{})

		//create 1 client
		clients := setupClients(tt.numClients, close_ch, &c)

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
			fmt.Println("Client ", client.Id, "putting key ", key, " val: ", value)
			node := base.FindNode(key, phy_nodes, &c)
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
		time.Sleep(time.Millisecond * time.Duration(tt.numClients))

		for i, key := range keys {
			client := clients[i]
			fmt.Println("Client ", client.Id, "getting key ", key, " val: ", keyValuePairs[key])
			node := base.FindNode(key, phy_nodes, &c)
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
				t.Errorf("got from client %d: %s, expected: %s", client.Id, client.NewestRead, keyValuePairs[key])
			}
		}
	}
}
