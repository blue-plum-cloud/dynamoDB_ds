package main

import (
	"base"
	"bufio"
	"config"
	"constants"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var wg sync.WaitGroup

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
				if prompt.config_type == "N" && (prompt.defaultValue > c.NUM_NODES) {
					fmt.Printf("WARNING: You are trying to set a %s value of %d which is larger than NUM_NODES. Please enter a value where %s ≤ NUM_NODES.\n",
						prompt.config_type, prompt.defaultValue, prompt.config_type)
					continue
				}
				if (prompt.config_type == "R" || prompt.config_type == "W") && (prompt.defaultValue > c.NUM_NODES) {
					fmt.Printf("WARNING: You are trying to set a %s value of %d which is larger than N. Please enter a value where %s ≤ N.\n",
						prompt.config_type, prompt.defaultValue, prompt.config_type)
					continue
				}
				prompt.setter(prompt.defaultValue)
				// fmt.Printf("set to default: %d.\n\n", prompt.defaultValue)
				break
			}

			// Convert the string input to int
			value, err := strconv.Atoi(input)

			if err == nil && value > 0 {
				if prompt.config_type == "N" && (value > c.NUM_NODES) {
					fmt.Printf("WARNING: You are trying to set a %s value of %d which is larger than NUM_NODES. Please enter a value where %s ≤ NUM_NODES.\n",
						prompt.config_type, value, prompt.config_type)
					continue
				}
				if (prompt.config_type == "R" || prompt.config_type == "W") && (value > c.N) {
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

func printStatus(phy_nodes []*base.Node) {
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
		fmt.Println("> Preference List")
		for key, tokens := range node.GetPrefList() {
			var ids []int
			for _, token := range tokens {
				ids = append(ids, token.Token.GetPID())
			}
			fmt.Printf("Token id = %d, pid = %d, %v, len() = %d\n", key.GetID(), key.GetPID(), ids, len(ids))

		}
		fmt.Println("===============")
	}
}

// checker to ensure multiple commands only allow put() or get()
func checkCommands(rawCommands []string, putRegex string, getRegex string) ([]string, bool) {
	allCommands := make([]string, 0)
	for i, cmd := range rawCommands {
		if i == len(rawCommands)-1 {
			break
		}
		cmd = strings.TrimSpace(cmd)
		matchedPut, _ := regexp.MatchString(putRegex, cmd)
		matchedGet, _ := regexp.MatchString(getRegex, cmd)
		if !matchedPut && !matchedGet {
			fmt.Println("Command chain should only consist of put() or get() commands!")
			return []string{}, false
		}
		allCommands = append(allCommands, cmd)

	}
	return allCommands, true
}

func generateClient(clients map[int]*base.Client, client_id int, close_ch chan struct{}, c *config.Config) {
	clients[client_id] = &base.Client{
		Id:        client_id,
		Close:     close_ch,
		Client_ch: make(chan base.Message),
		AwaitUids: make(map[int]*atomic.Bool)}
	go clients[client_id].StartListening(c)

	fmt.Printf("create client %d \n", client_id)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	fmt.Println("Starting the application...")
	reader := bufio.NewReader(os.Stdin)

	c := config.InstantiateConfig()
	SetConfigs(&c, reader)

	//create close_ch for goroutines
	close_ch := make(chan struct{})
	// client_ch := make(chan base.Message)
	// awaitUids := make(map[int](*atomic.Bool))
	clients := make(map[int](*base.Client))

	//node and token initialization
	phy_nodes := base.CreateNodes(close_ch, &c)
	base.InitializeTokens(phy_nodes, &c)

	// running jobId
	jobId := 0

	//run nodes
	for i := range phy_nodes {
		wg.Add(1)
		go phy_nodes[i].Start(&wg, &c)
	}

	//need to do this for every new client
	// go base.StartListening(close_ch, client_ch, awaitUids, &c)

	for {
		fmt.Print("\nEnter command: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input) // Remove trailing newline

		rawCommands := strings.Split(input, ";")
		// fmt.Println(rawCommands[0])

		// Regular expressions to match the commands
		putRegex := `^put\(([^,]+),([^)]+)\) (\d+)`
		getRegex := `^get\(([^)]+)\) (\d+)`
		killRegex := `kill\((\d+),\s?(\d+)\)`
		revRegex := `revive\((\d+)\)`

		//consider single input
		if len(rawCommands) == 1 {
			if input == "exit" {
				close(close_ch)
				break
			} else if input == "status" {
				printStatus(phy_nodes)
			} else if matched, _ := regexp.MatchString(putRegex, input); matched {
				//put
				key, value, client_id, err := base.ParsePutArg(putRegex, input)
				if err != nil {
					fmt.Println(err)
					continue
				}
				client, exists := clients[client_id]

				if !exists {
					generateClient(clients, client_id, close_ch, &c)
				}

				client = clients[client_id]
				token, node := base.FindNode(key, phy_nodes, &c)
				channel := (*node).GetChannel()
				newJob := jobId

				succ := false
				cnt := 1
				for !succ {
					channel <- base.Message{
						JobId:     newJob,
						Key:       key,
						Command:   constants.ALIVE_ACK,
						Data:      value,
						SrcID:     client_id,
						Client_Ch: client.Client_ch}
					succ = client.StartTimeout(newJob, constants.ALIVE_ACK, c.CLIENT_GET_TIMEOUT_MS)
					if succ {
						channel <- base.Message{
							JobId:     newJob,
							Key:       key,
							Command:   constants.CLIENT_REQ_WRITE,
							Data:      value,
							SrcID:     client_id,
							Client_Ch: client.Client_ch}
						client.StartTimeout(newJob, constants.CLIENT_REQ_WRITE, c.CLIENT_GET_TIMEOUT_MS)
					} else {
						new_node := base.FindPrefList(token, phy_nodes, cnt)
						if new_node == nil {
							fmt.Println("System currently busy, try again later :D")
							return
						}
						channel = (*new_node).GetChannel()
						fmt.Println("Looking for node handler...")
						cnt++
					}
					jobId++
					newJob = jobId
				}

			} else if matched, _ := regexp.MatchString(getRegex, input); matched {
				//get
				key, client_id, err := base.ParseGetArg(getRegex, input)
				if err != nil {
					fmt.Println(err)
					continue
				}
				client, exists := clients[client_id]

				if !exists {
					generateClient(clients, client_id, close_ch, &c)
				}

				client = clients[client_id]

				_, node := base.FindNode(key, phy_nodes, &c)
				channel := (*node).GetChannel()
				channel <- base.Message{
					JobId:     jobId,
					Key:       key,
					Command:   constants.CLIENT_REQ_READ,
					SrcID:     client_id,
					Client_Ch: client.Client_ch}
				client.StartTimeout(jobId, constants.CLIENT_REQ_READ, c.CLIENT_GET_TIMEOUT_MS)

			} else if matched, _ := regexp.MatchString(killRegex, input); matched {
				nodeIdx, duration, err := base.ParseKillArg(killRegex, input)
				if err != nil {
					fmt.Println(err)
					continue
				}

				node := phy_nodes[nodeIdx]
				channel := (*node).GetChannel()
				channel <- base.Message{JobId: jobId, Command: constants.CLIENT_REQ_KILL, Data: duration, SrcID: -1}
			} else if matched, _ := regexp.MatchString(revRegex, input); matched {
				nodeIdx, err := base.ParseRevArg(revRegex, input)
				if err != nil {
					fmt.Println(err)
					continue
				}

				node := phy_nodes[nodeIdx]
				channel := (*node).GetChannel()
				channel <- base.Message{JobId: jobId, Command: constants.CLIENT_REQ_REVIVE, SrcID: -1}
			} else {
				fmt.Println("Invalid input. Expected get(string) int;, put(string, string) int;, kill(int,int);, revive(int);, or exit;")
			}
			jobId++
		} else {
			cmds, is_correct := checkCommands(rawCommands, putRegex, getRegex)
			if is_correct {
				for _, input := range cmds {
					if matched, _ := regexp.MatchString(putRegex, input); matched {
						//put
						key, value, client_id, err := base.ParsePutArg(putRegex, input)
						if err != nil {
							fmt.Println(err)
							continue
						}
						client, exists := clients[client_id]

						if !exists {
							generateClient(clients, client_id, close_ch, &c)
						}

						client = clients[client_id]
						token, node := base.FindNode(key, phy_nodes, &c)
						channel := (*node).GetChannel()
						newJob := jobId

						succ := false
						cnt := 1
						for !succ {
							channel <- base.Message{
								JobId:     newJob,
								Key:       key,
								Command:   constants.ALIVE_ACK,
								Data:      value,
								SrcID:     client_id,
								Client_Ch: client.Client_ch}
							succ = client.StartTimeout(newJob, constants.ALIVE_ACK, c.CLIENT_GET_TIMEOUT_MS)
							if succ {
								channel <- base.Message{
									JobId:     newJob,
									Key:       key,
									Command:   constants.CLIENT_REQ_WRITE,
									Data:      value,
									SrcID:     client_id,
									Client_Ch: client.Client_ch}
								client.StartTimeout(newJob, constants.CLIENT_REQ_WRITE, c.CLIENT_GET_TIMEOUT_MS)
							} else {
								new_node := base.FindPrefList(token, phy_nodes, cnt)
								if new_node == nil {
									fmt.Println("System currently busy, try again later :D")
									return
								}
								channel = (*new_node).GetChannel()
								fmt.Println("Looking for node handler...")
								cnt++
							}
							jobId++
							newJob = jobId
						}
					} else if matched, _ := regexp.MatchString(getRegex, input); matched {
						//get
						key, client_id, err := base.ParseGetArg(getRegex, input)
						if err != nil {
							fmt.Println(err)
							continue
						}
						client, exists := clients[client_id]

						if !exists {
							generateClient(clients, client_id, close_ch, &c)
						}

						client = clients[client_id]

						_, node := base.FindNode(key, phy_nodes, &c)
						channel := (*node).GetChannel()
						channel <- base.Message{
							JobId:     jobId,
							Key:       key,
							Command:   constants.CLIENT_REQ_READ,
							SrcID:     client_id,
							Client_Ch: client.Client_ch}
						fmt.Println("sending get to node ", node.GetID())
						client.StartTimeout(jobId, constants.CLIENT_REQ_READ, c.CLIENT_GET_TIMEOUT_MS)

					}
					jobId++
				}
			}
		}

	}
	wg.Wait()
	fmt.Println("exiting program...")

}
