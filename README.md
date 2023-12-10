# dynamoDB_ds

Implementation of DynamoDB for 50.041 Distributed Systems and Computing

## Introduction
Our project attempts to implement selected aspects of DynamoDB, which is a distributed key-value datastore. As the scope of implementing the entirety of dynamoDB is too large given the timeline, we have scoped the project down to include the following features:
- Consistent hashing to partition physical nodes (nodes) into virtual nodes (tokens) 
- Random token distribution 
- Sloppy Quorum for reads and writes 
- Hinted Handoff to handle temporary failures 
- Preference list to handle temporary coordinator failures
- Multiple simultaneous requests from different clients

## System Overview

## Running the Program

### Starting up and configuring

To start the program, execute `go run main.go` from the project's root directory.

At the start, you will be prompted to set the configuration of DynamoDB. You can set the following values shown in the image below, or press the `Enter` key to use the defaults. Currently, the program does not allow you to set a larger value of R or W than N.

![Screenshot 2023-12-10 at 2 09 24 PM](https://github.com/blue-plum-cloud/dynamoDB_ds/assets/84310587/b7c45347-dd36-4b93-8e9c-2fff98742a3c)

Once the configuration is complete, the program will set up the physical nodes and allocate tokens (virtual nodes) according to the specifications set during configuration. DynamoDB is then ready for operation.

### Using DynamoDB via the CLI

DynamoDB allows for the following commands, `get` and `put`. The format for these commands are as follows:
- `get(key)`: Request to retrieve data from DynamoDB based on a `key` of type `string`. DynamoDB will return an acknowledgement to the client together with the stored `value` if the request is successful (DynamoDB is able to retrieve the stored value from at least `R` physical nodes). Otherwise, the client will time out.
- `put(key,value)`: Request to store data from DynamoDB based on a `key` of type `string` and a `value` of type `string`. DynamoDB will return an acknowledgement to the client if the value is stored and replicated successfully to at least `W` physical nodes. Otherwise, the client will time out.

The format for `get` and `put` to be entered to the CLI are as follows:
- `get`: `get(key) client_id` where `client_id` is a positive integer.
- `put`: `put(key,value) client_id` where `client_id` is a positive integer.

<img width="755" alt="Screenshot 2023-12-10 at 2 41 41 PM" src="https://github.com/blue-plum-cloud/dynamoDB_ds/assets/84310587/3827fcfa-90f4-4fb4-9a02-a4bf311afb35">

Any positive integer can be specified to be the `client_id`. The program will check if the client with that `client_id` exists, and retrieves the client's communication channel. If it does not exist, then the program will generate a new client goroutine and map it to the `client_id`.

Additionally, the CLI accepts additional commands:
- `wipe`: Wipes the memory of the environment by regenerating the same physical nodes specified in the configuration. The token allocation to physical nodes will not change.
- `status`: Visualizes the data, backups and preference list at each physical node (shown in the image below).
- `kill(node_id, duration)`: Instructs a physical node of id `node_id` to go down for `duration` milliseconds. It will not be able to respond to any requests while it is down.
- `revive(node_id)`: Instructs a physical node of id `node_id` to restart if it is down.


<img width="755" alt="Screenshot 2023-12-10 at 2 35 22 PM" src="https://github.com/blue-plum-cloud/dynamoDB_ds/assets/84310587/091ea9d8-cf2f-4614-9049-20c6c864b10f">


The CLI also accepts multiple `get` and/or `put` commands on the same line:

`<COMMAND_1> <CLIENT_ID1>; <COMMAND_2> <CLIENT_ID2>; ... ; <COMMAND_N> <CLIENT_IDN>;`

Multiple commands can be assigned to the same client. The commands will be parsed from left to right. Consequentially, the commands will also be executed in sequence from left to right.

<img width="755" alt="Screenshot 2023-12-10 at 2 44 54 PM" src="https://github.com/blue-plum-cloud/dynamoDB_ds/assets/84310587/f60e440b-d79d-4364-a660-6e5982e66f25">

## Tests

Use command `go test ./tests` to run tests. For verbose output, use flag `-v`.

To run single test, use flag `-run <testname>`

To clear test cache, use `go clean -testcache`

Detailed description about the tests can be found at [`./tests/TESTS.md`](./tests/TESTS.md)
