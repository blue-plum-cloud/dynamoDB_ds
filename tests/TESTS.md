# Tests
## Unit tests
- Node unit tests

## Other tests
The tests written can be categorised into the following:
- Initilisation tests
- No replication tests (get/put)
- Replication tests
- Sloppy quorum tests
- Hinted handoff tests

## Initilisation tests
I1. Ensure that tokens are allocated correctly to the nodes
- Tokens == Nodes
- Tokens > Nodes
- Tokens < Nodes
- Nodes == 0
- Tokens == 0
- Nodes < 0
- Tokens < 0

## No replication tests (get/put)
NR1. Ensure that get/put requests involving unique keys are correctly stored (no replication).
NR2. Ensure that get/put requests with many different updates are stored and updated correctly.

## Replication Tests
R1. Ensure that a single put request is replicated correctly. W == Expected replications == min(num_nodes, num_tokens, n)
- N < Nodes, Tokens == Nodes
- N == Nodes, Tokens == Nodes
- N > Nodes, Tokens == Nodes
- N > Nodes, Tokens > Nodes
- N > Nodes, Tokens < Nodes
- N < 0
- N == 0 

R2. Ensure that multiple unique put requests are replicated correctly
- Randomly generate key-value pairs
- PUT all key-value pairs
- Check if all key-value pairs have correct number of replications

R3. Ensure that updated put requests are replicated correctly
- Randomly generate key-value pairs
- PUT all key-value pairs
- Update value of key-value pairs
- PUT new values
- Check if all key-value pairs have correct number of replications

## Sloppy quorum tests
## Hinted handoff tests