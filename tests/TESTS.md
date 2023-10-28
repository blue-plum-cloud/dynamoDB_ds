# Tests
## Unit tests
- Node unit tests

## Other tests
The tests written can be categorised into the following:
- Initilisation tests
- Simple tests (get/put)
- Stress tests (get/put)
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

## Simple tests (get/put)
SIM1. Ensure that get/put requests involving unique keys are correctly stored.
SIM2. Ensure that get/put requests with one update are correctly stored and updated.

## Stress tests (get/put)
STR1. Ensure that get/put requests with many different updates are stored and updated correctly.

## Replication Tests
R1. Ensure that a single put request is replicated correctly
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