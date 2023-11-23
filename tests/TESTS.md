# Tests
## Unit tests
- Node unit tests

## Other tests
The tests written can be categorised into the following:
- Initilisation tests
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

## Replication Tests
R1. Ensure that a single put request is replicated correctly. W == Expected replications == min(num_nodes, num_tokens, n)
- N < Nodes, Tokens == Nodes
- N == Nodes, Tokens == Nodes
- N > Nodes, Tokens == Nodes
- N > Nodes, Tokens > Nodes
- N > Nodes, Tokens < Nodes
  
R2. Ensure that writes will not be successful if N is zero or negative
- N < 0
- N == 0 

R3. Ensure that multiple unique put requests are replicated correctly
- Randomly generate key-value pairs
- PUT all key-value pairs
- Check if all key-value pairs have correct number of replications

R4. Ensure that updated put requests are replicated correctly
- Randomly generate key-value pairs
- PUT all key-value pairs
- Update value of key-value pairs
- PUT new values
- Check if all key-value pairs have correct number of replications

## Sloppy quorum tests
Q1. Ensure that sloppy quorum writes are successful with no nodes down
- W = 1
- W < N
- W == N

Q2. Ensure that system can still handle invalid W values (will calculate W limit and use limit)
- W > N

Q3. Ensure that sloppy quorum writes successful with f failures
- W < N-f
- W == N-f

Q4. Check that sloppy quorum fails with too many failures
- W > N-f

Q5. Ensure that sloppy quorum reads are successful
- R = 1
- R < N
- R == N

Q6. Ensure that invalid sloppy quorum read values do not result in successful read
- R > N

## Hinted handoff tests
## Client Tests