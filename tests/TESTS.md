# Tests
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