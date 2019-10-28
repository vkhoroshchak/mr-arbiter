# ARB-1:swarm-mr-arbiter
This management node sends requests to data nodes and useful information to client, and also obtain the required data.

The tasks for management are as follows:
1. Receiving a requests from the client
2. Sending requests to free data nodes
3. Cyclically transfer information about data nodes and update the local base
4. Send requests to data nodes for operations map (), shuffle (), reduce ()
