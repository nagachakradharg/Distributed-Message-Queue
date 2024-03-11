### Message Queue:

- We are using a dictionary, where the keys are the topics and the value of a key is a list containing the messages.
- The communication is handled using `FLASK` with operations such as PUT for adding topics and messages and 
GET for retrieving topics and consuming messages.
- The REST services takes in a JSON as an input and outputs a JSON with the status and other actions of the request.

Below are the discription of each endpoint:

- `PUT Topic` : this operation is used to create a new key in the queue data structure (dict) with an empty list as it's value.

- `GET Topic`: this operation retrieves all the available topics currently in the queue.

- `PUT Message`: this operation is used to add a message to list under a particular topic.

- `GET Message`: this operation pops the first element in the list under a particular topic and returns it.

- `GET Status`: this operation is used for testing purposes and returns the STATE of the node 

### Election:

- When first starting the swarm (the collection of servers in different port), if there is only one server, it is the leader. If there are more servers, the server with the smallest port number becomes the first leader.
- When multiple instances are deployed, the election is used to determine the leader and the followers.
- The `config.json` file containing all the existing ports, is shared between all the instances.
- A timer with an election timeout duration between 150 - 300ms is present in every instance. Which ever node timeout first will increment it's term and becomes a candidate and initiates the election process using the `/election` endpoint.
- As long as the term of the candidate is greater than the follower and the most recent log and log length matches,
the follower votes for the candidate.
- The candidate becomes the leader as long as it has the majority votes and the followers will save the leader's port
within themselves.
- After the election process, the leader constantly sends heartbeat using the `/internal-message` endpoint to inform 
the other nodes that it's active, which also subsequently resets the followers election time-out to prevent from
restarting a new election.

`PUT` `/election` : the operation is used to respond to a candidate's election request.

`PUT` `/internal-message` : this operation is used for every other communication between the nodes such as heartbeat, log, executing a command, etc,..

### Fault Tolerance:

- The algorithm ensures that when a leader dies, the first follower to hit the election time-out becomes a candidate and will initiate the election process to select a new leader as described in the election part. A follower votes to the first candiate that send the electin request and increases it's term to the candidate's term (this to ensure that each follower can only vote for only one candidate in a election term). And a candiate becomes a leader only when at least half the total number of instances plus itself agrees to the election, otherwise, another election with increased term begins.
- When a client sends a request in a node that is not a leader, then that node replies to client that it's not the leader and it also sends the port of the leader to the client.
- When a client sends request to the leader, the leader first saves the command to it's log and asks the followers to add the command to their log as well. And when at least more than half returns success, the leader executes the command and also informs the followers to execute the command.
- If a node dies and restarts again later, and the log is found to be inconsistent with the leader's, then the `ensure` consensus part of the code loops back to the first index where the difference began and updates the log till the most recent index. The commands in the logs are also concurrently executed.
- Since all the active nodes have the same log (which is ensured by the ensure_consensus function), any follower can become a leader in the future in case of network delays, power issues or any other causes and as long as at least more than half of the total nodes are active, the fault tolerance of the Distributed Message Queue maintains the proper functioning of the system.

### Sources:

- Raft Paper
- raft.github.io
