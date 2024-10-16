import asyncio
from collections import defaultdict
from core.communication.tcp_server import TCPServer
from core.communication.tcp_client import TCPClient
from core.metadata.raft import RAFT

class MetadataManagerNode:
    def __init__(self, node_id, nodes=None, host='127.0.0.1', port=None):
        self.node_id = node_id
        self.host = host
        self.port = port if port else 8888 + node_id  # Assign a unique port per node
        self.nodes = nodes if nodes else []  # List of all nodes in the cluster
        self.topics = defaultdict(dict)  # Stores metadata about topics, partitions, leaders, followers
        self.server = TCPServer(self.host, self.port)
        self.clients = []  # List of TCPClient instances for communicating with other nodes
        self.lock = asyncio.Lock()
        self.is_leader = False  # Initially, no node is a leader
        self.raft = RAFT(node_id, self.nodes)  # RAFT instance for consensus

    async def start_node(self):
        # Start the TCP server to listen for incoming requests
        asyncio.create_task(self.server.start_server())
        print(f"[MetadataManagerNode {self.node_id}] Node started and listening on {self.host}:{self.port}")
        await self.raft.start()  # Start RAFT consensus

    async def create_topic(self, topic_name, num_partitions=1):
        # Function for leader to create a new topic and partition assignment
        async with self.lock:
            if not self.is_leader:
                print(f"[MetadataManagerNode {self.node_id}] Not the leader, cannot create topic.")
                return

            # Creating partitions for the topic
            self.topics[topic_name] = {
                partition: {
                    'leader': self.node_id,
                    'followers': []
                } for partition in range(num_partitions)
            }
            print(f"[MetadataManagerNode {self.node_id}] Created topic '{topic_name}' with {num_partitions} partitions")

    async def request_metadata_update(self, message, peer_host, peer_port):
        # Sends metadata updates to other nodes
        client = TCPClient(peer_host, peer_port)
        await client.send_message(message)

    async def handle_metadata_request(self, reader, writer):
        # Handles metadata requests coming from other nodes
        data = await reader.read(100)
        message = data.decode()
        print(f"[MetadataManagerNode {self.node_id}] Received metadata update: {message}")
        # Logic to update local metadata state can be added here

    def add_client(self, client_host, client_port):
        # Add a client instance to communicate with another MetadataManagerNode
        self.clients.append(TCPClient(client_host, client_port))

    async def become_leader(self):
        # Called when this node becomes the leader
        async with self.lock:
            self.is_leader = True
            print(f"[MetadataManagerNode {self.node_id}] Became the leader")

    async def step_down(self):
        # Called when this node is no longer the leader
        async with self.lock:
            self.is_leader = False
            print(f"[MetadataManagerNode {self.node_id}] Stepped down from leadership")

    def grant_vote(self, term, candidate_id):
        # Grant vote if the candidate's term is at least as large as the current term
        if term >= self.raft.current_term and (self.raft.voted_for is None or self.raft.voted_for == candidate_id):
            self.raft.current_term = term
            self.raft.voted_for = candidate_id
            print(f"[MetadataManagerNode {self.node_id}] Granted vote to {candidate_id} for term {term}")
            return True
        return False
