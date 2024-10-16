import asyncio
import random

class RAFT:
    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.nodes = nodes  # List of all nodes in the RAFT cluster
        self.current_term = 0
        self.voted_for = None
        self.state = 'follower'  # Can be 'follower', 'candidate', or 'leader'
        self.votes_received = 0
        self.leader_id = None
        self.lock = asyncio.Lock()
        self.election_timeout = random.uniform(5, 10)  # Election timeout between 5 to 10 seconds
        self.heartbeat_interval = 2  # Interval for sending heartbeats by leader

    async def start(self):
        asyncio.create_task(self.run_election_timer())

    async def run_election_timer(self):
        while True:
            await asyncio.sleep(self.election_timeout)
            if self.state != 'leader':
                await self.start_election()

    async def start_election(self):
        async with self.lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1  # Vote for self
            print(f"[RAFT {self.node_id}] Starting election for term {self.current_term}")

            # Send request vote to all other nodes
            await asyncio.gather(*(self.request_vote(node) for node in self.nodes if node.node_id != self.node_id))

            # Check if received majority votes
            if self.votes_received > len(self.nodes) // 2:
                await self.become_leader()

    async def request_vote(self, node):
        # Simulate requesting vote from a node
        await asyncio.sleep(random.uniform(0.1, 0.5))  # Simulate network delay
        if node.grant_vote(self.current_term, self.node_id):
            async with self.lock:
                self.votes_received += 1
                print(f"[RAFT {self.node_id}] Received vote from {node.node_id}")

    def grant_vote(self, term, candidate_id):
        # Grant vote if the candidate's term is at least as large as the current term
        if term >= self.current_term and (self.voted_for is None or self.voted_for == candidate_id):
            self.current_term = term
            self.voted_for = candidate_id
            return True
        return False

    async def become_leader(self):
        async with self.lock:
            self.state = 'leader'
            self.leader_id = self.node_id
            print(f"[RAFT {self.node_id}] Became leader for term {self.current_term}")
            # Start sending heartbeats to all other nodes
            asyncio.create_task(self.send_heartbeats())

    async def send_heartbeats(self):
        while self.state == 'leader':
            await asyncio.sleep(self.heartbeat_interval)
            print(f"[RAFT {self.node_id}] Sending heartbeats to followers")
            await asyncio.gather(*(self.send_heartbeat(node) for node in self.nodes if node.node_id != self.node_id))

    async def send_heartbeat(self, node):
        # Simulate sending heartbeat to a follower node
        await asyncio.sleep(random.uniform(0.1, 0.5))  # Simulate network delay
        node.receive_heartbeat(self.current_term, self.node_id)

    def receive_heartbeat(self, term, leader_id):
        # Update term and leader information if the term is valid
        if term >= self.current_term:
            self.current_term = term
            self.leader_id = leader_id
            self.state = 'follower'
            print(f"[RAFT {self.node_id}] Received heartbeat from leader {leader_id}")