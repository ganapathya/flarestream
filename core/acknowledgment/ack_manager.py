import asyncio

class AckManager:
    def __init__(self, replication_factor=2):
        self.replication_factor = replication_factor  # Number of acknowledgments required
        self.pending_acks = {}  # Stores pending acknowledgment information: (message_id -> set of acks)
        self.lock = asyncio.Lock()

    async def wait_for_acks(self, message_id):
        # Wait until the required number of acknowledgments are received
        async with self.lock:
            while len(self.pending_acks.get(message_id, set())) < self.replication_factor:
                await asyncio.sleep(0.1)  # Polling interval

        # Once the required number of acknowledgments is received, remove the message entry
        async with self.lock:
            if message_id in self.pending_acks:
                del self.pending_acks[message_id]

    async def add_ack(self, message_id, broker_id):
        # Add an acknowledgment from a broker for the given message
        async with self.lock:
            if message_id not in self.pending_acks:
                self.pending_acks[message_id] = set()
            self.pending_acks[message_id].add(broker_id)
            print(f"[AckManager] Received ack from broker {broker_id} for message {message_id}")

    async def register_message(self, message_id):
        # Register a new message that requires acknowledgment
        async with self.lock:
            self.pending_acks[message_id] = set()
            print(f"[AckManager] Registered message {message_id} for acknowledgment tracking")


