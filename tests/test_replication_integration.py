import unittest
import asyncio
from core.replication.replication_manager import ReplicationManager
from core.broker.broker import Broker
from core.metadata.metadata_manager import MetadataManagerNode

class TestReplicationIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Create metadata manager and two broker instances (leader and follower)
        self.metadata_manager = MetadataManagerNode(node_id=1)
        self.leader_broker = Broker(broker_id=1, metadata_manager=self.metadata_manager, port=9001)
        self.follower_broker = Broker(broker_id=2, metadata_manager=self.metadata_manager, port=9002)
        
        await self.leader_broker.create_topic("test_topic", num_partitions=1)
        
        # Start both broker servers
        asyncio.create_task(self.leader_broker.start_broker())
        asyncio.create_task(self.follower_broker.start_broker())
        await asyncio.sleep(1)  # Allow servers to start

        # Create a replication manager for the leader broker
        self.replication_manager = ReplicationManager(
            broker=self.leader_broker, 
            follower_brokers=[('127.0.0.1', 9002)]
        )

    async def test_replicate_message(self):
        # Replicate a message to the follower broker
        await self.replication_manager.replicate_message("test_topic", 0, "Replicated Message")

        # Wait briefly to allow the message to replicate
        await asyncio.sleep(1)

        # Verify that the message is present in the follower broker's storage
        follower_messages = self.follower_broker.topics["test_topic"][0]
        self.assertIn("Replicated Message", follower_messages)

if __name__ == "__main__":
    unittest.main()
