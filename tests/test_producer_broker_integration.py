import unittest
import asyncio
from unittest.mock import patch
from core.client.client import Producer
from core.broker.broker import Broker
from core.metadata.metadata_manager import MetadataManagerNode

class TestProducerBrokerIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Create a metadata manager and broker instance
        self.metadata_manager = MetadataManagerNode(node_id=1)
        self.broker = Broker(broker_id=1, metadata_manager=self.metadata_manager, port=9001)
        await self.broker.create_topic("test_topic", num_partitions=1)

        # Start the broker server
        asyncio.create_task(self.broker.start_broker())
        await asyncio.sleep(1)  # Allow server to start

    async def test_producer_publish(self):
        # Create a producer instance and publish a message
        producer = Producer(broker_host='127.0.0.1', broker_port=9001)
        await producer.publish("test_topic", 0, "Hello, Broker!")

        # Verify the message was published successfully
        messages = self.broker.topics["test_topic"][0]
        self.assertIn("Hello, Broker!", messages)

if __name__ == "__main__":
    unittest.main()
