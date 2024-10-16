import unittest
from unittest.mock import AsyncMock
from core.broker.broker import Broker
from core.metadata.metadata_manager import MetadataManagerNode

class TestBroker(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.metadata_manager = MetadataManagerNode(node_id=1)
        self.broker = Broker(broker_id=1, metadata_manager=self.metadata_manager, port=9001)
        await self.broker.create_topic("test_topic", num_partitions=1)

    async def test_publish_message(self):
        await self.broker.publish_message("test_topic", 0, "Test Message")
        messages = self.broker.topics["test_topic"][0]
        self.assertIn("Test Message", messages)

    async def test_consume_message(self):
        await self.broker.publish_message("test_topic", 0, "Test Message")
        message = await self.broker.consume_message("test_topic", 0, offset=0)
        self.assertEqual(message, "Test Message")

if __name__ == "__main__":
    unittest.main()
