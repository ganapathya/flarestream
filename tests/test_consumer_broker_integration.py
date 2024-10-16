import unittest
import asyncio
from unittest.mock import patch
from core.client.client import Consumer, Producer
from core.broker.broker import Broker
from core.metadata.metadata_manager import MetadataManagerNode

class TestConsumerBrokerIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Create a metadata manager and broker instance
        self.metadata_manager = MetadataManagerNode(node_id=1)
        self.broker = Broker(broker_id=1, metadata_manager=self.metadata_manager, port=9001)
        await self.broker.create_topic("test_topic", num_partitions=1)

        # Start the broker server
        asyncio.create_task(self.broker.start_broker())
        await asyncio.sleep(1)  # Allow server to start

        # Create a producer and publish a message for the consumer to consume
        producer = Producer(broker_host='127.0.0.1', broker_port=9001)
        await producer.publish("test_topic", 0, "Hello, Consumer!")

    @patch('core.communication.tcp_client.TCPClient.send_message')
    async def test_consumer_consume(self, mock_send_message):
        # Create a consumer instance
        consumer = Consumer(broker_host='127.0.0.1', broker_port=9001)
        await consumer.consume("test_topic", 0, 0)

        # Verify the message was consumed successfully
        mock_send_message.assert_awaited_once()

if __name__ == "__main__":
    unittest.main()
