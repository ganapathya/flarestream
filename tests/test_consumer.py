import unittest
from unittest.mock import AsyncMock, patch
from core.client.client import Consumer

class TestConsumer(unittest.IsolatedAsyncioTestCase):
    @patch('core.client.client.TCPClient')
    async def test_consume(self, MockTCPClient):
        mock_client = MockTCPClient.return_value
        mock_client.send_message = AsyncMock()
        
        consumer = Consumer(broker_host='127.0.0.1', broker_port=9001)
        await consumer.consume("test_topic", 0, 0)

        mock_client.send_message.assert_awaited_once()

if __name__ == "__main__":
    unittest.main()
