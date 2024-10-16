import unittest
from core.protocol.message_protocol import MessageProtocol

class TestMessageProtocol(unittest.TestCase):
    def test_serialize_message(self):
        message = MessageProtocol.serialize_message("publish", "test_topic", 0, "Hello")
        expected = '{"type": "publish", "topic_name": "test_topic", "partition": 0, "payload": "Hello"}'
        self.assertEqual(message, expected)

    def test_deserialize_message(self):
        message_str = '{"type": "publish", "topic_name": "test_topic", "partition": 0, "payload": "Hello"}'
        message = MessageProtocol.deserialize_message(message_str)
        expected = {
            "type": "publish",
            "topic_name": "test_topic",
            "partition": 0,
            "payload": "Hello"
        }
        self.assertEqual(message, expected)

if __name__ == "__main__":
    unittest.main()
