import asyncio
import json
from core.communication.tcp_client import TCPClient
from core.protocol.message_protocol import MessageProtocol


class Producer:
    def __init__(self, broker_host='127.0.0.1', broker_port=9000):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = TCPClient(self.broker_host, self.broker_port)

    async def publish(self, topic_name, partition, message):
        # Prepare the publish request
        # Use MessageProtocol to serialize the publish request
        serialized_message = MessageProtocol.serialize_message(
            message_type="publish",
            topic_name=topic_name,
            partition=partition,
            payload=message
        )
        await self.client.send_message(serialized_message)


class Consumer:
    def __init__(self, broker_host='127.0.0.1', broker_port=9000):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = TCPClient(self.broker_host, self.broker_port)

    async def consume(self, topic_name, partition, offset=0):
        # Use MessageProtocol to serialize the consume request
        serialized_message = MessageProtocol.serialize_message(
            message_type="consume",
            topic_name=topic_name,
            partition=partition,
            payload=None,
            offset=offset
        )
        await self.client.send_message(serialized_message)



