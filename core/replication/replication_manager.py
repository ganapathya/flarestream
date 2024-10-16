import asyncio
import json
from core.communication.tcp_client import TCPClient
from core.protocol.message_protocol import MessageProtocol


class ReplicationManager:
    def __init__(self, broker, follower_brokers):
        self.broker = broker  # The broker instance that this manager is attached to
        self.follower_brokers = follower_brokers  # List of follower broker addresses (host, port)

    async def replicate_message(self, topic_name, partition, message):
        # Use MessageProtocol to serialize the replication request
        serialized_message = MessageProtocol.serialize_message(
            message_type="replicate",
            topic_name=topic_name,
            partition=partition,
            payload=message
        )
        #tasks.append(client.send_message(serialized_message))


        # Send the replication request to each follower broker
        tasks = []
        for follower_host, follower_port in self.follower_brokers:
            client = TCPClient(follower_host, follower_port)
            tasks.append(client.send_message(serialized_message))

        # Wait for all replication requests to complete
        await asyncio.gather(*tasks)

    async def handle_replication_request(self, topic_name, partition, message):
        # Add the message to the local broker's topic/partition
        await self.broker.publish_message(topic_name, partition, message)
        print(f"[ReplicationManager] Replicated message to topic '{topic_name}', partition {partition}")


