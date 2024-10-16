import asyncio
import json
from collections import defaultdict
from core.communication.tcp_server import TCPServer
from core.communication.tcp_client import TCPClient
from core.protocol.message_protocol import MessageProtocol

class Broker:
    def __init__(self, broker_id, metadata_manager, host='127.0.0.1', port=None):
        self.broker_id = broker_id
        self.host = host
        self.port = port if port else 9000 + broker_id  # Assign unique port per broker
        self.metadata_manager = metadata_manager  # MetadataManagerNode instance
        self.topics = defaultdict(lambda: defaultdict(list))  # Topics -> Partitions -> Messages
        self.server = TCPServer(self.host, self.port)
        self.lock = asyncio.Lock()

    async def start_broker(self):
        # Start the TCP server to listen for incoming requests
        asyncio.create_task(self.server.start_server(self.handle_client))
        print(f"[Broker {self.broker_id}] Broker started and listening on {self.host}:{self.port}")

    async def handle_client(self, reader, writer):
        data = await reader.read(1024)
        message = data.decode()
        #request = json.loads(message)
        # Use MessageProtocol to deserialize the incoming message
        request = MessageProtocol.deserialize_message(message)


        request_type = request.get('type')
        topic_name = request.get('topic_name')
        partition = request.get('partition')
        payload = request.get('payload')

        if request_type == 'publish':
            await self.publish_message(topic_name, partition, payload)
            response = {"status": "success", "message": f"Message published to {topic_name}, partition {partition}"}
        elif request_type == 'consume':
            offset = request.get('offset', 0)
            message = await self.consume_message(topic_name, partition, offset)
            response = {"status": "success", "message": message} if message else {"status": "failure", "message": "No message available"}
        elif request_type == 'replicate':
            # Handle replication request
            await self.publish_message(topic_name, partition, payload)
            response = {"status": "success", "message": f"Message replicated to {topic_name}, partition {partition}"}
        else:
            response = {"status": "failure", "message": "Unknown request type"}

        writer.write(json.dumps(response).encode())
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def publish_message(self, topic_name, partition, message):
        async with self.lock:
            # Publish message to the specified topic and partition
            if topic_name in self.topics and partition in self.topics[topic_name]:
                self.topics[topic_name][partition].append(message)
                print(f"[Broker {self.broker_id}] Published message to {topic_name}, partition {partition}")
            else:
                print(f"[Broker {self.broker_id}] Topic {topic_name} or partition {partition} does not exist")

    async def consume_message(self, topic_name, partition, offset=0):
        async with self.lock:
            # Consume message from the specified topic and partition at the given offset
            if topic_name in self.topics and partition in self.topics[topic_name]:
                if offset < len(self.topics[topic_name][partition]):
                    message = self.topics[topic_name][partition][offset]
                    print(f"[Broker {self.broker_id}] Consumed message from {topic_name}, partition {partition}, offset {offset}")
                    return message
                else:
                    print(f"[Broker {self.broker_id}] No message at offset {offset} for {topic_name}, partition {partition}")
                    return None
            else:
                print(f"[Broker {self.broker_id}] Topic {topic_name} or partition {partition} does not exist")
                return None

    async def create_topic(self, topic_name, num_partitions=1):
        async with self.lock:
            # Create topic with the given number of partitions
            if topic_name not in self.topics:
                for partition in range(num_partitions):
                    self.topics[topic_name][partition] = []
                print(f"[Broker {self.broker_id}] Created topic '{topic_name}' with {num_partitions} partitions")
            else:
                print(f"[Broker {self.broker_id}] Topic '{topic_name}' already exists")

