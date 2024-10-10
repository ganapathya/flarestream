import threading
import random
import replication
from leader_election import LeaderElection

class Broker:
    brokers = []# List to keep track of all broker instances

    @classmethod
    def add_broker(cls, broker):
        cls.brokers.append(broker)
        print(f"[System] Broker added with ID {len(cls.brokers)}")

    def __init__(self):
        self.topics = {}
        self.lock = threading.Lock()
        self.replicator = None  # Initialize with no replicator until followers are assigned
        self.alive = True  # Broker starts as alive
        Broker.add_broker(self)

    def is_alive(self):
        return self.alive

    # Method to create a topic with a specified number of partitions and replication factor.
    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        with self.lock:
            if topic_name not in self.topics:
                followers = self._get_follower_brokers(replication_factor)
                self.topics[topic_name] = self._initialize_topic(num_partitions, replication_factor)
                self.replicator = replication.Replicator(self, followers)  # Create replicator
                print(f"[Broker {Broker.brokers.index(self) + 1}] Created topic: {topic_name} with {num_partitions} partitions and replication factor {replication_factor}")

                # Register topic with follower brokers
                for follower in followers:
                    follower.topics[topic_name] = {
                        'partitions': [[] for _ in range(num_partitions)],
                        'replication_factor': replication_factor,
                        'leaders': [self],
                        'followers': []
                    }
                    print(f"[Broker {Broker.brokers.index(follower) + 1}] Registered topic: {topic_name} as a follower.")
            else:
                print(f"[Broker {Broker.brokers.index(self) + 1}] Topic {topic_name} already exists.")

    # Helper method to initialize topic details
    def _initialize_topic(self, num_partitions, replication_factor):
        available_brokers = [broker for broker in Broker.brokers if broker != self]
        followers = random.sample(available_brokers, min(replication_factor - 1, len(available_brokers)))
        return {
            'partitions': [[] for _ in range(num_partitions)],
            'replication_factor': replication_factor,
            'leaders': [self],
            'followers': followers
        }

    # Helper method to get follower brokers based on replication factor
    def _get_follower_brokers(self, replication_factor):
        available_brokers = [broker for broker in Broker.brokers if broker != self]
        return random.sample(available_brokers, min(replication_factor - 1, len(available_brokers)))

    # Method to publish a message to a specific topic and partition.
    def publish(self, topic_name, message, partition=None):
        with self.lock:
            if topic_name in self.topics:
                self._handle_publish(topic_name, message, partition)
            else:
                print(f"[Broker {Broker.brokers.index(self) + 1}] Topic {topic_name} does not exist.")

    # Helper method to handle publishing logic
    def _handle_publish(self, topic_name, message, partition):
        topic_info = self.topics[topic_name]
        if partition is None:
            partition = len(topic_info['partitions'][0]) % len(topic_info['partitions'])
        if 0 <= partition < len(topic_info['partitions']):
            leader = topic_info['leaders'][0]  # Assuming only one leader per partition
            if not leader.is_alive():
                # Elect a new leader if the current leader is not alive
                leader_election = LeaderElection(Broker.brokers)
                new_leader = leader_election.elect_leader(topic_name)
                if new_leader:
                    topic_info['leaders'][0] = new_leader
                    leader = new_leader
                else:
                    print(f"[Broker {Broker.brokers.index(self) + 1}] No available leader for topic '{topic_name}'. Aborting publish.")
                    return

            topic_info['partitions'][partition].append((message, leader))
            print(f"[Broker {Broker.brokers.index(self) + 1}] Published message to {topic_name}, partition {partition} (Leader Broker {Broker.brokers.index(leader) + 1}): {message}")
            # Use replicator to replicate the message to followers
            if self.replicator:
                self.replicator.replicate_to_followers(topic_name, partition, message)
        else:
            print(f"[Broker {Broker.brokers.index(self) + 1}] Invalid partition {partition} for topic {topic_name}.")

    # Method for a consumer to consume a message from a specific topic and partition.
    def consume(self, topic_name, partition, offset=0):
        with self.lock:
            if topic_name in self.topics:
                return self._handle_consume(topic_name, partition, offset)
            else:
                print(f"[Broker {Broker.brokers.index(self) + 1}] Topic {topic_name} does not exist.")
                return None

    # Helper method to handle consuming logic
    def _handle_consume(self, topic_name, partition, offset):
        topic_info = self.topics[topic_name]
        if 0 <= partition < len(topic_info['partitions']):
            if offset < len(topic_info['partitions'][partition]):
                message, _ = topic_info['partitions'][partition][offset]
                print(f"[Broker {Broker.brokers.index(self) + 1}] Consumed message from {topic_name}, partition {partition}: {message}")
                return message
            else:
                print(f"[Broker {Broker.brokers.index(self) + 1}] No new messages in {topic_name}, partition {partition} at offset {offset}.")
                return None
        else:
            print(f"[Broker {Broker.brokers.index(self) + 1}] Invalid partition {partition} for topic {topic_name}.")
            return None