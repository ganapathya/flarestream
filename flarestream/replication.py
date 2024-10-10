import threading

class Replicator:
    def __init__(self, leader_broker, followers):
        self.leader_broker = leader_broker
        self.followers = followers
        self.lock = threading.Lock()

    # Method to replicate a message to follower brokers
    def replicate_to_followers(self, topic_name, partition, message):
        with self.lock:
            for follower in self.followers:
                follower_topic = follower.topics.get(topic_name)
                if follower_topic:
                    follower_topic['partitions'][partition].append((message, self.leader_broker))
                    print(f"[Replicator] Replicated message to follower Broker {follower} for topic {topic_name}, partition {partition}: {message}")
                else:
                    print(f"[Replicator] Topic {topic_name} does not exist on follower Broker {follower}.")