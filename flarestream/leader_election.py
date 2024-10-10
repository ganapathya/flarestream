import threading
import random

class LeaderElection:
    def __init__(self, brokers):
        self.brokers = brokers
        self.lock = threading.Lock()

    # Method to elect a new leader broker in case of a failure
    def elect_leader(self, topic_name):
        with self.lock:
            available_brokers = [broker for broker in self.brokers if broker.is_alive()]
            if not available_brokers:
                print(f"[LeaderElection] No brokers available to elect a leader for topic '{topic_name}'!")
                return None

            new_leader = random.choice(available_brokers)
            print(f"[LeaderElection] Broker {self.brokers.index(new_leader) + 1} elected as the new leader for topic '{topic_name}'.")
            return new_leader

    # Method to simulate broker failure
    def simulate_broker_failure(self, broker):
        broker.alive = False
        print(f"[LeaderElection] Broker {self.brokers.index(broker) + 1} has failed.")

    # Method to simulate broker recovery
    def simulate_broker_recovery(self, broker):
        broker.alive = True
        print(f"[LeaderElection] Broker {self.brokers.index(broker) + 1} has recovered.")