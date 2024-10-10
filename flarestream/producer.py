class Producer:
    def __init__(self, broker):
        self.broker = broker

    # Method to send a message to a specific topic and partition (optional)
    def send(self, topic_name, message, partition=None):
        self.broker.publish(topic_name, message, partition)
        print(f"[Producer] Sent message to topic '{topic_name}' with message: {message}")