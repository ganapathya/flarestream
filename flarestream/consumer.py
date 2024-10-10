class Consumer:
    def __init__(self, broker, topic_name, partition):
        self.broker = broker
        self.topic_name = topic_name
        self.partition = partition
        self.offset = 0

    # Method to consume a message from the broker
    def consume(self):
        message = self.broker.consume(self.topic_name, self.partition, self.offset)
        if message is not None:
            self.offset += 1
            print(f"[Consumer] Consumed message: {message}")
        else:
            print(f"[Consumer] No new message available in topic '{self.topic_name}' at partition {self.partition} and offset {self.offset}.")