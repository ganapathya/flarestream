import json

class MessageProtocol:
    @staticmethod
    def serialize_message(message_type, topic_name, partition, payload, offset=None):
        # Create a dictionary representing the message
        message = {
            "type": message_type,
            "topic_name": topic_name,
            "partition": partition,
            "payload": payload
        }
        # Add offset if it's part of the message
        if offset is not None:
            message["offset"] = offset
        # Convert dictionary to JSON string
        return json.dumps(message)

    @staticmethod
    def deserialize_message(message_str):
        # Convert JSON string back to a dictionary
        return json.loads(message_str)

