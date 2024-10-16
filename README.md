# Flarestream

Flarestream is a distributed message broker inspired by Kafka. It is designed to provide scalable, fault-tolerant messaging with features such as replication, leader election, and eventual consistency. This project includes different layers to handle communication, metadata management, message replication, and client interaction, all aimed at providing a distributed messaging solution.

## Features

- **Producer & Consumer API**: Allows producers to publish messages and consumers to retrieve messages.
- **Replication**: Replication of messages across brokers to ensure high availability and fault tolerance.
- **Leader Election**: Dynamic leader election for partitions using a RAFT-like protocol.
- **TCP Communication**: Custom TCP communication layer for broker-client and broker-broker communication.
- **At-least-once Acknowledgments**: Ensures messages are replicated successfully to a configurable number of brokers before acknowledgment.

## Project Structure

```
flarestream/
  ├── core/
  │    ├── communication/      # TCP client and server modules
  │    ├── metadata/           # Metadata manager and leader election
  │    ├── broker/             # Broker class that manages topics, partitions, and messages
  │    ├── client_layer/       # Producer and Consumer classes
  │    ├── acknowledgment/     # Acknowledgment manager for message tracking
  │    ├── replication/        # Replication manager for broker-to-broker communication
  │    ├── message_protocol.py # Message serialization and deserialization
  └── tests/                   # Unit and integration tests
```

## Getting Started

### Prerequisites

- Python 3.8+
- Anaconda or Virtualenv for environment management

### Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/yourusername/flarestream.git
   cd flarestream
   ```

2. Set up a virtual environment:

   ```sh
   python -m venv venv
   source venv/bin/activate   # On Windows, use `venv\Scripts\activate`
   ```

3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

### Running the Project

1. **Start Brokers**:

   - You need to start multiple brokers to set up the distributed messaging system.
   - Run the following command to start a broker:
     ```sh
     python -m flarestream.core.broker.broker
     ```

2. **Producer and Consumer**:
   - Use the `Producer` and `Consumer` classes to interact with the broker.
   - Example for running a producer:
     ```sh
     python -m flarestream.core.client_layer.producer
     ```
   - Example for running a consumer:
     ```sh
     python -m flarestream.core.client_layer.consumer
     ```

### Running Tests

To run all unit and integration tests:

```sh
python -m unittest discover -s tests
```

To run a specific test:

```sh
python -m unittest tests/test_producer_broker_integration.py
```

## Usage

- **Publishing Messages**: Producers can publish messages to specific topics and partitions.
- **Consuming Messages**: Consumers can retrieve messages by specifying the topic and partition.
- **Replication**: Messages are automatically replicated to follower brokers for reliability.
- **Leader Election**: Metadata manager handles leader election in case of broker failure.

## Configuration

- **Replication Factor**: Configurable replication factor for each topic.
- **Ports**: Brokers run on configurable ports, starting from 9000 onwards.
- **Acknowledge Mechanism**: Requires a certain number of followers to acknowledge a message before it's confirmed.

## Troubleshooting

- **ModuleNotFoundError**: Ensure that `PYTHONPATH` is set correctly to the root directory of the project.
- **Integration Test Failures**: Make sure that all brokers are up and running before running integration tests.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Acknowledgments

- Inspired by **Apache Kafka** and **etcd**.
- Uses a RAFT-like protocol for metadata and leader election.
