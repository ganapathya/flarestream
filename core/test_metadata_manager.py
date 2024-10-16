import asyncio
from metadata.metadata_manager import MetadataManagerNode

async def main():
    # Step 1: Initialize MetadataManager Nodes
    node1 = MetadataManagerNode(node_id=1)
    node2 = MetadataManagerNode(node_id=2, nodes=[node1])
    node3 = MetadataManagerNode(node_id=3, nodes=[node1, node2])

    # Update nodes list to reflect full connectivity
    node1.nodes = [node2, node3]
    node2.nodes.append(node3)

    # Step 2: Start all nodes
    await asyncio.gather(
        node1.start_node(),
        node2.start_node(),
        node3.start_node()
    )

    # Step 3: Give time for leader election
    await asyncio.sleep(5)

    # Step 4: Create a topic using the leader
    if node1.is_leader:
        await node1.create_topic("test_topic", num_partitions=3)
    elif node2.is_leader:
        await node2.create_topic("test_topic", num_partitions=3)
    elif node3.is_leader:
        await node3.create_topic("test_topic", num_partitions=3)

    # Step 5: Simulate leader failure and observe failover
    print("\n--- Simulating Leader Failure ---\n")
    if node1.is_leader:
        await node1.step_down()
    elif node2.is_leader:
        await node2.step_down()
    elif node3.is_leader:
        await node3.step_down()

    # Allow time for new leader election
    await asyncio.sleep(10)

    # Step 6: Create another topic using the new leader
    if node1.is_leader:
        await node1.create_topic("new_topic", num_partitions=2)
    elif node2.is_leader:
        await node2.create_topic("new_topic", num_partitions=2)
    elif node3.is_leader:
        await node3.create_topic("new_topic", num_partitions=2)

# Run the main function
asyncio.run(main())
