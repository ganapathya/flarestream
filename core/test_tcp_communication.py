import asyncio
from communication.tcp_server import TCPServer
from communication.tcp_client import TCPClient

async def main():
    # Start the server in a background task
    server = TCPServer()
    asyncio.create_task(server.start_server())

    # Give the server a moment to start
    await asyncio.sleep(1)

    # Start the client to send a test message
    client = TCPClient()
    await client.send_message("Hello, Server!")

# Run the main async function
asyncio.run(main())
