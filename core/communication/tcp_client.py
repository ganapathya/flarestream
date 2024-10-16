import asyncio

class TCPClient:
    def __init__(self, host='127.0.0.1', port=8888):
        self.host = host
        self.port = port

    async def send_message(self, message):
        reader, writer = await asyncio.open_connection(self.host, self.port)
        print(f"[TCPClient] Connected to server {self.host}:{self.port}")

        print(f"[TCPClient] Sending: {message}")
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(100)
        print(f"[TCPClient] Received: {data.decode()}")

        print(f"[TCPClient] Closing connection")
        writer.close()
        await writer.wait_closed()