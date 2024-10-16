import asyncio

class TCPServer:
    def __init__(self, host='127.0.0.1', port=8888):
        self.host = host
        self.port = port

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"[TCPServer] Accepted connection from {addr}")

        while True:
            data = await reader.read(100)
            if not data:
                print(f"[TCPServer] Connection closed by {addr}")
                break

            message = data.decode()
            print(f"[TCPServer] Received {message} from {addr}")

            response = f"[TCPServer] Echo: {message}"
            writer.write(response.encode())
            await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def start_server(self, handle_client):
        # Start the TCP server and pass the client handler function
        server = await asyncio.start_server(handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f"[TCPServer] Serving on {addr}")

        async with server:
            await server.serve_forever()
