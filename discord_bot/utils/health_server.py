"""
HTTP health server for Docker/Kubernetes liveness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import asyncio
import json
import logging


class HealthServer:
    """
    Lightweight HTTP health endpoint.

    Responds 200 {"status": "ok"} when the bot is ready and not closed,
    503 {"status": "unavailable"} otherwise.
    """

    def __init__(self, bot, port=8080):
        self.bot = bot
        self.logger = logging.getLogger('health_server')
        self.port = port

    async def serve(self):
        """Asyncio coroutine — schedule with asyncio.create_task()."""
        server = await asyncio.start_server(self._handle, '0.0.0.0', self.port)
        self.logger.info(f'Health server listening on port {self.port}')
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer):
        """Handle a single HTTP request and write the response."""
        try:
            # Read the request line (we don't need to inspect it)
            await reader.readline()
            # Drain remaining headers so the client doesn't get a RST
            while True:
                line = await reader.readline()
                if line in (b'\r\n', b'\n', b''):
                    break

            if self.bot.is_ready() and not self.bot.is_closed():
                status_line = b'HTTP/1.1 200 OK\r\n'
                body = json.dumps({'status': 'ok'}).encode()
            else:
                status_line = b'HTTP/1.1 503 Service Unavailable\r\n'
                body = json.dumps({'status': 'unavailable'}).encode()

            headers = (
                b'Content-Type: application/json\r\n'
                + b'Content-Length: ' + str(len(body)).encode() + b'\r\n'
                + b'Connection: close\r\n'
                + b'\r\n'
            )
            writer.write(status_line + headers + body)
            await writer.drain()
        except Exception as e:
            self.logger.debug(f'Health server handler error: {e}')
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
