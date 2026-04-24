"""
HTTP health server for Docker/Kubernetes liveness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import asyncio
import json
import logging

from sqlalchemy import text

from discord_bot.utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)

class HealthServer:
    """
    Lightweight HTTP health endpoint.

    Responds 200 {"status": "ok"} when the bot is ready and not closed,
    503 {"status": "unavailable"} otherwise.

    If db_engine is provided, also runs a SELECT 1 ping against the database.
    A DB failure returns 503 with {"status": "unavailable", "db": "unavailable"}.
    """

    def __init__(self, bot, port=8080, db_engine=None):
        self.bot = bot
        self.port = port
        self._db_engine = db_engine

    async def _db_ping(self):
        """Return True if the database responds to SELECT 1."""
        try:
            async with self._db_engine.connect() as conn:
                await conn.execute(text('SELECT 1'))
            return True
        except Exception:
            return False

    async def serve(self):
        """Asyncio coroutine — schedule with asyncio.create_task()."""
        server = await asyncio.start_server(self._handle, '0.0.0.0', self.port)
        logger.info(f'Health server listening on port {self.port}')
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

            bot_ok = self.bot.is_ready() and not self.bot.is_closed()

            if self._db_engine is not None:
                db_ok = await self._db_ping()
            else:
                db_ok = True

            if bot_ok and db_ok:
                status_line = b'HTTP/1.1 200 OK\r\n'
                payload = {'status': 'ok'}
            else:
                status_line = b'HTTP/1.1 503 Service Unavailable\r\n'
                payload = {'status': 'unavailable'}

            if self._db_engine is not None:
                payload['db'] = 'ok' if db_ok else 'unavailable'

            body = json.dumps(payload).encode()
            headers = (
                b'Content-Type: application/json\r\n'
                + b'Content-Length: ' + str(len(body)).encode() + b'\r\n'
                + b'Connection: close\r\n'
                + b'\r\n'
            )
            writer.write(status_line + headers + body)
            await writer.drain()
        except Exception as e:
            logger.debug(f'Health server handler error: {e}')
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


class DispatchHealthServer:
    """
    Lightweight HTTP health endpoint for the dispatcher process.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise.
    """

    def __init__(self, redis_url: str, port: int = 8080):
        self._redis_url = redis_url
        self.port = port
        self._client = None

    async def serve(self):
        """Asyncio coroutine — schedule with asyncio.create_task()."""
        self._client = get_redis_client(self._redis_url)
        try:
            server = await asyncio.start_server(self._handle, '0.0.0.0', self.port)
            logger.info(f'Redis health server listening on port {self.port}')
            async with server:
                await server.serve_forever()
        finally:
            await self._client.aclose()

    async def _handle(self, reader, writer):
        """Handle a single HTTP request and write the response."""
        try:
            await reader.readline()
            while True:
                line = await reader.readline()
                if line in (b'\r\n', b'\n', b''):
                    break

            try:
                await self._client.ping()
                status_line = b'HTTP/1.1 200 OK\r\n'
                body = json.dumps({'status': 'ok'}).encode()
            except Exception:
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
            logger.debug(f'Redis health server handler error: {e}')
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
