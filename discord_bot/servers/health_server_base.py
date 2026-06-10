"""
Shared raw-asyncio HTTP framing for the bot and dispatcher health servers.

Kept separate from aiohttp-based servers (servers/base.py) — these endpoints
are minimal so they can run on the dispatcher's slim image without pulling
aiohttp into the request path. They're also intentionally free of sqlalchemy
imports so the dispatcher can import the base without that dependency.
"""
import asyncio
import json
import logging

logger = logging.getLogger(__name__)


class HealthServerBase:
    """
    Minimal raw-asyncio HTTP health endpoint.

    Subclasses implement ``_check()`` to return ``(ok: bool, extra: dict)``.
    The base handles request framing, status line, headers, and writer cleanup.
    """

    def __init__(self, port: int, bind_address: str):
        self.port = port
        self.bind_address = bind_address

    async def _check(self) -> tuple[bool, dict]:
        """Return (overall_ok, extra_payload_fields)."""
        raise NotImplementedError

    async def serve(self):
        """Asyncio coroutine — schedule with asyncio.create_task()."""
        server = await asyncio.start_server(self._handle, self.bind_address, self.port)
        logger.info(f'{type(self).__name__} listening on {self.bind_address}:{self.port}')
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

            ok, extra = await self._check()
            if ok:
                status_line = b'HTTP/1.1 200 OK\r\n'
                payload = {'status': 'ok'}
            else:
                status_line = b'HTTP/1.1 503 Service Unavailable\r\n'
                payload = {'status': 'unavailable'}
            payload.update(extra)

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
            logger.debug(f'{type(self).__name__} handler error: {e}')
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
