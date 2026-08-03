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

from discord_bot.utils.loop_health import LOOP_HEALTH

logger = logging.getLogger(__name__)


_READINESS_PATHS = (b'/ready', b'/readyz', b'/readiness')


async def close_writer(writer):
    """Close an asyncio writer, swallowing the OSError that wait_closed may raise."""
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass


class HealthServerBase:
    """
    Minimal raw-asyncio HTTP health endpoint.

    Subclasses implement ``_check()`` to return ``(ok: bool, extra: dict)`` for
    liveness. Override ``_readiness_check()`` to add stricter peer-dependency
    probes for the readiness endpoint; defaults to the liveness result.

    Routing: ``/ready``, ``/readyz``, and ``/readiness`` invoke
    ``_readiness_check()``. Any other path invokes ``_check()``.

    Background-loop health is folded in here, once, for every process that has a
    health server (bot, dispatcher, broker, downloader): a stalled loop fails the
    probe and is named in the payload. Doing it in the base rather than in each
    subclass is the point — the heartbeat gauge and the probes then report the
    same bit, so "the alert fired" and "the pod is unhealthy" can never disagree.
    """

    def __init__(self, port: int, bind_address: str):
        self.port = port
        self.bind_address = bind_address

    async def _check(self) -> tuple[bool, dict]:
        """Return (overall_ok, extra_payload_fields) for liveness."""
        raise NotImplementedError

    async def _readiness_check(self) -> tuple[bool, dict]:
        """Return (overall_ok, extra_payload_fields) for readiness; defaults to _check()."""
        return await self._check()

    @staticmethod
    def _apply_loop_health(ok: bool, extra: dict) -> tuple[bool, dict]:
        """
        Fold registered background-loop health into a probe result.

        A stalled loop (no successful iteration inside its staleness window)
        fails the probe; loops that stopped deliberately during shutdown do not.
        Processes with no registered loops are unaffected, and the ``loops`` key
        is omitted entirely rather than reported as an empty dict.
        """
        loops = LOOP_HEALTH.snapshot()
        if not loops:
            return ok, extra
        stalled = LOOP_HEALTH.stalled_names()
        if stalled:
            logger.warning('Health probe failing: stalled background loops: %s', ', '.join(stalled))
        return ok and not stalled, {**extra, 'loops': loops}

    async def serve(self):
        """Asyncio coroutine — schedule with asyncio.create_task()."""
        server = await asyncio.start_server(self._handle, self.bind_address, self.port)
        logger.info(f'{type(self).__name__} listening on {self.bind_address}:{self.port}')
        async with server:
            await server.serve_forever()

    async def _handle(self, reader, writer):
        """Handle a single HTTP request and write the response."""
        try:
            # Parse the request line for path-based routing
            request_line = await reader.readline()
            parts = request_line.split(b' ', 2)
            path = parts[1] if len(parts) >= 2 else b'/'
            # Strip query string for the routing decision
            path = path.split(b'?', 1)[0]
            # Drain remaining headers so the client doesn't get a RST
            while True:
                line = await reader.readline()
                if line in (b'\r\n', b'\n', b''):
                    break

            if path in _READINESS_PATHS:
                ok, extra = await self._readiness_check()
            else:
                ok, extra = await self._check()
            ok, extra = self._apply_loop_health(ok, extra)
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
            await close_writer(writer)
