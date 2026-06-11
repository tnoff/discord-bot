"""
HTTP health server for Docker/Kubernetes liveness + readiness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import asyncio
from urllib.parse import urlsplit

from sqlalchemy import text

from discord_bot.servers.health_server_base import HealthServerBase, close_writer


_DISPATCH_PROBE_TIMEOUT_SECONDS = 1.0


class HealthServer(HealthServerBase):
    """
    Lightweight HTTP health endpoint.

    ``/health`` (liveness): 200 when the bot is ready+open; 503 otherwise.
    If db_engine is provided, also runs a SELECT 1 ping against the database
    and reports it as ``db`` in the response payload.

    ``/ready`` (readiness): liveness checks plus, when ``dispatch_http_url``
    is set, a TCP probe to the dispatcher's host:port. The probe fails fast
    so the bot reports NotReady during dispatcher outages without delaying
    the kubelet probe loop.
    """

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, bot, port=8080, bind_address='0.0.0.0', db_engine=None,  # nosec B104
                 dispatch_http_url=None):
        super().__init__(port=port, bind_address=bind_address)
        self.bot = bot
        self._db_engine = db_engine
        self._dispatch_http_url = dispatch_http_url

    async def _db_ping(self):
        """Return True if the database responds to SELECT 1."""
        try:
            async with self._db_engine.connect() as conn:
                await conn.execute(text('SELECT 1'))
            return True
        except Exception:
            return False

    async def _dispatch_probe(self):
        """Return True if a TCP connection to dispatch_http_url succeeds within the timeout."""
        parts = urlsplit(self._dispatch_http_url)
        host, port = parts.hostname, parts.port
        if not host or not port:
            return False
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=_DISPATCH_PROBE_TIMEOUT_SECONDS,
            )
        except (OSError, asyncio.TimeoutError):
            return False
        await close_writer(writer)
        return True

    async def _check(self):
        bot_ok = self.bot.is_ready() and not self.bot.is_closed()
        if self._db_engine is None:
            return bot_ok, {}
        db_ok = await self._db_ping()
        return bot_ok and db_ok, {'db': 'ok' if db_ok else 'unavailable'}

    async def _readiness_check(self):
        ok, extra = await self._check()
        if not self._dispatch_http_url:
            return ok, extra
        dispatch_ok = await self._dispatch_probe()
        extra = {**extra, 'dispatch': 'ok' if dispatch_ok else 'unavailable'}
        return ok and dispatch_ok, extra
