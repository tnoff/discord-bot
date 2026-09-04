"""
HTTP health server for Docker/Kubernetes liveness + readiness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import asyncio
from urllib.parse import urlsplit

from discord_bot.servers.db_probe import db_ping
from discord_bot.servers.health_server_base import HealthServerBase, close_writer
from discord_bot.utils.otel import AttributeNaming, METER_PROVIDER, MetricNaming


_DISPATCH_PROBE_TIMEOUT_SECONDS = 1.0

# Counts each dispatcher readiness probe by outcome. Only incremented from the
# bot pod (cli.bot), which probes the remote dispatcher; a flapping outcome is an
# early warning for the readiness-split regression class.
_READY_CHECK_COUNTER = METER_PROVIDER.create_counter(
    name=MetricNaming.DISPATCHER_READY_CHECK.value,
    description='Dispatcher readiness probe outcomes from the bot pod',
    unit='1',
)


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
        db_ok = await db_ping(self._db_engine)
        return bot_ok and db_ok, {'db': 'ok' if db_ok else 'unavailable'}

    async def _readiness_check(self):
        ok, extra = await self._check()
        if not self._dispatch_http_url:
            return ok, extra
        dispatch_ok = await self._dispatch_probe()
        _READY_CHECK_COUNTER.add(1, {
            AttributeNaming.OUTCOME.value: 'ok' if dispatch_ok else 'unavailable',
        })
        extra = {**extra, 'dispatch': 'ok' if dispatch_ok else 'unavailable'}
        return ok and dispatch_ok, extra
