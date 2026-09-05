"""
HTTP health server for Docker/Kubernetes liveness + readiness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import asyncio
from urllib.parse import urlsplit

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

# A SECOND counter rather than a `peer` dimension on the one above. The dashboard
# panel in docker-apps reads `sum by (outcome) (rate(dispatcher_ready_check[5m]))`
# and is titled for the dispatcher; adding a dimension would silently fold db-pod
# results into it and make a correct-looking panel wrong. Two probes, two names.
_DATABASE_PEER_COUNTER = METER_PROVIDER.create_counter(
    name=MetricNaming.DATABASE_PEER_READY_CHECK.value,
    description='Bot-side TCP reachability of the discord-db pod',
    unit='1',
)


class HealthServer(HealthServerBase):
    """
    Lightweight HTTP health endpoint.

    ``/health`` (liveness): 200 when the bot is ready+open; 503 otherwise.

    ``/ready`` (readiness): liveness checks plus a TCP probe to each configured
    peer pod -- the dispatcher, and since MR 4b the db pod. The probes fail fast
    so the bot reports NotReady during a peer outage without delaying the kubelet
    probe loop.

    **The SELECT 1 ping this class used to run is gone, and its replacement is a
    TCP probe rather than a query.** The bot holds no engine any more, so there is
    nothing here to ping; what it depends on is the db pod being reachable, which
    is the same shape as its dispatcher dependency and now uses the same
    mechanism. Keeping the old ping would also have kept `sqlalchemy` in this
    module's import chain and therefore in the bot image, which is the thing MR 4b
    exists to remove.
    """

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, bot, port=8080, bind_address='0.0.0.0',  # nosec B104
                 dispatch_http_url=None, database_http_url=None):
        super().__init__(port=port, bind_address=bind_address)
        self.bot = bot
        self._dispatch_http_url = dispatch_http_url
        self._database_http_url = database_http_url

    async def _tcp_probe(self, url):
        """Return True if a TCP connection to url's host:port succeeds within the timeout."""
        parts = urlsplit(url)
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
        return self.bot.is_ready() and not self.bot.is_closed(), {}

    async def _readiness_check(self):
        ok, extra = await self._check()
        # Each peer is probed and reported independently. Short-circuiting on the
        # first failure would make the payload say which probe ran, not which
        # dependency is down -- and with two peers that is the difference between
        # "the dispatcher is gone" and "everything is gone".
        for label, url, counter in (('dispatch', self._dispatch_http_url, _READY_CHECK_COUNTER),
                                    ('database', self._database_http_url, _DATABASE_PEER_COUNTER)):
            if not url:
                continue
            peer_ok = await self._tcp_probe(url)
            counter.add(1, {
                AttributeNaming.OUTCOME.value: 'ok' if peer_ok else 'unavailable',
            })
            extra = {**extra, label: 'ok' if peer_ok else 'unavailable'}
            ok = ok and peer_ok
        return ok, extra
