"""
HTTP health server for Docker/Kubernetes liveness + readiness probes.
Runs as an asyncio task inside the bot's event loop.
"""
from typing import ClassVar

from discord_core.servers.health_server_base import HealthServerBase


class HealthServer(HealthServerBase):
    """
    Lightweight HTTP health endpoint.

    ``/health`` (liveness) and ``/ready`` (readiness) answer the same question:
    200 when the bot is ready+open and no background loop is stalled, 503
    otherwise.

    **Neither probes a peer pod.** ``/ready`` used to TCP-probe the dispatcher,
    db, broker, downloader and search pods, so any of them restarting (a deploy,
    a node move) flipped this pod NotReady, and five sequential 1s probes could
    outlast the kubelet's own timeout and fail the probe on their own. The
    gateway holds the Discord connection and its voice sessions; it should
    outlive its peers. A peer that is down already surfaces where it matters:
    the clients' circuit breakers and "unavailable" replies, and the seam checks.
    """

    POD_NAME: ClassVar[str] = 'bot'

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, bot, port=8080, bind_address='0.0.0.0'):  # nosec B104
        super().__init__(port=port, bind_address=bind_address)
        self.bot = bot

    async def _check(self):
        return self.bot.is_ready() and not self.bot.is_closed(), {}
