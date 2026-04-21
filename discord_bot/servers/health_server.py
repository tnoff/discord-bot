"""
HTTP health server for Docker/Kubernetes liveness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import logging

from discord_bot.servers.base import BaseHealthServer

logger = logging.getLogger(__name__)


class HealthServer(BaseHealthServer):
    """
    Lightweight HTTP health endpoint.

    Responds 200 {"status": "ok"} when the bot is ready and not closed,
    503 {"status": "unavailable"} otherwise.
    """

    def __init__(self, bot, port=8080):
        super().__init__(port)
        self.bot = bot

    async def _check_health(self) -> bool:
        return self.bot.is_ready() and not self.bot.is_closed()
