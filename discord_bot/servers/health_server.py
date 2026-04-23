"""
HTTP health server for Docker/Kubernetes liveness probes.
Runs as an asyncio task inside the bot's event loop.
"""
import logging

from discord_bot.servers.base import BaseHealthServer, _DbPingMixin

logger = logging.getLogger(__name__)


class HealthServer(_DbPingMixin, BaseHealthServer):
    """
    Lightweight HTTP health endpoint.

    Responds 200 {"status": "ok"} when the bot is ready and not closed,
    503 {"status": "unavailable"} otherwise.  If db_engine is provided,
    a DB ping is also performed and a "db" field is included in the response.
    """

    def __init__(self, bot, port=8080, db_engine=None):
        super().__init__(port)
        self.bot = bot
        self._db_engine = db_engine

    async def _check_health(self) -> bool:
        bot_ok = self.bot.is_ready() and not self.bot.is_closed()
        if self._db_engine is not None:
            self._last_db_ok = await self._db_ping()
            return bot_ok and self._last_db_ok
        return bot_ok

    async def _extra_body(self) -> dict:
        if self._last_db_ok is not None:
            return {'db': 'ok' if self._last_db_ok else 'unavailable'}
        return {}
