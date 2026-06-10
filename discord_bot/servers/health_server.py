"""
HTTP health server for Docker/Kubernetes liveness probes.
Runs as an asyncio task inside the bot's event loop.
"""
from sqlalchemy import text

from discord_bot.servers.health_server_base import HealthServerBase


class HealthServer(HealthServerBase):
    """
    Lightweight HTTP health endpoint.

    Responds 200 {"status": "ok"} when the bot is ready and not closed,
    503 {"status": "unavailable"} otherwise.

    If db_engine is provided, also runs a SELECT 1 ping against the database.
    A DB failure returns 503 with {"status": "unavailable", "db": "unavailable"}.
    """

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, bot, port=8080, bind_address='0.0.0.0', db_engine=None):  # nosec B104
        super().__init__(port=port, bind_address=bind_address)
        self.bot = bot
        self._db_engine = db_engine

    async def _db_ping(self):
        """Return True if the database responds to SELECT 1."""
        try:
            async with self._db_engine.connect() as conn:
                await conn.execute(text('SELECT 1'))
            return True
        except Exception:
            return False

    async def _check(self):
        bot_ok = self.bot.is_ready() and not self.bot.is_closed()
        if self._db_engine is None:
            return bot_ok, {}
        db_ok = await self._db_ping()
        return bot_ok and db_ok, {'db': 'ok' if db_ok else 'unavailable'}
