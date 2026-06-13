"""
HTTP health server for the dispatcher process.
Separate module from health_server so it doesn't pull in sqlalchemy — the
dispatcher image installs only the base dependency set.
"""
from discord_bot.clients.redis_client import RedisManager
from discord_bot.servers.health_server_base import HealthServerBase


class DispatchHealthServer(HealthServerBase):
    """
    Lightweight HTTP health endpoint for the dispatcher process.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise.
    """

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, redis_manager: RedisManager, port: int = 8080, bind_address: str = '0.0.0.0'):  # nosec B104
        super().__init__(port=port, bind_address=bind_address)
        self.redis_manager = redis_manager

    async def _check(self):
        try:
            await self.redis_manager.client.ping()
            return True, {}
        except Exception:
            return False, {}
