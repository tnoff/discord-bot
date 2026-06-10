"""
HTTP health server for the dispatcher process.
Separate module from health_server so it doesn't pull in sqlalchemy — the
dispatcher image installs only the base dependency set.
"""
from discord_bot.clients.redis_client import get_redis_client
from discord_bot.servers.health_server_base import HealthServerBase


class DispatchHealthServer(HealthServerBase):
    """
    Lightweight HTTP health endpoint for the dispatcher process.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise.
    """

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, redis_url: str, port: int = 8080, bind_address: str = '0.0.0.0'):  # nosec B104
        super().__init__(port=port, bind_address=bind_address)
        self._redis_url = redis_url
        self._client = None

    async def serve(self):
        """Asyncio coroutine — schedule with asyncio.create_task()."""
        self._client = get_redis_client(self._redis_url)
        try:
            await super().serve()
        finally:
            await self._client.aclose()

    async def _check(self):
        try:
            await self._client.ping()
            return True, {}
        except Exception:
            return False, {}
