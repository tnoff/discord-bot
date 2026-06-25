'''
Shared health endpoint for the Redis-backed subprocesses (dispatcher, broker).

Both processes treat Redis as their source of truth, so "healthy" means the same
thing — a successful ping — for each.  Housing that single ``_check`` here means
neither DispatchHealthServer nor BrokerHealthServer carries a duplicate copy
(pylint's duplicate-code check compares those modules pairwise).  Kept apart from
health_server.py so it stays free of the sqlalchemy import the slim dispatcher
and broker images don't want on the health path.
'''
from discord_bot.clients.redis_client import RedisManager
from discord_bot.servers.health_server_base import HealthServerBase


class RedisPingHealthServer(HealthServerBase):
    '''
    Lightweight HTTP health endpoint backed by a Redis ping.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise.
    '''

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
