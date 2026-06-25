"""
HTTP health server for the dispatcher process.
Separate module from health_server so it doesn't pull in sqlalchemy — the
dispatcher image installs only the base dependency set.
"""
from discord_bot.servers.redis_health_server import RedisPingHealthServer


class DispatchHealthServer(RedisPingHealthServer):
    """
    Lightweight HTTP health endpoint for the dispatcher process.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise.
    """
