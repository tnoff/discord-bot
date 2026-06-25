'''
HTTP health server for the standalone broker process.

A thin alias over RedisPingHealthServer: the broker pod is healthy when Redis
(its source of truth) is reachable.  Kept in its own module/name so logs and
telemetry distinguish it from the dispatcher's health server.
'''
from discord_bot.servers.redis_health_server import RedisPingHealthServer


class BrokerHealthServer(RedisPingHealthServer):
    '''
    Lightweight HTTP health endpoint for the broker process.

    Responds 200 {"status": "ok"} when Redis is reachable (ping succeeds),
    503 {"status": "unavailable"} otherwise.
    '''
