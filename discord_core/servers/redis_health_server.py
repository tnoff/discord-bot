'''
Shared health endpoint for the Redis-backed subprocesses (dispatcher, broker,
downloader, search).

Liveness and readiness answer different questions here, and only readiness asks
Redis. Housing both in one class means the four pods cannot drift apart, and
neither DispatchHealthServer nor BrokerHealthServer carries a duplicate copy
(pylint's duplicate-code check compares those modules pairwise).  Kept apart from
health_server.py so it stays free of the sqlalchemy import the slim dispatcher
and broker images don't want on the health path.
'''
import asyncio

from discord_core.clients.redis_client import RedisManager
from discord_core.servers.health_server_base import HealthServerBase

# Shorter than the kubelet's one-second probe timeout, so a readiness check against a Redis that
# is not answering returns 503 on its own instead of holding the connection until the probe gives up.
READINESS_PING_TIMEOUT_SECONDS = 0.8


class RedisPingHealthServer(HealthServerBase):
    '''
    Lightweight HTTP health endpoint for the pods that keep their work in Redis.

    ``/health`` (liveness) is ok while the process is alive and its background loops
    are making progress (the base class folds those in). It does NOT ask Redis. Redis
    being away is something a restart cannot fix, and the kubelet restarts a pod after
    three failed probes, about a minute: on 2026-10-28 a Redis that was down for six
    minutes had the search, downloader, dispatcher and broker pods killed by their liveness
    probes one after another. It is not enough to ignore the result either, because a ping
    against a Service with no ready endpoints hangs rather than refuses, and a probe that
    times out counts the same as one that fails.

    ``/ready``, ``/readyz`` and ``/readiness`` answer 200 when Redis answers a PING
    within READINESS_PING_TIMEOUT_SECONDS and 503 otherwise. Nothing in Kubernetes
    probes them today (the Services' readiness probes are TCP checks on the service
    ports), so this is the probe to point at if a pod should leave its Service while
    Redis is away. Every probe is counted on pod_ready_check either way.
    '''

    # bandit B104: '0.0.0.0' default is intentional — health endpoint must be reachable from outside the container; override via MonitoringHealthServerConfig.bind_address
    def __init__(self, redis_manager: RedisManager, port: int = 8080,
                 bind_address: str = '0.0.0.0', pod: str | None = None):  # nosec B104
        super().__init__(port=port, bind_address=bind_address, pod=pod)
        self.redis_manager = redis_manager

    async def _check(self):
        return True, {}

    async def _readiness_check(self):
        try:
            await asyncio.wait_for(self.redis_manager.client.ping(), timeout=READINESS_PING_TIMEOUT_SECONDS)
            return True, {}
        except Exception:
            return False, {}
