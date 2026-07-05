import redis
import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff


class RedisManager:
    '''Owns one shared async Redis connection for a process.'''

    def __init__(self, url: str):
        self._url = url
        self._client: aioredis.Redis | None = None

    @property
    def client(self) -> aioredis.Redis:
        '''Return the shared Redis client. Raises if start() has not been called.'''
        if self._client is None:
            raise RuntimeError('RedisManager has not been started')
        return self._client

    async def start(self) -> None:
        '''Open the Redis connection.

        Resilience options let a transient master blip (e.g. a Valkey failover or
        restart) self-heal instead of surfacing an unhandled ConnectionError:
          - retry: exponential backoff, up to 3 attempts per command
          - retry_on_error: retry commands that hit a connection/timeout error
          - health_check_interval: proactively ping idle connections
          - socket_keepalive / socket_connect_timeout: detect dead sockets fast
        '''
        self._client = aioredis.from_url(
            self._url,
            decode_responses=True,
            retry=Retry(ExponentialBackoff(), 3),
            retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
            health_check_interval=30,
            socket_keepalive=True,
            socket_connect_timeout=5,
        )

    async def close(self) -> None:
        '''Close the Redis connection if open.'''
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @classmethod
    def from_client(cls, client: aioredis.Redis) -> 'RedisManager':
        '''Create a RedisManager wrapping an already-open client (useful in tests).'''
        manager = cls.__new__(cls)
        manager._url = None
        manager._client = client
        return manager
