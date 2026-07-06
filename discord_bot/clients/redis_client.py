import redis
import redis.asyncio as aioredis
from redis.asyncio.retry import Retry
from redis.asyncio.sentinel import Sentinel
from redis.backoff import ExponentialBackoff


def _resilience_kwargs() -> dict:
    '''Connection kwargs shared by the direct-URL and Sentinel connection paths.

    These let a transient primary blip (a Valkey failover or a restart) self-heal
    instead of surfacing an unhandled ConnectionError:
      - retry: exponential backoff, up to 3 attempts per command
      - retry_on_error: retry commands that hit a connection/timeout error
      - health_check_interval: proactively ping idle connections
      - socket_keepalive / socket_connect_timeout: detect dead sockets fast
    On the Sentinel path a retried command also re-resolves the primary, so a
    promotion is transparent to callers.
    '''
    return {
        'decode_responses': True,
        'retry': Retry(ExponentialBackoff(), 3),
        'retry_on_error': [redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
        'health_check_interval': 30,
        'socket_keepalive': True,
        'socket_connect_timeout': 5,
    }


class RedisManager:
    '''Owns one shared async Redis connection for a process.

    Two connection modes:
      - direct URL (``url``): local/dev or a single Valkey endpoint.
      - Sentinel (``sentinels`` + ``service_name``): prod HA — the client asks
        Sentinel for the current primary on every connection, so a failover is
        transparent to callers.
    '''

    def __init__(self, url: str | None = None, *,
                 sentinels: list[tuple[str, int]] | None = None,
                 service_name: str | None = None):
        self._url = url
        self._sentinels = sentinels
        self._service_name = service_name
        self._sentinel: Sentinel | None = None
        self._client: aioredis.Redis | None = None

    @property
    def client(self) -> aioredis.Redis:
        '''Return the shared Redis client. Raises if start() has not been called.'''
        if self._client is None:
            raise RuntimeError('RedisManager has not been started')
        return self._client

    async def start(self) -> None:
        '''Open the Redis connection (direct URL, or the primary behind Sentinel).'''
        if self._sentinels:
            self._sentinel = Sentinel(
                self._sentinels,
                sentinel_kwargs={'socket_connect_timeout': 5, 'socket_keepalive': True},
            )
            self._client = self._sentinel.master_for(self._service_name, **_resilience_kwargs())
        else:
            self._client = aioredis.from_url(self._url, **_resilience_kwargs())

    async def close(self) -> None:
        '''Close the Redis connection, and any Sentinel connections, if open.'''
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        if self._sentinel is not None:
            for sentinel in self._sentinel.sentinels:
                await sentinel.aclose()
            self._sentinel = None

    @classmethod
    def from_client(cls, client: aioredis.Redis) -> 'RedisManager':
        '''Create a RedisManager wrapping an already-open client (useful in tests).'''
        manager = cls.__new__(cls)
        manager._url = None
        manager._sentinels = None
        manager._service_name = None
        manager._sentinel = None
        manager._client = client
        return manager

    @classmethod
    def from_general_config(cls, general_config) -> 'RedisManager':
        '''Build a RedisManager from a GeneralConfig, preferring Sentinel HA.

        ``general_config.redis_sentinel`` (a RedisSentinelConfig) is duck-typed
        here to avoid a config import cycle.
        '''
        sentinel = general_config.redis_sentinel
        if sentinel is not None:
            return cls(sentinels=sentinel.sentinel_addrs(), service_name=sentinel.service_name)
        return cls(general_config.redis_url)
