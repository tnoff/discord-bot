import redis.asyncio as aioredis


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
        '''Open the Redis connection.'''
        self._client = aioredis.from_url(self._url, decode_responses=True)

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
