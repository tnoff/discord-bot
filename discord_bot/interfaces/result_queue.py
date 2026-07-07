'''
Storage interface for the bot-ready DownloadResult queue.

Lives in its own module — separate from broker_protocols.py — so the
dispatcher pod can pull RedisBundleStore / RedisWorkQueue from
workers/redis_queues.py without transitively importing the broker engine
(MediaBrokerBase pulls VideoCacheClient → discord_bot.database → sqlalchemy,
which the dispatcher pod doesn't ship).

Single-process deployments use AsyncioDownloadResultQueue (in-memory);
HA deployments use RedisDownloadResultQueue so any broker pod can answer
GET /results/next, and broker-pod restarts don't lose work.
'''
from abc import ABC, abstractmethod

from discord_bot.types.download import DownloadResult


class DownloadResultQueue(ABC):
    '''Abstract bot-ready DownloadResult queue.'''

    @abstractmethod
    async def put(self, result: DownloadResult) -> None:
        '''Append a DownloadResult to the back of the queue.'''

    @abstractmethod
    async def get_nowait(self) -> DownloadResult | None:
        '''Pop the oldest DownloadResult, or None if the queue is empty.'''

    @abstractmethod
    async def depth(self) -> int:
        '''Return the number of results currently waiting in the queue.'''
