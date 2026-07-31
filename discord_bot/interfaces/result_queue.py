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
from discord_bot.types.search_resolution import SearchResolution


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


class SearchResultQueue(ABC):
    '''Abstract bot-ready SearchResolution queue.

    Sibling of DownloadResultQueue: carries resolved searches from the search
    worker back to the cog's process_search_results loop.  Single-process uses
    AsyncioSearchResultQueue; HA uses RedisSearchResultQueue so any broker pod
    can answer GET /search-results/next.
    '''

    @abstractmethod
    async def put(self, resolution: SearchResolution) -> None:
        '''Append a SearchResolution to the back of the queue.'''

    @abstractmethod
    async def get_nowait(self) -> SearchResolution | None:
        '''Pop the oldest SearchResolution, or None if the queue is empty.'''

    @abstractmethod
    async def depth(self) -> int:
        '''Return the number of resolutions currently waiting in the queue.'''
