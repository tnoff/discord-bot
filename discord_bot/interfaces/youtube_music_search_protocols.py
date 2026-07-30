'''
Search engine base + cog-facing YoutubeMusicSearchClient Protocol.

Two roles, mirroring interfaces/download_protocols.py:

  YoutubeMusicSearchWorkerBase (ABC) — the search *engine*.  Owns the
  queue-agnostic bits: resolving a query to a YouTube videoId via the injected
  YoutubeMusicClient, the 429 failure tracking, and the backoff window.  The
  per-guild input queue is declared as abstract hooks so a subclass can back it
  with an in-process DistributedQueue (AsyncioYoutubeMusicSearchWorker) or Redis
  (a future RedisYoutubeMusicSearchWorker for HA).

  YoutubeMusicSearchClient (Protocol) — the cog-facing handle.
  InMemoryYoutubeMusicSearchClient wraps a worker in single-process mode; a
  future HttpYoutubeMusicSearchClient will forward the same surface to a search
  pod.

The cog runs the search loop against the Protocol and lets config decide which
implementation backs it.  Unlike the DownloadClient Protocol (which hides
resolution behind a background worker loop), the search Protocol exposes
resolve()/backoff_wait() because the bot-side loop still drives one search at a
time in single-process mode.
'''
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from functools import partial
from random import randint, seed
from time import time
from typing import Callable, Protocol, runtime_checkable

from discord_bot.exceptions import ExitEarlyException
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.common import LoggingConfig, get_logger
from discord_bot.utils.failure_queue import FailureQueue, FailureStatus
from discord_bot.utils.integrations.youtube_music import (
    YoutubeMusicClient, YoutubeMusicRetryException,
)


class YoutubeMusicSearchWorkerBase(ABC):
    '''
    Search engine base: the queue-agnostic YouTube-Music resolution + backoff.

    Owns everything that does not depend on where the input queue lives — the
    YoutubeMusicClient call (offloaded to a thread), the 429 FailureQueue, and
    the backoff window.  The per-guild input queue is declared as abstract hooks
    (submit routes through _enqueue; the loop pulls via get_input_nowait) so a
    subclass can back it with an in-process DistributedQueue or Redis.
    '''
    def __init__(
        self,
        logging_config: LoggingConfig,
        client: YoutubeMusicClient,
        failure_queue: FailureQueue,
        wait_period_minimum: int,
        wait_period_max_variance: int,
    ):
        '''
        Init search engine.

        client : YoutubeMusicClient, injected so the ytmusicapi dependency is
                 supplied by the caller (the HA search pod builds its own; the
                 bot pod's future HTTP client never imports one).
        failure_queue : FailureQueue tracking recent 429s for backoff scaling.
        wait_period_minimum : Minimum backoff wait time in seconds.
        wait_period_max_variance : Maximum extra random variance in seconds.
        '''
        self._client = client
        self._failure_queue = failure_queue
        self._wait_period_minimum = wait_period_minimum
        self._wait_period_max_variance = wait_period_max_variance
        self._wait_timestamp: float | None = None
        self.logger = get_logger('youtube_music_search', logging_config)
        self.logging_config = logging_config

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a search request; resolution happens on the search loop.'''
        await self._enqueue(guild_id, media_request, priority=priority)

    async def resolve(self, media_request: MediaRequest) -> str | None:
        '''
        Resolve a search request to a YouTube videoId (or None if nothing
        matched).

        Records a passing FailureStatus on success.  On a 429
        YoutubeMusicRetryException it records a failing FailureStatus, arms the
        backoff window scaled by the current failure count, and RE-RAISES — the
        cog loop owns the retry_count / re-enqueue / lifecycle decisions.
        '''
        loop = asyncio.get_running_loop()
        try:
            video_id = await loop.run_in_executor(
                None, partial(self._client.search, media_request.search_result.raw_search_string))
        except YoutubeMusicRetryException as error:
            self._failure_queue.add_item(FailureStatus(success=False,
                                                       exception_type=type(error).__name__,
                                                       exception_message=str(error)))
            self.logger.info(f'Youtube music search failure queue status: {self._failure_queue.get_status_summary()}')
            self.set_wait_timestamp(backoff_multiplier=2 ** self._failure_queue.size)
            raise
        self._failure_queue.add_item(FailureStatus())
        return video_id

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''Arm the search backoff window: wait_period_minimum * multiplier + jitter.'''
        seed(time())
        window = self._wait_period_minimum * backoff_multiplier
        # bandit B311: backoff jitter, not security-sensitive
        jitter = randint(1000, self._wait_period_max_variance * 1000) / 1000  # nosec B311
        self._wait_timestamp = datetime.now(timezone.utc).timestamp() + window + jitter
        self.logger.info(f'Waiting on youtube music search backoff, waiting until {self._wait_timestamp}')

    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''
        Sleep until the backoff window elapses, returning early if it is already
        clear.  Raises ExitEarlyException if the shutdown event fires.
        '''
        if self._wait_timestamp is None:
            return
        sleep_duration = max(0, self._wait_timestamp - datetime.now(timezone.utc).timestamp())
        if shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')
        if sleep_duration == 0:
            return
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_duration)
        except asyncio.TimeoutError:
            return
        raise ExitEarlyException('Exiting bot wait loop')

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Whole seconds left in the current backoff window, or None if unset.'''
        if self._wait_timestamp is None:
            return None
        remaining = self._wait_timestamp - datetime.now(timezone.utc).timestamp()
        return max(0, int(remaining))

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the search failure queue.'''
        return self._failure_queue.get_status_summary()

    # ------------------------------------------------------------------
    # Queue interface — backed by the subclass
    # ------------------------------------------------------------------

    @abstractmethod
    async def _enqueue(self, guild_id: int, media_request: MediaRequest,
                       priority: int | None = None) -> None:
        '''Append a request to the per-guild input queue.'''

    @abstractmethod
    def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending request, raising asyncio.QueueEmpty if none.'''

    @abstractmethod
    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''

    @abstractmethod
    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''

    @abstractmethod
    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''


@runtime_checkable
class YoutubeMusicSearchClient(Protocol):
    '''
    Cog-facing search handle.  InMemoryYoutubeMusicSearchClient forwards this
    surface to a wrapped worker; a future HttpYoutubeMusicSearchClient will
    forward it to a search pod.
    '''
    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a search request.'''

    def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending request, raising asyncio.QueueEmpty if none.'''

    async def resolve(self, media_request: MediaRequest) -> str | None:
        '''Resolve a request to a videoId (or None); re-raises on 429.'''

    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''Sleep out any active backoff window.'''

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''Arm the backoff window.'''

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild.'''

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the input queue for a guild.'''

    async def queue_size(self, guild_id: int) -> int:
        '''Pending request count for a guild.'''

    @property
    def failure_summary(self) -> str:
        '''Human-readable failure-queue summary.'''

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff window, or None.'''
