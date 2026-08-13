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
  InMemoryYoutubeMusicSearchClient wraps a worker in single-process mode;
  HttpYoutubeMusicSearchClient forwards the submit/clear/block/status half to a
  standalone search pod (the pop-and-resolve half belongs to the pod itself).

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
from typing import TYPE_CHECKING, Callable, Protocol, runtime_checkable

from discord_bot.exceptions import ExitEarlyException, YoutubeMusicRetryException
from discord_bot.types.clear_guild_result import ClearGuildResult
from discord_bot.types.media_request import MediaRequest
from discord_bot.utils.common import LoggingConfig, get_logger
from discord_bot.utils.failure_queue import FailureQueue, FailureStatus

if TYPE_CHECKING:  # pragma: no cover
    # Annotation only — importing it for real would pull ytmusicapi into every
    # process that touches this base, including the HA bot, which never builds a
    # client (it is injected by the caller; see the constructor docstring).
    from discord_bot.utils.integrations.youtube_music import YoutubeMusicClient


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
        client: 'YoutubeMusicClient',
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

        Records a passing outcome on success.  On a 429
        YoutubeMusicRetryException it records a failing outcome (which arms the
        backoff window scaled by the current failure count) and RE-RAISES — the
        cog loop owns the retry_count / re-enqueue / lifecycle decisions.  The
        failure/success bookkeeping is factored into _record_search_success /
        _record_search_failure hooks so a Redis-backed subclass can share both
        the failure count and the backoff window across pods.
        '''
        loop = asyncio.get_running_loop()
        try:
            video_id = await loop.run_in_executor(
                None, partial(self._client.search, media_request.search_result.raw_search_string))
        except YoutubeMusicRetryException as error:
            await self._record_search_failure(error)
            raise
        await self._record_search_success()
        return video_id

    async def _record_search_success(self) -> None:
        '''
        Record a passing search on the failure queue (drains one prior failure).

        Overridden by the Redis worker to pop the shared cross-pod failure ZSET.
        '''
        self._failure_queue.add_item(FailureStatus())

    async def _record_search_failure(self, error: YoutubeMusicRetryException) -> None:
        '''
        Record a 429 on the failure queue and arm the backoff window scaled by the
        resulting failure count.

        Overridden by the Redis worker to share the failure count and the backoff
        window across pods.
        '''
        self._failure_queue.add_item(FailureStatus(success=False,
                                                   exception_type=type(error).__name__,
                                                   exception_message=str(error)))
        self.logger.info(f'Youtube music search failure queue status: {self._failure_queue.get_status_summary()}')
        self.set_wait_timestamp(backoff_multiplier=2 ** self._failure_queue.size)

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''Arm the search backoff window: wait_period_minimum * multiplier + jitter.'''
        seed(time())
        window = self._wait_period_minimum * backoff_multiplier
        # bandit B311: backoff jitter, not security-sensitive
        jitter = randint(1000, self._wait_period_max_variance * 1000) / 1000  # nosec B311
        self._wait_timestamp = datetime.now(timezone.utc).timestamp() + window + jitter
        self.logger.info(f'Waiting on youtube music search backoff, waiting until {self._wait_timestamp}')

    async def backoff_wait(self, shutdown_event: asyncio.Event,
                           max_wait_seconds: float | None = None) -> None:
        '''
        Sleep until the backoff window elapses, returning early if it is already
        clear.  Raises ExitEarlyException if the shutdown event fires.

        max_wait_seconds : Cap on this call's sleep.  The caller is expected to
            re-call until backoff_seconds_remaining clears, which is what lets
            the search loop wait out a long 429 window in slices instead of one
            uninterrupted sleep.  That matters because loop health is time-based
            (utils/loop_health): a window of wait_period_minimum * 2**failures
            passes the 300 s staleness default at four failures, and an
            iteration that never returns inside the window reads as a wedge —
            dropping the heartbeat to 0 and failing the livenessProbe, which
            restarts the pod over a rate limit a restart cannot fix.
        '''
        if self._wait_timestamp is None:
            return
        sleep_duration = max(0, self._wait_timestamp - datetime.now(timezone.utc).timestamp())
        if shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')
        if sleep_duration == 0:
            return
        if max_wait_seconds is not None:
            sleep_duration = min(sleep_duration, max_wait_seconds)
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
    async def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending request, raising asyncio.QueueEmpty if none.

        Async (unlike a plain in-memory get_nowait) so a Redis-backed subclass can
        do its pop I/O inline, mirroring DownloadWorkerBase.get_input_nowait.
        '''

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

    async def get_input_nowait(self) -> MediaRequest:
        '''Pop the next pending request, raising asyncio.QueueEmpty if none.'''

    async def resolve(self, media_request: MediaRequest) -> str | None:
        '''Resolve a request to a videoId (or None); re-raises on 429.'''

    async def backoff_wait(self, shutdown_event: asyncio.Event,
                           max_wait_seconds: float | None = None) -> None:
        '''Sleep out any active backoff window, at most max_wait_seconds.'''

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''Arm the backoff window.'''

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild.'''

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> ClearGuildResult:
        '''Clear the input queue for a guild.

        Returns a ClearGuildResult carrying the dropped requests plus the
        bundle_uuids of any items the predicate preserved (so the cog can skip
        deleting those bundles, including in HA where the predicate runs on the
        search pod, not the bot).'''

    async def queue_size(self, guild_id: int) -> int:
        '''Pending request count for a guild.'''

    @property
    def failure_summary(self) -> str:
        '''Human-readable failure-queue summary.'''

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff window, or None.'''
