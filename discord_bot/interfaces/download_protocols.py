'''
Download engine base.  The cog-facing DownloadClient Protocol now lives in
interfaces/download_client_protocol (re-exported below).

Two shapes live here, mirroring interfaces/broker_protocols.py:

  DownloadWorkerBase (ABC) — the download *engine*.  Owns the queue-agnostic
    yt-dlp pipeline (create_source, backoff, retry, broker reporting) and the
    run() consumer loop, and declares the per-guild queue surface as abstract
    hooks.  AsyncioDownloadWorker (workers/asyncio_download_worker.py) backs it
    with in-process DistributedQueues; a later RedisDownloadWorker will back it
    with Redis for HA.

  DownloadClient (Protocol) — the cog-facing handle.  InMemoryDownloadClient
    (clients/download_client.py) wraps a DownloadWorkerBase for single-process
    deployments; a future HttpDownloadClient will forward the same surface to a
    remote downloader pod.

The cog depends only on the DownloadClient Protocol and lets config decide which
impl is constructed — mirroring the BrokerClient seam.
'''
import asyncio
from abc import ABC, abstractmethod
from asyncio import QueueEmpty, sleep
from datetime import datetime, timezone
from functools import partial
import hashlib
from pathlib import Path
import random
import shutil
from time import time
from typing import Callable, List

from opentelemetry.instrumentation.utils import suppress_instrumentation
from opentelemetry.trace.status import StatusCode
from opentelemetry.trace import SpanKind
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.interfaces.broker_client_protocol import BrokerClient
from discord_bot.interfaces.download_client_protocol import (
    DownloadClient, RETRY_BACKOFF_SECONDS_MINIMUM,
)
from discord_bot.types.clear_guild_result import ClearGuildResult

# DownloadClient and RETRY_BACKOFF_SECONDS_MINIMUM moved to
# interfaces/download_client_protocol so that annotating a download handle does
# not import this module's engine deps (yt_dlp, boto3 via integrations/s3).
# Re-exported here so existing imports keep working —
# same move, same reason, as BrokerClient before them.
__all__ = ['DownloadClient', 'RETRY_BACKOFF_SECONDS_MINIMUM', 'ClearGuildResult']
from discord_bot.utils.audio import edit_audio_file, AudioProcessingError
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.types.download import (
    DownloadErrorType, LifecycleEvent, DownloadResult, DownloadStatus, LifecycleStatusUpdate,
)
from discord_bot.utils.failure_queue import FailureQueue, FailureStatus
from discord_bot.utils.integrations.s3 import upload_file
from discord_bot.utils.integrations.egress_probe import (
    cached_exit_attributes, cached_exit_hostname, PoolExitIpProbe, UNKNOWN_EXIT,
)
from discord_bot.utils.integrations.egress_pool import (
    EGRESS_MODE_HTTP_PROXY, build_exit_resolver, DownloadEgress, Egress,
    ExitClients, ExitPool, HttpProxyEgress, PoolEgress,
)
from discord_bot.utils.otel import (
    AttributeNaming, capture_span_context,
    otel_span_wrapper, span_links_from_context,
)
from discord_bot.utils.common import get_logger, LoggingConfig


class DirectItemAvailableException(Exception):
    '''Raised by backoff_wait when a DIRECT item arrives during the wait period.'''

class DownloadClientException(Exception):
    '''
    Generic class for download client errors
    '''
    def __init__(self, message, user_message=None):
        self.message = message
        super().__init__(self.message)
        self.user_message = user_message

class DownloadTerminalException(DownloadClientException):
    '''
    Download Client Exception which should not be retried
    '''
    def __init__(self, message, user_message=None):
        self.message = message
        super().__init__(self.message, user_message=user_message)

class RetryableException(DownloadClientException):
    '''
    Throw when we can retry download
    '''
    def __init__(self, message, media_request: MediaRequest, user_message=None):
        self.message = message
        super().__init__(self.message, user_message=user_message)
        self.media_request = media_request

class RetryLimitExceeded(DownloadClientException):
    '''When retry limit has been exceeded'''

class InvalidFormatException(DownloadTerminalException):
    '''
    When requested format not available
    '''

class VideoNotFoundException(DownloadTerminalException):
    '''
    When no videos are found
    '''

class MetadataCheckFailedException(DownloadTerminalException):
    '''
    Video failed metadata checked
    '''

class VideoAgeRestrictedException(DownloadTerminalException):
    '''
    Video has age restrictions, cannot download
    '''

class VideoUnavailableException(DownloadTerminalException):
    '''
    Video Unavailable while downloading
    '''

class VideoViolatedTermsException(DownloadTerminalException):
    '''
    Video Removed for Violating Terms of Service
    '''

class PrivateVideoException(DownloadTerminalException):
    '''
    Private Video while downloading
    '''

class VideoTooLong(MetadataCheckFailedException):
    '''
    Max length of video duration exceeded
    '''

class VideoBanned(MetadataCheckFailedException):
    '''
    Video is on banned list
    '''

class BotDownloadFlagged(RetryableException):
    '''
    Youtube flagged download as a bot
    '''

OTEL_SPAN_PREFIX = 'music.download_client'
# Idle backoff for the run() consumer loop when no request is pending. Sleeping
# ONLY on the empty path (not before a successful dequeue) keeps back-to-back
# downloads at full throughput while cutting idle allocation churn (OOM fix).
#
# Also paces create_source's NO_EXIT_AVAILABLE yield, where every exit is leased or
# backed off and the item goes straight back on the queue.
#
# Every idle poll costs redis work per driver, and pool mode runs one driver per
# worker_count, so the pod's floor traffic is this rate times the driver count. The
# trade is pickup latency: a request queued just after a poll waits up to this long
# before any driver sees it, on a path where the download itself then takes seconds.
_IDLE_POLL_BACKOFF_SECONDS = 1.0
YTDLP_OUTPUT_TEMPLATE = '%(extractor)s.%(id)s.%(ext)s'
# bandit B104: yt-dlp's source-address config, not a server bind; '0.0.0.0' lets the OS pick (avoids ipv6 issues)
YTDLP_SOURCE_ADDRESS = '0.0.0.0'  # nosec B104
# Ceiling on the doubling, so raising max_download_retries can't strand a request
# for an hour. At the 30s default the sequence is 30/60/120/240/300…
RETRY_BACKOFF_SECONDS_MAXIMUM = 300

def match_generator(max_video_length: int, banned_videos_list: List[str]):
    '''
    Generate filters for yt-dlp
    '''
    def filter_function(info, *, incomplete): #pylint:disable=unused-argument
        '''
        Throw errors if filters dont match
        '''
        duration = info.get('duration')
        vid_url = info.get('webpage_url')
        if duration and max_video_length and duration > max_video_length:
            raise VideoTooLong('Video Too Long', user_message=f'Video duration {duration} seconds exceeds max duration of {max_video_length} seconds')
        if vid_url and banned_videos_list:
            for banned_url in banned_videos_list:
                if vid_url == banned_url:
                    raise VideoBanned('Video Banned', user_message='Video is banned by bot maintainer')

    return filter_function


class DownloadWorkerBase(ABC):
    '''
    Download engine base: the queue-agnostic yt-dlp pipeline + consumer loop.

    Owns everything that does not depend on where the input queue lives — the
    yt-dlp client, backoff/failure tracking, create_source, and the run()
    consumer loop that reports results to the broker.  The per-guild input queue
    is declared here as abstract hooks (submit routes through _enqueue_request;
    the loop pulls via _dequeue_direct / _merged_get_nowait and interrupts
    backoff via backoff_wait) so a subclass can back it with in-process queues
    (AsyncioDownloadWorker) or Redis (a future RedisDownloadWorker).
    '''
    def __init__(
        self,
        logging_config: LoggingConfig,
        download_dir: Path,
        extra_ytdlp_options: dict | None = None,
        max_video_length: int | None = None,
        banned_video_list: List[str] | None = None,
        failure_queue: FailureQueue | None = None,
        wait_period_minimum: int = 30,
        wait_period_max_variance: int = 10,
        bucket_name: str | None = None,
        normalize_audio: bool = False,
        broker: BrokerClient | None = None,
        max_retries: int = 3,
        retry_backoff_seconds_minimum: int = RETRY_BACKOFF_SECONDS_MINIMUM,
        egress_mode: str = EGRESS_MODE_HTTP_PROXY,
        egress_exits: List[str] | None = None,
    ):
        '''
        Init download engine

        ytdl : YoutubeDL Client
        failure_queue : Optional FailureQueue for tracking download failures
        wait_period_minimum : Minimum backoff wait time in seconds
        wait_period_max_variance : Maximum extra random variance in seconds
        bucket_name : S3 bucket to upload to immediately after download;
                      when set the local file is deleted and DownloadResult.file_name
                      holds the S3 object key instead of a local path
        broker : MediaBroker for lifecycle status updates; optional for backwards compatibility
        max_retries : Maximum download retries before returning RETRY_LIMIT_EXCEEDED
        retry_backoff_seconds_minimum : First retry's hold-off; doubles per attempt.
                                        0 restores the pre-existing immediate requeue.
        '''
        ytdlopts = {
            'format': 'bestaudio/best',
            'restrictfilenames': True,
            'noplaylist': True,
            'nocheckcertificate': True,
            'ignoreerrors': False,
            'logtostderr': False,
            'logger': get_logger('ytdlp', logging_config),
            'default_search': 'auto',
            'source_address': YTDLP_SOURCE_ADDRESS,
            # Relative output template + a home path, so a concurrent (pool-mode)
            # download can redirect just its home to a per-request scratch dir
            # (set on the leased client in _create_source) without rebuilding the
            # client. The filename stays per-video, so the S3 cache key is unchanged.
            'outtmpl': YTDLP_OUTPUT_TEMPLATE,
            'paths': {'home': str(download_dir)},
        }
        if extra_ytdlp_options:
            for key, value in extra_ytdlp_options.items():
                ytdlopts[key] = value
        if max_video_length or banned_video_list:
            ytdlopts['match_filter'] = match_generator(max_video_length, banned_video_list)
        # Egress strategy: one object, never None. http-proxy routes every download
        # through a single fixed-proxy client; any other mode leases a distinct exit
        # per download (build_exit_resolver fails loud on an unknown mode). The
        # download path asks self._egress for a client + exit and never branches.
        if egress_mode == EGRESS_MODE_HTTP_PROXY:
            self._egress: Egress = HttpProxyEgress(YoutubeDL(ytdlopts))
        else:
            self._egress = PoolEgress(
                ExitPool(egress_exits or []),
                ExitClients(ytdlopts, build_exit_resolver(egress_mode)),
            )
        self._broker = broker
        self._download_dir = download_dir
        self._max_retries = max_retries
        self._retry_backoff_minimum = retry_backoff_seconds_minimum
        self.failure_queue: FailureQueue | None = failure_queue
        self._wait_period_minimum = wait_period_minimum
        self._wait_period_max_variance = wait_period_max_variance
        self._wait_timestamp: float | None = None
        self.bucket_name: str | None = bucket_name
        self.normalize_audio: bool = normalize_audio
        self.logger = get_logger('download_client', logging_config)
        self.logging_config = logging_config
        # Optional ExitProbe, wired by the downloader entrypoint; None on the
        # in-process/bot path, in which case exit attribution reads 'unknown'.
        self._exit_probe = None
        # Pool modes lease a different exit per download, so a single-exit probe
        # cannot answer "which IP did THIS download leave from".  Build a per-exit
        # probe instead; the entrypoint schedules its refresh loop.  Until an exit
        # resolves, attribution falls back to the exit name alone as before.
        self._pool_exit_ip_probe = None
        if self._egress.is_pool:
            self._pool_exit_ip_probe = PoolExitIpProbe(self._egress.exit_names,
                                                       self._egress.client_for_exit)

    @property
    def pool_exit_ip_probe(self):
        '''Per-exit IP probe for pool modes, or None for the fixed proxy.'''
        return self._pool_exit_ip_probe

    def set_exit_probe(self, exit_probe) -> None:
        '''Attach an ExitProbe whose cached exit the download path attributes to.'''
        self._exit_probe = exit_probe

    async def _reserve_youtube_exit(self, _exit_name: str) -> bool:
        '''
        Atomically reserve an exit for a YouTube download; return True once the exit
        is claimed for this download, False if it is unavailable (backed off or
        claimed by another task/pod).

        Base: the in-process worker keeps no shared per-exit state, so every exit is
        always reservable. The Redis worker overrides this to SET-NX the exit's
        per-exit YouTube window, which gives cross-task + cross-pod exclusion and
        doubles as the per-exit spacing/backoff gate.
        '''
        return True

    async def _reserve_direct_exit(self, _exit_name: str) -> bool:
        '''Reserve an exit for a DIRECT download.  DIRECT items aren't YouTube-rate-
        limited, so an exit is always reservable — the pool's in-pod leased set is
        enough and no cross-pod window is claimed.  Mirrors _reserve_youtube_exit's
        signature so the pool can take either.'''
        return True

    def _log_exit_failure(self, error_type: DownloadErrorType, exit_name: str | None = None) -> None:
        '''
        Log the egress exit a YouTube failure left from, so failures can be grouped
        by exit in Loki without paying the metric cardinality of a per-exit label.
        Shared by the in-process base failure branch and the Redis worker's
        YouTube-failure path so both attribute failures to the exit that was live.

        exit_name is the leased pool exit (pool mode) — it names the exit directly,
        since the probe isn't wired when the pool owns exit selection. When None
        (fixed http-proxy mode) we fall back to the probe-discovered exit.
        '''
        exit_hostname = exit_name if exit_name is not None else cached_exit_hostname(self._exit_probe)
        self.logger.warning('Download failure (%s) attributed to egress exit %s',
                             error_type.value, exit_hostname)

    @property
    def wait_timestamp(self) -> float | None:
        '''The Unix timestamp at which the current backoff period ends, or None.'''
        return self._wait_timestamp

    @wait_timestamp.setter
    def wait_timestamp(self, value: float | None) -> None:
        self._wait_timestamp = value

    def set_wait_timestamp(self, backoff_multiplier: int = 1) -> None:
        '''
        Set the next download wait timestamp with optional backoff multiplier.
        '''
        new_timestamp = int(datetime.now(timezone.utc).timestamp())
        new_timestamp = new_timestamp + (self._wait_period_minimum * backoff_multiplier)
        random.seed(time())
        # bandit B311: backoff jitter, not security-sensitive
        new_timestamp = new_timestamp + (random.randint(1000, self._wait_period_max_variance * 1000) / 1000)  # nosec B311
        self._wait_timestamp = new_timestamp

    async def update_tracking(self, result: DownloadResult, exit_name: str | None = None) -> int | None:
        '''
        Update failure queue and backoff timestamp based on a DownloadResult.
        Returns backoff_seconds_remaining so callers need not re-query.

        exit_name is the exit this download leased (pool mode); it names the exit
        in failure attribution and, in the Redis worker, keys the per-exit backoff
        window.  None on the fixed http-proxy path.

        Async so a Redis-backed subclass can persist shared backoff/failure state
        inline; the in-process base body itself does no awaiting.
        '''
        error_type = result.status.error_type

        if result.status.success:
            if self.failure_queue is not None:
                self.failure_queue.add_item(FailureStatus())
            # Only set backoff timestamp for youtube (or unknown extractor)
            extractor = (result.ytdlp_data or {}).get('extractor')
            if extractor is None or extractor == 'youtube':
                self.set_wait_timestamp()
            return self.backoff_seconds_remaining

        if error_type in {DownloadErrorType.RETRY_LIMIT_EXCEEDED, DownloadErrorType.RETRYABLE, DownloadErrorType.BOT_FLAGGED}:
            self._log_exit_failure(error_type, exit_name)
            if self.failure_queue is not None:
                self.failure_queue.add_item(FailureStatus(
                    success=False,
                    exception_type=error_type.value,
                    exception_message=result.status.error_detail or '',
                ))
                multiplier = 2 ** self.failure_queue.size
            else:
                multiplier = 1
            if result.media_request.search_result.search_type != SearchType.DIRECT:
                self.set_wait_timestamp(backoff_multiplier=multiplier)
            return self.backoff_seconds_remaining

        # Terminal error — minimum wait, no failure item
        if result.media_request.search_result.search_type != SearchType.DIRECT:
            self.set_wait_timestamp()
        return self.backoff_seconds_remaining

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''
        Seconds remaining in the current backoff period, or None if no timestamp set.
        '''
        if self._wait_timestamp is None:
            return None
        return max(0, int(self._wait_timestamp - datetime.now(timezone.utc).timestamp()))

    @property
    def failure_summary(self) -> str:
        '''
        Human-readable summary of the failure queue.
        '''
        if self.failure_queue is None:
            return '0 failures in queue'
        return self.failure_queue.get_status_summary()

    # ------------------------------------------------------------------
    # Queue interface — backed by the subclass
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def has_direct_pending(self) -> bool:
        '''True when at least one DIRECT item is waiting to bypass backoff.'''

    @abstractmethod
    async def _enqueue_request(self, guild_id: int, media_request: MediaRequest,
                               priority: int | None = None) -> None:
        '''Route a MediaRequest to the correct input queue based on its search type.'''

    @abstractmethod
    async def _enqueue_deferred_request(self, guild_id: int, media_request: MediaRequest,
                                        ready_at: float) -> None:
        '''Hold a MediaRequest out of the input queue until ready_at (epoch seconds).

        Held requests must survive a pod restart wherever the input queue does —
        a retry parked in process memory on the Redis worker would silently
        vanish, which is exactly the failure the durable queue exists to prevent.
        '''

    @abstractmethod
    async def _promote_ready_retries(self) -> None:
        '''Move every deferred request whose ready_at has passed onto the input queue.

        Called once per consumer-loop iteration, so it runs at the idle poll rate
        per driver; implementations must cost ~one round trip when nothing is due.
        '''

    def _retry_delay_seconds(self, media_request: MediaRequest, retry_count: int) -> int:
        '''Seconds to hold this retry off the queue, doubling per attempt.

        Returns 0 for DIRECT items (not YouTube-rate-limited, and they bypass
        backoff everywhere else) and when the backoff is configured off, in which
        case the retry is re-queued immediately as it was before.
        '''
        if media_request.search_result.search_type == SearchType.DIRECT:
            return 0
        if self._retry_backoff_minimum <= 0:
            return 0
        delay = self._retry_backoff_minimum * (2 ** max(0, retry_count - 1))
        return int(min(delay, RETRY_BACKOFF_SECONDS_MAXIMUM))

    async def _requeue_after_retry(self, media_request: MediaRequest, delay_seconds: int) -> None:
        '''Re-queue a retried request, deferring it when a hold-off applies.'''
        if delay_seconds <= 0:
            await self._enqueue_request(media_request.guild_id, media_request)
            return
        ready_at = datetime.now(timezone.utc).timestamp() + delay_seconds
        await self._enqueue_deferred_request(media_request.guild_id, media_request, ready_at)

    @abstractmethod
    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown).'''

    @abstractmethod
    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''

    @abstractmethod
    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''

    @abstractmethod
    async def _dequeue_direct(self) -> MediaRequest:
        '''Dequeue the next DIRECT item, raising QueueEmpty if none available.'''

    @abstractmethod
    async def _merged_get_nowait(self) -> MediaRequest:
        '''
        Dequeue the next item across both queues ordered by submission timestamp,
        raising QueueEmpty if both are empty.
        '''

    @abstractmethod
    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''
        Wait until the backoff timestamp elapses, the shutdown event fires, or a
        DIRECT item becomes available.

        Raises ExitEarlyException if shutdown is signalled.
        Raises DirectItemAvailableException if a DIRECT item arrives during the wait.
        '''

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download.'''
        if media_request.span_context is None:
            media_request.span_context = capture_span_context()
        await self._enqueue_request(guild_id, media_request, priority=priority)

    async def get_input_nowait(self) -> MediaRequest:
        '''Return the next pending MediaRequest, raising QueueEmpty if none available.'''
        return await self._merged_get_nowait()

    async def _peek_next_request(self) -> MediaRequest:
        '''
        Poll for the next MediaRequest with client instrumentation suppressed.

        run() polls every _IDLE_POLL_BACKOFF_SECONDS whether or not work exists, so
        the redis spans this emits are almost entirely idle noise — with a driver per
        egress exit they were ~98% of the downloader's span volume, at a rate set by
        the poll interval rather than by anything happening.  Suppressing them keeps
        the trace backend describing real work instead of an empty queue.

        The suppression is scoped to the peek alone, so every span that describes a
        download survives: create_source, submit, the audio/upload spans, and the
        redis writes on the result path all run outside this call.  The backoff
        branch's _dequeue_direct() peeks stay instrumented — they only run while a
        backoff is active, which is rare and worth seeing.
        '''
        with suppress_instrumentation():
            return await self._merged_get_nowait()

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''
        Consumer loop: dequeues one MediaRequest, downloads it, and puts a
        DownloadResult onto the result queue.  Retryable errors are requeued
        without emitting to the result queue.  Intended to be driven by
        return_loop_runner as a background task.

        When no backoff is active, items are served from both queues in
        submission-timestamp order (DIRECT and non-DIRECT interleaved).

        When backoff is active, only DIRECT items are served immediately;
        non-DIRECT items wait for the backoff to expire.  A DIRECT item
        arriving mid-wait interrupts the wait via DirectItemAvailableException.
        '''
        # Deferred retries become eligible on wall-clock time, not on an event, so
        # something has to look. This is the only place that runs regardless of
        # which dequeue branch is taken below.
        await self._promote_ready_retries()
        if self.backoff_seconds_remaining:
            try:
                media_request = await self._dequeue_direct()
            except QueueEmpty:
                try:
                    await self.backoff_wait(shutdown_event)
                except DirectItemAvailableException:
                    media_request = await self._dequeue_direct()
                else:
                    try:
                        media_request = await self._peek_next_request()
                    except QueueEmpty:
                        # Idle: nothing ready after backoff — back off before the
                        # loop runner re-calls rather than busy-spinning.
                        await sleep(_IDLE_POLL_BACKOFF_SECONDS)
                        return
        else:
            try:
                media_request = await self._peek_next_request()
            except QueueEmpty:
                # Idle: no pending request — back off before re-poll instead of
                # busy-spinning every ~10ms (which throttled busy downloads too).
                await sleep(_IDLE_POLL_BACKOFF_SECONDS)
                return

        request_uuid = str(media_request.uuid)
        # IN_PROGRESS is pushed inside create_source once an exit is actually leased,
        # not here — else a request that finds every exit busy (NO_EXIT_AVAILABLE)
        # would be stamped IN_PROGRESS while it's really still queued, and under
        # sustained pool contention hundreds of waiting requests would show as
        # "in progress" and churn bundle renders.
        result = await self.create_source(media_request, self._max_retries)

        if result.status.error_type == DownloadErrorType.NO_EXIT_AVAILABLE:
            # Pure contention — every exit was busy, the item was never attempted and
            # never left QUEUED. Re-queue it unchanged (no retry_count bump, no RETRY
            # UI); create_source already yielded so this isn't a tight loop.
            await self._enqueue_request(media_request.guild_id, media_request)
            return

        if not result.status.success and result.status.error_type in {
            DownloadErrorType.RETRYABLE, DownloadErrorType.BOT_FLAGGED
        }:
            media_request.download_retry_information.retry_count += 1
            retry_delay = self._retry_delay_seconds(
                media_request, media_request.download_retry_information.retry_count)
            self.logger.info('Retryable error on "%s" (retry %d/%d in %ds): %s',
                             media_request, media_request.download_retry_information.retry_count,
                             self._max_retries, retry_delay, result.status.error_detail)
            self.logger.info('Failure queue: %s', self.failure_summary)
            if self._broker is not None:
                await self._broker.update_request_status(request_uuid, LifecycleStatusUpdate(
                    event=LifecycleEvent.RETRY,
                    error_detail=result.status.error_detail,
                    # This request's own hold-off, not the pod-global window: with
                    # the retry deferred, the wait is now a real per-request number
                    # rather than "whenever some exit frees".
                    backoff_seconds=retry_delay or self.backoff_seconds_remaining,
                    retry_count=media_request.download_retry_information.retry_count,
                    max_retries=self._max_retries,
                ))
            await self._requeue_after_retry(media_request, retry_delay)
            return

        # Report the finished result to the broker, which persists a successful
        # download (zone=AVAILABLE) and queues every result for the cog's
        # process_download_results router to drain via broker.next_result().
        # In single-process this is the in-memory broker; in HA it POSTs to the
        # broker pod.  Replaces the old download-client-owned result queue.
        if self._broker is not None:
            await self._broker.register_download_result(result)

    @staticmethod
    def _make_error_result(
        error_type: DownloadErrorType,
        media_request: MediaRequest,
        span_context: dict | None,
        error_detail: str,
        user_message: str | None = None,
    ) -> DownloadResult:
        '''Build a failed DownloadResult with no file data.'''
        return DownloadResult(
            status=DownloadStatus(success=False, error_type=error_type, user_message=user_message, error_detail=error_detail),
            media_request=media_request,
            ytdlp_data=None,
            file_name=None,
            span_context=span_context,
        )

    def __prepare_data_source(self, media_request: MediaRequest, max_retries: int, egress: DownloadEgress):
        '''
        Prepare source from youtube url

        media_request: Media Request from inputs
        max_retries: Max retries before throwing hands up
        egress: the DownloadEgress this download uses (client + exit)
        '''
        span_attributes = media_request_attributes(media_request)
        # Stamp the exit this download left from: the leased exit in a pool mode, or
        # the probe-discovered exit for the fixed http proxy.
        if egress.exit_name is not None:
            # The lease names the exit; the pool probe supplies its IP once resolved.
            # Both matter: the hostname says which relay was chosen, the IP is what
            # the origin actually saw, and only comparing the two catches a download
            # leaving from an address the lease did not intend.
            exit_hostname = egress.exit_name
            exit_ip = UNKNOWN_EXIT
            if self._pool_exit_ip_probe is not None:
                exit_ip = self._pool_exit_ip_probe.ip_for(egress.exit_name) or UNKNOWN_EXIT
        else:
            exit_hostname, exit_ip = cached_exit_attributes(self._exit_probe)
        span_attributes[AttributeNaming.EGRESS_HOSTNAME.value] = exit_hostname
        span_attributes[AttributeNaming.EGRESS_IP.value] = exit_ip
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.create_source', kind=SpanKind.CLIENT, attributes=span_attributes, links=span_links_from_context(media_request.span_context)) as span:
            span_context = capture_span_context()
            try:
                data = egress.client.extract_info(media_request.search_result.resolved_search_string, download=media_request.download_file)
            except MetadataCheckFailedException as error:
                span.record_exception(error)
                span.set_status(StatusCode.OK)
                error_type = DownloadErrorType.BANNED if isinstance(error, VideoBanned) else DownloadErrorType.TOO_LONG
                return self._make_error_result(error_type, media_request, span_context, str(error), user_message=error.user_message)
            except DownloadError as error:
                error_str = str(error)
                if 'Private video' in error_str:
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return self._make_error_result(DownloadErrorType.PRIVATE_VIDEO, media_request, span_context, error_str, user_message='Video is private, cannot download')
                if 'This video has been removed for violating' in error_str:
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return self._make_error_result(DownloadErrorType.TERMS_VIOLATION, media_request, span_context, error_str, user_message='Video is unvailable due to violating terms of service, cannot download')
                if 'Video unavailable' in error_str:
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return self._make_error_result(DownloadErrorType.UNAVAILABLE, media_request, span_context, error_str, user_message='Video is unavailable, cannot download')
                if 'Sign in to confirm your age. This video may be inappropriate for some users' in error_str:
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return self._make_error_result(DownloadErrorType.AGE_RESTRICTED, media_request, span_context, error_str, user_message='Video is age restricted, cannot download')
                if 'Requested format is not available' in error_str:
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return self._make_error_result(DownloadErrorType.INVALID_FORMAT, media_request, span_context, error_str, user_message='Video is not available in requested format')
                if 'Sign in to confirm you' in error_str and 'not a bot' in error_str:
                    span.record_exception(error)
                    if media_request.download_retry_information.retry_count + 1 >= max_retries:
                        span.set_status(StatusCode.ERROR)
                        return self._make_error_result(DownloadErrorType.RETRY_LIMIT_EXCEEDED, media_request, span_context, error_str)
                    span.set_status(StatusCode.OK)
                    return self._make_error_result(DownloadErrorType.BOT_FLAGGED, media_request, span_context, error_str)
                # Fallback
                span.record_exception(error)
                if media_request.download_retry_information.retry_count + 1 >= max_retries:
                    span.set_status(StatusCode.ERROR)
                    return self._make_error_result(DownloadErrorType.RETRY_LIMIT_EXCEEDED, media_request, span_context, error_str)
                span.set_status(StatusCode.OK)
                return self._make_error_result(DownloadErrorType.RETRYABLE, media_request, span_context, error_str)
            # Make sure we get the first media_request here
            # Since we don't pass "url" directly anymore
            try:
                data = data['entries'][0]
            except IndexError as error:
                span.set_status(StatusCode.OK)
                span.record_exception(error)
                return self._make_error_result(DownloadErrorType.NOT_FOUND, media_request, span_context, str(error), user_message=f'No videos found for search "{str(media_request)}"')
            # Key Error if a single video is passed
            except KeyError:
                pass

            file_path = None
            if media_request.download_file:
                try:
                    file_path = Path(data['requested_downloads'][0]['filepath'])
                    if not file_path.exists():
                        file_path = None
                except (KeyError, IndexError):
                    file_path = None
                if file_path is None:
                    span.set_status(StatusCode.ERROR)
                    return self._make_error_result(DownloadErrorType.FILE_NOT_FOUND, media_request, span_context, 'No file path returned from download')
                file_size_bytes = file_path.stat().st_size
                # bandit B324: corruption check against yt-dlp's reported MD5, not used for security
                computed_md5 = hashlib.md5(file_path.read_bytes(), usedforsecurity=False).hexdigest()
                ytdlp_md5 = data.get('requested_downloads', [{}])[0].get('md5')
                if ytdlp_md5 and ytdlp_md5 != computed_md5:
                    self.logger.warning('Checksum mismatch after yt-dlp download: expected=%s actual=%s file=%s', ytdlp_md5, computed_md5, file_path)
            return DownloadResult(status=DownloadStatus(success=True), media_request=media_request, ytdlp_data=data, file_name=file_path, file_size_bytes=file_size_bytes if media_request.download_file else None, span_context=span_context)

    def __upload_s3(self, file_path: Path):
        if not self.bucket_name:
            return file_path
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.upload_s3', kind=SpanKind.CLIENT):
            # Key is deterministic per video so the same video always maps to the
            # same S3 object, enabling cache reuse across requests.
            # With a single downloader instance this is fine, but if multiple
            # downloaders run concurrently they could race to upload the same key.
            # If concurrency is ever added, consider a download queue or a
            # check-then-skip pattern (list_objects before uploading).
            s3_key = f'cache/{file_path.name}'
            upload_file(self.bucket_name, file_path, s3_key)
            file_path.unlink()
            file_path = Path(s3_key)
        return file_path

    async def create_source(self, media_request: MediaRequest, max_retries: int) -> DownloadResult:
        '''
        Acquire an egress (a client + exit) for this download, run it, and release
        the egress afterwards.  Returns a RETRYABLE result if no exit is available.
        '''
        # DIRECT items aren't YouTube-rate-limited, so they reserve an exit
        # unconditionally; only YouTube items claim the per-exit window (cross-pod
        # exclusion + spacing/backoff) at reserve time.
        reserve = (
            self._reserve_direct_exit
            if media_request.search_result.search_type == SearchType.DIRECT
            else self._reserve_youtube_exit
        )
        egress: DownloadEgress | None = await self._egress.acquire(reserve)
        if egress is None:
            # Pool mode with every exit in-flight or backed off (HttpProxyEgress never
            # returns None). Returning RETRYABLE requeues the item, but the driver loop
            # re-pops immediately — so yield briefly here rather than tight-spinning
            # pop -> reserve-fail -> requeue until an exit frees.
            await sleep(_IDLE_POLL_BACKOFF_SECONDS)
            return self._make_error_result(
                DownloadErrorType.NO_EXIT_AVAILABLE, media_request, None, 'No egress exit available')
        if egress.exit_name is not None:
            # Pool mode: name the exit this download leaves from (the span carries
            # it too, but this is greppable without a tracing backend).
            self.logger.info('Download egress via exit %s', egress.exit_name)
        # Mark IN_PROGRESS only now — an exit is leased and the download is about to
        # run, so a contended (NO_EXIT_AVAILABLE) request never reaches this and stays
        # QUEUED in the bundle.
        if self._broker is not None:
            await self._broker.update_request_status(
                str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
            )
        try:
            return await self._create_source(media_request, max_retries, egress)
        finally:
            self._egress.release(egress)

    async def _create_source(self, media_request: MediaRequest, max_retries: int, egress: DownloadEgress) -> DownloadResult:
        '''
        Download through an acquired egress + post-process. Calls update_tracking on
        the result; PCM conversion runs after it so the backoff timer reflects
        download time only.
        '''
        loop = asyncio.get_running_loop()
        # Isolate concurrent (pool-mode) downloads: two downloads of the SAME video
        # otherwise share the per-video scratch path (%(id)s) and clobber each
        # other — one unlinks the file mid-convert. Redirect this download's home to
        # a per-request subdir; the filename stays per-video so the S3 cache key is
        # unchanged. Safe to mutate the leased client's paths — an exit is held by
        # one download at a time.
        scratch_home = None
        if self._egress.is_pool:
            scratch_home = self._download_dir / str(media_request.uuid)
            scratch_home.mkdir(parents=True, exist_ok=True)
            egress.client.params['paths'] = {'home': str(scratch_home)}
        try:
            to_run = partial(self.__prepare_data_source, media_request=media_request,
                             max_retries=max_retries, egress=egress)
            result = await loop.run_in_executor(None, to_run)
            await self.update_tracking(result, egress.exit_name)
            if result.status.success and result.file_name is not None:
                try:
                    pcm_path = await loop.run_in_executor(None, edit_audio_file, result.file_name, self.normalize_audio, self.logging_config)
                    post_process_timestamp = datetime.now(timezone.utc)
                    self.logger.info(
                        'Audio post-processing complete: file=%s download_ts=%s post_process_ts=%s',
                        pcm_path, result.download_timestamp, post_process_timestamp,
                    )
                    result = result.model_copy(update={
                        'file_name': pcm_path,
                        # The PCM is what gets uploaded and cached, so the cache
                        # must size THAT, not the compressed download it came
                        # from. s16le/48k stereo runs ~12x the size of a 128 kbps
                        # source; leaving the pre-conversion size here made
                        # max_cache_size_mb evict against a number an order of
                        # magnitude too small, so the bucket ran ~12x its cap.
                        'file_size_bytes': pcm_path.stat().st_size,
                        'post_process_timestamp': post_process_timestamp,
                    })
                except AudioProcessingError as error:
                    self.logger.warning('Audio processing failed for %s', result.file_name)
                    result = result.model_copy(update={
                        'status': DownloadStatus(
                            success=False,
                            error_type=DownloadErrorType.RETRYABLE,
                            user_message='Audio processing failed for download',
                            error_detail=str(error),
                        ),
                    })
                # Finally upload result to s3 and update the filepath
                result.file_name = await loop.run_in_executor(None, self.__upload_s3, result.file_name)
            return result
        finally:
            # In S3 mode the media was uploaded and its local copy unlinked, so the
            # per-request subdir holds only leftover scratch — remove it. (Non-S3 dev
            # mode keeps the file in download_dir and never opens a subdir.)
            if scratch_home is not None and self.bucket_name:
                await loop.run_in_executor(None, partial(shutil.rmtree, scratch_home, ignore_errors=True))
