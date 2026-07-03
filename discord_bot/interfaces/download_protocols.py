'''
Download engine base + cog-facing DownloadClient Protocol.

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
from time import time
from typing import Callable, List, Protocol

from opentelemetry.trace.status import StatusCode
from opentelemetry.trace import SpanKind
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.interfaces.broker_protocols import BrokerClient
from discord_bot.utils.audio import edit_audio_file, AudioProcessingError
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.types.download import (
    DownloadErrorType, LifecycleEvent, DownloadResult, DownloadStatus, LifecycleStatusUpdate,
)
from discord_bot.utils.failure_queue import FailureQueue, FailureStatus
from discord_bot.utils.integrations.s3 import upload_file
from discord_bot.utils.otel import capture_span_context, otel_span_wrapper, span_links_from_context
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
YTDLP_OUTPUT_TEMPLATE = '%(extractor)s.%(id)s.%(ext)s'
# bandit B104: yt-dlp's source-address config, not a server bind; '0.0.0.0' lets the OS pick (avoids ipv6 issues)
YTDLP_SOURCE_ADDRESS = '0.0.0.0'  # nosec B104

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


class DownloadClient(Protocol):
    '''
    Cog-facing handle for the download pipeline.

    The producer surface (submit / block_guild / clear_guild_queue /
    queue_size) is synchronous; the cog drives the consumer via run() as a
    background loop.  failure_summary / backoff_seconds_remaining expose the
    worker's backoff state for logging and metrics.
    '''
    def submit(self, guild_id: int, media_request: MediaRequest,
               priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download; results are reported to the broker.'''

    def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''

    def clear_guild_queue(self, guild_id: int,
                          preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                          ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''

    def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the download failure queue.'''

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff period, or None if not set.'''

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''Consume one queued request and download it; driven as a background loop.'''


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
            'outtmpl': str(download_dir / f'{YTDLP_OUTPUT_TEMPLATE}'),
        }
        if extra_ytdlp_options:
            for key, value in extra_ytdlp_options.items():
                ytdlopts[key] = value
        if max_video_length or banned_video_list:
            ytdlopts['match_filter'] = match_generator(max_video_length, banned_video_list)
        self.ytdl = YoutubeDL(ytdlopts)
        self._broker = broker
        self._max_retries = max_retries
        self.failure_queue: FailureQueue | None = failure_queue
        self._wait_period_minimum = wait_period_minimum
        self._wait_period_max_variance = wait_period_max_variance
        self._wait_timestamp: float | None = None
        self.bucket_name: str | None = bucket_name
        self.normalize_audio: bool = normalize_audio
        self.logger = get_logger('download_client', logging_config)
        self.logging_config = logging_config

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

    def update_tracking(self, result: DownloadResult) -> int | None:
        '''
        Update failure queue and backoff timestamp based on a DownloadResult.
        Returns backoff_seconds_remaining so callers need not re-query.
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
    def _enqueue_request(self, guild_id: int, media_request: MediaRequest,
                         priority: int | None = None) -> None:
        '''Route a MediaRequest to the correct input queue based on its search type.'''

    @abstractmethod
    def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown).'''

    @abstractmethod
    def clear_guild_queue(self, guild_id: int,
                          preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                          ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''

    @abstractmethod
    def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''

    @abstractmethod
    def _dequeue_direct(self) -> MediaRequest:
        '''Dequeue the next DIRECT item, raising QueueEmpty if none available.'''

    @abstractmethod
    def _merged_get_nowait(self) -> MediaRequest:
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

    def submit(self, guild_id: int, media_request: MediaRequest,
               priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download.'''
        if media_request.span_context is None:
            media_request.span_context = capture_span_context()
        self._enqueue_request(guild_id, media_request, priority=priority)

    def get_input_nowait(self) -> MediaRequest:
        '''Return the next pending MediaRequest, raising QueueEmpty if none available.'''
        return self._merged_get_nowait()

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
        await sleep(0.01)
        if self.backoff_seconds_remaining:
            try:
                media_request = self._dequeue_direct()
            except QueueEmpty:
                try:
                    await self.backoff_wait(shutdown_event)
                except DirectItemAvailableException:
                    media_request = self._dequeue_direct()
                else:
                    try:
                        media_request = self._merged_get_nowait()
                    except QueueEmpty:
                        return
        else:
            try:
                media_request = self._merged_get_nowait()
            except QueueEmpty:
                return

        request_uuid = str(media_request.uuid)
        if self._broker is not None:
            await self._broker.update_request_status(
                request_uuid, LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
            )
        result = await self.create_source(media_request, self._max_retries)

        if not result.status.success and result.status.error_type in {
            DownloadErrorType.RETRYABLE, DownloadErrorType.BOT_FLAGGED
        }:
            media_request.download_retry_information.retry_count += 1
            self.logger.info('Retryable error on "%s": %s', media_request, result.status.error_detail)
            self.logger.info('Failure queue: %s', self.failure_summary)
            if self._broker is not None:
                await self._broker.update_request_status(request_uuid, LifecycleStatusUpdate(
                    event=LifecycleEvent.RETRY,
                    error_detail=result.status.error_detail,
                    backoff_seconds=self.backoff_seconds_remaining,
                ))
            self._enqueue_request(media_request.guild_id, media_request)
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

    def __prepare_data_source(self, media_request: MediaRequest, max_retries: int):
        '''
        Prepare source from youtube url

        media_request: Media Request from inputs
        max_retries: Max retries before throwing hands up
        '''
        span_attributes = media_request_attributes(media_request)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.create_source', kind=SpanKind.CLIENT, attributes=span_attributes, links=span_links_from_context(media_request.span_context)) as span:
            span_context = capture_span_context()
            try:
                data = self.ytdl.extract_info(media_request.search_result.resolved_search_string, download=media_request.download_file)
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
        Download data from youtube search. Automatically calls update_tracking on the result.
        PCM conversion runs after update_tracking so the backoff timer reflects download time only.
        '''
        loop = asyncio.get_running_loop()
        to_run = partial(self.__prepare_data_source, media_request=media_request, max_retries=max_retries)
        result = await loop.run_in_executor(None, to_run)
        self.update_tracking(result)
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
