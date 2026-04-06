import asyncio
from asyncio import QueueEmpty, sleep
from datetime import datetime, timezone
from functools import partial
import hashlib
from pathlib import Path
import random
from time import time
from typing import Callable, List

from opentelemetry.trace.status import StatusCode
from opentelemetry.trace import SpanKind
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.audio import edit_audio_file, AudioProcessingError
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.types.download import (
    DownloadErrorType, DownloadEvent, DownloadResult, DownloadStatus, DownloadStatusUpdate,
)
from discord_bot.utils.distributed_queue import DistributedQueue
from discord_bot.utils.failure_queue import FailureQueue, FailureStatus
from discord_bot.utils.integrations.s3 import upload_file
from discord_bot.utils.otel import capture_span_context, otel_span_wrapper, span_links_from_context
from discord_bot.utils.common import get_logger, LoggingConfig

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

class DownloadClient():
    '''
    Download Client using yt-dlp
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
        broker=None,
        max_retries: int = 3,
        queue_max_size: int = 100,
    ):
        '''
        Init download client

        ytdl : YoutubeDL Client
        failure_queue : Optional FailureQueue for tracking download failures
        wait_period_minimum : Minimum backoff wait time in seconds
        wait_period_max_variance : Maximum extra random variance in seconds
        bucket_name : S3 bucket to upload to immediately after download;
                      when set the local file is deleted and DownloadResult.file_name
                      holds the S3 object key instead of a local path
        broker : MediaBroker for lifecycle status updates; optional for backwards compatibility
        max_retries : Maximum download retries before returning RETRY_LIMIT_EXCEEDED
        queue_max_size : Per-guild capacity for the input and result queues
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
            'source_address': '0.0.0.0',  # ipv6 addresses cause issues sometimes
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
        self._input_queue: DistributedQueue[MediaRequest] = DistributedQueue(queue_max_size)
        self._result_queue: DistributedQueue[DownloadResult] = DistributedQueue(queue_max_size)
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
        new_timestamp = new_timestamp + (random.randint(1000, self._wait_period_max_variance * 1000) / 1000)
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
            self.set_wait_timestamp(backoff_multiplier=multiplier)
            return self.backoff_seconds_remaining

        # Terminal error — minimum wait, no failure item
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

    async def backoff_wait(self, shutdown_event: asyncio.Event) -> None:
        '''
        Wait until the backoff timestamp elapses or the shutdown event fires.

        Raises ExitEarlyException (imported by caller) if shutdown is signalled.
        Instead of importing ExitEarlyException here, we re-raise via the caller
        after returning — callers check shutdown_event themselves after this returns.
        Actually, we mirror the existing youtube_backoff_time logic: raise on shutdown.
        We import lazily to avoid circular imports.
        '''
        if self._wait_timestamp is None:
            return

        now = datetime.now(timezone.utc).timestamp()
        sleep_duration = max(0, self._wait_timestamp - now)

        if shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')

        if sleep_duration == 0:
            return

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=sleep_duration,
            )
            raise ExitEarlyException('Exiting bot wait loop')
        except asyncio.TimeoutError:
            return

    # ------------------------------------------------------------------
    # Queue interface
    # ------------------------------------------------------------------

    def submit(self, guild_id: int, media_request: MediaRequest,
               priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download.'''
        if media_request.span_context is None:
            media_request.span_context = capture_span_context()
        self._input_queue.put_nowait(guild_id, media_request, priority=priority)

    def result_queue_depth(self) -> int:
        '''Total number of completed results waiting to be processed across all guilds.'''
        return sum(item.queue.size() for item in self._result_queue.queues.values())

    def get_result_nowait(self) -> DownloadResult:
        '''
        Return the next completed DownloadResult, raising QueueEmpty if none available.
        Results include both successes and terminal failures.
        '''
        return self._result_queue.get_nowait()

    def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown).'''
        return self._input_queue.block(guild_id)

    def clear_guild_queue(self, guild_id: int,
                          preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                          ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''
        return self._input_queue.clear_queue(guild_id, preserve_predicate=preserve_predicate)

    def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''
        return self._input_queue.size(guild_id) or 0

    def get_input_nowait(self) -> MediaRequest:
        '''Return the next pending MediaRequest, raising QueueEmpty if none available.'''
        return self._input_queue.get_nowait()

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''
        Consumer loop: waits for any active backoff, then dequeues one
        MediaRequest, downloads it, and puts a DownloadResult onto the
        result queue.  Retryable errors are requeued without emitting to
        the result queue.  Intended to be driven by return_loop_runner as
        a background task.

        Backoff wait is done BEFORE dequeuing so that a shutdown during
        the wait never discards an item from the queue.
        '''
        await sleep(0.01)
        await self.backoff_wait(shutdown_event)
        try:
            media_request = self._input_queue.get_nowait()
        except QueueEmpty:
            return

        request_uuid = str(media_request.uuid)
        if self._broker is not None:
            self._broker.update_request_status(
                request_uuid, DownloadStatusUpdate(event=DownloadEvent.IN_PROGRESS)
            )
        result = await self.create_source(media_request, self._max_retries)

        if not result.status.success and result.status.error_type in {
            DownloadErrorType.RETRYABLE, DownloadErrorType.BOT_FLAGGED
        }:
            media_request.download_retry_information.retry_count += 1
            self.logger.info('Retryable error on "%s": %s', media_request, result.status.error_detail)
            self.logger.info('Failure queue: %s', self.failure_summary)
            if self._broker is not None:
                self._broker.update_request_status(request_uuid, DownloadStatusUpdate(
                    event=DownloadEvent.RETRY,
                    error_detail=result.status.error_detail,
                    backoff_seconds=self.backoff_seconds_remaining,
                ))
            self._input_queue.put_nowait(media_request.guild_id, media_request)
            return

        self._result_queue.put_nowait(media_request.guild_id, result)

    def __prepare_data_source(self, media_request: MediaRequest, max_retries: int):
        '''
        Prepare source from youtube url

        media_request: Media Request from inputs
        max_retries: Max retries before throwing hands up
        '''
        span_attributes = media_request_attributes(media_request)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.create_source', kind=SpanKind.CLIENT, attributes=span_attributes, links=span_links_from_context(media_request.span_context)) as span:
            try:
                data = self.ytdl.extract_info(media_request.search_result.resolved_search_string, download=media_request.download_file)
            except MetadataCheckFailedException as error:
                span.record_exception(error)
                span.set_status(StatusCode.OK)
                error_type = DownloadErrorType.BANNED if isinstance(error, VideoBanned) else DownloadErrorType.TOO_LONG
                return DownloadResult(status=DownloadStatus(success=False, error_type=error_type, user_message=error.user_message, error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
            except DownloadError as error:
                if 'Private video' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.PRIVATE_VIDEO, user_message='Video is private, cannot download', error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                if 'This video has been removed for violating' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.TERMS_VIOLATION, user_message='Video is unvailable due to violating terms of service, cannot download', error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                if 'Video unavailable' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.UNAVAILABLE, user_message='Video is unavailable, cannot download', error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                if 'Sign in to confirm your age. This video may be inappropriate for some users' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.AGE_RESTRICTED, user_message='Video is age restricted, cannot download', error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                if 'Requested format is not available' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.INVALID_FORMAT, user_message='Video is not available in requested format', error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                if 'Sign in to confirm you' in str(error) and 'not a bot' in str(error):
                    span.record_exception(error)
                    if media_request.download_retry_information.retry_count + 1 >= max_retries:
                        span.set_status(StatusCode.ERROR)
                        return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRY_LIMIT_EXCEEDED, error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                    span.set_status(StatusCode.OK)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                # Fallback
                span.record_exception(error)
                if media_request.download_retry_information.retry_count + 1 >= max_retries:
                    span.set_status(StatusCode.ERROR)
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRY_LIMIT_EXCEEDED, error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
                span.set_status(StatusCode.OK)
                return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRYABLE, error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
            # Make sure we get the first media_request here
            # Since we don't pass "url" directly anymore
            try:
                data = data['entries'][0]
            except IndexError as error:
                span.set_status(StatusCode.OK)
                span.record_exception(error)
                return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.NOT_FOUND, user_message=f'No videos found for search "{str(media_request)}"', error_detail=str(error)), media_request=media_request, ytdlp_data=None, file_name=None)
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
                    return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.FILE_NOT_FOUND, error_detail='No file path returned from download'), media_request=media_request, ytdlp_data=None, file_name=None)
                file_size_bytes = file_path.stat().st_size
                computed_md5 = hashlib.md5(file_path.read_bytes()).hexdigest()
                ytdlp_md5 = data.get('requested_downloads', [{}])[0].get('md5')
                if ytdlp_md5 and ytdlp_md5 != computed_md5:
                    self.logger.warning('Checksum mismatch after yt-dlp download: expected=%s actual=%s file=%s', ytdlp_md5, computed_md5, file_path)
            return DownloadResult(status=DownloadStatus(success=True), media_request=media_request, ytdlp_data=data, file_name=file_path, file_size_bytes=file_size_bytes if media_request.download_file else None)

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
