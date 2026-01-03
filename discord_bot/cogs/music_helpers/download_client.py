from asyncio import QueueFull
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from functools import partial
from math import exp
from pathlib import Path
from shutil import copyfile
from typing import Callable, List

from opentelemetry.trace.status import StatusCode
from opentelemetry.trace import SpanKind
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.database import VideoCache

from discord_bot.cogs.music_helpers.media_request import MediaRequest, media_request_attributes
from discord_bot.cogs.music_helpers.media_download import MediaDownload
from discord_bot.utils.otel import otel_span_wrapper
from discord_bot.utils.queue import Queue


@dataclass
class DownloadFailureMode:
    '''
    Download Failure Mode, each individual case
    '''
    exception_type: str
    exception_message: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DownloadFailureQueue:
    '''
    Download Failure Rate Tracking
    '''
    def __init__(self, max_size: int = 100, max_age_seconds: int = 300,
                 base_wait_seconds: int = 300, decay_tau_seconds: int = 60,
                 max_backoff_factor: float = 3.0, aggressiveness: float = 1.0):
        '''
        Download failure queue to track how often failures have been happening

        max_size : Track the last X items
        max_age_seconds : Maximum age of failures to keep (in seconds)
        base_wait_seconds: Base backoff wait
        # should usually be 1/3 to 1/5 of max_age_seconds window.
        decay_tau_seconds: Exponential decay constant, simply "How quickly should old failures fade out"
        max_backoff_factor: Cap on backoff multiplier
        aggressiveness: Aggressiveness of factor curve, simply "Given a certain failure score, how fast should we approach the max backoff?"
        '''
        # Validate parameters
        if decay_tau_seconds <= 0:
            raise ValueError("decay_tau_seconds must be positive")
        if max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        if max_backoff_factor < 1.0:
            raise ValueError("max_backoff_factor must be >= 1.0")
        if aggressiveness <= 0:
            raise ValueError("aggressiveness must be positive")

        self.queue: Queue[DownloadFailureMode] = Queue(maxsize=max_size)
        self.max_age_seconds = max_age_seconds

        self.base_wait = base_wait_seconds
        self.decay_tau = decay_tau_seconds
        self.max_factor = max_backoff_factor
        self.k = aggressiveness

    def add_item(self, new_item: DownloadFailureMode) -> bool:
        '''
        Add new item and clean old entries
        '''
        # Clean old items before adding new one
        self._clean_old_items()

        while True:
            try:
                self.queue.put_nowait(new_item)
                return True
            except QueueFull:
                self.queue.get_nowait()

    def _clean_old_items(self):
        '''
        Remove items older than max_age_seconds
        '''
        if self.max_age_seconds <= 0:
            return

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self.max_age_seconds)

        # Get all items, filter out old ones, and rebuild queue
        items = self.queue.clear()
        fresh_items = [item for item in items if item.created_at > cutoff]

        for item in fresh_items:
            try:
                self.queue.put_nowait(item)
            except QueueFull:
                break

    @property
    def size(self):
        '''
        Size of queue
        '''
        return self.queue.size()

    def get_backoff_multiplier(self) -> float:
        '''
        Calculate backoff multiplier based on failure rate relative to wait period

        Returns a multiplier
        '''
        now = datetime.now(timezone.utc)
        score = 0.0

        # Use items() method to safely iterate over queue contents
        for failure in self.queue.items():
            age = (now - failure.created_at).total_seconds()
            score += exp(-age / self.decay_tau)

        factor = 1.0 + (self.max_factor - 1.0) * (
            1.0 - exp(-self.k * score)
        )

        return min(self.max_factor, factor)


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

class ExistingFileException(Exception):
    '''
    Throw when existing file found
    '''
    def __init__(self, message, video_cache: VideoCache = None):
        self.message = message
        super().__init__(message)
        self.video_cache = video_cache


OTEL_SPAN_PREFIX = 'music.download_client'

def match_generator(max_video_length: int, banned_videos_list: List[str], video_cache_search: Callable = None):
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
        # Check if video exists within cache, and raise
        extractor = info.get('extractor')
        vid_id = info.get('id')
        if video_cache_search:
            result = video_cache_search(extractor, vid_id)
            if result:
                raise ExistingFileException('File already downloaded', video_cache=result)

    return filter_function

class DownloadClient():
    '''
    Download Client using yt-dlp
    '''
    def __init__(self, ytdl: YoutubeDL, download_dir: Path):
        '''
        Init download client

        ytdl : YoutubeDL Client
        download_dir : Directory to place after tempfile download
        '''
        self.ytdl: YoutubeDL = ytdl
        self.download_dir: Path = download_dir

    def __prepare_data_source(self, media_request: MediaRequest):
        '''
        Prepare source from youtube url
        '''
        span_attributes = media_request_attributes(media_request)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.create_source', kind=SpanKind.CLIENT, attributes=span_attributes) as span:
            try:
                data = self.ytdl.extract_info(media_request.search_string, download=media_request.download_file)
            except MetadataCheckFailedException as error:
                span.record_exception(error)
                span.set_status(StatusCode.OK)
                raise
            except DownloadError as error:
                if 'Private video' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise PrivateVideoException('Video is private', user_message='Video is private, cannot download') from error
                if 'This video has been removed for violating' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise VideoViolatedTermsException('Video taken down', user_message='Video is unvailable due to violating terms of service, cannot download') from error
                if 'Video unavailable' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise VideoUnavailableException('Video is unavailable', user_message='Video is unavailable, cannot download') from error
                if 'Sign in to confirm your age. This video may be inappropriate for some users' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise VideoAgeRestrictedException('Video Aged restricted', user_message='Video is age restricted, cannot download') from error
                if 'Requested format is not available' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise InvalidFormatException('Video format not available', user_message='Video is not available in requested format') from error
                if 'Sign in to confirm you'in str(error) and 'not a bot' in str(error):
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(error)
                    media_request.retry_count += 1
                    raise BotDownloadFlagged('Bot flagged download', media_request=media_request) from error
                span.set_status(StatusCode.ERROR)
                span.record_exception(error)
                media_request.retry_count += 1
                raise RetryableException('Can retry media download', media_request=media_request) from error
            # Make sure we get the first media_request here
            # Since we don't pass "url" directly anymore
            try:
                data = data['entries'][0]
            except IndexError as error:
                raise VideoNotFoundException('No videos found', user_message=f'No videos found for search "{str(media_request)}"') from error
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
                # Move file to download dir after finished
                new_path = self.download_dir / file_path.name
                # Rename might not work if file on diff filesystem
                try:
                    copyfile(str(file_path), str(new_path))
                    file_path.unlink()
                    file_path = new_path
                except FileNotFoundError as e:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(e)
                    return None
            span.set_status(StatusCode.OK)
            return MediaDownload(file_path, data, media_request)

    async def create_source(self, media_request: MediaRequest, loop):
        '''
        Download data from youtube search
        '''
        to_run = partial(self.__prepare_data_source, media_request=media_request)
        return await loop.run_in_executor(None, to_run)
