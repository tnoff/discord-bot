from functools import partial
from pathlib import Path
from shutil import copyfile
from typing import Callable, List

from opentelemetry.trace.status import StatusCode
from opentelemetry.trace import SpanKind
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

from discord_bot.database import VideoCache

from discord_bot.cogs.music_helpers.source_dict import SourceDict, source_dict_attributes
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.utils.otel import otel_span_wrapper

class DownloadClientException(Exception):
    '''
    Generic class for download client errors
    '''
    def __init__(self, message, user_message=None):
        self.message = message
        super().__init__(self.message)
        self.user_message = user_message

class VideoNotFoundException(DownloadClientException):
    '''
    When no videos are found
    '''

class MetadataCheckFailedException(DownloadClientException):
    '''
    Video failed metadata checked
    '''

class VideoAgeRestrictedException(DownloadClientException):
    '''
    Video has age restrictions, cannot download
    '''

class VideoUnavailableException(DownloadClientException):
    '''
    Video Unavailable while downloading
    '''

class PrivateVideoException(DownloadClientException):
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

class BotDownloadFlagged(DownloadClientException):
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
        if duration and max_video_length and duration > max_video_length:
            raise VideoTooLong('Video Too Long', user_message=f'Video duration {duration} exceeds max length of {max_video_length}, skipping')
        vid_url = info.get('webpage_url')
        if vid_url and banned_videos_list:
            for banned_url in banned_videos_list:
                if vid_url == banned_url:
                    raise VideoBanned('Video Banned', user_message=f'Video url "{vid_url}" is banned, skipping')
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
        self.ytdl = ytdl
        self.download_dir = download_dir

    def __prepare_data_source(self, source_dict: SourceDict):
        '''
        Prepare source from youtube url
        '''
        span_attributes = source_dict_attributes(source_dict)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.create_source', kind=SpanKind.CLIENT, attributes=span_attributes) as span:
            try:
                data = self.ytdl.extract_info(source_dict.search_string, download=source_dict.download_file)
            except MetadataCheckFailedException as error:
                span.record_exception(error)
                span.set_status(StatusCode.OK)
                raise
            except DownloadError as error:
                if 'Private video' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise PrivateVideoException('Video is private', user_message=f'Video from search "{str(source_dict)}" is unvailable, cannot download') from error
                if 'Video unavailable' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise VideoUnavailableException('Video is unavailable', user_message=f'Video from search "{str(source_dict)}" is unavailable, cannot download') from error
                if 'Sign in to confirm your age. This video may be inappropriate for some users' in str(error):
                    span.set_status(StatusCode.OK)
                    span.record_exception(error)
                    raise VideoAgeRestrictedException('Video Aged restricted', user_message=f'Video from search "{str(source_dict)}" is age restricted, cannot download') from error
                if 'Sign in to confirm you'in str(error) and 'not a bot' in str(error):
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(error)
                    raise BotDownloadFlagged('Bot flagged download', user_message=f'Video from search "{str(source_dict)}" flagged as bot download, skipping') from error
                span.set_status(StatusCode.ERROR)
                span.record_exception(error)
                raise
            # Make sure we get the first source_dict here
            # Since we don't pass "url" directly anymore
            try:
                data = data['entries'][0]
            except IndexError as error:
                raise VideoNotFoundException('No videos found', user_message=f'No videos found for searhc "{str(source_dict)}"') from error
            # Key Error if a single video is passed
            except KeyError:
                pass

            file_path = None
            if source_dict.download_file:
                try:
                    file_path = Path(data['requested_downloads'][0]['filepath'])
                    if not file_path.exists():
                        file_path = None
                except (KeyError, IndexError):
                    file_path = None
                # Move file to download dir after finished
                new_path = self.download_dir / file_path.name
                # Rename might not work if file on diff filesystem
                copyfile(str(file_path), str(new_path))
                file_path.unlink()
                file_path = new_path
            span.set_status(StatusCode.OK)
            return SourceDownload(file_path, data, source_dict)

    async def create_source(self, source_dict: SourceDict, loop):
        '''
        Download data from youtube search
        '''
        to_run = partial(self.__prepare_data_source, source_dict=source_dict)
        return await loop.run_in_executor(None, to_run)
