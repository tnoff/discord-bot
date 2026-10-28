'''
Stand-ins for the search and download workers, for tests of the music request pipeline.

Each ``yield_*`` returns a class to patch in where the real client or worker would be built.
'''
from pathlib import Path
from typing import List

from aiohttp import ClientConnectionError

from discord_core.clients.search_client import SearchException
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchCollection, SearchResult

from tests.fakes.asyncio_download_worker import AsyncioDownloadWorker


def yield_fake_search_client(media_request: MediaRequest = None):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            if media_request:
                # Convert MediaRequest to SearchResult
                search_result = SearchResult(
                    search_type=media_request.search_result.search_type,
                    raw_search_string=media_request.search_result.raw_search_string
                )
                return SearchCollection(search_results=[search_result])
            return SearchCollection(search_results=[])

    return FakeSearchClient

def yield_fake_download_worker(media_download: MediaDownload):

    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(
                None,
                Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            if media_download is None:
                result = DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.UNAVAILABLE, user_message='No result'), media_request=media_request, ytdlp_data=None, file_name=None)
            else:
                ytdlp_data = {
                    'id': media_download.id,
                    'title': media_download.title,
                    'webpage_url': media_download.webpage_url,
                    'uploader': media_download.uploader,
                    'duration': media_download.duration,
                    'extractor': media_download.extractor,
                }
                result = DownloadResult(status=DownloadStatus(success=True), media_request=media_request, ytdlp_data=ytdlp_data, file_name=media_download.file_path)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker

def yield_download_worker_download_exception():
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(
                None,
                Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.UNAVAILABLE, user_message='whoopsie'), media_request=media_request, ytdlp_data=None, file_name=None)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker

def yield_download_worker_download_error():
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(
                None,
                Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRYABLE), media_request=media_request, ytdlp_data=None, file_name=None)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker

def yield_search_client_check_source(source_dict_list: List[MediaRequest]):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            # Convert MediaRequest list to SearchResult list
            search_results = []
            for media_request in source_dict_list:
                search_result = SearchResult(
                    search_type=media_request.search_result.search_type,
                    raw_search_string=media_request.search_result.raw_search_string
                )
                search_results.append(search_result)
            return SearchCollection(search_results=search_results)

    return FakeSearchClient

def yield_search_client_check_source_raises():
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            raise SearchException('foo', user_message='woopsie')

    return FakeSearchClient

def yield_search_client_check_source_raises_transport():
    '''
    check_source failing the way the search pod failing looks.

    Not a SearchException: since the media_search cutover check_source is an HTTP
    round trip, so a pod that is rolling, draining or gone raises out of aiohttp
    instead, which is the exact class of failure the SearchException clause was
    never written to catch.
    '''
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            raise ClientConnectionError('search pod is not accepting connections')

    return FakeSearchClient


def _worker_returning(make_result):
    '''A download worker whose every download ends in ``make_result(media_request)``.'''
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(
                None,
                Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = make_result(media_request)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker


def _failure(media_request, error_type, **status):
    return DownloadResult(status=DownloadStatus(success=False, error_type=error_type, **status),
                          media_request=media_request, ytdlp_data=None, file_name=None)


def yield_download_worker_bot_flagged(error_detail: str = 'foo'):
    '''Every download fails as BOT_FLAGGED.'''
    return _worker_returning(lambda request: _failure(request, DownloadErrorType.BOT_FLAGGED,
                                                      error_detail=error_detail))


def yield_download_worker_retryable_exception():
    '''Every download fails as RETRYABLE.'''
    return _worker_returning(lambda request: _failure(request, DownloadErrorType.RETRYABLE,
                                                      error_detail='Test retryable error'))


def yield_download_worker_retry_limit_exceeded():
    '''Every download fails as RETRY_LIMIT_EXCEEDED.'''
    return _worker_returning(lambda request: _failure(
        request, DownloadErrorType.RETRY_LIMIT_EXCEEDED,
        error_detail='Too many retries', user_message='retry limit'))


def yield_download_worker_video_too_long():
    '''Every download fails as TOO_LONG, a content-check rejection.'''
    return _worker_returning(lambda request: _failure(
        request, DownloadErrorType.TOO_LONG, error_detail='Video Too Long',
        user_message='Video duration 1176 seconds exceeds max duration of 900 seconds'))


def yield_download_worker_success_no_data():
    '''Every download succeeds but carries no yt-dlp metadata.'''
    return _worker_returning(lambda request: DownloadResult(
        status=DownloadStatus(success=True), media_request=request, ytdlp_data=None, file_name=None))


def fake_envelope(fake_context) -> CommandEnvelope:
    '''The command that asked for a search, as the commands pod receives it.'''
    return CommandEnvelope(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        author_id=fake_context['author'].id,
        author_display_name=fake_context['author'].display_name,
        channel_name=fake_context['channel'].name,
    )
