import asyncio
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.status import StatusCode

from discord_bot.cogs.music import Music
from discord_bot.exceptions import ExitEarlyException

from discord_bot.interfaces import download_protocols
from discord_bot.workers.asyncio_download_worker import AsyncioDownloadWorker
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType

from tests.cogs.test_music import music_config, BASE_MUSIC_CONFIG, yield_download_worker_download_exception, yield_fake_download_worker, yield_download_worker_download_error
from tests.helpers import fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, random_string #pylint:disable=unused-import

@pytest.mark.asyncio()
async def test_download_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            await cog.download_client.submit(fake_context['guild'].id, sd.media_request)
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_hits_cache(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = music_config({
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                },
                'storage': {
                    'bucket_name': 'test-bucket',
                }
            }
        }
    })
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.workers.asyncio_broker.get_file', return_value=True)
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            await cog.download_client.submit(fake_context['guild'].id, sd.media_request)
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            assert cog.players[fake_context['guild'].id].get_queue_items()

def yield_download_worker_bot_flagged():
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(None, Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail='foo'), media_request=media_request, ytdlp_data=None, file_name=None)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker

@pytest.mark.asyncio()
async def test_download_queue_bot_warning(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_download_worker_bot_flagged())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_download_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def _bump_value():
        return True

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_download_worker_download_exception())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_download_error(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_download_worker_download_error())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_result(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(None))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_player_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            await cog.download_client.submit(fake_context['guild'].id, sd.media_request)
            cog.players[fake_context['guild'].id].shutdown_called = True
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_player_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = MagicMock()
            await cog.download_client.submit(fake_context['guild'].id, sd.media_request)
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            assert fake_context['guild'].id not in cog.players


@pytest.mark.asyncio()
async def test_download_files_bot_shutdown(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """process_download_results raises ExitEarlyException immediately when bot_shutdown_event is set."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.bot_shutdown_event.set()
    with pytest.raises(ExitEarlyException):
        await cog.process_download_results()


@pytest.mark.asyncio()
async def test_download_files_empty_queue(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """process_download_results returns early without error when result queue is empty."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    # Result queue is empty — should return at the QueueEmpty guard
    await cog.process_download_results()


def yield_download_worker_retry_limit_exceeded():
    """Fake download client that returns a RETRY_LIMIT_EXCEEDED DownloadResult"""
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(None, Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(
                status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRY_LIMIT_EXCEEDED,
                                      error_detail='Too many retries', user_message='retry limit'),
                media_request=media_request, ytdlp_data=None, file_name=None)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker


@pytest.mark.asyncio()
async def test_download_retry_limit_exceeded(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """download_files handles RETRY_LIMIT_EXCEEDED by returning a bad video message."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker',
                 side_effect=yield_download_worker_retry_limit_exceeded())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    assert not cog.players[fake_context['guild'].id].get_queue_items()


def _make_playlist_add_request(fake_context):  # pylint: disable=redefined-outer-name
    """Helper: create a PlaylistAddRequest for a direct URL."""
    url = f'https://yt.example/{random_string()}'
    search_result = SearchResult(search_type=SearchType.DIRECT, raw_search_string=url)
    return PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name=fake_context['author'].display_name,
        requester_id=fake_context['author'].id,
        search_result=search_result,
        playlist_id=1,
    )


def yield_download_worker_success_no_data():
    """Fake download client that returns success but ytdlp_data=None."""
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(None, Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(
                status=DownloadStatus(success=True),
                media_request=media_request, ytdlp_data=None, file_name=None)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker


@pytest.mark.asyncio()
async def test_download_playlist_add_request_no_ytdlp_data(mocker, fake_engine, fake_context):  # pylint: disable=redefined-outer-name
    """download_files marks PlaylistAddRequest as failed when ytdlp_data is None."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker',
                 side_effect=yield_download_worker_success_no_data())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = MagicMock()
    req = _make_playlist_add_request(fake_context)
    # In production the request is registered with the broker before download;
    # register here so the FAILED lifecycle push lands on it.
    await cog.media_broker.register_request(req)
    await cog.download_client.submit(fake_context['guild'].id, req)
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage  # pylint: disable=import-outside-toplevel
    assert req.lifecycle_stage == MediaRequestLifecycleStage.FAILED


@pytest.mark.asyncio()
async def test_download_playlist_add_request_cache_hit(mocker, fake_engine, fake_context):  # pylint: disable=redefined-outer-name
    """download_files handles cache hit for PlaylistAddRequest via __add_playlist_item."""
    config = music_config({
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                },
                'storage': {
                    'bucket_name': 'test-bucket',
                }
            }
        }
    })
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
            cog.dispatcher = MagicMock()

            # Create a PlaylistAddRequest for the same URL as the cached download
            search_result = SearchResult(
                search_type=SearchType.DIRECT,
                raw_search_string=sd.webpage_url,
            )
            req = PlaylistAddRequest(
                guild_id=fake_context['guild'].id,
                channel_id=fake_context['channel'].id,
                requester_name=fake_context['author'].display_name,
                requester_id=fake_context['author'].id,
                search_result=search_result,
                playlist_id=1,
            )
            await cog.download_client.submit(fake_context['guild'].id, req)
            # Patch __add_playlist_item to avoid DB operations
            mocker.patch.object(cog, '_Music__add_playlist_item')
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            # __add_playlist_item should have been called (playlist add path)
            cog._Music__add_playlist_item.assert_called_once()  # pylint: disable=protected-access


@pytest.mark.asyncio()
async def test_download_files_runs_cache_cleanup(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''process_download_results triggers media_broker.cache_cleanup after adding the source.'''
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

            cog.media_broker.cache_cleanup = AsyncMock(return_value=True)

            await cog.download_client.submit(fake_context['guild'].id, sd.media_request)
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()

            cog.media_broker.cache_cleanup.assert_awaited_once()


@pytest.mark.asyncio()
async def test_ensure_video_download_result_none_pushes_failed(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """__ensure_video_download_result pushes FAILED to the broker when the
    MediaDownload is None (download produced nothing)."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    media_request = fake_source_dict(fake_context)
    await cog.media_broker.register_request(media_request)

    result = await cog._Music__ensure_video_download_result(media_request, None)  # pylint: disable=protected-access

    from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage  # pylint: disable=import-outside-toplevel
    assert result is False
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert media_request.failure_reason is not None


@pytest.mark.asyncio()
async def test_run_idle_empty_queue_backs_off(mocker):
    """run() sleeps the idle backoff (not every iteration) when both input queues
    are empty and no backoff is active — cutting idle busy-loop churn."""
    sleep_mock = mocker.patch('discord_bot.interfaces.download_protocols.sleep')
    worker = AsyncioDownloadWorker(None, Path('/tmp'))
    assert worker.backoff_seconds_remaining is None  # else-branch (no backoff)
    await worker.run(asyncio.Event())
    sleep_mock.assert_awaited_once_with(download_protocols._IDLE_POLL_BACKOFF_SECONDS)  # pylint: disable=protected-access


@pytest.mark.asyncio()
async def test_run_idle_backoff_active_empty_queue_backs_off(mocker):
    """With backoff active but nothing queued, run() reaches the merged-empty path
    after the backoff wait and applies the idle backoff sleep before returning."""
    sleep_mock = mocker.patch('discord_bot.interfaces.download_protocols.sleep')
    worker = AsyncioDownloadWorker(None, Path('/tmp'))
    # Future timestamp → backoff_seconds_remaining truthy → backoff branch taken.
    worker.wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600
    assert worker.backoff_seconds_remaining
    # backoff_wait has its own dedicated tests; stub it to "elapsed, no DIRECT item"
    # so control falls through to the real (empty) merged_get_nowait idle path.
    mocker.patch.object(worker, 'backoff_wait', new=AsyncMock(return_value=None))
    await worker.run(asyncio.Event())
    sleep_mock.assert_awaited_once_with(download_protocols._IDLE_POLL_BACKOFF_SECONDS)  # pylint: disable=protected-access


def yield_download_worker_video_too_long():
    """Fake download client that returns a TOO_LONG DownloadResult"""
    class FakeDownloadWorker(AsyncioDownloadWorker):
        def __init__(self, *_args, **kwargs):
            super().__init__(None, Path('/tmp'),
                failure_queue=kwargs.get('failure_queue'),
                wait_period_minimum=kwargs.get('wait_period_minimum', 30),
                wait_period_max_variance=kwargs.get('wait_period_max_variance', 10),
                broker=kwargs.get('broker'),
            )

        async def create_source(self, media_request, *_args, **_kwargs):
            result = DownloadResult(
                status=DownloadStatus(success=False, error_type=DownloadErrorType.TOO_LONG,
                                      error_detail='Video Too Long',
                                      user_message='Video duration 1176 seconds exceeds max duration of 900 seconds'),
                media_request=media_request, ytdlp_data=None, file_name=None)
            await self.update_tracking(result)
            return result

    return FakeDownloadWorker


@pytest.mark.asyncio()
async def test_download_video_too_long_marks_request_rejected(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """A content-check terminal error (TOO_LONG) lands as a rejection, not a plain failure."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker',
                 side_effect=yield_download_worker_video_too_long())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.media_broker.register_request(s)
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    exporter = _consumer_span_exporter(mocker)
    await cog.process_download_results()
    from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage  # pylint: disable=import-outside-toplevel
    assert s.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert s.rejected is True
    # A declined video is not a fault — the consumer span the error-rate alert
    # watches must not go red for it.
    assert _process_results_span_status(exporter) is StatusCode.OK


@pytest.mark.asyncio()
async def test_download_retry_limit_exceeded_keeps_consumer_span_error(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """A genuine terminal failure still marks the consumer span ERROR."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker',
                 side_effect=yield_download_worker_retry_limit_exceeded())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.media_broker.register_request(s)
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.run(cog.bot_shutdown_event)
    exporter = _consumer_span_exporter(mocker)
    await cog.process_download_results()
    assert s.rejected is False
    assert _process_results_span_status(exporter) is StatusCode.ERROR


def _consumer_span_exporter(mocker):
    """Swap in a recording tracer so span *status* survives to an assertion.

    The suite runs with no global tracer provider, so spans are non-recording
    and drop their status entirely (see tests/utils/test_otel.py).
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    mocker.patch('discord_bot.utils.otel.TRACER', provider.get_tracer('test'))
    return exporter


def _process_results_span_status(exporter):
    """Status of the process_download_results consumer span."""
    spans = [s for s in exporter.get_finished_spans() if s.name.endswith('process_download_results')]
    assert len(spans) == 1
    return spans[0].status.status_code
