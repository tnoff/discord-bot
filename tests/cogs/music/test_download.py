from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.exceptions import ExitEarlyException

from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType

from tests.cogs.test_music import BASE_MUSIC_CONFIG, yield_download_client_download_exception, yield_fake_download_client, yield_download_client_download_error
from tests.helpers import fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, random_string #pylint:disable=unused-import

@pytest.mark.asyncio()
async def test_download_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.download_queue.put_nowait(fake_context['guild'].id, sd.media_request)
            await cog.download_files()
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_hits_cache(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
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
    } | BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music_helpers.media_broker.get_file', return_value=True)
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.dispatcher = MagicMock()
            await cog.media_broker.register_download(sd)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.download_queue.put_nowait(fake_context['guild'].id, sd.media_request)
            await cog.download_files()
            assert cog.players[fake_context['guild'].id].get_queue_items()

def yield_download_client_bot_flagged():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            self.backoff_seconds_remaining = None
            self.failure_summary = '0 failures in queue'

        async def create_source(self, media_request, *_args, **_kwargs):
            return DownloadResult(status=DownloadStatus(success=False, error_type=DownloadErrorType.BOT_FLAGGED, error_detail='foo'), media_request=media_request, ytdlp_data=None, file_name=None)

        def update_tracking(self, _result):
            pass

        async def backoff_wait(self, _shutdown_event):
            pass

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_bot_warning(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_bot_flagged())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_download_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def _bump_value():
        return True

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_download_error(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def _bump_value():
        return True
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_error())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_result(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(None))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_player_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    cog.players[fake_context['guild'].id].shutdown_called = True
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_no_player_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert fake_context['guild'].id not in cog.players


@pytest.mark.asyncio()
async def test_download_files_bot_shutdown(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """download_files raises ExitEarlyException immediately when bot_shutdown_event is set."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    cog.bot_shutdown_event.set()
    with pytest.raises(ExitEarlyException):
        await cog.download_files()


@pytest.mark.asyncio()
async def test_download_files_empty_queue(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """download_files returns early without error when download queue is empty."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    # Queue is empty — should return at the QueueEmpty guard
    await cog.download_files()


def yield_download_client_retry_limit_exceeded():
    """Fake download client that returns a RETRY_LIMIT_EXCEEDED DownloadResult"""
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            self.backoff_seconds_remaining = None
            self.failure_summary = '0 failures in queue'

        async def create_source(self, media_request, *_args, **_kwargs):
            return DownloadResult(
                status=DownloadStatus(success=False, error_type=DownloadErrorType.RETRY_LIMIT_EXCEEDED,
                                      error_detail='Too many retries', user_message='retry limit'),
                media_request=media_request, ytdlp_data=None, file_name=None)

        def update_tracking(self, _result):
            pass

        async def backoff_wait(self, _shutdown_event):
            pass

    return FakeDownloadClient


@pytest.mark.asyncio()
async def test_download_retry_limit_exceeded(mocker, fake_context):  # pylint: disable=redefined-outer-name
    """download_files handles RETRY_LIMIT_EXCEEDED by returning a bad video message."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.DownloadClient',
                 side_effect=yield_download_client_retry_limit_exceeded())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    cog.dispatcher = MagicMock()
    s = fake_source_dict(fake_context)
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
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


def yield_download_client_success_no_data():
    """Fake download client that returns success but ytdlp_data=None."""
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            self.backoff_seconds_remaining = None
            self.failure_summary = '0 failures in queue'

        async def create_source(self, media_request, *_args, **_kwargs):
            return DownloadResult(
                status=DownloadStatus(success=True),
                media_request=media_request, ytdlp_data=None, file_name=None)

        def update_tracking(self, _result):
            pass

        async def backoff_wait(self, _shutdown_event):
            pass

    return FakeDownloadClient


@pytest.mark.asyncio()
async def test_download_playlist_add_request_no_ytdlp_data(mocker, fake_engine, fake_context):  # pylint: disable=redefined-outer-name
    """download_files marks PlaylistAddRequest as failed when ytdlp_data is None."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch('discord_bot.cogs.music.DownloadClient',
                 side_effect=yield_download_client_success_no_data())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    cog.dispatcher = MagicMock()
    req = _make_playlist_add_request(fake_context)
    cog.download_queue.put_nowait(fake_context['guild'].id, req)
    await cog.download_files()
    from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage  # pylint: disable=import-outside-toplevel
    assert req.lifecycle_stage == MediaRequestLifecycleStage.FAILED


@pytest.mark.asyncio()
async def test_download_playlist_add_request_cache_hit(mocker, fake_engine, fake_context):  # pylint: disable=redefined-outer-name
    """download_files handles cache hit for PlaylistAddRequest via __add_playlist_item."""
    config = {
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
    } | BASE_MUSIC_CONFIG
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], config, fake_engine)
    cog.dispatcher = MagicMock()

    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            # Bypass DownloadClient: register directly (no S3 upload in this path)
            await cog.media_broker.register_download(sd)

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
            cog.download_queue.put_nowait(fake_context['guild'].id, req)
            # Patch __add_playlist_item to avoid DB operations
            mocker.patch.object(cog, '_Music__add_playlist_item')
            await cog.download_files()
            # __add_playlist_item should have been called (cache hit path)
            cog._Music__add_playlist_item.assert_called_once()  # pylint: disable=protected-access


@pytest.mark.asyncio()
async def test_download_files_updates_cache_count_when_cleanup_returns_true(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''When cache_cleanup() returns True, _cache_count is refreshed (music.py line 841).'''
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

            # Force cache_cleanup to return True and get_cache_count to return a known value
            cog.media_broker.cache_cleanup = AsyncMock(return_value=True)
            cog.media_broker.get_cache_count = AsyncMock(return_value=7)

            cog.download_queue.put_nowait(fake_context['guild'].id, sd.media_request)
            await cog.download_files()

            cog.media_broker.get_cache_count.assert_awaited_once()
            assert cog._cache_count == 7  # pylint: disable=protected-access
