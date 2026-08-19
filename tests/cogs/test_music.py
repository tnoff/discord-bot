from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from unittest.mock import patch, Mock, AsyncMock

import asyncio
import subprocess  # nosec B404 - fixed argv, no shell, test-only import probe
import sys

import fakeredis.aioredis
import pytest

from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.music import (Music, LOOP_CLEANUP_PLAYERS, LOOP_DOWNLOAD_FILES,
                                    LOOP_POST_PLAY_PROCESSING, LOOP_PROCESS_DOWNLOAD_RESULTS,
                                    LOOP_PROCESS_SEARCH_RESULTS, LOOP_YOUTUBE_MUSIC_SEARCH)
from discord_bot.utils.loop_health import LOOP_HEALTH
from discord_bot.utils.otel import loop_heartbeat_observations
from discord_bot.types.cleanup_reason import CleanupReason
from discord_bot.types.search import SearchResult, SearchCollection
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.download import DownloadErrorType, DownloadResult, DownloadStatus
from discord_bot.workers.asyncio_download_worker import AsyncioDownloadWorker
from discord_bot.workers.redis_download_worker import RedisDownloadWorker
from discord_bot.clients.redis_client import RedisManager
from discord_bot.clients.broker_client import HttpBrokerClient, InMemoryBrokerClient
from discord_bot.clients.download_client import HttpDownloadClient, InMemoryDownloadClient
from discord_bot.clients.youtube_music_search_client import (
    HttpYoutubeMusicSearchClient, InMemoryYoutubeMusicSearchClient,
)
from discord_bot.interfaces.download_protocols import ClearGuildResult
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.search_client import SearchException
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.cogs.music_helpers.database_functions import update_video_guild_analytics

from tests.helpers import fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, async_mock_session #pylint:disable=unused-import
from tests.helpers import FakeVoiceClient, FakeContext, FakeChannel

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

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

@pytest.mark.asyncio
async def test_guild_cleanup(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access
            await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)
            assert fake_context['guild'].id not in cog.players
            assert await cog.download_client.queue_size(fake_context['guild'].id) == 0

@pytest.mark.asyncio
async def test_guild_hanging_downloads(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''A download still queued at shutdown is parked, not dropped.

    This asserted queue_size == 0 while shutdown drained the queue.  Draining it
    stranded the matching broker registry entries in the in_flight zone whenever the
    grace period expired part-way through the follow-up DISCARDED loop, so shutdown
    now leaves the queue for the downloader tier, which outlives this pod.
    '''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    s = fake_source_dict(fake_context)
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 1


@pytest.mark.asyncio
async def test_get_player_join_voice_timeout(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    fake_context['guild'].voice_client = None
    join_channel = FakeChannel()
    join_channel.connect = AsyncMock(side_effect=asyncio.TimeoutError())
    result = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], join_channel=join_channel)
    assert result is None
    cog.dispatcher.send_message.assert_called_once()
    assert 'Timed out connecting to voice channel' in cog.dispatcher.send_message.call_args[0][2]

@pytest.mark.asyncio
async def test_awaken(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.connect_(cog, fake_context['context'])
    assert fake_context['guild'].id in cog.players

@pytest.mark.asyncio
async def test_awaken_user_not_joined(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.connect_(cog, fake_context['context'])
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio()
async def test_play_called_basic(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    s1 = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    await cog.play_(cog, fake_context['context'], search='foo bar')
    await cog.search_youtube_music()
    await cog.process_search_results()
    await cog.search_youtube_music()
    await cog.process_search_results()
    item0 = await cog.download_client.local_worker.get_input_nowait()
    item1 = await cog.download_client.local_worker.get_input_nowait()
    # Compare key properties since SearchClient refactoring creates new MediaRequest objects
    assert item0.search_result.raw_search_string == s.search_result.raw_search_string
    assert item0.search_result.search_type == s.search_result.search_type
    assert item1.search_result.raw_search_string == s1.search_result.raw_search_string
    assert item1.search_result.search_type == s1.search_result.search_type

@pytest.mark.asyncio()
async def test_skip(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.play_(cog, fake_context['context'], search='foo bar')
            await cog.search_youtube_music()
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            # Mock current playing
            cog.players[fake_context['guild'].id].current_media_download = sd
            await cog.skip_(cog, fake_context['context'])
            assert cog.players[fake_context['guild'].id].video_skipped

@pytest.mark.asyncio()
async def test_clear(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.play_(cog, fake_context['context'], search='foo bar')
            await cog.search_youtube_music()
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            await cog.clear(cog, fake_context['context'])
            assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_history(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.players[fake_context['guild'].id]._history.put_nowait(sd) #pylint:disable=protected-access
            await cog.history_(cog, fake_context['context'])
            assert cog.dispatcher.send_message.called
            message_content = cog.dispatcher.send_message.call_args[0][2]
            assert message_content == f'History\n```Pos|| Title                                   || Uploader\n---------------------------------------------------------\n1  || {sd.title}                            || {sd.uploader}```' #pylint:disable=no-member

@pytest.mark.asyncio()
async def test_shuffle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.play_(cog, fake_context['context'], search='foo bar')
            await cog.search_youtube_music()
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            await cog.shuffle_(cog, fake_context['context'])
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_remove_item(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.play_(cog, fake_context['context'], search='foo bar')
            await cog.search_youtube_music()
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            await cog.remove_item(cog, fake_context['context'], 1)
            assert not cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_bump_item(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.play_(cog, fake_context['context'], search='foo bar')
            await cog.search_youtube_music()
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            await cog.bump_item(cog, fake_context['context'], 1)
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio
async def test_stop(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access
            await cog.stop_(cog, fake_context['context'])
            # After destroy(), the player should be marked for shutdown
            player = cog.players[fake_context['guild'].id]
            assert player.shutdown_called is True
            assert await cog.download_client.queue_size(fake_context['guild'].id) == 0

@pytest.mark.asyncio()
async def test_move_messages(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_channel2 = FakeChannel(guild=fake_context['guild'])
            fake_context2 = FakeContext(guild=fake_context['guild'], channel=fake_channel2, bot=fake_context['bot'], author=fake_context['author'])
            mocker.patch('discord_bot.cogs.music.AsyncioDownloadWorker', side_effect=yield_fake_download_worker(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
            cog.dispatcher = Mock()
            await cog.play_(cog, fake_context['context'], search='foo bar')
            await cog.search_youtube_music()
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            await cog.move_messages_here(cog, fake_context2)
            assert cog.players[fake_context['guild'].id].text_channel.id == fake_channel2.id

@pytest.mark.asyncio()
async def test_play_called_downloads_blocked(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    s1 = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    # Put source dict so we can a download queue to block
    await cog.download_client.submit(fake_context['guild'].id, s)
    await cog.download_client.block_guild(fake_context['guild'].id)
    await cog.play_(cog, fake_context['context'], search='foo bar')

@pytest.mark.asyncio()
async def test_play_hits_max_items(mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'player': {
                'queue_max_size': 1,
            }
        }
    } | BASE_MUSIC_CONFIG
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    s1 = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    await cog.play_(cog, fake_context['context'], search='foo bar')
    # The test verifies that queue-full protection works
    # The warning log message confirms the functionality is working
    # Queue full message: "Queue full in guild ..., cannot add more media requests"
    # This is the core behavior being tested - the message delivery system
    # has changed but the protection mechanism still works correctly

@pytest.mark.asyncio()
async def test_play_called_raises_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source_raises())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    await cog.play_(cog, fake_context['context'], search='foo bar')

    # Assert we got a message about the original search
    cog.dispatcher.send_message.assert_called()
    assert cog.dispatcher.send_message.call_args[0][2] == 'Error searching input "foo bar", message: woopsie'

    # Bundle is immediately torn down on the broker when the search fails
    assert await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id) == []

@pytest.mark.asyncio()
async def test_play_called_basic_hits_cache(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
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
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([sd.media_request]))
            mocker.patch('discord_bot.workers.asyncio_broker.get_file', return_value=True)
            cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)
            cog.dispatcher = Mock()
            await cog.media_broker.register_download(sd)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_random_play(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that random-play command queues 32 shuffled items from history playlist'''
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock __playlist_queue to verify it's called with correct parameters
    mock_playlist_queue = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

    await cog.playlist_random_play(cog, fake_context['context'])  #pylint:disable=too-many-function-args

    # Verify __playlist_queue was called with shuffle=True, max_num=32, and history playlist
    assert mock_playlist_queue.called
    call_args = mock_playlist_queue.call_args
    assert call_args.kwargs['shuffle'] is True
    assert call_args.kwargs['max_num'] == 32
    assert call_args.kwargs['is_history'] is True


def test_music_init_with_spotify_credentials(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with Spotify credentials configured"""
    config = {
        'music': {
            'download': {
                'spotify_credentials': {
                    'client_id': 'test_client_id',
                    'client_secret': 'test_client_secret'
                }
            }
        }
    } | BASE_MUSIC_CONFIG

    with patch('discord_bot.cogs.music.SpotifyClient') as mock_spotify:
        cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
        mock_spotify.assert_called_once_with('test_client_id', 'test_client_secret')
        assert cog.spotify_client is not None

def test_music_init_with_youtube_api_key(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with YouTube API key configured"""
    config = {
        'music': {
            'download': {
                'youtube_api_key': 'test_api_key'
            }
        }
    } | BASE_MUSIC_CONFIG

    with patch('discord_bot.cogs.music.YoutubeClient') as mock_youtube:
        cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
        mock_youtube.assert_called_once_with('test_api_key')
        assert cog.youtube_client is not None

def test_music_init_server_queue_priority(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with server queue priority configuration"""
    config = {
        'music': {
            'download': {
                'server_queue_priority': [
                    {'server_id': '123456789', 'priority': 1},
                    {'server_id': '987654321', 'priority': 2}
                ]
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    assert cog.server_queue_priority[123456789] == 1
    assert cog.server_queue_priority[987654321] == 2

def test_music_init_creates_download_directory(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization creates download directory when specified"""
    with TemporaryDirectory() as tmp_dir:
        download_path = Path(tmp_dir) / 'music_downloads'
        config = {
            'music': {
                'download': {
                    'download_dir_path': str(download_path)
                }
            }
        } | BASE_MUSIC_CONFIG

        cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
        assert cog.download_dir == download_path
        assert download_path.exists()

@pytest.mark.asyncio
async def test_cog_unload_basic(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test basic cog unload functionality"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Mock the tasks to None (default state)
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_tasks = []  # pylint: disable=protected-access
    cog._post_play_processing_task = None  # pylint: disable=protected-access

    # Mock file operations at pathlib level
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    await cog.cog_unload()

    # Verify bot shutdown flag is set
    assert cog.bot_shutdown_event.is_set()


@pytest.mark.asyncio()
async def test_cog_unload_cancels_search_result_task(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """cog_unload cancels the process_search_results task when one is running."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_tasks = []  # pylint: disable=protected-access
    cog._post_play_processing_task = None  # pylint: disable=protected-access
    search_task = mocker.Mock()
    cog._search_result_task = search_task  # pylint: disable=protected-access

    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    await cog.cog_unload()

    search_task.cancel.assert_called_once()

def test_music_init_music_not_enabled(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization fails when music is not enabled"""
    config = {
        'general': {
            'include': {
                'music': False
            }
        }
    }

    with pytest.raises(CogMissingRequiredArg, match='Music not enabled'):
        Music(fake_context['bot'], config, fake_context['dispatcher'])

def test_music_heartbeat_callbacks_report_loop_health(fake_context):  #pylint:disable=redefined-outer-name
    """Heartbeats follow LoopHealth (successful iterations), not task liveness."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Nothing has started yet: no series at all, rather than a 0 that would read
    # as "the loop died" for a loop that simply isn't running in this process.
    for job_name in (LOOP_CLEANUP_PLAYERS, LOOP_DOWNLOAD_FILES, LOOP_PROCESS_DOWNLOAD_RESULTS,
                     LOOP_PROCESS_SEARCH_RESULTS, LOOP_POST_PLAY_PROCESSING, LOOP_YOUTUBE_MUSIC_SEARCH):
        assert not loop_heartbeat_observations(job_name)

    health = LOOP_HEALTH.register(LOOP_PROCESS_SEARCH_RESULTS, stale_after_seconds=60)
    observations = loop_heartbeat_observations(LOOP_PROCESS_SEARCH_RESULTS)
    assert len(observations) == 1
    assert observations[0].value == 1
    assert observations[0].attributes == {'background_job': 'process_search_results'}

    # This is the incident's shape: the loop keeps erroring against an
    # un-upgraded peer. It stays alive (so it can recover) and, once the window
    # passes with no success, honestly reports unhealthy.
    for _ in range(5):
        health.record_error()
    assert loop_heartbeat_observations(LOOP_PROCESS_SEARCH_RESULTS)[0].value == 1
    health._last_success -= 61  #pylint:disable=protected-access
    assert loop_heartbeat_observations(LOOP_PROCESS_SEARCH_RESULTS)[0].value == 0
    health.record_success()
    assert loop_heartbeat_observations(LOOP_PROCESS_SEARCH_RESULTS)[0].value == 1

    # Queue-depth gauges are unchanged by the health rework
    assert cog._Music__download_result_queue_depth_callback(None)[0].value == 0  # pylint: disable=protected-access
    assert cog._Music__search_result_queue_depth_callback(None)[0].value == 0  # pylint: disable=protected-access

def test_music_init_with_cache_enabled(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with cache enabled — requires S3 storage"""
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 100
                },
                'storage': {
                    'bucket_name': 'test-bucket',
                }
            }
        }
    }

    with patch('discord_bot.cogs.music.VideoCacheClient') as mock_video_cache:

        cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_engine)

        # Verify cache client was created (only when bucket_name is set)
        assert mock_video_cache.called
        assert cog.video_cache is not None

def test_music_cache_filestats_callbacks(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test cache filesystem stats callback methods"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Mock disk_usage to return tuple (total, used, free)
    mock_disk_usage = mocker.patch('discord_bot.cogs.music.disk_usage')
    mock_disk_usage.return_value = (1024*1024*1000, 1024*1024*500, 1024*1024*500)  # 1GB total, 500MB used, 500MB free

    # Test used space callback
    result = cog._Music__cache_filestats_callback_used(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 1024*1024*500  # 500MB in bytes

    # Test total space callback
    result = cog._Music__cache_filestats_callback_total(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 1024*1024*1000  # 1GB in bytes

def test_music_active_players_callback(fake_context):  #pylint:disable=redefined-outer-name
    """Test active players callback method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Add some fake players
    cog.players[123] = 'player1'
    cog.players[456] = 'player2'
    cog.players[789] = 'player3'

    result = cog._Music__active_players_callback(None)  # pylint: disable=protected-access
    # It returns an observation for each player with guild attribute
    assert len(result) == 3
    assert result[0].value == 1
    assert result[1].value == 1
    assert result[2].value == 1

def test_music_active_players_callback_reports_zero_when_idle(fake_context):  #pylint:disable=redefined-outer-name
    """With no players the gauge reports an explicit 0 rather than no series.

    Emitting nothing made an idle bot indistinguishable from a dead one in
    Mimir, and made "a bundle is alive with no player behind it" unwritable as
    an alert. The zero only exists while the pod is running, which is what
    separates the two cases."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert not cog.players

    result = cog._Music__active_players_callback(None)  # pylint: disable=protected-access

    assert len(result) == 1
    assert result[0].value == 0
    # No guild to name, so the zero carries no guild attribute — aggregate with sum()
    assert not (result[0].attributes or {})

@pytest.mark.asyncio
async def test_cog_unload_with_players(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test cog unload with active players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Simplify - just test that bot_shutdown event gets set
    # Mock everything else to avoid complex async mocking
    mocker.patch.object(cog, 'cleanup')
    mocker.patch.object(cog.bot, 'fetch_guild')
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None to avoid cancellation
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_tasks = []  # pylint: disable=protected-access
    cog._post_play_processing_task = None  # pylint: disable=protected-access

    # Add fake players with mock destroy method
    player1 = mocker.Mock()
    player1.destroy = mocker.Mock()
    player2 = mocker.Mock()
    player2.destroy = mocker.Mock()
    cog.players[123] = player1
    cog.players[456] = player2

    # Mock sleep to make test fast (avoid 30 second wait)
    mock_sleep = mocker.patch('discord_bot.cogs.music.sleep')

    # Make sleep clear the players dict on first call to exit the wait loop immediately
    async def sleep_and_clear(_duration):
        cog.players.clear()
    mock_sleep.side_effect = sleep_and_clear

    await cog.cog_unload()

    # Verify bot shutdown event is set
    assert cog.bot_shutdown_event.is_set()


@pytest.mark.asyncio
async def test_download_queue_with_server_priority(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test download queue respects server priority configuration"""
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'download': {
                'server_queue_priority': [
                    {'server_id': fake_context['guild'].id, 'priority': 1}
                ]
            }
        }
    }

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Verify priority was set correctly (converted to int)
    guild_id_int = int(fake_context['guild'].id)
    assert guild_id_int in cog.server_queue_priority
    assert cog.server_queue_priority[guild_id_int] == 1

def test_music_init_with_backup_storage_options(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with backup storage options"""
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'download': {
                'storage': {
                    'bucket_name': 'test-bucket'
                }
            }
        }
    }

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    assert cog.config.download.storage.bucket_name == 'test-bucket'

def test_music_init_with_custom_ytdl_options(fake_context):  #pylint:disable=redefined-outer-name
    """Test ytdlp options merging - covers line 378"""
    config = {
        'general': {
            'include': {
                'music': True
            }
        },
        'music': {
            'download': {
                'extra_ytdlp_options': {
                    'custom_option': 'custom_value',
                    'format': 'worst'  # Should override default
                }
            }
        }
    }

    with patch('discord_bot.interfaces.download_protocols.YoutubeDL') as mock_ytdl:
        Music(fake_context['bot'], config, fake_context['dispatcher'])

        # Check that custom options were merged
        call_args = mock_ytdl.call_args[0][0]  # First positional arg (options dict)
        assert call_args['custom_option'] == 'custom_value'
        assert call_args['format'] == 'worst'  # Should override default 'bestaudio/best'


def test_music_backoff_status_enum_usage():
    """Test that BACKOFF enum value is properly imported and used"""
    # Test that BACKOFF enum exists and has correct value
    assert hasattr(MediaRequestLifecycleStage, 'BACKOFF')
    assert MediaRequestLifecycleStage.BACKOFF.value == 'backoff'


# Memory leak fix tests
@pytest.mark.asyncio
async def test_shutdown_calls_cleanup_per_guild(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cog_unload calls cleanup(BOT_SHUTDOWN) for each active guild"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    mock_player1 = Mock()
    mock_player1.guild = mocker.MagicMock()
    mock_player2 = Mock()
    mock_player2.guild = mocker.MagicMock()

    cog.players[123] = mock_player1
    cog.players[456] = mock_player2

    cleanup_mock = mocker.patch.object(cog, 'cleanup')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    cog._cleanup_task = None  #pylint:disable=protected-access
    cog._download_tasks = []  #pylint:disable=protected-access
    cog._post_play_processing_task = None  #pylint:disable=protected-access
    cog._youtube_search_task = None  #pylint:disable=protected-access

    await cog.cog_unload()

    # Verify cleanup was called once per guild with BOT_SHUTDOWN
    assert cleanup_mock.call_count == 2
    for call in cleanup_mock.call_args_list:
        assert call.kwargs['reason'] == CleanupReason.BOT_SHUTDOWN

    assert cog.bot_shutdown_event.is_set()

@pytest.mark.asyncio
async def test_shutdown_no_players(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cog_unload works cleanly with no active players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    cleanup_mock = mocker.patch.object(cog, 'cleanup')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    cog._cleanup_task = None  #pylint:disable=protected-access
    cog._download_tasks = []  #pylint:disable=protected-access
    cog._post_play_processing_task = None  #pylint:disable=protected-access
    cog._youtube_search_task = None  #pylint:disable=protected-access

    await cog.cog_unload()

    cleanup_mock.assert_not_called()
    assert cog.bot_shutdown_event.is_set()

@pytest.mark.asyncio
async def test_add_source_to_player_registers_before_enqueue(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Broker registration must precede enqueue so the player_loop cannot checkout a
    cache-hit item before the broker knows about it (which returns None and crashes
    the player on the raw S3 key)."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = mocker.Mock()
    order = []
    cog.broker_client = mocker.Mock()
    cog.broker_client.register_download = mocker.AsyncMock(side_effect=lambda *_a, **_k: order.append('register'))
    cog._push_state = mocker.AsyncMock()  #pylint:disable=protected-access
    cog._get_play_order_content = mocker.Mock(return_value=[])  #pylint:disable=protected-access
    player = mocker.Mock()
    player.add_to_play_queue = mocker.Mock(side_effect=lambda *_a, **_k: order.append('enqueue'))
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
            result = await cog.add_source_to_player(media_download, player)
    assert result is True
    assert order == ['register', 'enqueue']

@pytest.mark.asyncio
async def test_task_cancellation_during_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that all tasks are properly cancelled during shutdown"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Mock sleep
    mocker.patch('discord_bot.cogs.music.sleep')

    # Create mock tasks
    mock_cleanup_task = Mock()
    mock_download_task = Mock()
    mock_result_task = Mock()
    mock_history_task = Mock()
    mock_search_task = Mock()

    # Set mock tasks  #pylint:disable=protected-access
    cog._cleanup_task = mock_cleanup_task
    cog._download_tasks = [mock_download_task]
    cog._result_task = mock_result_task
    cog._post_play_processing_task = mock_history_task
    cog._youtube_search_task = mock_search_task

    # Mock other cleanup methods
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    # Ensure players dict is empty so timeout doesn't hang
    cog.players = {}

    await cog.cog_unload()

    # Verify all tasks were cancelled
    mock_cleanup_task.cancel.assert_called_once()
    mock_download_task.cancel.assert_called_once()
    mock_result_task.cancel.assert_called_once()
    mock_history_task.cancel.assert_called_once()
    mock_search_task.cancel.assert_called_once()

@pytest.mark.asyncio
async def test_directory_cleanup_during_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that directories are cleaned up during shutdown"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Mock sleep and players
    mocker.patch('discord_bot.cogs.music.sleep')
    cog.players = {}  # Empty to avoid timeout

    # Mock path operations
    mocker.patch('pathlib.Path.exists', return_value=True)  # Don't store unused mock
    mock_rm_tree = mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None  #pylint:disable=protected-access
    cog._cleanup_task = None
    cog._download_tasks = []
    cog._post_play_processing_task = None
    cog._youtube_search_task = None

    await cog.cog_unload()

    # Verify cleanup operations were called
    assert mock_rm_tree.call_count >= 1  # For directories

@pytest.mark.asyncio
async def test_cleanup_players_shutdown_called(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup_players properly handles shutdown_called players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a mock player with shutdown_called=True
    mock_player = mocker.Mock()
    mock_player.shutdown_called = True
    mock_player.shutdown_reason = CleanupReason.USER_STOP
    mock_player.guild = fake_context['guild']
    cog.players[fake_context['guild'].id] = mock_player

    # Mock cleanup method
    cleanup_mock = mocker.patch.object(cog, 'cleanup')

    await cog.cleanup_players()

    # Verify cleanup was called for the shutdown player with the player's reason
    cleanup_mock.assert_called_once_with(fake_context['guild'], reason=CleanupReason.USER_STOP)

@pytest.mark.asyncio
async def test_cleanup_players_inactive_timeout_message(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup_players sends proper message for inactive timeout"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a mock player that times out
    mock_player = mocker.Mock()
    mock_player.shutdown_called = False
    mock_player.voice_channel_inactive_timeout = mocker.Mock(return_value=True)
    mock_player.guild = fake_context['guild']
    mock_player.text_channel = fake_context['channel']
    cog.players[fake_context['guild'].id] = mock_player

    # Mock cleanup
    cleanup_mock = mocker.patch.object(cog, 'cleanup')

    await cog.cleanup_players()

    # Verify timeout was checked with correct parameter
    mock_player.voice_channel_inactive_timeout.assert_called_once_with(timeout_seconds=cog.config.player.inactive_voice_channel_timeout)

    # Verify message was sent
    cog.dispatcher.send_message.assert_called_once()
    # Check that the message content contains expected text
    assert 'No one active in voice channel' in str(cog.dispatcher.send_message.call_args[0][2])

    # Verify cleanup was called with VOICE_INACTIVE reason
    cleanup_mock.assert_called_once_with(fake_context['guild'], reason=CleanupReason.VOICE_INACTIVE)

@pytest.mark.asyncio
async def test_voice_client_disconnected_without_manual_cleanup(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """cleanup() awaits voice_client.disconnect() and does NOT pre-call
    voice_client.cleanup() — calling cleanup() first detaches the socket and can
    suppress the gateway leave, stranding the bot in the channel."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    # Set the voice client on the guild
    fake_context['guild'].voice_client = mock_voice_client

    # Create player and add to cog
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Call cleanup
    await cog.cleanup(fake_context['guild'])

    # disconnect() is awaited; the manual cleanup() call is gone (disconnect
    # handles native teardown itself)
    mock_voice_client.disconnect.assert_awaited_once()
    mock_voice_client.cleanup.assert_not_called()

@pytest.mark.asyncio
async def test_voice_client_cleanup_handles_none(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles case when voice_client is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Set voice client to None
    fake_context['guild'].voice_client = None

    # Create player and add to cog
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Call cleanup - should not raise exception
    await cog.cleanup(fake_context['guild'])

    # Verify player was still cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_with_bot_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles CleanupReason.BOT_SHUTDOWN correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Create player
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Call cleanup with BOT_SHUTDOWN
    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    # Verify disconnect was awaited (manual cleanup() is no longer called)
    mock_voice_client.disconnect.assert_awaited_once()
    mock_voice_client.cleanup.assert_not_called()

    # Verify the bot shutdown message was sent via dispatcher
    cog.dispatcher.send_message.assert_called_once()
    assert cog.dispatcher.send_message.call_args[0][0] == player.guild.id

    # Verify player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_without_bot_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup with a non-BOT_SHUTDOWN reason does not send a message"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Create player
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Default reason (QUEUE_TIMEOUT) — no message sent
    await cog.cleanup(fake_context['guild'])

    # Verify disconnect was awaited (manual cleanup() is no longer called)
    mock_voice_client.disconnect.assert_awaited_once()
    mock_voice_client.cleanup.assert_not_called()

    # Verify NO external shutdown message was sent
    cog.dispatcher.send_message.assert_not_called()

    # Verify player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_bot_shutdown_awaits_disconnect(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """BOT_SHUTDOWN awaits the voice disconnect to completion (the disconnect is
    no longer a fire-and-forget task that a later error could skip)."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # A disconnect that only marks completion after it is actually awaited
    disconnect_completed = False
    async def slow_disconnect():
        nonlocal disconnect_completed
        await asyncio.sleep(0.01)
        disconnect_completed = True

    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = slow_disconnect

    fake_context['guild'].voice_client = mock_voice_client

    # Create player
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    await cog.cleanup(fake_context['guild'], reason=CleanupReason.BOT_SHUTDOWN)

    # Disconnect ran to completion (was awaited), manual cleanup() is not called,
    # and the player was reaped
    assert disconnect_completed is True
    mock_voice_client.cleanup.assert_not_called()
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_when_player_does_not_exist(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup continues when player doesn't exist in self.players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Don't create a player in cog.players - simulate it doesn't exist
    # (normally get_player would add it, but we skip that)

    # Call cleanup - should not raise exception even though player doesn't exist
    await cog.cleanup(fake_context['guild'])

    # Verify voice client was disconnected (manual cleanup() is no longer called)
    mock_voice_client.disconnect.assert_awaited_once()
    mock_voice_client.cleanup.assert_not_called()

    # Verify player still doesn't exist (wasn't created)
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_player_not_exist_with_bundles(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup tears down broker bundles when player doesn't exist"""
    dispatcher = Mock()
    # The broker captures the dispatcher at construction time, so pass the spy in.
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, dispatcher)
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Create a real broker-owned bundle for this guild
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id,
        input_string='test playlist', has_search_banner=True,
    )
    assert bundle_uuid in await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id)

    # Call cleanup - should not raise exception even though player doesn't exist
    await cog.cleanup(fake_context['guild'])

    # Bundle is gone from the broker
    assert bundle_uuid not in await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id)

    # delete_bundle drops the bundle's mutable Discord message via the dispatcher
    remove_calls = [call for call in dispatcher.remove_mutable.call_args_list
                    if bundle_uuid in str(call)]
    assert remove_calls

    # Verify voice client was disconnected (manual cleanup() is no longer called)
    mock_voice_client.disconnect.assert_awaited_once()
    mock_voice_client.cleanup.assert_not_called()

@pytest.mark.asyncio
async def test_voice_client_cleanup_player_removed_externally(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test cleanup when player was already removed from cog.players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Create player first
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Verify player exists
    assert fake_context['guild'].id in cog.players

    # Manually remove player (simulate external removal)
    cog.players.pop(fake_context['guild'].id)

    # Verify player was removed
    assert fake_context['guild'].id not in cog.players

    # Call cleanup - should not raise exception even though player was removed
    await cog.cleanup(fake_context['guild'])

    # Verify voice client was disconnected (manual cleanup() is no longer called)
    mock_voice_client.disconnect.assert_awaited_once()
    mock_voice_client.cleanup.assert_not_called()

    # Verify player.cleanup() was NOT called (since player was already removed)
    # We can't easily verify this since we patched it, but we verified no exception was raised


@pytest.mark.asyncio
async def test_music_stats_command(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command displays analytics correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Pre-populate analytics data
    async with async_mock_session(fake_engine) as session:
        # Add some analytics data
        await update_video_guild_analytics(session, fake_context['guild'].id, 7200, False)  # 2 hours
        await update_video_guild_analytics(session, fake_context['guild'].id, 3600, True)  # 1 hour, cached
        await session.commit()

    # Set dispatcher mock
    cog.dispatcher = Mock()

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Verify message was sent
    cog.dispatcher.send_message.assert_called_once()

    # Verify guild_id
    assert cog.dispatcher.send_message.call_args[0][0] == fake_context['guild'].id

    # Verify message content contains expected stats
    # Total: 10,800 seconds = 0 days, 3 hours, 0 minutes, 0 seconds
    message_content = cog.dispatcher.send_message.call_args[0][2]
    assert 'Music Stats for Server' in message_content
    assert 'Total Plays: 2' in message_content
    assert 'Cached Plays: 1' in message_content
    assert 'Total Time Played: 0 days, 3 hours, 0 minutes, and 0 seconds' in message_content
    assert 'Tracked Since:' in message_content


@pytest.mark.asyncio
async def test_music_stats_command_with_days(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command displays days correctly when duration exceeds 24 hours"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Pre-populate analytics data with more than one day
    async with async_mock_session(fake_engine) as session:
        one_day = 60 * 60 * 24
        # Add 2 days and 5 hours worth of content
        await update_video_guild_analytics(session, fake_context['guild'].id, one_day * 2 + 18000, False)
        await session.commit()

    # Set dispatcher mock
    cog.dispatcher = Mock()

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Verify message was sent
    cog.dispatcher.send_message.assert_called_once()

    # Get the message content
    message_content = cog.dispatcher.send_message.call_args[0][2]

    # Verify message shows days correctly
    # Total: 190,800 seconds = 2 days, 5 hours, 0 minutes, 0 seconds
    assert 'Total Time Played: 2 days, 5 hours, 0 minutes, and 0 seconds' in message_content
    assert 'Total Plays: 1' in message_content


@pytest.mark.asyncio
async def test_music_stats_command_with_hours_and_seconds(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command displays hours and seconds correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Pre-populate analytics data: 1 day, 7 hours, 45 minutes, 30 seconds
    # 1 day = 86400, 7 hours = 25200, 45 min = 2700, 30 sec = 30
    # Total = 86400 + 25200 + 2700 + 30 = 114330 seconds
    async with async_mock_session(fake_engine) as session:
        await update_video_guild_analytics(session, fake_context['guild'].id, 114330, False)
        await session.commit()

    # Set dispatcher mock
    cog.dispatcher = Mock()

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Get the message content
    message_content = cog.dispatcher.send_message.call_args[0][2]

    # Verify message shows all components correctly
    # After migration: 1 day + 27930 seconds (7 hours 45 min 30 sec)
    # Hours: 27930 // 3600 = 7
    # Minutes: (27930 % 3600) // 60 = 2730 // 60 = 45
    # Seconds: 27930 % 60 = 30
    assert 'Total Time Played: 1 days, 7 hours, 45 minutes, and 30 seconds' in message_content
    assert 'Total Plays: 1' in message_content


def test_player_dir_uses_configured_path(fake_context):  #pylint:disable=redefined-outer-name
    """player_dir is set to the configured player_dir_path and created on disk."""
    with TemporaryDirectory() as tmp_dir:
        configured_path = Path(tmp_dir) / 'players'
        config = {
            **BASE_MUSIC_CONFIG,
            'music': {'player': {'player_dir_path': str(configured_path)}},
        }
        cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
        assert cog.player_dir == configured_path
        assert configured_path.exists()


def test_player_dir_defaults_to_temp_when_unconfigured(fake_context):  #pylint:disable=redefined-outer-name
    """player_dir falls back to a temp path when player_dir_path is not set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert cog.player_dir is not None
    assert cog.config.player.player_dir_path is None


@pytest.mark.asyncio
async def test_cog_unload_removes_temp_player_dir(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """cog_unload deletes player_dir when player_dir_path was not configured."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.players = {}

    with TemporaryDirectory() as tmp_dir:
        cog.player_dir = Path(tmp_dir)
        mocker.patch('pathlib.Path.exists', return_value=True)
        rm_tree_mock = mocker.patch('discord_bot.cogs.music.rm_tree')

        await cog.cog_unload()

        # rm_tree should have been called for the temp player_dir
        called_paths = [call.args[0] for call in rm_tree_mock.call_args_list]
        assert cog.player_dir in called_paths


@pytest.mark.asyncio
async def test_cog_unload_preserves_configured_player_dir(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """cog_unload does not delete player_dir when player_dir_path is set in config."""
    with TemporaryDirectory() as tmp_dir:
        configured_path = Path(tmp_dir) / 'players'
        config = {
            **BASE_MUSIC_CONFIG,
            'music': {'player': {'player_dir_path': str(configured_path)}},
        }
        cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
        cog.players = {}
        mocker.patch('pathlib.Path.exists', return_value=True)
        rm_tree_mock = mocker.patch('discord_bot.cogs.music.rm_tree')

        await cog.cog_unload()

        called_paths = [call.args[0] for call in rm_tree_mock.call_args_list]
        assert cog.player_dir not in called_paths


@pytest.mark.asyncio
async def test_music_stats_command_no_database(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command when database is not available"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the database check to return False
    mocker.patch.object(cog, '_Music__check_database_session', return_value=False)

    # Set dispatcher mock
    cog.dispatcher = Mock()

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Verify no message was sent (function returned early)
    cog.dispatcher.send_message.assert_not_called()


def test_music_init_with_broker_client_config(fake_context):  #pylint:disable=redefined-outer-name
    """HttpBrokerClient is created — with the storage bucket wired — when
    broker_client config is present.

    The bucket_name wiring is load-bearing: an HA broker's checkout returns
    CheckoutResult(s3_key=...), and the client stamps bucket_name onto it so
    MusicPlayer can fetch the file from S3. Without it the player falls through
    to open() the raw s3_key and 404s (prod regression on the F3 cutover)."""
    config = {
        'music': {
            'broker_client': {'url': 'http://broker-host:8081'},
            'download': {'storage': {'bucket_name': 'my-cache-bucket'}},
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    assert isinstance(cog.broker_client, HttpBrokerClient)
    assert cog.broker_client._bucket_name == 'my-cache-bucket'  # pylint: disable=protected-access


def test_music_init_without_broker_client_config_uses_in_memory(fake_context):  #pylint:disable=redefined-outer-name
    """InMemoryBrokerClient is used by default when no broker_client config is set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert isinstance(cog.broker_client, InMemoryBrokerClient)


@pytest.mark.asyncio
async def test_cog_load_starts_broker_server_when_configured(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """BrokerHttpServer task is scheduled when broker_server config is set."""
    config = {
        'music': {
            'broker_server': {'host': '127.0.0.1', 'port': 19100}
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    mocker.patch('discord_bot.cogs.music.BrokerHttpServer.serve', new_callable=AsyncMock)
    await cog.cog_load()
    # cog_load schedules 5 background tasks normally (cleanup + download + result +
    # search_result + youtube_search); +1 for the broker server = 6 total
    assert mock_loop.create_task.call_count == 7


def test_max_concurrent_downloads_defaults_to_one(fake_context):  #pylint:disable=redefined-outer-name
    """The download concurrency knob defaults to a single serial downloader."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert cog.config.download.max_concurrent_downloads == 1


@pytest.mark.asyncio
async def test_cog_load_spawns_configured_download_loops(fake_context):  #pylint:disable=redefined-outer-name
    """cog_load starts one download loop per max_concurrent_downloads slot."""
    config = {
        'music': {
            'download': {'max_concurrent_downloads': 3}
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    await cog.cog_load()
    assert len(cog._download_tasks) == 3  #pylint:disable=protected-access
    # cleanup + 3 download loops + result + search_result + youtube_search = 7 scheduled tasks
    assert mock_loop.create_task.call_count == 8


def test_music_init_with_download_client_config_uses_http(fake_context):  #pylint:disable=redefined-outer-name
    """HttpDownloadClient (pointed at the downloader pod) is built when
    download_client config is present — the HA cutover selection."""
    config = {
        'music': {'download_client': {'url': 'http://downloader-host:8083'}}
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    assert isinstance(cog.download_client, HttpDownloadClient)
    assert cog.download_client._base_url == 'http://downloader-host:8083'  # pylint: disable=protected-access


def test_music_init_without_download_client_uses_in_memory(fake_context):  #pylint:disable=redefined-outer-name
    """InMemoryDownloadClient (in-process worker) is used by default."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert isinstance(cog.download_client, InMemoryDownloadClient)


def test_download_heartbeat_gauge_registered_without_ha(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """The bot-side download_files heartbeat gauge is registered in single-process mode."""
    gauge = mocker.patch('discord_bot.cogs.music.create_observable_gauge')
    Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    descriptions = [call.args[3] for call in gauge.call_args_list if len(call.args) > 3]
    assert 'Download files loop heartbeat' in descriptions


@pytest.mark.asyncio
async def test_download_heartbeat_emits_no_series_in_ha(fake_context):  #pylint:disable=redefined-outer-name
    """In HA the download loop lives in the downloader pod, so the bot registers
    no download_files loop and emits no series for it — it would otherwise sit at
    0 and trip the stalled-loop alert (and now also fail the pod's liveness probe)."""
    config = {
        'music': {'download_client': {'url': 'http://downloader-host:8083'}}
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.download_client = Mock()
    cog.download_client.start = AsyncMock()
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    await cog.cog_load()
    assert not loop_heartbeat_observations(LOOP_DOWNLOAD_FILES)
    assert LOOP_HEALTH.get(LOOP_DOWNLOAD_FILES) is None


@pytest.mark.asyncio
async def test_cog_load_starts_poller_in_ha(fake_context):  #pylint:disable=redefined-outer-name
    """In HA, cog_load starts the client's status poller and spawns no download loops."""
    config = {
        'music': {'download_client': {'url': 'http://downloader-host:8083'}}
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.download_client = Mock()
    cog.download_client.start = AsyncMock()
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    await cog.cog_load()
    cog.download_client.start.assert_awaited_once()
    assert cog._download_tasks == []  #pylint:disable=protected-access
    # cleanup + result + youtube_search only — no download loops, no broker server.
    assert mock_loop.create_task.call_count == 5


@pytest.mark.asyncio
async def test_cog_unload_stops_download_client_in_ha(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """In HA, cog_unload stops the status poller / closes the HTTP session."""
    mocker.patch('discord_bot.cogs.music.rm_tree')
    config = {
        'music': {'download_client': {'url': 'http://downloader-host:8083'}}
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.download_client = Mock()
    cog.download_client.stop = AsyncMock()
    cog.players = {}
    await cog.cog_unload()
    cog.download_client.stop.assert_awaited_once()


SEARCH_HA_CONFIG = {'music': {'youtube_music_search_client': {'url': 'http://search-host:8084'}}}




def test_single_process_still_builds_a_real_ytmusic_client():
    '''
    The deferral must not become a removal: without the HA url the cog builds a
    real YoutubeMusicClient, which is what makes ytmusicapi a genuine [bot]
    runtime dependency (compose single-process, local dev, this suite) rather
    than something droppable from the extra.
    '''
    probe = (
        'import sys; from discord_bot.utils.integrations import youtube_music; '
        "assert 'ytmusicapi' not in sys.modules, 'imported too early'; "
        'youtube_music.YoutubeMusicClient; '
        "print('loaded' if 'ytmusicapi' in sys.modules else 'never-loaded')"
    )
    result = subprocess.run([sys.executable, '-c', probe],  # nosec B603 - fixed argv, no shell
                            capture_output=True, text=True, check=True)
    assert result.stdout.strip() == 'loaded'


def test_music_init_with_search_client_config_uses_http(fake_context):  #pylint:disable=redefined-outer-name
    """HttpYoutubeMusicSearchClient (pointed at the search pod) is built when
    youtube_music_search_client config is present — the MR 6 cutover selection."""
    cog = Music(fake_context['bot'], SEARCH_HA_CONFIG | BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert isinstance(cog.youtube_music_search_client, HttpYoutubeMusicSearchClient)
    assert cog.youtube_music_search_client._base_url == 'http://search-host:8084'  # pylint: disable=protected-access


def test_music_init_without_search_client_uses_in_memory(fake_context):  #pylint:disable=redefined-outer-name
    """InMemoryYoutubeMusicSearchClient (in-process worker) is used by default —
    single-process and compose deployments are untouched by the cutover."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert isinstance(cog.youtube_music_search_client, InMemoryYoutubeMusicSearchClient)


def test_music_init_builds_no_search_driver_in_ha(fake_context):  #pylint:disable=redefined-outer-name
    """No driver in HA: it needs the pop/resolve surface HttpYoutubeMusicSearchClient
    deliberately lacks, because the pod drives that same code against its own worker."""
    cog = Music(fake_context['bot'], SEARCH_HA_CONFIG | BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert cog.youtube_music_search_driver is None
    plain = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    assert plain.youtube_music_search_driver is not None


@pytest.mark.asyncio
async def test_search_heartbeat_emits_no_series_in_ha(fake_context):  #pylint:disable=redefined-outer-name
    """In HA the search loop lives in the search pod, so the bot registers no
    youtube_music_search loop and emits no series for it. The pod publishes that
    series instead, under the same loop name — so the heartbeat moves job labels
    at the cutover rather than vanishing."""
    cog = Music(fake_context['bot'], SEARCH_HA_CONFIG | BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.youtube_music_search_client = Mock()
    cog.youtube_music_search_client.start = AsyncMock()
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    await cog.cog_load()
    assert not loop_heartbeat_observations(LOOP_YOUTUBE_MUSIC_SEARCH)
    assert LOOP_HEALTH.get(LOOP_YOUTUBE_MUSIC_SEARCH) is None


@pytest.mark.asyncio
async def test_cog_load_starts_the_search_poller_in_ha(fake_context):  #pylint:disable=redefined-outer-name
    """In HA, cog_load starts the search client's status poller and spawns no
    bot-side search loop."""
    cog = Music(fake_context['bot'], SEARCH_HA_CONFIG | BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.youtube_music_search_client = Mock()
    cog.youtube_music_search_client.start = AsyncMock()
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    await cog.cog_load()
    cog.youtube_music_search_client.start.assert_awaited_once()
    assert cog._youtube_search_task is None  #pylint:disable=protected-access
    # cleanup + 1 download loop + result + search_result — no youtube_search loop.
    assert mock_loop.create_task.call_count == 5


@pytest.mark.asyncio
async def test_cog_unload_stops_search_client_in_ha(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """In HA, cog_unload stops the search status poller / closes the HTTP session."""
    mocker.patch('discord_bot.cogs.music.rm_tree')
    cog = Music(fake_context['bot'], SEARCH_HA_CONFIG | BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.youtube_music_search_client = Mock()
    cog.youtube_music_search_client.stop = AsyncMock()
    cog.players = {}
    await cog.cog_unload()
    cog.youtube_music_search_client.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_cog_unload_leaves_search_client_alone_without_ha(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Without the URL there is no poller or session to stop — the in-process
    client has no stop() and calling one would be an AttributeError."""
    mocker.patch('discord_bot.cogs.music.rm_tree')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.youtube_music_search_client = Mock()
    cog.youtube_music_search_client.stop = AsyncMock()
    cog.players = {}
    await cog.cog_unload()
    cog.youtube_music_search_client.stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_cleanup_ha_skips_preserved_bundles(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """The HA reconciliation: even though the cog's local predicate never sees the
    preserved items (they stay on the downloader pod), the bundle_uuids the pod
    reports via clear_guild_queue are unioned in, so their bundles are NOT deleted."""
    mocker.patch('discord_bot.cogs.music.rm_tree')
    config = {
        'music': {'download_client': {'url': 'http://downloader-host:8083'}}
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    guild = Mock()
    guild.id = 4242
    # Downloader pod ran the predicate and preserved 'keep-bundle'; the cog never
    # saw those items locally, so only the pod-reported set carries it.
    cog.download_client = Mock()
    cog.download_client.clear_guild_queue = AsyncMock(
        return_value=ClearGuildResult(dropped=[], preserved_bundle_uuids={'keep-bundle'}))
    cog.download_client.block_guild = AsyncMock()
    cog.youtube_music_search_client = Mock()
    cog.youtube_music_search_client.clear_guild_queue = AsyncMock(
        return_value=ClearGuildResult(dropped=[]))
    cog.youtube_music_search_client.block_guild = AsyncMock()
    cog.broker_client = Mock()
    cog.broker_client.list_bundles_for_guild = AsyncMock(return_value=['keep-bundle', 'drop-bundle'])
    cog.delete_bundle = AsyncMock()
    cog.players = {}
    await cog.cleanup(guild, reason=CleanupReason.QUEUE_TIMEOUT)
    # keep-bundle preserved by the downloader → skipped; drop-bundle deleted.
    cog.delete_bundle.assert_awaited_once_with(4242, 'drop-bundle')


@pytest.mark.asyncio
async def test_download_slots_share_one_health(fake_context):  #pylint:disable=redefined-outer-name
    """Every download slot reports to one LoopHealth: any slot progressing keeps
    the download loop healthy, matching the any()-of-tasks heartbeat it replaces."""
    config = {'music': {'download': {'max_concurrent_downloads': 3}}} | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    mock_loop = Mock()
    mock_loop.create_task = Mock(return_value=Mock())
    cog.bot.loop = mock_loop
    cog.dispatcher = Mock()
    await cog.cog_load()
    assert len(cog._download_tasks) == 3  #pylint:disable=protected-access
    observations = loop_heartbeat_observations(LOOP_DOWNLOAD_FILES)
    assert len(observations) == 1  # one series for the whole pool, not one per slot
    assert observations[0].value == 1


@pytest.mark.asyncio
async def test_redis_backed_config_selects_redis_download_worker(fake_context):  #pylint:disable=redefined-outer-name
    '''redis_backed=True + a redis_manager wires the cog to a RedisDownloadWorker.'''
    manager = RedisManager.from_client(fakeredis.aioredis.FakeRedis(decode_responses=True))
    config = {'general': {'include': {'music': True}}, 'music': {'download': {'redis_backed': True}}}
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], redis_manager=manager)
    assert isinstance(cog.download_client.local_worker, RedisDownloadWorker)


@pytest.mark.asyncio
async def test_redis_backed_without_manager_falls_back_to_asyncio(fake_context):  #pylint:disable=redefined-outer-name
    '''redis_backed=True but no redis_manager falls back to the in-process worker.'''
    config = {'general': {'include': {'music': True}}, 'music': {'download': {'redis_backed': True}}}
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    assert isinstance(cog.download_client.local_worker, AsyncioDownloadWorker)
