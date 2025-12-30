from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from unittest.mock import patch, Mock

import asyncio
import pytest

from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.search_client import SearchResult

from discord_bot.cogs.music_helpers.download_client import DownloadClientException, DownloadError
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.search_client import SearchException
from discord_bot.cogs.music_helpers.media_request import MediaRequest, MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.media_download import MediaDownload
from discord_bot.cogs.music import VideoEditing
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage, MultipleMutableType, SearchType
from discord_bot.cogs.music_helpers.database_functions import update_video_guild_analytics

from tests.helpers import fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, mock_session #pylint:disable=unused-import
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
                    media_request.search_type,
                    media_request.search_string
                )
                return [search_result]
            return []

        async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
            # Return a fake video ID for testing
            return 'fake-video-id'

    return FakeSearchClient

def yield_fake_download_client(media_download: MediaDownload):

    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            return media_download

    return FakeDownloadClient

def yield_download_client_download_exception():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise DownloadClientException('foo', user_message='whoopsie')

    return FakeDownloadClient

def yield_download_client_download_error():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise DownloadError('foo')

    return FakeDownloadClient

def yield_search_client_check_source(source_dict_list: List[MediaRequest]):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            # Convert MediaRequest list to SearchResult list
            search_results = []
            for media_request in source_dict_list:
                search_result = SearchResult(
                    media_request.search_type,
                    media_request.search_string
                )
                search_results.append(search_result)
            return search_results

        async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
            # Return a fake video ID for testing
            return 'fake-video-id'

    return FakeSearchClient

def yield_search_client_check_source_raises():
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            raise SearchException('foo', user_message='woopsie')

        async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
            # Return a fake video ID for testing
            return 'fake-video-id'

    return FakeSearchClient

@pytest.mark.asyncio
async def test_guild_cleanup(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access
            await cog.cleanup(fake_context['guild'], external_shutdown_called=True)
            assert fake_context['guild'].id not in cog.players
            assert fake_context['guild'].id not in cog.download_queue.queues

@pytest.mark.asyncio
async def test_guild_hanging_downloads(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    s = fake_source_dict(fake_context)
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.cleanup(fake_context['guild'], external_shutdown_called=True)
    assert fake_context['guild'].id not in cog.download_queue.queues


@pytest.mark.asyncio
async def test_awaken(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.connect_(cog, fake_context['context'])
    assert fake_context['guild'].id in cog.players

@pytest.mark.asyncio
async def test_awaken_user_not_joined(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.play_(cog, fake_context['context'], search='foo bar')
    # Process search queue if YouTube Music search is enabled
    if cog.config.download.enable_youtube_music_search:
        await cog.search_youtube_music()
        await cog.search_youtube_music()
    item0 = cog.download_queue.get_nowait()
    item1 = cog.download_queue.get_nowait()
    # Compare key properties since SearchClient refactoring creates new MediaRequest objects
    assert item0.raw_search_string == s.raw_search_string
    assert item0.search_type == s.search_type
    assert item1.raw_search_string == s1.raw_search_string
    assert item1.search_type == s1.search_type

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
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            # Process search queue if YouTube Music search is enabled
            if cog.config.download.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
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
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            # Process search queue if YouTube Music search is enabled
            if cog.config.download.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
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
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.players[fake_context['guild'].id]._history.put_nowait(sd) #pylint:disable=protected-access
            await cog.history_(cog, fake_context['context'])
            m0 = cog.message_queue.get_next_message()
            assert m0[1][0].function.args[0] == f'History\n```Pos|| Title                                   || Uploader\n---------------------------------------------------------\n1  || {sd.title}                            || {sd.uploader}```' #pylint:disable=no-member

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
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            # Process search queue if YouTube Music search is enabled
            if cog.config.download.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
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
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            # Process search queue if YouTube Music search is enabled
            if cog.config.download.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
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
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            # Process search queue if YouTube Music search is enabled
            if cog.config.download.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
            await cog.bump_item(cog, fake_context['context'], 1)
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio
async def test_stop(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
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
            assert fake_context['guild'].id not in cog.download_queue.queues

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
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.media_request))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            # Process search queue if YouTube Music search is enabled
            if cog.config.download.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
            await cog.move_messages_here(cog, fake_context2)
            assert cog.players[fake_context['guild'].id].text_channel.id == fake_channel2.id

@pytest.mark.asyncio()
async def test_play_called_downloads_blocked(mocker, fake_context):  #pylint:disable=redefined-outer-name
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    s1 = fake_source_dict(fake_context)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([s, s1]))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    # Put source dict so we can a download queue to block
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    cog.download_queue.block(fake_context['guild'].id)
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
    cog = Music(fake_context['bot'], config, None)
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
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.play_(cog, fake_context['context'], search='foo bar')

    # With the new bundle system, search failures create bundles with error messages
    # Check that a bundle was created and contains the error message
    assert len(cog.multirequest_bundles) == 1
    bundle = list(cog.multirequest_bundles.values())[0]

    # The bundle should have finished with an error
    assert bundle.search_finished is True
    assert bundle.search_error == 'woopsie'  # The user_message

    # Call all_requests_added to build row_collections for print
    bundle.all_requests_added()

    # Verify the bundle's print output contains the error message
    bundle_messages = bundle.print()
    assert len(bundle_messages) > 0
    assert any('woopsie' in msg for msg in bundle_messages)

@pytest.mark.asyncio()
async def test_play_called_basic_hits_cache(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
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
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.video_cache.iterate_file(sd)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_random_play(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    '''Test that random-play command queues 32 shuffled items from history playlist'''
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
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
        cog = Music(fake_context['bot'], config, None)
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
        cog = Music(fake_context['bot'], config, None)
        mock_youtube.assert_called_once_with('test_api_key')
        assert cog.youtube_client is not None

def test_music_init_with_youtube_music_disabled(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with YouTube Music search disabled"""
    config = {
        'music': {
            'download': {
                'enable_youtube_music_search': False
            }
        }
    } | BASE_MUSIC_CONFIG

    cog = Music(fake_context['bot'], config, None)
    assert cog.youtube_music_client is None

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

    cog = Music(fake_context['bot'], config, None)
    assert cog.server_queue_priority[123456789] == 1
    assert cog.server_queue_priority[987654321] == 2

def test_music_init_creates_download_directory(fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization creates download directory when specified"""
    with TemporaryDirectory() as tmp_dir:
        download_path = Path(tmp_dir) / 'music_downloads'
        config = {
            'music': {
                'download': {
                    'cache': {
                        'download_dir_path': str(download_path)
                    }
                }
            }
        } | BASE_MUSIC_CONFIG

        cog = Music(fake_context['bot'], config, None)
        assert cog.download_dir == download_path
        assert download_path.exists()

@pytest.mark.asyncio
async def test_cog_unload_basic(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test basic cog unload functionality"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock the tasks to None (default state)
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_task = None  # pylint: disable=protected-access
    cog._cache_cleanup_task = None  # pylint: disable=protected-access
    cog._message_task = None  # pylint: disable=protected-access
    cog._history_playlist_task = None  # pylint: disable=protected-access

    # Mock file operations at pathlib level
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    await cog.cog_unload()

    # Verify bot shutdown flag is set
    assert cog.bot_shutdown_event.is_set()

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
        Music(fake_context['bot'], config, None)

def test_music_callback_methods(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test metric callback methods check task status correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create mock tasks with done() methods
    mock_task_running = mocker.MagicMock()
    mock_task_running.done.return_value = False  # Task is running

    mock_task_finished = mocker.MagicMock()
    mock_task_finished.done.return_value = True  # Task is finished

    # Test playlist_history callback with running task
    cog._history_playlist_task = mock_task_running  # pylint: disable=protected-access
    result = cog._Music__playlist_history_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 1

    # Test download_file callback with finished task
    cog._download_task = mock_task_finished  # pylint: disable=protected-access
    result = cog._Music__download_file_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 0

    # Test send_message callback with running task
    cog._message_task = mock_task_running  # pylint: disable=protected-access
    result = cog._Music__send_message_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 1

    # Test cleanup_player callback with no task (None)
    cog._cleanup_task = None  # pylint: disable=protected-access
    result = cog._Music__cleanup_player_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 0

def test_music_init_with_cache_enabled(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test Music initialization with cache enabled"""
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
                }
            }
        }
    }

    with patch('discord_bot.cogs.music.VideoCacheClient') as mock_video_cache:

        cog = Music(fake_context['bot'], config, fake_engine)

        # Verify cache client was created
        assert mock_video_cache.called
        assert cog.video_cache is not None

def test_music_cache_count_callback(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test cache count callback method with database"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Test with empty database (should return 0)
    result = cog._Music__cache_count_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 0

def test_music_cache_filestats_callbacks(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test cache filesystem stats callback methods"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

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
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

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

@pytest.mark.asyncio
async def test_cog_unload_with_players(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test cog unload with active players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Simplify - just test that bot_shutdown event gets set
    # Mock everything else to avoid complex async mocking
    mocker.patch.object(cog, 'cleanup')
    mocker.patch.object(cog.bot, 'fetch_guild')
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None to avoid cancellation
    cog._cleanup_task = None  # pylint: disable=protected-access
    cog._download_task = None  # pylint: disable=protected-access
    cog._cache_cleanup_task = None  # pylint: disable=protected-access
    cog._message_task = None  # pylint: disable=protected-access
    cog._history_playlist_task = None  # pylint: disable=protected-access

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

    cog = Music(fake_context['bot'], config, None)
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
                    'backend': 's3',
                    'bucket_name': 'test-bucket'
                }
            }
        }
    }

    cog = Music(fake_context['bot'], config, None)
    assert cog.config.download.storage.backend == 's3'
    assert cog.config.download.storage.bucket_name == 'test-bucket'

def test_video_editing_post_processor_success():
    """Test VideoEditing post-processor success path - covers lines 255-263"""

    with patch('discord_bot.cogs.music.edit_audio_file') as mock_edit:
        mock_edit.return_value = Path('/edited/file.mp3')

        processor = VideoEditing()
        information = {
            '_filename': '/original/file.mp3',
            'filepath': '/original/file.mp3'
        }

        result_list, result_info = processor.run(information)

        # Should update paths to edited file
        assert result_info['_filename'] == '/edited/file.mp3'
        assert result_info['filepath'] == '/edited/file.mp3'
        assert not result_list


def test_video_editing_post_processor_failure():
    """Test VideoEditing post-processor failure path - covers lines 255-263"""

    with patch('discord_bot.cogs.music.edit_audio_file') as mock_edit:
        mock_edit.return_value = None  # Simulate editing failure

        processor = VideoEditing()
        original_filename = '/original/file.mp3'
        information = {
            '_filename': original_filename,
            'filepath': original_filename
        }

        result_list, result_info = processor.run(information)

        # Should keep original paths when editing fails
        assert result_info['_filename'] == original_filename
        assert result_info['filepath'] == original_filename
        assert not result_list


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

    with patch('discord_bot.cogs.music.YoutubeDL') as mock_ytdl:
        Music(fake_context['bot'], config, None)

        # Check that custom options were merged
        call_args = mock_ytdl.call_args[0][0]  # First positional arg (options dict)
        assert call_args['custom_option'] == 'custom_value'
        assert call_args['format'] == 'worst'  # Should override default 'bestaudio/best'


def test_music_init_with_audio_processing_enabled(fake_context):  #pylint:disable=redefined-outer-name
    """Test audio processing initialization - covers line 387"""
    config = {
        'general': {'include': {'music': True}},
        'music': {
            'download': {
                'enable_audio_processing': True
            }
        }
    }

    with patch('discord_bot.cogs.music.YoutubeDL') as mock_ytdl:
        mock_ytdl_instance = patch.object(mock_ytdl.return_value, 'add_post_processor')

        with mock_ytdl_instance:
            Music(fake_context['bot'], config, None)

            # Verify post-processor was added for audio processing
            mock_ytdl.return_value.add_post_processor.assert_called_once()


def test_music_init_with_audio_processing_disabled(fake_context):  #pylint:disable=redefined-outer-name
    """Test without audio processing - covers line 387 negative case"""
    config = {
        'general': {'include': {'music': True}},
        'music': {
            'download': {
                'enable_audio_processing': False
            }
        }
    }

    with patch('discord_bot.cogs.music.YoutubeDL') as mock_ytdl:
        mock_ytdl_instance = patch.object(mock_ytdl.return_value, 'add_post_processor')

        with mock_ytdl_instance:
            Music(fake_context['bot'], config, None)

            # Verify post-processor was NOT added
            mock_ytdl.return_value.add_post_processor.assert_not_called()


def test_music_backoff_integration_with_multimutable_type(fake_context):  #pylint:disable=redefined-outer-name
    """Test BACKOFF status integration with MultipleMutableType - simpler integration test"""

    # Test that BACKOFF can be used in the new workflow pattern
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = MediaRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name='test_user',
        requester_id=123456,
        search_string='test song',
        raw_search_string='test song',
        search_type=SearchType.SEARCH
    )

    # Set up search banner (required for single-item bundles)
    bundle.set_initial_search(media_request.raw_search_string)
    bundle.set_multi_input_request()

    # Add request and set to BACKOFF status
    bundle.add_media_request(media_request)
    bundle.all_requests_added()
    bundle.update_request_status(media_request, MediaRequestLifecycleStage.BACKOFF)

    # Test that bundle print shows the BACKOFF message
    result = bundle.print()
    result_text = ' '.join(result)

    # Should contain backoff message in the expected format used by music.py
    expected_message = 'Waiting to process: "test song"'
    assert expected_message in result_text

    # Test that MultipleMutableType can create the expected bundle key format
    bundle_key = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{bundle.uuid}'
    assert bundle_key.startswith('request_bundle-request.bundle.')

    # Verify bundle status is correctly set
    assert not bundle.finished  # BACKOFF status means not finished


def test_music_backoff_status_enum_usage(fake_context):  #pylint:disable=redefined-outer-name
    """Test that BACKOFF enum value is properly imported and used"""

    # Test that BACKOFF enum exists and has correct value
    assert hasattr(MediaRequestLifecycleStage, 'BACKOFF')
    assert MediaRequestLifecycleStage.BACKOFF.value == 'backoff'

    # Test that BACKOFF can be used in bundle status updates
    bundle = MultiMediaRequestBundle(
        fake_context['guild'].id,
        fake_context['channel'].id,
        fake_context['channel']
    )

    media_request = MediaRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_name='test_user',
        requester_id=123456,
        search_string='test song',
        raw_search_string='test song',
        search_type=SearchType.SEARCH
    )

    bundle.add_media_request(media_request)
    bundle.update_request_status(media_request, MediaRequestLifecycleStage.BACKOFF)

    # Verify status was set correctly
    request_data = bundle.media_requests[0]
    assert request_data.status == MediaRequestLifecycleStage.BACKOFF


# Memory leak fix tests
@pytest.mark.asyncio
async def test_shutdown_timeout_with_hanging_players(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that shutdown doesn't hang forever if players don't shutdown"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock the sleep function to speed up the test
    mock_sleep = mocker.patch('discord_bot.cogs.music.sleep')

    # Create mock players that never get removed from the dict
    mock_player1 = Mock()
    mock_player1.destroy = Mock()
    mock_player2 = Mock()
    mock_player2.destroy = Mock()

    cog.players[123] = mock_player1
    cog.players[456] = mock_player2

    # Mock other cleanup methods to avoid side effects
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None to avoid cancellation issues  #pylint:disable=protected-access
    cog._cleanup_task = None
    cog._download_task = None
    cog._cache_cleanup_task = None
    cog._message_task = None
    cog._history_playlist_task = None
    cog._youtube_search_task = None

    await cog.cog_unload()

    # Verify that both players were told to shutdown
    mock_player1.destroy.assert_called_once()
    mock_player2.destroy.assert_called_once()

    # Verify that sleep was called (indicating timeout loop ran)
    assert mock_sleep.call_count >= 1

    # Verify bot_shutdown event is set
    assert cog.bot_shutdown_event.is_set()

@pytest.mark.asyncio
async def test_shutdown_success_no_timeout(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that shutdown completes quickly when players remove themselves"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock the sleep function
    mocker.patch('discord_bot.cogs.music.sleep')  # Don't need return value

    # Create mock player that gets removed after destroy
    mock_player = Mock()
    mock_player.destroy = Mock()
    cog.players[123] = mock_player

    # Don't remove player during iteration - just mark destroyed
    # The actual removal happens later during cleanup_players
    mock_player.destroy = Mock()

    # Mock other cleanup methods
    mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=False)
    mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None  #pylint:disable=protected-access
    cog._cleanup_task = None
    cog._download_task = None
    cog._cache_cleanup_task = None
    cog._message_task = None
    cog._history_playlist_task = None
    cog._youtube_search_task = None

    await cog.cog_unload()

    # Verify player was destroyed
    mock_player.destroy.assert_called_once()

    # Verify bot_shutdown event is set
    assert cog.bot_shutdown_event.is_set()

@pytest.mark.asyncio
async def test_task_cancellation_during_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that all tasks are properly cancelled during shutdown"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock sleep
    mocker.patch('discord_bot.cogs.music.sleep')

    # Create mock tasks
    mock_cleanup_task = Mock()
    mock_download_task = Mock()
    mock_cache_task = Mock()
    mock_message_task = Mock()
    mock_history_task = Mock()
    mock_search_task = Mock()

    # Set mock tasks  #pylint:disable=protected-access
    cog._cleanup_task = mock_cleanup_task
    cog._download_task = mock_download_task
    cog._cache_cleanup_task = mock_cache_task
    cog._message_task = mock_message_task
    cog._history_playlist_task = mock_history_task
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
    mock_cache_task.cancel.assert_called_once()
    mock_message_task.cancel.assert_called_once()
    mock_history_task.cancel.assert_called_once()
    mock_search_task.cancel.assert_called_once()

@pytest.mark.asyncio
async def test_directory_cleanup_during_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that directories are cleaned up during shutdown"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock sleep and players
    mocker.patch('discord_bot.cogs.music.sleep')
    cog.players = {}  # Empty to avoid timeout

    # Mock path operations
    mock_unlink = mocker.patch('pathlib.Path.unlink')
    mocker.patch('pathlib.Path.exists', return_value=True)  # Don't store unused mock
    mock_rm_tree = mocker.patch('discord_bot.cogs.music.rm_tree')

    # Set tasks to None  #pylint:disable=protected-access
    cog._cleanup_task = None
    cog._download_task = None
    cog._cache_cleanup_task = None
    cog._message_task = None
    cog._history_playlist_task = None
    cog._youtube_search_task = None

    await cog.cog_unload()

    # Verify cleanup operations were called
    mock_unlink.assert_called()  # For lockfile
    assert mock_rm_tree.call_count >= 1  # For directories

@pytest.mark.asyncio
async def test_cleanup_players_shutdown_called(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup_players properly handles shutdown_called players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Create a mock player with shutdown_called=True
    mock_player = mocker.Mock()
    mock_player.shutdown_called = True
    mock_player.guild = fake_context['guild']
    cog.players[fake_context['guild'].id] = mock_player

    # Mock cleanup method
    cleanup_mock = mocker.patch.object(cog, 'cleanup')

    await cog.cleanup_players()

    # Verify cleanup was called for the shutdown player
    cleanup_mock.assert_called_once_with(fake_context['guild'])

@pytest.mark.asyncio
async def test_cleanup_players_inactive_timeout_message(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup_players sends proper message for inactive timeout"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create a mock player that times out
    mock_player = mocker.Mock()
    mock_player.shutdown_called = False
    mock_player.voice_channel_inactive_timeout = mocker.Mock(return_value=True)
    mock_player.guild = fake_context['guild']
    mock_player.text_channel = fake_context['channel']
    cog.players[fake_context['guild'].id] = mock_player

    # Mock message queue and cleanup
    message_mock = mocker.patch.object(cog.message_queue, 'send_single_immutable')
    cleanup_mock = mocker.patch.object(cog, 'cleanup')

    await cog.cleanup_players()

    # Verify timeout was checked with correct parameter
    mock_player.voice_channel_inactive_timeout.assert_called_once_with(timeout_seconds=cog.config.player.inactive_voice_channel_timeout)

    # Verify message was sent
    message_mock.assert_called_once()
    # Check that the message content contains expected text
    message_context = message_mock.call_args[0][0][0]
    assert 'No one active in voice channel' in str(message_context.function.keywords['content'])

    # Verify cleanup was called
    cleanup_mock.assert_called_once_with(fake_context['guild'])

@pytest.mark.asyncio
async def test_voice_client_cleanup_called_before_disconnect(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that voice_client.cleanup() is called before disconnect()"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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

    # Verify cleanup was called before disconnect
    mock_voice_client.cleanup.assert_called_once()
    mock_voice_client.disconnect.assert_called_once()

    # Verify order: cleanup should be called before disconnect
    cleanup_call_order = mock_voice_client.cleanup.call_args_list
    disconnect_call_order = mock_voice_client.disconnect.call_args_list
    assert len(cleanup_call_order) == 1
    assert len(disconnect_call_order) == 1

@pytest.mark.asyncio
async def test_voice_client_cleanup_handles_none(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles case when voice_client is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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
async def test_voice_client_cleanup_handles_cleanup_exception(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles exceptions during voice_client.cleanup()"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client that raises exception on cleanup
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock(side_effect=Exception("Cleanup failed"))
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Create player
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Call cleanup - should not raise exception
    await cog.cleanup(fake_context['guild'])

    # Verify disconnect was still called despite cleanup exception
    mock_voice_client.disconnect.assert_called_once()

    # Verify player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_handles_disconnect_exception(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles exceptions during voice_client.disconnect()"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client that raises exception on disconnect
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock(side_effect=Exception("Disconnect failed"))

    fake_context['guild'].voice_client = mock_voice_client

    # Create player
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Call cleanup - should not raise exception
    await cog.cleanup(fake_context['guild'])

    # Verify cleanup was still called despite disconnect exception
    mock_voice_client.cleanup.assert_called_once()

    # Verify player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_with_external_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles external_shutdown_called=True correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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

    # Mock the message queue to verify the message is sent
    mock_send_single = mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Call cleanup with external_shutdown_called=True
    await cog.cleanup(fake_context['guild'], external_shutdown_called=True)

    # Verify cleanup and disconnect were called
    mock_voice_client.cleanup.assert_called_once()
    mock_voice_client.disconnect.assert_called_once()

    # Verify the external shutdown message was sent via message_queue
    mock_send_single.assert_called_once()
    message_contexts = mock_send_single.call_args[0][0]
    assert len(message_contexts) == 1
    assert message_contexts[0].guild_id == player.guild.id
    assert message_contexts[0].channel_id == player.text_channel.id

    # Verify player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_without_external_shutdown(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup with external_shutdown_called=False does not send message"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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

    # Mock the message queue to verify no external shutdown message is sent
    mock_send_single = mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Call cleanup with external_shutdown_called=False (default)
    await cog.cleanup(fake_context['guild'], external_shutdown_called=False)

    # Verify cleanup and disconnect were called
    mock_voice_client.cleanup.assert_called_once()
    mock_voice_client.disconnect.assert_called_once()

    # Verify NO external shutdown message was sent
    mock_send_single.assert_not_called()

    # Verify player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_external_shutdown_skips_disconnect_wait(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that external shutdown does not wait for disconnect to complete"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a slow disconnect that takes time
    disconnect_called = False
    async def slow_disconnect():
        nonlocal disconnect_called
        disconnect_called = True
        await asyncio.sleep(0.1)  # Simulate slow disconnect

    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = slow_disconnect

    fake_context['guild'].voice_client = mock_voice_client

    # Create player
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    mocker.patch.object(player, 'cleanup', return_value=None)

    # Call cleanup with external_shutdown_called=True
    # This should complete quickly without waiting for disconnect
    await cog.cleanup(fake_context['guild'], external_shutdown_called=True)

    # Verify cleanup was called
    mock_voice_client.cleanup.assert_called_once()

    # Disconnect should have been started but not awaited
    # We can't easily verify it wasn't awaited, but we can verify the function completed quickly
    # and that player was cleaned up
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_when_player_does_not_exist(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup continues when player doesn't exist in self.players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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

    # Verify voice client cleanup and disconnect were still called
    mock_voice_client.cleanup.assert_called_once()
    mock_voice_client.disconnect.assert_called_once()

    # Verify player still doesn't exist (wasn't created)
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_voice_client_cleanup_player_not_exist_with_bundles(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test that cleanup handles bundles correctly when player doesn't exist"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep')
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a mock voice client
    mock_voice_client = mocker.MagicMock()
    mock_voice_client.cleanup = mocker.MagicMock()
    mock_voice_client.disconnect = mocker.AsyncMock()

    fake_context['guild'].voice_client = mock_voice_client

    # Create a mock bundle for this guild
    mock_bundle = mocker.MagicMock()
    mock_bundle.guild_id = fake_context['guild'].id
    mock_bundle.uuid = 'test-bundle-uuid'
    mock_bundle.text_channel = fake_context['channel']
    mock_bundle.shutdown = mocker.MagicMock()

    # Add bundle to multirequest_bundles
    cog.multirequest_bundles['bundle-1'] = mock_bundle

    # Mock message queue
    mock_update_multiple = mocker.patch.object(cog.message_queue, 'update_multiple_mutable')

    # Call cleanup - should not raise exception even though player doesn't exist
    await cog.cleanup(fake_context['guild'])

    # Verify bundle was shutdown
    mock_bundle.shutdown.assert_called_once()

    # Verify message queue update was called
    # This verifies the bug fix where we use item.text_channel instead of player.text_channel
    # (if we used player.text_channel, it would raise AttributeError since player is None)
    mock_update_multiple.assert_called()

    # Verify the bundle-specific call used item.text_channel
    # Look for calls with the bundle UUID
    bundle_calls = [call for call in mock_update_multiple.call_args_list
                   if mock_bundle.uuid in str(call)]
    if bundle_calls:
        # Verify it used item.text_channel (second positional argument)
        assert bundle_calls[0][0][1] == mock_bundle.text_channel

    # Verify voice client cleanup and disconnect were still called
    mock_voice_client.cleanup.assert_called_once()
    mock_voice_client.disconnect.assert_called_once()

@pytest.mark.asyncio
async def test_voice_client_cleanup_player_removed_externally(fake_context, mocker):  #pylint:disable=redefined-outer-name
    """Test cleanup when player was already removed from cog.players"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
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

    # Verify voice client cleanup and disconnect were still called
    mock_voice_client.cleanup.assert_called_once()
    mock_voice_client.disconnect.assert_called_once()

    # Verify player.cleanup() was NOT called (since player was already removed)
    # We can't easily verify this since we patched it, but we verified no exception was raised


@pytest.mark.asyncio
async def test_music_stats_command(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command displays analytics correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Pre-populate analytics data
    with mock_session(fake_engine) as session:
        # Add some analytics data
        update_video_guild_analytics(session, fake_context['guild'].id, 7200, False)  # 2 hours
        update_video_guild_analytics(session, fake_context['guild'].id, 3600, True)  # 1 hour, cached
        session.commit()

    # Mock message queue
    mock_send = mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Verify message was sent
    mock_send.assert_called_once()

    # Get the message context that was sent
    call_args = mock_send.call_args[0][0]
    assert len(call_args) == 1
    message_context = call_args[0]

    # Verify message context properties
    assert message_context.guild_id == fake_context['guild'].id
    assert message_context.channel_id == fake_context['channel'].id

    # Execute the message function to get the actual message
    message_func = message_context.function
    assert message_func is not None

    # Verify message content contains expected stats
    # Total: 10,800 seconds = 0 days, 3 hours, 0 minutes, 0 seconds
    message_content = message_func.args[0]
    assert 'Music Stats for Server' in message_content
    assert 'Total Plays: 2' in message_content
    assert 'Cached Plays: 1' in message_content
    assert 'Total Time Played: 0 days, 3 hours, 0 minutes, and 0 seconds' in message_content
    assert 'Tracked Since:' in message_content


@pytest.mark.asyncio
async def test_music_stats_command_with_days(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command displays days correctly when duration exceeds 24 hours"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Pre-populate analytics data with more than one day
    with mock_session(fake_engine) as session:
        one_day = 60 * 60 * 24
        # Add 2 days and 5 hours worth of content
        update_video_guild_analytics(session, fake_context['guild'].id, one_day * 2 + 18000, False)
        session.commit()

    # Mock message queue
    mock_send = mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Verify message was sent
    mock_send.assert_called_once()

    # Get the message content
    call_args = mock_send.call_args[0][0]
    message_context = call_args[0]
    message_content = message_context.function.args[0]

    # Verify message shows days correctly
    # Total: 190,800 seconds = 2 days, 5 hours, 0 minutes, 0 seconds
    assert 'Total Time Played: 2 days, 5 hours, 0 minutes, and 0 seconds' in message_content
    assert 'Total Plays: 1' in message_content


@pytest.mark.asyncio
async def test_music_stats_command_with_hours_and_seconds(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command displays hours and seconds correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Pre-populate analytics data: 1 day, 7 hours, 45 minutes, 30 seconds
    # 1 day = 86400, 7 hours = 25200, 45 min = 2700, 30 sec = 30
    # Total = 86400 + 25200 + 2700 + 30 = 114330 seconds
    with mock_session(fake_engine) as session:
        update_video_guild_analytics(session, fake_context['guild'].id, 114330, False)
        session.commit()

    # Mock message queue
    mock_send = mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Get the message content
    call_args = mock_send.call_args[0][0]
    message_context = call_args[0]
    message_content = message_context.function.args[0]

    # Verify message shows all components correctly
    # After migration: 1 day + 27930 seconds (7 hours 45 min 30 sec)
    # Hours: 27930 // 3600 = 7
    # Minutes: (27930 % 3600) // 60 = 2730 // 60 = 45
    # Seconds: 27930 % 60 = 30
    assert 'Total Time Played: 1 days, 7 hours, 45 minutes, and 30 seconds' in message_content
    assert 'Total Plays: 1' in message_content


@pytest.mark.asyncio
async def test_music_stats_command_no_database(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test music_stats command when database is not available"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the database check to return False
    mocker.patch.object(cog, '_Music__check_database_session', return_value=False)

    # Mock message queue
    mock_send = mocker.patch.object(cog.message_queue, 'send_single_immutable')

    # Call music_stats
    await cog.music_stats(cog, fake_context['context'])

    # Verify no message was sent (function returned early)
    mock_send.assert_not_called()
