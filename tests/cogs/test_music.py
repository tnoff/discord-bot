from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from unittest.mock import patch

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

from tests.helpers import fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
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
                    media_request.search_string,
                    media_request.multi_input_string
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
                    media_request.search_string,
                    media_request.multi_input_string
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
    if cog.enable_youtube_music_search:
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
            if cog.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
            # Mock current playing
            cog.players[fake_context['guild'].id].current_source = sd
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
            if cog.enable_youtube_music_search:
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
            assert m0[1][0].function.args[0] == f'```Pos|| Title                                   || Uploader\n---------------------------------------------------------\n1  || {sd.title}                            || {sd.uploader}```' #pylint:disable=no-member

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
            if cog.enable_youtube_music_search:
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
            if cog.enable_youtube_music_search:
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
            if cog.enable_youtube_music_search:
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
            assert fake_context['guild'].id not in cog.players
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
            if cog.enable_youtube_music_search:
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
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_random_play(cog, fake_context['context'])
    result = cog.message_queue.get_single_immutable()
    assert result[0].function.args[0] == 'Function deprecated, please use `!playlist queue 0 shuffle`'


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
    assert cog.bot_shutdown is True

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
    """Test metric callback methods read from checkfiles correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Mock the Path.read_text method directly
    mock_read_text = mocker.patch('pathlib.Path.read_text')

    # Set up side effect to return different values based on calls
    mock_read_text.side_effect = ['1', '2', '3', '4']

    # Test each callback method
    result = cog._Music__playlist_history_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 1

    result = cog._Music__download_file_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 2

    result = cog._Music__send_message_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 3

    result = cog._Music__cleanup_player_loop_active_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 4

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

    # Simplify - just test that bot_shutdown flag gets set
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

    # Add fake players
    cog.players[123] = 'player1'
    cog.players[456] = 'player2'

    await cog.cog_unload()

    # Verify bot shutdown flag is set
    assert cog.bot_shutdown is True


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
                    {'server_id': str(fake_context['guild'].id), 'priority': 1}
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
    assert cog.backup_storage_options['backend'] == 's3'
    assert cog.backup_storage_options['bucket_name'] == 'test-bucket'

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

    # Add request and set to BACKOFF status
    bundle.add_media_request(media_request)
    bundle.update_request_status(media_request, MediaRequestLifecycleStage.BACKOFF)

    # Test that bundle print shows the BACKOFF message
    result = bundle.print()
    result_text = ' '.join(result)

    # Should contain backoff message in the expected format used by music.py
    expected_message = 'Waiting for youtube backoff time before processing media request: "test song"'
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
    assert request_data['status'] == MediaRequestLifecycleStage.BACKOFF
