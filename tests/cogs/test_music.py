from datetime import datetime
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from unittest.mock import patch, MagicMock

from discord.errors import NotFound
import pytest

from discord_bot.database import Playlist, PlaylistItem, VideoCache, VideoCacheBackup
from discord_bot.exceptions import ExitEarlyException, CogMissingRequiredArg
from discord_bot.cogs.music import Music, match_generator, VideoEditing

from discord_bot.cogs.music_helpers.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.download_client import VideoTooLong, VideoBanned
from discord_bot.cogs.music_helpers.download_client import ExistingFileException, BotDownloadFlagged, DownloadClientException, DownloadError
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.message_queue import SourceLifecycleStage
from discord_bot.cogs.music_helpers.search_client import SearchException
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload

from tests.helpers import mock_session, fake_source_dict, fake_source_download, FakeChannel
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import FakeResponse, FakeMessage, FakeVoiceClient, FakeContext

BASE_MUSIC_CONFIG = {
    'general': {
        'include': {
            'music': True
        }
    },
}

def test_match_generator_no_data():
    func = match_generator(None, None)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    result = func(info, incomplete=None) #pylint:disable=assignment-from-no-return
    assert result is None

def test_match_generator_too_long():
    func = match_generator(1, None)
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoTooLong) as exc:
        func(info, incomplete=None)
    assert 'Video Too Long' in str(exc.value)

def test_match_generator_banned_vidoes():
    func = match_generator(None, ['https://example.com/foo'])
    info = {
        'duration': 100,
        'webpage_url': 'https://example.com/foo',
        'id': '1234',
        'extractor': 'foo extractor'
    }
    with pytest.raises(VideoBanned) as exc:
        func(info, incomplete=None)
    assert 'Video Banned' in str(exc.value)


def test_match_generator_video_exists(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(Path(tmp_dir), fake_context=fake_context) as sd:
            x = VideoCacheClient(Path(tmp_dir), 10, partial(mock_session, fake_engine), None, None)
            x.iterate_file(sd)
            func = match_generator(None, None, video_cache_search=partial(x.search_existing_file))
            info = {
                'duration': 120,
                'webpage_url': sd.webpage_url, #pylint:disable=no-member
                'id': sd.id, #pylint:disable=no-member
                'extractor': sd.extractor, #pylint:disable=no-member
            }
            with pytest.raises(ExistingFileException) as exc:
                func(info, incomplete=None)
            assert 'File already downloaded' in str(exc)
            assert exc.value.video_cache

@pytest.mark.asyncio
async def test_message_loop(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    assert await cog.send_messages() is True

@pytest.mark.asyncio
async def test_message_loop_bot_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    cog.bot_shutdown = True
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    with pytest.raises(ExitEarlyException) as exc:
        await cog.send_messages()
    assert 'Bot in shutdown and i dont have any more messages, exiting early' in str(exc.value)

@pytest.mark.asyncio
async def test_message_loop_send_single_message(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.iterate_single_message([partial(fake_context['channel'].send, 'test message')])
    await cog.send_messages()
    assert fake_context['channel'].messages[0].content == 'test message'

@pytest.mark.asyncio
async def test_message_play_order(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    cog.message_queue.iterate_play_order(fake_context['guild'].id)
    result = await cog.send_messages()
    assert result is True

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    x = fake_source_dict(fake_context)
    cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.SEND, fake_context['channel'].send, 'Original message')
    await cog.send_messages()
    assert x.message.content == 'Original message'

@pytest.mark.asyncio
async def test_message_loop_source_lifecycle_delete(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    def delete_message_raise(*args, **kwargs):
        raise NotFound(FakeResponse(), 'Message not found')

    x = fake_source_dict(fake_context)
    cog.message_queue.iterate_source_lifecycle(x, SourceLifecycleStage.DELETE, delete_message_raise, '')
    assert not await cog.send_messages()

@pytest.mark.asyncio
async def test_get_player(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players

@pytest.mark.asyncio
async def test_get_player_and_then_check_voice(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['guild'].voice_client = None
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    assert fake_context['guild'].id in cog.players
    result = await cog.get_player(fake_context['guild'].id, check_voice_client_active=True)
    assert result is None

@pytest.mark.asyncio
async def test_get_player_join_channel(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], join_channel=fake_context['channel'])
    assert fake_context['guild'].id in cog.players

@pytest.mark.asyncio
async def test_get_player_no_create(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=False) is None

@pytest.mark.asyncio
async def test_player_should_update_player_queue_false(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage()
    fake_context['channel'].messages = [fake_message]
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        fake_message,
    ]
    result = await cog.player_should_update_queue_order(player)
    assert not result

@pytest.mark.asyncio
async def test_player_should_update_player_queue_true(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage()
    fake_message_dos = FakeMessage()
    fake_context['channel'].messages = [fake_message]
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        fake_message_dos,
    ]
    result = await cog.player_should_update_queue_order(player)
    assert result

@pytest.mark.asyncio
async def test_player_clear_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        FakeMessage(content='```Num|Wait|Message\n01|02:00|Foo Song ///Uploader```')
    ]
    result = await cog.clear_player_queue(player.guild.id)
    assert not cog.player_messages[player.guild.id]
    assert result is True

@pytest.mark.asyncio
async def test_player_update_queue_order_only_new(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].content == f'```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || {sd.title} /// {sd.uploader}```' #pylint:disable=no-member

@pytest.mark.asyncio
async def test_player_update_queue_order_delete_and_edit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        FakeMessage(content='foo bar'),
        FakeMessage(content='second message')
    ]
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].content == f'```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || {sd.title} /// {sd.uploader}```' #pylint:disable=no-member
            assert len(cog.player_messages[player.guild.id]) == 1

@pytest.mark.asyncio
async def test_player_update_queue_order_no_edit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    fake_message = FakeMessage(id='first-123', content='```Pos|| Wait Time|| Title /// Uploader\n--------------------------------------------------------------------------------------------------\n1  || 0:00:00  || Foo Title /// Foo Uploader```') #pylint:disable=no-member
    mocker.patch.object(MusicPlayer, 'start_tasks')
    player = await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.player_messages[player.guild.id] = [
        fake_message,
    ]
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            player.add_to_play_queue(sd)
            await cog.player_update_queue_order(player.guild.id)
            assert cog.player_messages[player.guild.id][0].id == 'first-123'

@pytest.mark.asyncio
async def test_get_player_check_voice_client_active(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    assert await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], check_voice_client_active=True) is None

@pytest.mark.asyncio
async def test_youtube_backoff_time_doesnt_exist_yet(fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    assert await cog.youtube_backoff_time(10, 10)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time(freezer, fake_context):  #pylint:disable=redefined-outer-name
    sd = SourceDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_lockfile(sd)
    freezer.move_to('2025-01-01 16:00:00 UTC')
    await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_backoff_time_with_bot_shutdown(freezer, fake_context):  #pylint:disable=redefined-outer-name
    sd = SourceDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_lockfile(sd)
    cog.bot_shutdown = True
    freezer.move_to('2025-01-01 16:00:00 UTC')
    with pytest.raises(ExitEarlyException) as exc:
        await cog.youtube_backoff_time(cog.youtube_wait_period_min, cog.youtube_wait_period_max_variance)
    assert 'Exiting bot wait loop' in str(exc.value)

@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_youtube_last_update_time_with_more_backoff(freezer, fake_context):  #pylint:disable=redefined-outer-name
    sd = SourceDownload(None, {
        'extractor': 'youtube'
    }, None)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    cog.update_download_lockfile(sd, add_additional_backoff=60)
    assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732860'
    cog.update_download_lockfile(sd)
    assert cog.last_download_lockfile.read_text(encoding='utf-8') == '1735732800'


@pytest.mark.asyncio
async def test_cleanup_players_just_bot(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'], create_player=True, join_channel=fake_context['channel'])
    fake_context['channel'].members = [fake_context['bot'].user]
    await cog.cleanup_players()
    assert fake_context['guild'].id not in cog.players

@pytest.mark.asyncio
async def test_history_playlist_update(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            with mock_session(fake_engine) as session:
                assert session.query(Playlist).count() == 1
                assert session.query(PlaylistItem).count() == 1

            # Run twice to exercise dupes aren't created
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            with mock_session(fake_engine) as session:
                assert session.query(Playlist).count() == 1
                assert session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_history_playlist_update_delete_extra_items(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'playlist': {
                'server_playlist_max_size': 1,
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            s2 = fake_source_dict(fake_context)
            sd2 = SourceDownload(sd.file_path, {'webpage_url': 'https://foo.example.dos'}, s2)
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd2))
            await cog.playlist_history_update()

            with mock_session(fake_engine) as session:
                assert session.query(Playlist).count() == 1
                assert session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_guild_cleanup(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
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

def yield_fake_search_client(source_dict: SourceDict = None):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            return [source_dict]

    return FakeSearchClient

def yield_fake_download_client(source_download: SourceDownload):

    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            return source_download

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_no_download(mocker, fake_context):  #pylint:disable=redefined-outer-name

    async def fake_callback(source_download: SourceDownload):
        source_download.i_was_called = True

    s = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id,
                   'https://foo.example.com/title', SearchType.DIRECT, download_file=False, post_download_callback_functions=[fake_callback])
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example.com/title'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert sd.i_was_called #pylint:disable=no-member

@pytest.mark.asyncio()
async def test_download_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            cog = Music(fake_context['bot'], config, fake_engine)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.download_queue.put_nowait(fake_context['guild'].id, sd.source_dict)
            await cog.download_files()
            assert cog.players[fake_context['guild'].id].get_queue_items()

def yield_fake_download_client_from_cache(video_cache: VideoCache):

    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise ExistingFileException('foo', video_cache=video_cache)

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_hits_cache(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.video_cache.iterate_file(sd)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.download_queue.put_nowait(fake_context['guild'].id, sd.source_dict)
            await cog.download_files()
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio()
async def test_download_queue_existing_video(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            with mock_session(fake_engine) as db_session:
                video_cache = VideoCache(base_path=str(sd.base_path), video_url='https://foo.bar.example.com', count=0)
                db_session.add(video_cache)
                db_session.commit()
                mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client_from_cache(video_cache))
                cog = Music(fake_context['bot'], config, fake_engine)
                await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
                cog.download_queue.put_nowait(fake_context['guild'].id, sd.source_dict)
                await cog.download_files()
                assert cog.players[fake_context['guild'].id].get_queue_items()

def yield_download_client_bot_flagged():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise BotDownloadFlagged('foo', user_message='woopsie')

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

def yield_download_client_download_exception():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise DownloadClientException('foo', user_message='whoopsie')

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_download_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def bump_value():
        return True

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'https://foo.example', SearchType.DIRECT,
                    video_non_exist_callback_functions=[partial(bump_value)])
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.download_queue.put_nowait(fake_context['guild'].id, s)
    await cog.download_files()
    assert not cog.players[fake_context['guild'].id].get_queue_items()

def yield_download_client_download_error():
    class FakeDownloadClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def create_source(self, *_args, **_kwargs):
            raise DownloadError('foo')

    return FakeDownloadClient

@pytest.mark.asyncio()
async def test_download_queue_download_error(mocker, fake_context):  #pylint:disable=redefined-outer-name
    async def bump_value():
        return True
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_error())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    s = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'https://foo.example', SearchType.DIRECT,
                    video_non_exist_callback_functions=[partial(bump_value)])
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

@pytest.mark.asyncio
async def test_cache_cleanup_no_op(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            cog.players[fake_context['guild'].id].add_to_play_queue(sd)
            cog.video_cache.iterate_file(sd)
            cog.video_cache.ready_remove()
            await cog.cache_cleanup()
            assert cog.video_cache.get_webpage_url_item(sd.source_dict)

@pytest.mark.asyncio
async def test_cache_cleanup_uploads_object_storage(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                },
                'storage': {
                    'backend': 's3',
                    'bucket_name': 'foo',
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog.players[fake_context['guild'].id].add_to_play_queue(sd)
            mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
            cog.video_cache.iterate_file(sd)
            await cog.cache_cleanup()
            with mock_session(fake_engine) as session:
                assert session.query(VideoCache).count() == 1
                assert session.query(VideoCacheBackup).count() == 1

@pytest.mark.asyncio
async def test_cache_cleanup_removes(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 1,
                },
                'storage': {
                    'backend': 's3',
                    'bucket_name': 'foo',
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            with fake_source_download(tmp_dir, fake_context=fake_context) as sd2:
                mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.upload_file', return_value=True)
                mocker.patch('discord_bot.cogs.music_helpers.video_cache_client.delete_file', return_value=True)
                cog.video_cache.iterate_file(sd)
                cog.video_cache.iterate_file(sd2)
                cog.video_cache.ready_remove()
                await cog.cache_cleanup()
                assert not cog.video_cache.get_webpage_url_item(sd.source_dict)

@pytest.mark.asyncio
async def test_cache_cleanup_skips_source_in_transit(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                    'max_cache_files': 1,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd2:
                cog.video_cache.iterate_file(sd)
                cog.video_cache.iterate_file(sd2)
                cog.video_cache.ready_remove()
                cog.sources_in_transit[sd.source_dict.uuid] = str(sd.base_path)
                await cog.cache_cleanup()
                assert cog.video_cache.get_webpage_url_item(sd.source_dict)

@pytest.mark.asyncio
async def test_add_source_to_player_caches_video(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            await cog.add_source_to_player(sd, cog.players[fake_context['guild'].id])
            assert cog.players[fake_context['guild'].id].get_queue_items()
            assert cog.video_cache.get_webpage_url_item(sd.source_dict)


@pytest.mark.asyncio
async def test_add_source_to_player_puts_blocked(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'music': {
            'download': {
                'cache': {
                    'enable_cache_files': True,
                }
            }
        }
    } | BASE_MUSIC_CONFIG
    cog = Music(fake_context['bot'], config, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.players[fake_context['guild'].id]._play_queue.block() #pylint:disable=protected-access
    with TemporaryDirectory() as tmp_dir:
        s = SourceDict(fake_context['guild'].id, fake_context['author'].display_name, fake_context['author'].id, 'foo artist foo title', SearchType.SPOTIFY)
        with fake_source_download(tmp_dir, source_dict=s) as sd:
            result = await cog.add_source_to_player(sd, cog.players[fake_context['guild'].id])
            assert not result

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

def yield_search_client_check_source(source_dict_list: List[SourceDict]):
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            return source_dict_list

    return FakeSearchClient

def yield_search_client_check_source_raises():
    class FakeSearchClient():
        def __init__(self, *_args, **_kwargs):
            pass

        async def check_source(self, *_args, **_kwargs):
            raise SearchException('foo', user_message='woopsie')

    return FakeSearchClient

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
    item0 = cog.download_queue.get_nowait()
    item1 = cog.download_queue.get_nowait()
    assert item0 == s
    assert item1 == s1

@pytest.mark.asyncio()
async def test_skip(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.source_dict))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
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
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.source_dict))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
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
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.players[fake_context['guild'].id]._history.put_nowait(sd) #pylint:disable=protected-access
            await cog.history_(cog, fake_context['context'])
            m0 = cog.message_queue.get_next_message()
            assert m0[1][0].args[0] == f'```Pos|| Title /// Uploader\n--------------------------------------------------------------------------------------\n1  || {sd.title} /// {sd.uploader}```' #pylint:disable=no-member

@pytest.mark.asyncio()
async def test_shuffle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.source_dict))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
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
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.source_dict))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
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
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.source_dict))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
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
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
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
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            fake_channel2 = FakeChannel(guild=fake_context['guild'])
            fake_context2 = FakeContext(guild=fake_context['guild'], channel=fake_channel2, bot=fake_context['bot'], author=fake_context['author'])
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(sd.source_dict))
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
            await cog.play_(cog, fake_context['context'], search='foo bar')
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
    cog.message_queue.get_next_message()
    m1 = cog.message_queue.get_next_message()
    assert m1[1].source_dict == s1
    assert m1[1].message_content == f'❌ {s1} (failed: play queue is full)'

@pytest.mark.asyncio()
async def test_play_called_raises_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source_raises())
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)
    await cog.play_(cog, fake_context['context'], search='foo bar')
    m0 = cog.message_queue.get_next_message()
    assert m0[1][0].args[0] == 'woopsie'

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
        with fake_source_download(tmp_dir, fake_context=fake_context, is_direct_search=True) as sd:
            mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
            mocker.patch.object(MusicPlayer, 'start_tasks')
            mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_search_client_check_source([sd.source_dict]))
            cog = Music(fake_context['bot'], config, fake_engine)
            cog.video_cache.iterate_file(sd)
            await cog.play_(cog, fake_context['context'], search='foo bar')
            assert cog.players[fake_context['guild'].id].get_queue_items()

@pytest.mark.asyncio
async def test_create_playlist(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count()

@pytest.mark.asyncio
async def test_create_playlist_invalid_name(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create(cog, fake_context['context'], name='__playhistory__derp')
    with mock_session(fake_engine) as db_session:
        assert not db_session.query(Playlist).count()

@pytest.mark.asyncio
async def test_create_playlist_same_name_twice(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count() == 1

@pytest.mark.asyncio
async def test_list_playlist(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list(cog, fake_context['context'])

    _result0 = cog.message_queue.get_single_message()
    result1 = cog.message_queue.get_single_message()
    assert result1[0].args[0] == '```ID || Playlist Name                                                   || Last Queued\n---------------------------------------------------------------------------------------------\n0  || History Playlist                                                || N/A\n1  || new-playlist                                                    || N/A```'


@pytest.mark.asyncio
async def test_list_playlist_with_history(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list(cog, fake_context['context'])

    _result0 = cog.message_queue.get_single_message()
    result1 = cog.message_queue.get_single_message()
    assert result1[0].args[0] == '```ID || Playlist Name                                                   || Last Queued\n---------------------------------------------------------------------------------------------\n0  || History Playlist                                                || N/A\n1  || new-playlist                                                    || N/A```'

@pytest.mark.asyncio()
async def test_playlsit_add_item_invalid_history(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_item_add(cog, fake_context['context'], 0, search='https://foo.example')
    result0 = cog.message_queue.get_single_message()

    assert result0[0].args[0] == '❌ Cannot add to history playlist, is reserved and cannot be added to manually'

@pytest.mark.asyncio()
async def test_playlsit_add_item_function(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add(cog, fake_context['context'], 1, search='https://foo.example')
    await cog.download_files()
    with mock_session(fake_engine) as db_session:
        assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio()
async def test_playlist_remove_item(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add(cog, fake_context['context'], 1, search='https://foo.example')
    await cog.download_files()
    await cog.playlist_item_remove(cog, fake_context['context'], 1, 1)
    with mock_session(fake_engine) as db_session:
        assert db_session.query(PlaylistItem).count() == 0

@pytest.mark.asyncio()
async def test_playlist_show(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add(cog, fake_context['context'], 1, search='https://foo.example')
    await cog.download_files()

    await cog.playlist_show(cog, fake_context['context'], 1)
    cog.message_queue.get_next_message()
    cog.message_queue.get_next_message()
    m2 = cog.message_queue.get_next_message()
    assert m2[1][0].args[0] == '```Pos|| Title /// Uploader\n----------------------------------------------------------------------\n1  || foo /// foobar```'

@pytest.mark.asyncio()
async def test_playlist_delete(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name

    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add(cog, fake_context['context'], 1, search='https://foo.example')
    await cog.download_files()

    await cog.playlist_delete(cog, fake_context['context'], 1)
    with mock_session(fake_engine) as db_session:
        assert db_session.query(PlaylistItem).count() == 0
        assert db_session.query(Playlist).count() == 0

@pytest.mark.asyncio()
async def test_playlist_delete_history(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name

    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_delete(cog, fake_context['context'], 0)
    result = cog.message_queue.get_single_message()
    assert result[0].args[0] == 'Cannot delete history playlist, is reserved'



@pytest.mark.asyncio
async def test_playlist_rename(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_rename(cog, fake_context['context'], 1, playlist_name='foo-bar-playlist')
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count() == 1
        item = db_session.query(Playlist).first()
        assert item.name == 'foo-bar-playlist'

@pytest.mark.asyncio
async def test_playlist_rename_history(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_rename(cog, fake_context['context'], 0, playlist_name='foo-bar-playlist')
    result = cog.message_queue.get_single_message()
    assert result[0].args[0] == 'Cannot rename history playlist, is reserved'

@pytest.mark.asyncio
async def test_history_save(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access

            await cog.playlist_history_save(cog, fake_context['context'], name='foobar')
            with mock_session(fake_engine) as db_session:
                # 2 since history playlist will have been created
                assert db_session.query(Playlist).count() == 2
                assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio
async def test_queue_save(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._play_queue.put(sd) #pylint:disable=protected-access

            await cog.playlist_queue_save(cog, fake_context['context'], name='foobar')
            with mock_session(fake_engine) as db_session:
                # 2 since history playlist will have been created
                assert db_session.query(Playlist).count() == 2
                assert db_session.query(PlaylistItem).count() == 1



@pytest.mark.asyncio()
async def test_play_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add(cog, fake_context['context'], 1, search='https://foo.example')
    await cog.download_files()

    await cog.playlist_queue(cog, fake_context['context'], 1)
    assert cog.download_queue.queues[fake_context['guild'].id]


@pytest.mark.asyncio
async def test_playlist_history_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            await cog.playlist_queue(cog, fake_context['context'], 0)
            assert cog.download_queue.queues[fake_context['guild'].id]

@pytest.mark.asyncio
async def test_random_play_deletes_no_existent_video(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            await cog.playlist_queue(cog, fake_context['context'], 0)
            await cog.download_files()
            with mock_session(fake_engine) as db_session:
                assert db_session.query(Playlist).count() == 1
                assert db_session.query(PlaylistItem).count() == 0

@pytest.mark.asyncio()
async def test_playlist_merge(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_create(cog, fake_context['context'], name='delete-me')
    await cog.playlist_item_add(cog, fake_context['context'], 2, search='https://foo.example')
    await cog.download_files()
    await cog.playlist_merge(cog, fake_context['context'], 1, 2)
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count() == 1
        assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio()
async def test_playlist_merge_history(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = SourceDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_merge(cog, fake_context['context'], 0, 1)
    cog.message_queue.get_single_message()
    result = cog.message_queue.get_single_message()
    assert result[0].args[0] == 'Cannot merge history playlist, is reserved'

@pytest.mark.asyncio()
async def test_random_play(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_random_play(cog, fake_context['context'])
    result = cog.message_queue.get_single_message()
    assert result[0].args[0] == 'Function deprecated, please use `!playlist queue 0`'

def test_video_editing_postprocessor():
    """Test VideoEditing post-processor with successful edit"""
    # Test successful edit
    processor = VideoEditing()
    mock_path = MagicMock()
    mock_path.__str__ = MagicMock(return_value='/path/to/file.mp3')

    with patch('discord_bot.cogs.music.Path', return_value=mock_path), \
         patch('discord_bot.cogs.music.edit_audio_file', return_value='/path/to/edited.mp3'):

        info = {'_filename': '/path/to/file.mp3'}
        result_list, result_info = processor.run(info)

        assert not result_list
        assert result_info['_filename'] == '/path/to/edited.mp3'
        assert result_info['filepath'] == '/path/to/edited.mp3'

def test_video_editing_postprocessor_no_edit():
    """Test VideoEditing post-processor when edit fails/returns None"""
    processor = VideoEditing()
    mock_path = MagicMock()
    mock_path.__str__ = MagicMock(return_value='/path/to/file.mp3')

    with patch('discord_bot.cogs.music.Path', return_value=mock_path), \
         patch('discord_bot.cogs.music.edit_audio_file', return_value=None):

        info = {'_filename': '/path/to/file.mp3'}
        result_list, result_info = processor.run(info)

        assert not result_list
        assert result_info['_filename'] == '/path/to/file.mp3'
        assert result_info['filepath'] == '/path/to/file.mp3'

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

def test_get_history_playlist_method(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test __get_history_playlist private method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Call the private method - it returns playlist ID
    result = cog._Music__get_history_playlist(fake_context['guild'].id)  # pylint: disable=protected-access

    # Verify a playlist ID was returned
    assert result is not None
    assert isinstance(result, int)

    # Verify it was saved to database
    with mock_session(fake_engine) as session:
        playlists = session.query(Playlist).all()
        assert len(playlists) == 1
        assert playlists[0].server_id == str(fake_context['guild'].id)
        assert playlists[0].name.startswith('__playhistory__')
        assert playlists[0].is_history is True

def test_playlist_insert_item_method(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test __playlist_insert_item private method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with mock_session(fake_engine) as session:
        # Create a playlist first
        playlist = Playlist(
            server_id=str(fake_context['guild'].id),
            name='test-playlist',
            created_at=datetime.now(),
            is_history=False
        )
        session.add(playlist)
        session.commit()
        playlist_id = playlist.id

        # Insert an item
        cog._Music__playlist_insert_item(  # pylint: disable=protected-access
            playlist_id,
            'https://example.com/video',
            'Test Video Title',
            'Test Uploader'
        )

        # Verify item was inserted
        items = session.query(PlaylistItem).all()
        assert len(items) == 1
        assert items[0].playlist_id == playlist_id
        assert items[0].video_url == 'https://example.com/video'
        assert items[0].title == 'Test Video Title'
        assert items[0].uploader == 'Test Uploader'

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

@pytest.mark.asyncio
async def test_update_download_lockfile_method(fake_context):  #pylint:disable=redefined-outer-name
    """Test update_download_lockfile method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    with TemporaryDirectory() as tmp_dir:
        with fake_source_download(tmp_dir, fake_context=fake_context) as sd:
            # Test basic lockfile update
            cog.update_download_lockfile(sd)

            # Verify lockfile was created and contains timestamp
            assert cog.last_download_lockfile.exists()
            timestamp_str = cog.last_download_lockfile.read_text(encoding='utf-8')
            timestamp = int(timestamp_str)
            assert timestamp > 0

            # Test with additional backoff
            original_timestamp = timestamp
            cog.update_download_lockfile(sd, add_additional_backoff=60)

            new_timestamp_str = cog.last_download_lockfile.read_text(encoding='utf-8')
            new_timestamp = int(new_timestamp_str)
            assert new_timestamp == original_timestamp + 60
