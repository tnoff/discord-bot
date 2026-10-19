import asyncio
from datetime import datetime
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.download import LifecycleEvent
from discord_core.types.media_download import MediaDownload
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult

from discord_core.types.playlist import PlaylistEntry, PlaylistItemEntry, PlaylistItemWrite

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer
from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.cogs.music_helpers.player_gateway import CogPlayerGateway
from discord_gateway.command_logic.enqueue import MediaEnqueuer
from discord_gateway.command_logic.playlist import PlaylistCommands
from discord_gateway.command_logic.player_gateway import EnsureResult, PlayerStatus
from discord_gateway.types.playlist_add_result import PlaylistAddResult
from discord_gateway.tests.cogs.test_music import music_config, BASE_MUSIC_CONFIG, yield_fake_download_worker, yield_fake_search_client, yield_download_worker_download_exception
from discord_db.database import Playlist, PlaylistItem

from tests.helpers import async_mock_session, fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, fake_stores #pylint:disable=unused-import
from tests.helpers import FakeVoiceClient
from tests.helpers import attach_in_process_broker
from tests.helpers import attach_in_process_search
from tests.helpers import attach_in_process_download
from tests.helpers import play_through_in_broker, queue_in_broker


def attach_playlist_store(cog, playlist=None, items=None):
    '''Give a database-less cog a playlist store with fixed answers.

    These paths used to be driven by patching async_retry_database_commands and
    feeding it a positional list of canned return values, on a cog built with no
    engine at all. That asserted the order of calls into a helper rather than any
    behaviour, and it broke the moment the calls moved -- which is what a seam
    is for. Here the double answers the store's own methods, and it answers with
    real entries: a MagicMock would satisfy any attribute the code asked for and
    keep passing against a shape that no longer exists.

    cog : Music cog to attach to
    playlist : PlaylistEntry get_playlist should return, or None
    items : PlaylistItemEntry list list_items should return
    '''
    store = AsyncMock()
    store.get_playlist.return_value = playlist
    store.list_items.return_value = items if items is not None else []
    store.mark_queued.return_value = True
    cog.playlist_store = store
    return store


def fake_playlist_items(count, start=0):
    '''Build detached playlist items the way the store hands them back.

    count : How many items to build
    start : First id/url suffix
    '''
    return [
        PlaylistItemEntry(id=index, video_url=f'https://ex.com/{index}',
                          title=f't{index}', uploader='up', playlist_id=1)
        for index in range(start, start + count)
    ]

@pytest.mark.asyncio
async def test_list_playlist(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list.callback(cog, fake_context['context'])

    # Index 0: playlist_create message; index 1: playlist_list message
    assert cog.dispatcher.send_message.call_args_list[1][0][2] == 'Playlist List\n```ID || Playlist Name                                                   || Last Queued\n------------------------------------------------------------------------------------\n0  || Channel History                                                 || N/A\n1  || new-playlist                                                    || N/A```'


@pytest.mark.asyncio
async def test_list_playlist_with_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list.callback(cog, fake_context['context'])

    # Index 0: playlist_create message; index 1: playlist_list message
    assert cog.dispatcher.send_message.call_args_list[1][0][2] == 'Playlist List\n```ID || Playlist Name                                                   || Last Queued\n------------------------------------------------------------------------------------\n0  || Channel History                                                 || N/A\n1  || new-playlist                                                    || N/A```'

@pytest.mark.asyncio()
async def test_playlist_add_item_invalid_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_item_add.callback(cog, fake_context['context'], 0, search='https://foo.example')

    assert cog.dispatcher.send_message.call_args[0][2] == 'Unable to add "https://foo.example" to history playlist, is reserved and cannot be added to manually'

@pytest.mark.asyncio()
async def test_playlsit_add_item_function(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1

@pytest.mark.asyncio()
async def test_playlist_remove_item(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 1)
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 0

@pytest.mark.asyncio()
async def test_playlist_show(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()

    cog.dispatcher.reset_mock()
    await cog.playlist_show.callback(cog, fake_context['context'], 1)
    assert cog.dispatcher.send_message.call_args[0][2] == 'Playlist 1 Items\n```Pos|| Title                           || Uploader\n-------------------------------------------------\n1  || foo                             || foobar```'

@pytest.mark.asyncio()
async def test_playlist_delete(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name

    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()

    await cog.playlist_delete.callback(cog, fake_context['context'], 1)
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 0
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 0

@pytest.mark.asyncio()
async def test_playlist_delete_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name

    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.dispatcher = MagicMock()
    await cog.playlist_delete.callback(cog, fake_context['context'], 0)
    assert cog.dispatcher.send_message.call_args[0][2] == 'Cannot delete history playlist, is reserved'



@pytest.mark.asyncio
async def test_playlist_rename_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.dispatcher = MagicMock()
    await cog.playlist_rename.callback(cog, fake_context['context'], 0, playlist_name='foo-bar-playlist')
    assert cog.dispatcher.send_message.call_args[0][2] == 'Cannot rename history playlist, is reserved'

@pytest.mark.asyncio
async def test_history_save(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await play_through_in_broker(cog, sd)

            await cog.playlist_history_save.callback(cog, fake_context['context'], name='foobar')
            async with async_mock_session(fake_engine) as db_session:
                # 2 since history playlist will have been created
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 2
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1

@pytest.mark.asyncio
async def test_queue_save(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await queue_in_broker(cog, sd)

            await cog.playlist_queue_save.callback(cog, fake_context['context'], name='foobar')
            async with async_mock_session(fake_engine) as db_session:
                # 2 since history playlist will have been created
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 2
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1



@pytest.mark.asyncio()
async def test_play_queue(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()

    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert await cog.download_client.queue_size(fake_context['guild'].id) > 0


@pytest.mark.asyncio
async def test_playlist_history_queue(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
            attach_in_process_broker(cog)
            attach_in_process_download(cog)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            # The broker's history worker records plays now; seed the history playlist directly.
            history_playlist_id = await cog.playlist_store.ensure_history_playlist(fake_context['guild'].id)
            await cog.playlist_store.record_history_item(
                history_playlist_id,
                PlaylistItemWrite(video_url=sd.webpage_url, title=sd.title, uploader=sd.uploader), 64)

            await cog.playlist_queue.callback(cog, fake_context['context'], 0)
            assert await cog.download_client.queue_size(fake_context['guild'].id) > 0

@pytest.mark.asyncio
async def test_random_play_deletes_no_existent_video(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
            attach_in_process_broker(cog)
            attach_in_process_download(cog, worker_cls=yield_download_worker_download_exception())
            search_driver = attach_in_process_search(cog)
            cog.dispatcher = MagicMock()
            cog.bot.loop = asyncio.get_running_loop()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            # The broker's history worker records plays now; seed the history playlist directly.
            history_playlist_id = await cog.playlist_store.ensure_history_playlist(fake_context['guild'].id)
            await cog.playlist_store.record_history_item(
                history_playlist_id,
                PlaylistItemWrite(video_url=sd.webpage_url, title=sd.title, uploader=sd.uploader), 64)

            await cog.playlist_queue.callback(cog, fake_context['context'], 0)
            await search_driver.run_once(cog.bot_shutdown_event)
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            async with async_mock_session(fake_engine) as db_session:
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 0

@pytest.mark.asyncio()
async def test_playlist_merge(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_create.callback(cog, fake_context['context'], name='delete-me')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 2, search='https://foo.example')
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    await cog.playlist_merge.callback(cog, fake_context['context'], 1, 2)
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1
        assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1

@pytest.mark.asyncio()
async def test_playlist_merge_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_gateway.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_merge.callback(cog, fake_context['context'], 0, 1)
    # Index 0: playlist_create message; index 1: merge error message
    assert cog.dispatcher.send_message.call_args_list[1][0][2] == 'Cannot merge history playlist, is reserved'

@pytest.mark.asyncio
async def test_playlist_insert_item_method(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """The cog's store writes an item row.

    This drove `__playlist_insert_item`, a private cog method that took the
    caller's session. That method is gone: counting the items, comparing to the
    ceiling and inserting are one transaction in the store now, because split
    across a caller they are a check-then-act two players can both pass. The
    behaviour it covered is the same, reached through the seam.
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)

    playlist = await cog.playlist_store.create_playlist(fake_context['guild'].id, 'test-playlist')
    outcomes = await cog.playlist_store.add_items(
        playlist.id,
        [PlaylistItemWrite(video_url='https://example.com/video',
                           title='Test Video Title', uploader='Test Uploader')],
        cog.config.playlist.server_playlist_max_size)

    assert [outcome.status.value for outcome in outcomes] == ['added']
    async with async_mock_session(fake_engine) as session:
        items = (await session.execute(select(PlaylistItem))).scalars().all()
    assert len(items) == 1
    assert items[0].playlist_id == playlist.id
    assert items[0].title == 'Test Video Title'
    assert items[0].video_url == 'https://example.com/video'
    assert items[0].uploader == 'Test Uploader'

@pytest.mark.asyncio
async def test_get_history_playlist_method(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test __get_history_playlist private method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)

    # Call the private method - it returns playlist ID
    result = await cog._Music__get_history_playlist(fake_context['guild'].id)  # pylint: disable=protected-access

    # Verify a playlist ID was returned
    assert result is not None
    assert isinstance(result, int)

    # Verify it was saved to database
    async with async_mock_session(fake_engine) as session:
        playlists = (await session.execute(select(Playlist))).scalars().all()
        assert len(playlists) == 1
        assert playlists[0].server_id == fake_context['guild'].id
        assert playlists[0].name.startswith('__playhistory__')
        assert playlists[0].is_history is True

@pytest.mark.asyncio
async def test_playlist_queue_with_shuffle_and_max_num(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test playlist queue with shuffle and max_num arguments in different orders"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test 1: shuffle followed by max_num
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'shuffle', '16')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        16,          # max_num=16
        is_history=False
    )

    # Test 2: max_num followed by shuffle
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, '16', 'shuffle')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        16,          # max_num=16
        is_history=False
    )

@pytest.mark.asyncio
async def test_playlist_queue_with_only_shuffle(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test playlist queue with only shuffle argument"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test with only shuffle
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'shuffle')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        None,        # max_num=None
        is_history=False
    )

@pytest.mark.asyncio
async def test_playlist_queue_with_only_max_num(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test playlist queue with only max_num argument"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test with only max_num
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, '25')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        False,       # shuffle=False
        25,          # max_num=25
        is_history=False
    )

@pytest.mark.asyncio
async def test_playlist_queue_with_no_arguments(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test playlist queue with no additional arguments"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test with no additional arguments
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        False,       # shuffle=False
        None,        # max_num=None
        is_history=False
    )

@pytest.mark.asyncio
async def test_playlist_queue_parameter_parsing_edge_cases(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test edge cases for playlist queue parameter parsing"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test mixed order parameters with multiple numbers (should use first number found)
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'shuffle', '25', '50')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        25,          # max_num=25 (first number found)
        is_history=False
    )

    # Test case sensitivity - SHUFFLE should work
    playlist_queue_mock.reset_mock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'SHUFFLE', '10')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        10,          # max_num=10
        is_history=False
    )

    # Test zero as max_num (should be handled properly)
    playlist_queue_mock.reset_mock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, '0')
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id
        False,       # shuffle=False
        0,           # max_num=0
        is_history=False
    )

@pytest.mark.asyncio
async def test_playlist_queue_history_playlist_basic_command(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that '!playlist queue 0' works for history playlist - entire playlist, no shuffle, no limit"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)

    # Create a player to ensure history playlist exists
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Test the basic command: !playlist queue 0 (history playlist, no arguments)
    await cog.playlist_queue.callback(cog, fake_context['context'], 0)
    playlist_queue_mock.assert_called_with(
        mocker.ANY,  # envelope
        mocker.ANY,  # playlist_id (history playlist ID)
        False,       # shuffle=False (no shuffle)
        None,        # max_num=None (no limit, entire playlist)
        is_history=True  # This should be history playlist
    )

@pytest.mark.asyncio
async def test_playlist_queue_comprehensive_integration(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Comprehensive integration test for all playlist queue functionality"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create player and playlists for comprehensive testing
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist-1')
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist-2')

    # Mock the __playlist_queue method to capture all calls
    playlist_queue_calls = []

    async def capture_playlist_queue(*args, **kwargs):
        playlist_queue_calls.append((args, kwargs))
        return None

    mocker.patch.object(PlaylistCommands, 'queue_items', side_effect=capture_playlist_queue)  #pylint:disable=protected-access

    # Test various command combinations
    test_cases = [
        # (playlist_index, args, expected_shuffle, expected_max_num, expected_is_history)
        (0, [], False, None, True),  # History playlist, no args
        (1, ['shuffle'], True, None, False),  # Regular playlist, shuffle only
        (2, ['10'], False, 10, False),  # Regular playlist, max_num only
        (1, ['shuffle', '5'], True, 5, False),  # shuffle then max_num
        (2, ['15', 'shuffle'], True, 15, False),  # max_num then shuffle
        (0, ['shuffle', '20'], True, 20, True),  # History playlist with args
    ]

    for i, (playlist_index, args, expected_shuffle, expected_max_num, expected_is_history) in enumerate(test_cases):
        playlist_queue_calls.clear()
        await cog.playlist_queue.callback(cog, fake_context['context'], playlist_index, *args)

        # Verify the call was made with expected parameters
        assert len(playlist_queue_calls) == 1, f"Test case {i}: Expected 1 call, got {len(playlist_queue_calls)}"

        call_args, call_kwargs = playlist_queue_calls[0]
        # call_args: (env, playlist_id, shuffle, max_num)
        assert call_args[2] == expected_shuffle, f"Test case {i}: Expected shuffle={expected_shuffle}, got {call_args[2]}"
        assert call_args[3] == expected_max_num, f"Test case {i}: Expected max_num={expected_max_num}, got {call_args[3]}"
        assert call_kwargs.get('is_history', False) == expected_is_history, f"Test case {i}: Expected is_history={expected_is_history}, got {call_kwargs.get('is_history', False)}"

    # Verify our tests covered both regular and history playlists
    history_tests = [case for case in test_cases if case[4]]  # is_history=True
    regular_tests = [case for case in test_cases if not case[4]]  # is_history=False

    assert len(history_tests) >= 2, "Should test history playlist functionality"
    assert len(regular_tests) >= 4, "Should test regular playlist functionality"

@pytest.mark.asyncio
async def test_playlist_show_empty_playlist_message_context_fix(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist show on empty playlist creates proper MessageContext (bug fix for 'str' object has no attribute 'function')"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a player to ensure history playlist exists
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Create an empty playlist
    await cog.playlist_create.callback(cog, fake_context['context'], name='empty-playlist')

    # Reset dispatcher to isolate playlist_show messages
    cog.dispatcher.reset_mock()

    # Show the empty playlist - this should not crash
    await cog.playlist_show.callback(cog, fake_context['context'], 1)

    # Verify message was sent via dispatcher
    cog.dispatcher.send_message.assert_called_once()

    # Verify the message content is correct
    assert 'No items in playlist' in str(cog.dispatcher.send_message.call_args[0][2]), \
           f"Message should contain 'No items in playlist', got: {cog.dispatcher.send_message.call_args[0][2]}"

@pytest.mark.asyncio
async def test_playlist_queue_empty_playlist_user_feedback(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist queue on empty playlist provides helpful user feedback message"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a player to ensure voice functionality works
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Create an empty playlist
    await cog.playlist_create.callback(cog, fake_context['context'], name='empty-playlist')

    # Reset dispatcher to isolate playlist_queue messages
    cog.dispatcher.reset_mock()

    # Try to queue the empty playlist - should get helpful message
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)

    # Verify user gets helpful feedback message
    cog.dispatcher.send_message.assert_called_once()

    # Verify the message content is correct
    message_text = str(cog.dispatcher.send_message.call_args[0][2])
    assert 'contains no items to queue' in message_text, \
           f"Message should contain 'contains no items to queue', got: {message_text}"
    assert 'empty-playlist' in message_text, \
           f"Message should contain playlist name 'empty-playlist', got: {message_text}"

@pytest.mark.asyncio
async def test_playlist_queue_empty_history_playlist_feedback(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist queue on empty history playlist provides helpful feedback with correct name"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog.dispatcher = MagicMock()

    # Create a player to ensure history playlist exists
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Try to queue empty history playlist (playlist index 0)
    await cog.playlist_queue.callback(cog, fake_context['context'], 0)

    # Verify user gets helpful feedback message
    cog.dispatcher.send_message.assert_called_once()

    # Verify the message content shows "Channel History" (not the database playlist name)
    message_text = str(cog.dispatcher.send_message.call_args[0][2])
    assert 'contains no items to queue' in message_text, \
           f"Message should contain 'contains no items to queue', got: {message_text}"
    assert 'Channel History' in message_text, \
           f"Message should contain 'Channel History', got: {message_text}"

@pytest.mark.asyncio
async def test_playlist_queue_adds_history_playlist_item_id(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist queue operations add history_playlist_item_id to media requests"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)

    # The store answers with real entries, so a request built from one carries
    # the item id the history path depends on.
    attach_playlist_store(
        cog,
        playlist=PlaylistEntry(id=123, name='Test Playlist'),
        items=[
            PlaylistItemEntry(id=1, video_url='https://youtube.com/watch?v=123',
                              title='Video 1', uploader='up', playlist_id=123),
            PlaylistItemEntry(id=2, video_url='https://youtube.com/watch?v=456',
                              title='Video 2', uploader='up', playlist_id=123),
        ])

    # Mock the enqueue_media_requests method
    captured_requests = []
    async def mock_enqueue(ctx, entries, bundle_uuid, player=None):  #pylint:disable=unused-argument
        captured_requests.extend(entries)
        return True

    with patch.object(MediaEnqueuer, 'enqueue_media_requests', side_effect=mock_enqueue):
        with patch.object(cog, 'get_player', return_value=MagicMock()):
            # Call the private playlist queue method directly
            # pylint: disable=protected-access
            await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), 123, False, 0, False)

            # Verify media requests were created with history_playlist_item_id
            assert len(captured_requests) == 2

            for req in captured_requests:
                assert req.history_playlist_item_id in [1, 2]


@pytest.mark.asyncio
async def test_playlist_queue_completion_messaging_simplified(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist queue completion messaging is simplified in new version"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)

    attach_playlist_store(
        cog,
        playlist=PlaylistEntry(id=1, name='Test Playlist'),
        items=[
            PlaylistItemEntry(id=1, video_url='https://youtube.com/watch?v=123',
                              title='Video 1', uploader='up', playlist_id=1),
        ])

    cog.dispatcher = MagicMock()
    with patch.object(MediaEnqueuer, 'enqueue_media_requests', return_value=False):  # finished_all = False
        with patch.object(cog, 'get_player', return_value=MagicMock()):
            # Call the private playlist queue method directly
            # pylint: disable=protected-access
            await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), 123, False, 0, False)

            # Verify only failure message is sent (hit limit case)
            cog.dispatcher.send_message.assert_called_once()
            # The message should contain the playlist name and indicate limit hit
            # We can't easily test the exact message without executing the partial function


@pytest.mark.asyncio
async def test_playlist_queue_bundle_creation_with_channel_id(fake_context):  #pylint:disable=redefined-outer-name
    """Test that enqueue_media_requests registers requests against a broker bundle"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    attach_in_process_search(cog)

    # Test the method that routes entries into the broker bundle
    entries = [fake_source_dict(fake_context), fake_source_dict(fake_context)]

    # Create a mock player
    mock_player = MagicMock()
    mock_player.guild = fake_context['guild']
    mock_player.text_channel = fake_context['channel']

    # Create a broker-owned bundle for the test
    bundle_uuid = await cog.broker_client.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )

    # Call enqueue_media_requests directly to test bundle population
    result = await cog.enqueue_logic.enqueue_media_requests(envelope_from_ctx(fake_context['context']), entries, bundle_uuid)

    assert result is True

    # Verify the broker holds the bundle with correct guild/channel and the
    # two requests attached.
    state = cog.broker_client.local_broker.get_bundle_state(bundle_uuid)
    assert state is not None
    assert state.guild_id == fake_context['guild'].id
    assert state.channel_id == fake_context['channel'].id
    assert len(state.bundled_requests) == 2


@pytest.mark.asyncio
async def test_history_playlist_queue_behavior(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test history playlist queue retains special behavior"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)

    # The stored name is overridden by the is_history branch, so it is set to
    # something recognisably different here.
    attach_playlist_store(
        cog,
        playlist=PlaylistEntry(id=1, name='Auto-generated History', is_history=True),
        items=[
            PlaylistItemEntry(id=1, video_url='https://youtube.com/watch?v=123',
                              title='Video 1', uploader='up', playlist_id=1),
        ])

    cog.dispatcher = MagicMock()
    with patch.object(MediaEnqueuer, 'enqueue_media_requests', return_value=False):  # finished_all = False
        with patch.object(cog, 'get_player', return_value=MagicMock()):
            # Call history playlist queue (playlist_id = guild_id for history)
            # pylint: disable=protected-access
            await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), fake_context['guild'].id, False, 0, True)

            # For history playlists, should still send completion message
            cog.dispatcher.send_message.assert_called_once()

            # Message should mention "Channel History" not the database playlist name
            # This is set by the special is_history logic


# ---------------------------------------------------------------------------
# Coverage gap tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_playlist_group_no_subcommand(fake_context):  #pylint:disable=redefined-outer-name
    """playlist group fires 'Invalid sub command' when invoked without a subcommand"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    fake_context['context'].invoked_subcommand = None
    await cog.playlist.callback(cog, fake_context['context'])
    assert 'Invalid sub command' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_get_history_playlist_existing(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_history_playlist returns existing id on second call without creating a new one"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    # pylint: disable=protected-access
    playlist_id1 = await cog._Music__get_history_playlist(fake_context['guild'].id)
    playlist_id2 = await cog._Music__get_history_playlist(fake_context['guild'].id)
    assert playlist_id1 == playlist_id2
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist).where(Playlist.is_history == True))).scalar() == 1  #pylint:disable=singleton-comparison


@pytest.mark.asyncio
async def test_playlist_queue_save_create_fails(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save returns early when __playlist_create returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    # name with PLAYHISTORY_PREFIX causes __playlist_create to return None
    await cog.playlist_queue_save.callback(cog, fake_context['context'], name='__playhistory__invalid')
    # Only __playlist_create's rejection message should be sent
    assert cog.dispatcher.send_message.call_count == 1
    assert 'cannot contain' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_queue_save_no_player(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save sends 'No player connected' when no player exists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_queue_save.callback(cog, fake_context['context'], name='save-test')
    assert any('No player connected' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_save_empty_queue(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save sends 'no videos' message when player queue is empty"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    with patch.object(CogPlayerGateway, 'status', return_value=PlayerStatus(exists=True)):
        await cog.playlist_queue_save.callback(cog, fake_context['context'], name='save-empty')
    assert any('no videos' in call[0][2].lower() for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_save_max_length(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save stops adding items and sends message when playlist is full"""
    config = music_config({'music': {'playlist': {'server_playlist_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as item1, \
                fake_media_download(tmp_dir, fake_context=fake_context) as item2:
            await queue_in_broker(cog, item1, item2)
            with patch.object(CogPlayerGateway, 'status', return_value=PlayerStatus(exists=True)):
                await cog.playlist_queue_save.callback(cog, fake_context['context'], name='full-test')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('Cannot add more items' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_queue_save_duplicate(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save sends 'likely already exists' for duplicate items"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as item1, \
                fake_media_download(tmp_dir, fake_context=fake_context) as item2:
            # Two queue entries for the same video
            item2.webpage_url = item1.webpage_url
            item2.title = item1.title
            await queue_in_broker(cog, item1, item2)
            with patch.object(CogPlayerGateway, 'status', return_value=PlayerStatus(exists=True)):
                await cog.playlist_queue_save.callback(cog, fake_context['context'], name='dup-test')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('likely already exists' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_queue_internal_shuffle(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_queue shuffles items when shuffle=True"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    attach_playlist_store(cog, playlist=PlaylistEntry(id=1, name='Playlist'),
                          items=fake_playlist_items(3))
    with patch.object(MediaEnqueuer, 'enqueue_media_requests', return_value=True):
        # pylint: disable=protected-access
        await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), 1, True, 0, False)
    # No exception means shuffle path executed
    assert cog.dispatcher.send_message.call_count == 0


@pytest.mark.asyncio
async def test_playlist_queue_internal_max_num_negative(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_queue sends error and returns when max_num < 0"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    attach_playlist_store(cog, playlist=PlaylistEntry(id=1, name='Playlist'),
                          items=fake_playlist_items(1, start=1))
    with patch.object(MediaEnqueuer, 'enqueue_media_requests', return_value=True):
        # pylint: disable=protected-access
        await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), 1, False, -1, False)
    assert any('Invalid number of videos' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_internal_max_num_truncates(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_queue truncates items to max_num when max_num < len(items)"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    attach_playlist_store(cog, playlist=PlaylistEntry(id=1, name='Playlist'),
                          items=fake_playlist_items(5))
    captured = []

    async def capture_enqueue(_ctx, items, *_args, **_kwargs):
        captured.extend(items)
        return True

    with patch.object(MediaEnqueuer, 'enqueue_media_requests', side_effect=capture_enqueue):
        # pylint: disable=protected-access
        await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), 1, False, 2, False)
    assert len(captured) == 2


@pytest.mark.asyncio
async def test_playlist_queue_internal_max_num_larger_than_list(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_queue uses full list when max_num >= len(items)"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    attach_playlist_store(cog, playlist=PlaylistEntry(id=1, name='Playlist'),
                          items=fake_playlist_items(2))
    captured = []

    async def capture_enqueue(_ctx, items, *_args, **_kwargs):
        captured.extend(items)
        return True

    with patch.object(MediaEnqueuer, 'enqueue_media_requests', side_effect=capture_enqueue):
        # pylint: disable=protected-access
        await cog.playlist_logic.queue_items(envelope_from_ctx(fake_context['context']), 1, False, 5, False)
    assert len(captured) == 2


@pytest.mark.asyncio
async def test_playlist_queue_no_voice(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_queue returns early when author is not in a voice channel"""
    # author.voice is None by default — triggers AttributeError in __check_author_voice_chat
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert any('not in voice chat' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_no_db_check(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_queue returns early when db_engine is None (after voice check passes)"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_queue_player_fails(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_queue returns early when the player cannot be ensured"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(CogPlayerGateway, 'ensure', new=AsyncMock(return_value=EnsureResult(ok=False)))
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    # __check_database_session and the player (not ensured) → function returns
    assert cog.dispatcher.send_message.call_count == 0


@pytest.mark.asyncio
async def test_playlist_queue_playlist_not_found_cmd(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_queue returns None when __get_playlist returns no playlist_id"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch.object(CogPlayerGateway, 'ensure', new=AsyncMock(return_value=EnsureResult(ok=True)))
    # index=1 on empty DB → "No playlists in database" → returns None
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert any('No playlists in database' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_merge_max_length(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_merge stops and sends 'already max size' when PlaylistMaxLength raised"""
    config = music_config({'music': {'playlist': {'server_playlist_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='p1')
    await cog.playlist_create.callback(cog, fake_context['context'], name='p2')
    async with async_mock_session(fake_engine) as db_session:
        playlists = (await db_session.execute(select(Playlist).where(Playlist.is_history == False))).scalars().all()  #pylint:disable=singleton-comparison
    p1_id = playlists[0].id
    p2_id = playlists[1].id
    await cog.playlist_store.add_items(
        p1_id, [PlaylistItemWrite(video_url='https://ex.com/a', title='A', uploader='up')], 1)
    await cog.playlist_store.add_items(
        p2_id, [PlaylistItemWrite(video_url='https://ex.com/b', title='B', uploader='up')], 1)
    cog.dispatcher.reset_mock()
    await cog.playlist_merge.callback(cog, fake_context['context'], '1', '2')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('already max size' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_random_play_no_voice(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns early when author is not in a voice channel"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert any('not in voice chat' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_random_play_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns early when db_engine is None"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_random_play_player_fails(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns early when the player cannot be ensured"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(CogPlayerGateway, 'ensure', new=AsyncMock(return_value=EnsureResult(ok=False)))
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert cog.dispatcher.send_message.call_count == 0


@pytest.mark.asyncio
async def test_playlist_random_play_no_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns None when no history playlist exists"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(CogPlayerGateway, 'ensure', new=AsyncMock(return_value=EnsureResult(ok=True)))
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert any('Invalid playlist index 0' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_item_add_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_item_add returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='test')
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_add_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_item_add returns None when __get_playlist returns no playlist_id"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='test')
    assert cog.dispatcher.send_message.call_count == 1
    assert 'No playlists in database' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_random_play_success(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_random_play calls __playlist_queue when all prerequisites are met"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    mocker.patch.object(CogPlayerGateway, 'ensure', new=AsyncMock(return_value=EnsureResult(ok=True)))
    # pylint: disable=protected-access
    await cog._Music__get_history_playlist(fake_context['guild'].id)
    playlist_queue_mock = mocker.patch.object(PlaylistCommands, 'queue_items', new_callable=AsyncMock)
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    playlist_queue_mock.assert_called_once()
    _args, kwargs = playlist_queue_mock.call_args
    assert kwargs.get('shuffle') is True
    assert kwargs.get('max_num') == 32
    assert kwargs.get('is_history') is True


@pytest.mark.asyncio
async def test_add_playlist_item_marks_failed_when_playlist_full(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__add_playlist_item pushes FAILED with the 'playlist too long' reason
    when __playlist_insert_item raises PlaylistMaxLength."""
    config = music_config({'music': {'playlist': {'server_playlist_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    push_mock = AsyncMock()
    mocker.patch.object(MediaEnqueuer, 'push_state', push_mock)

    async with async_mock_session(fake_engine) as session:
        playlist = Playlist(server_id=fake_context['guild'].id, name='full',
                            created_at=datetime.now(), is_history=False)
        session.add(playlist)
        await session.commit()
        await session.refresh(playlist)
        # Saturate to max so the next insert raises PlaylistMaxLength.
        session.add(PlaylistItem(title='t', video_url='https://ex.com/a',
                                 uploader='u', playlist_id=playlist.id))
        await session.commit()
        playlist_id = playlist.id

    request = PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_id=fake_context['author'].id,
        requester_name=fake_context['author'].display_name,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='https://ex.com/b'),
        playlist_id=playlist_id,
    )
    result = PlaylistAddResult(webpage_url='https://ex.com/b', title='B', uploader='u')
    await cog.enqueue_logic.add_playlist_item(request, result)  #pylint:disable=protected-access
    push_mock.assert_awaited_once()
    args, kwargs = push_mock.await_args
    assert args[0] is request
    assert args[1] is LifecycleEvent.FAILED
    assert 'playlist too long' in kwargs['failure_reason']


@pytest.mark.asyncio
async def test_add_playlist_item_marks_failed_when_item_already_exists(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__add_playlist_item pushes FAILED with 'already exists' when the item
    URL is already in the playlist (insert returns None)."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    cog.dispatcher = MagicMock()
    push_mock = AsyncMock()
    mocker.patch.object(MediaEnqueuer, 'push_state', push_mock)

    async with async_mock_session(fake_engine) as session:
        playlist = Playlist(server_id=fake_context['guild'].id, name='dup',
                            created_at=datetime.now(), is_history=False)
        session.add(playlist)
        await session.commit()
        await session.refresh(playlist)
        session.add(PlaylistItem(title='same', video_url='https://ex.com/same',
                                 uploader='u', playlist_id=playlist.id))
        await session.commit()
        playlist_id = playlist.id

    request = PlaylistAddRequest(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        requester_id=fake_context['author'].id,
        requester_name=fake_context['author'].display_name,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='https://ex.com/same'),
        playlist_id=playlist_id,
    )
    result = PlaylistAddResult(webpage_url='https://ex.com/same', title='same', uploader='u')
    await cog.enqueue_logic.add_playlist_item(request, result)  #pylint:disable=protected-access
    push_mock.assert_awaited_once()
    args, kwargs = push_mock.await_args
    assert args[0] is request
    assert args[1] is LifecycleEvent.FAILED
    assert 'already exists' in kwargs['failure_reason']


@pytest.mark.asyncio
async def test_public_view_is_none_when_the_playlist_vanishes_mid_lookup(fake_context):  #pylint:disable=redefined-outer-name
    """A playlist deleted between the two store calls resolves to no index.

    The lookup is a fetch followed by a list, and those are two round trips
    once the store is remote -- so a `!playlist delete` landing between them is
    a real interleaving, not a hypothetical. None is the answer; the caller
    prints it as the ID and carries on.
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    store = attach_playlist_store(
        cog, playlist=PlaylistEntry(id=7, name='vanishing',
                                    server_id=fake_context['guild'].id, is_history=False))
    store.list_playlists.return_value = []

    # pylint: disable=protected-access
    assert await cog.playlist_logic.get_playlist_public_view(7, fake_context['guild'].id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('stub,args,kwargs,method,logic_args', [
    ('playlist_create', (), {'name': 'mine'}, 'create', ('mine',)),
    ('playlist_list', (), {}, 'list_playlists', ()),
    ('playlist_show', (3,), {}, 'show', (3,)),
    ('playlist_delete', (3,), {}, 'delete', (3,)),
    ('playlist_rename', (3,), {'playlist_name': 'new'}, 'rename', (3, 'new')),
    ('playlist_merge', ('1', '2'), {}, 'merge', ('1', '2')),
    ('playlist_item_remove', (3, 4), {}, 'item_remove', (3, 4)),
])
async def test_store_only_stubs_hand_the_envelope_to_the_logic(mocker, fake_context, stub, args, kwargs, method, logic_args):  #pylint:disable=redefined-outer-name
    '''The behaviour of these commands is tested in tests/command_logic/test_playlist.py; this
    checks only that each stub forwards the envelope and its parsed arguments.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    handler = mocker.patch.object(PlaylistCommands, method, new_callable=AsyncMock)
    await getattr(cog, stub).callback(cog, fake_context['context'], *args, **kwargs)
    handler.assert_awaited_once_with(envelope_from_ctx(fake_context['context']), *logic_args)
