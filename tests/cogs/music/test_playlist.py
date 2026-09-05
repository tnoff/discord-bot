import asyncio
from datetime import datetime, timezone, timedelta
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock, AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.database import Playlist, PlaylistItem
from discord_bot.cogs.music import Music
from discord_bot.types.history_playlist_item import HistoryPlaylistItem
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.download import LifecycleEvent
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.playlist_add_result import PlaylistAddResult
from discord_bot.types.playlist import PlaylistEntry, PlaylistItemEntry, PlaylistItemWrite
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.music_player import MusicPlayer

from tests.cogs.test_music import music_config, BASE_MUSIC_CONFIG, yield_fake_download_worker, yield_fake_search_client, yield_download_worker_download_exception
from tests.helpers import async_mock_session, fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, fake_stores #pylint:disable=unused-import
from tests.helpers import FakeVoiceClient
from tests.helpers import attach_in_process_broker
from tests.helpers import attach_in_process_search
from tests.helpers import attach_in_process_download


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
async def test_create_playlist(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar()

@pytest.mark.asyncio
async def test_create_playlist_invalid_name(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='__playhistory__derp')
    async with async_mock_session(fake_engine) as db_session:
        assert not (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar()

@pytest.mark.asyncio
async def test_create_playlist_same_name_twice(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1

@pytest.mark.asyncio
async def test_create_playlist_message_includes_public_id(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlist creation message includes the public playlist ID"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create first playlist - should get public ID 1
    await cog.playlist_create.callback(cog, fake_context['context'], name='first-playlist')
    assert cog.dispatcher.send_message.call_args_list[0][0][2] == 'Created playlist "first-playlist" with ID 1'

    # Create second playlist - should get public ID 2
    await cog.playlist_create.callback(cog, fake_context['context'], name='second-playlist')
    # Newest first, so the playlist just created is always index 1 and the
    # earlier one has shifted down. That is what production has always done.
    assert cog.dispatcher.send_message.call_args_list[1][0][2] == 'Created playlist "second-playlist" with ID 1'

    # Verify playlists were actually created in database
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 2

@pytest.mark.asyncio
async def test_create_playlist_message_with_none_public_id(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test playlist creation message handles None public ID gracefully"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Mock __get_playlist_public_view to return None
    mocker.patch.object(cog, '_Music__get_playlist_public_view', return_value=None)

    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')
    assert cog.dispatcher.send_message.call_args[0][2] == 'Created playlist "test-playlist" with ID None'

@pytest.mark.asyncio
async def test_list_playlist(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list.callback(cog, fake_context['context'])

    # Index 0: playlist_create message; index 1: playlist_list message
    assert cog.dispatcher.send_message.call_args_list[1][0][2] == 'Playlist List\n```ID || Playlist Name                                                   || Last Queued\n------------------------------------------------------------------------------------\n0  || Channel History                                                 || N/A\n1  || new-playlist                                                    || N/A```'


@pytest.mark.asyncio
async def test_list_playlist_with_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_item_add.callback(cog, fake_context['context'], 0, search='https://foo.example')

    assert cog.dispatcher.send_message.call_args[0][2] == 'Unable to add "https://foo.example" to history playlist, is reserved and cannot be added to manually'

@pytest.mark.asyncio()
async def test_playlsit_add_item_function(fake_engine, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.dispatcher = MagicMock()
    await cog.playlist_delete.callback(cog, fake_context['context'], 0)
    assert cog.dispatcher.send_message.call_args[0][2] == 'Cannot delete history playlist, is reserved'



@pytest.mark.asyncio
async def test_playlist_rename(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_rename.callback(cog, fake_context['context'], 1, playlist_name='foo-bar-playlist')
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1
        item = (await db_session.execute(select(Playlist))).scalars().first()
        assert item.name == 'foo-bar-playlist'

@pytest.mark.asyncio
async def test_playlist_rename_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    cog.dispatcher = MagicMock()
    await cog.playlist_rename.callback(cog, fake_context['context'], 0, playlist_name='foo-bar-playlist')
    assert cog.dispatcher.send_message.call_args[0][2] == 'Cannot rename history playlist, is reserved'

@pytest.mark.asyncio
async def test_history_save(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access

            await cog.playlist_history_save.callback(cog, fake_context['context'], name='foobar')
            async with async_mock_session(fake_engine) as db_session:
                # 2 since history playlist will have been created
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 2
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1

@pytest.mark.asyncio
async def test_queue_save(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._play_queue.put(sd) #pylint:disable=protected-access

            await cog.playlist_queue_save.callback(cog, fake_context['context'], name='foobar')
            async with async_mock_session(fake_engine) as db_session:
                # 2 since history playlist will have been created
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 2
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1



@pytest.mark.asyncio()
async def test_play_queue(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    s = fake_source_dict(fake_context)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
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
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
            attach_in_process_broker(cog)
            attach_in_process_download(cog)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.post_play_processing()

            await cog.playlist_queue.callback(cog, fake_context['context'], 0)
            assert await cog.download_client.queue_size(fake_context['guild'].id) > 0

@pytest.mark.asyncio
async def test_random_play_deletes_no_existent_video(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.post_play_processing()

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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_broker(cog)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    search_driver = attach_in_process_search(cog)
    cog.dispatcher = MagicMock()
    cog.bot.loop = asyncio.get_running_loop()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    attach_in_process_download(cog, worker_cls=yield_fake_download_worker(sd))
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test 1: shuffle followed by max_num
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'shuffle', '16')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        16,          # max_num=16
        is_history=False
    )

    # Test 2: max_num followed by shuffle
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, '16', 'shuffle')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test with only shuffle
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'shuffle')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test with only max_num
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, '25')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test with no additional arguments
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)

    # Create a playlist first
    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')

    # Test mixed order parameters with multiple numbers (should use first number found)
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'shuffle', '25', '50')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        25,          # max_num=25 (first number found)
        is_history=False
    )

    # Test case sensitivity - SHUFFLE should work
    playlist_queue_mock.reset_mock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, 'SHUFFLE', '10')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
        mocker.ANY,  # playlist_id
        True,        # shuffle=True
        10,          # max_num=10
        is_history=False
    )

    # Test zero as max_num (should be handled properly)
    playlist_queue_mock.reset_mock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1, '0')
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)

    # Create a player to ensure history playlist exists
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Test the basic command: !playlist queue 0 (history playlist, no arguments)
    await cog.playlist_queue.callback(cog, fake_context['context'], 0)
    playlist_queue_mock.assert_called_with(
        fake_context['context'],
        mocker.ANY,  # player object
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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

    mocker.patch.object(cog, '_Music__playlist_queue', side_effect=capture_playlist_queue)  #pylint:disable=protected-access

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
        # call_args: (ctx, player, playlist_id, shuffle, max_num)
        assert call_args[3] == expected_shuffle, f"Test case {i}: Expected shuffle={expected_shuffle}, got {call_args[3]}"
        assert call_args[4] == expected_max_num, f"Test case {i}: Expected max_num={expected_max_num}, got {call_args[4]}"
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
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
async def test_get_playlist_public_view_history_playlist_returns_zero(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that history playlists return public view index 0"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Create a test history playlist
    async with async_mock_session(fake_engine) as db_session:
        history_playlist = Playlist(
            name="Channel History",
            server_id=fake_context['guild'].id,
            is_history=True
        )
        db_session.add(history_playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(history_playlist)

        # Test the function
        result = await cog._Music__get_playlist_public_view(history_playlist.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert result == 0


@pytest.mark.asyncio
async def test_get_playlist_public_view_first_playlist_returns_one(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that the first non-history playlist returns public view index 1"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Create test playlists
    async with async_mock_session(fake_engine) as db_session:
        # Create the first playlist (should be index 1)
        playlist1 = Playlist(
            name="First Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist1)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist1)

        # Test the function
        result = await cog._Music__get_playlist_public_view(playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert result == 1


@pytest.mark.asyncio
async def test_get_playlist_public_view_multiple_playlists_correct_ordering(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that multiple playlists return correct public view indices based on creation order"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Create test playlists in specific order
    async with async_mock_session(fake_engine) as db_session:
        # Create playlists in order
        playlist1 = Playlist(
            name="First Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        playlist2 = Playlist(
            name="Second Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        playlist3 = Playlist(
            name="Third Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )

        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.add(playlist2)  #pylint:disable=no-member
        db_session.add(playlist3)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist1)
        await db_session.refresh(playlist2)
        await db_session.refresh(playlist3)

        # Test each playlist returns correct index
        result1 = await cog._Music__get_playlist_public_view(playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access
        result2 = await cog._Music__get_playlist_public_view(playlist2.id, fake_context['guild'].id)  #pylint:disable=protected-access
        result3 = await cog._Music__get_playlist_public_view(playlist3.id, fake_context['guild'].id)  #pylint:disable=protected-access

        # Newest first. These carry no explicit created_at, so they used to tie
        # on NULL and come back in heap order -- which is why this test and
        # test_get_playlist_public_view_ordering_by_creation_time asserted
        # opposite directions and both passed. The model default gives them
        # distinct timestamps now, so only one direction can be right, and
        # production says it is this one.
        assert result3 == 1
        assert result2 == 2
        assert result1 == 3


@pytest.mark.asyncio
async def test_get_playlist_public_view_ignores_history_playlists_in_ordering(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that history playlists don't affect the public view ordering of regular playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    async with async_mock_session(fake_engine) as db_session:
        # Create a history playlist first
        history_playlist = Playlist(
            name="Channel History",
            server_id=fake_context['guild'].id,
            is_history=True
        )

        # Create regular playlists
        playlist1 = Playlist(
            name="First Regular Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        playlist2 = Playlist(
            name="Second Regular Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )

        db_session.add(history_playlist)  #pylint:disable=no-member
        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.add(playlist2)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(history_playlist)
        await db_session.refresh(playlist1)
        await db_session.refresh(playlist2)

        # History playlist should return 0
        history_result = await cog._Music__get_playlist_public_view(history_playlist.id, fake_context['guild'].id)  #pylint:disable=protected-access

        # Regular playlists should be ordered 1, 2 (ignoring history)
        result1 = await cog._Music__get_playlist_public_view(playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access
        result2 = await cog._Music__get_playlist_public_view(playlist2.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert history_result == 0
        # Newest first among the non-history playlists.
        assert result2 == 1
        assert result1 == 2


@pytest.mark.asyncio
async def test_get_playlist_public_view_different_servers_isolated(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that playlists from different servers don't affect each other's public view indices"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Create second fake guild for testing
    other_guild_id = fake_context['guild'].id + 1

    async with async_mock_session(fake_engine) as db_session:
        # Create playlists for first server
        server1_playlist1 = Playlist(
            name="Server 1 - Playlist 1",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        server1_playlist2 = Playlist(
            name="Server 1 - Playlist 2",
            server_id=fake_context['guild'].id,
            is_history=False
        )

        # Create playlists for second server
        server2_playlist1 = Playlist(
            name="Server 2 - Playlist 1",
            server_id=other_guild_id,
            is_history=False
        )

        db_session.add(server1_playlist1)  #pylint:disable=no-member
        db_session.add(server1_playlist2)  #pylint:disable=no-member
        db_session.add(server2_playlist1)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(server1_playlist1)
        await db_session.refresh(server1_playlist2)
        await db_session.refresh(server2_playlist1)

        # Server 1 playlists should be ordered 1, 2
        s1_result1 = await cog._Music__get_playlist_public_view(server1_playlist1.id, fake_context['guild'].id)  #pylint:disable=protected-access
        s1_result2 = await cog._Music__get_playlist_public_view(server1_playlist2.id, fake_context['guild'].id)  #pylint:disable=protected-access

        # Server 2 playlist should be index 1 (not affected by server 1)
        s2_result1 = await cog._Music__get_playlist_public_view(server2_playlist1.id, other_guild_id)  #pylint:disable=protected-access

        # Newest first within each guild; the guilds stay independent.
        assert s1_result2 == 1
        assert s1_result1 == 2
        assert s2_result1 == 1


@pytest.mark.asyncio
async def test_get_playlist_public_view_nonexistent_playlist_returns_none(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that requesting a non-existent playlist returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Create a regular playlist for comparison
    async with async_mock_session(fake_engine) as db_session:
        playlist = Playlist(
            name="Test Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member

        # Test with non-existent playlist ID
        nonexistent_id = 99999
        result = await cog._Music__get_playlist_public_view(nonexistent_id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert result is None


@pytest.mark.asyncio
async def test_get_playlist_public_view_cross_server_playlist_returns_none(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that requesting a playlist from a different server returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Create second fake guild
    other_guild_id = str(int(fake_context['guild'].id) + 1)

    async with async_mock_session(fake_engine) as db_session:
        # Create playlist for first server
        playlist = Playlist(
            name="Server 1 Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist)

        # Try to get the playlist's public view from a different server
        result = await cog._Music__get_playlist_public_view(playlist.id, str(other_guild_id))  #pylint:disable=protected-access

        assert result is None


@pytest.mark.asyncio
async def test_get_playlist_public_view_ordering_by_creation_time(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Playlists are ordered by created_at DESC -- newest first, which is index 1.

    This and the neighbouring test used to assert opposite directions and both
    passed, because the ORDER BY applied here (explicit timestamps) and did
    nothing there (none supplied, and every row NULL). Populating created_at
    forced a choice, and production answered it: all 32 playlist rows already
    carried distinct timestamps, so DESC had been in effect the whole time and
    the public index has always been newest-first. The neighbour now agrees
    with this one rather than the other way round.
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    async with async_mock_session(fake_engine) as db_session:
        # Create playlists with specific creation timestamps
        base_time = datetime.now(timezone.utc)

        # Create in reverse chronological order to test ordering
        playlist_newest = Playlist(
            name="Newest Playlist",
            server_id=fake_context['guild'].id,
            is_history=False,
            created_at=base_time + timedelta(hours=2)
        )
        playlist_middle = Playlist(
            name="Middle Playlist",
            server_id=fake_context['guild'].id,
            is_history=False,
            created_at=base_time + timedelta(hours=1)
        )
        playlist_oldest = Playlist(
            name="Oldest Playlist",
            server_id=fake_context['guild'].id,
            is_history=False,
            created_at=base_time
        )

        # Add in non-chronological order
        db_session.add(playlist_newest)  #pylint:disable=no-member
        db_session.add(playlist_oldest)  #pylint:disable=no-member
        db_session.add(playlist_middle)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        await db_session.refresh(playlist_newest)
        await db_session.refresh(playlist_oldest)
        await db_session.refresh(playlist_middle)

        # Ordering is by created_at DESC, not by insert order: these were added
        # newest-first above and come back newest-first regardless.
        oldest_result = await cog._Music__get_playlist_public_view(playlist_oldest.id, fake_context['guild'].id)  #pylint:disable=protected-access
        middle_result = await cog._Music__get_playlist_public_view(playlist_middle.id, fake_context['guild'].id)  #pylint:disable=protected-access
        newest_result = await cog._Music__get_playlist_public_view(playlist_newest.id, fake_context['guild'].id)  #pylint:disable=protected-access

        assert newest_result == 1   # Newest created = index 1
        assert middle_result == 2   # Second newest = index 2
        assert oldest_result == 3   # Oldest created = index 3


@pytest.mark.asyncio
async def test_get_playlist_public_view_handles_empty_server(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test behavior when server has no playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    # Try to get public view for non-existent playlist on server with no playlists
    result = await cog._Music__get_playlist_public_view(1, fake_context['guild'].id)  #pylint:disable=protected-access

    assert result is None


@pytest.mark.asyncio
async def test_get_playlist_public_view_mixed_history_and_regular_complex(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test complex scenario with mixed history and regular playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)

    async with async_mock_session(fake_engine) as db_session:
        base_time = datetime.now(timezone.utc)

        # Create complex mix of playlists
        playlists = [
            Playlist(name="Regular 1", server_id=fake_context['guild'].id, is_history=False, created_at=base_time),
            Playlist(name="History 1", server_id=fake_context['guild'].id, is_history=True, created_at=base_time + timedelta(minutes=10)),
            Playlist(name="Regular 2", server_id=fake_context['guild'].id, is_history=False, created_at=base_time + timedelta(minutes=20)),
            Playlist(name="History 2", server_id=fake_context['guild'].id, is_history=True, created_at=base_time + timedelta(minutes=30)),
            Playlist(name="Regular 3", server_id=fake_context['guild'].id, is_history=False, created_at=base_time + timedelta(minutes=40)),
        ]

        for playlist in playlists:
            db_session.add(playlist)  #pylint:disable=no-member
        await db_session.commit()  #pylint:disable=no-member
        for playlist in playlists:
            await db_session.refresh(playlist)

        results = []
        for playlist in playlists:
            result = await cog._Music__get_playlist_public_view(playlist.id, fake_context['guild'].id)  #pylint:disable=protected-access
            results.append(result)

        # History playlists should return 0
        # Regular playlists are ordered by creation time DESC (newest first)
        # Regular 1 (oldest): created at base_time -> index 3
        # Regular 2 (middle): created at base_time+20min -> index 2
        # Regular 3 (newest): created at base_time+40min -> index 1
        expected = [3, 0, 2, 0, 1]  # Regular 1=3, History 1=0, Regular 2=2, History 2=0, Regular 3=1

        assert results == expected

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

    with patch.object(cog, 'enqueue_media_requests', side_effect=mock_enqueue):
        with patch.object(cog, 'get_player', return_value=MagicMock()):
            # Call the private playlist queue method directly
            # pylint: disable=protected-access
            await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 123, False, 0, False)

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
    with patch.object(cog, 'enqueue_media_requests', return_value=False):  # finished_all = False
        with patch.object(cog, 'get_player', return_value=MagicMock()):
            # Call the private playlist queue method directly
            # pylint: disable=protected-access
            await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 123, False, 0, False)

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
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )

    # Call enqueue_media_requests directly to test bundle population
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle_uuid, mock_player)

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
    with patch.object(cog, 'enqueue_media_requests', return_value=False):  # finished_all = False
        with patch.object(cog, 'get_player', return_value=MagicMock()):
            # Call history playlist queue (playlist_id = guild_id for history)
            # pylint: disable=protected-access
            await cog._Music__playlist_queue(fake_context['context'], MagicMock(), fake_context['guild'].id, False, 0, True)

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
    cog.dispatcher = MagicMock()
    fake_context['context'].invoked_subcommand = None
    await cog.playlist.callback(cog, fake_context['context'])
    assert 'Invalid sub command' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_create_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_create returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='test')
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_list_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_list returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_list.callback(cog, fake_context['context'])
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_list_no_playlists(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_list sends 'No playlists in database' when DB is empty"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_list.callback(cog, fake_context['context'])
    assert 'No playlists in database' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_list_with_last_queued(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_list formats last_queued date when set on a playlist"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='dated-playlist')
    async with async_mock_session(fake_engine) as db_session:
        p = (await db_session.execute(select(Playlist).where(Playlist.is_history == False))).scalars().first()  #pylint:disable=singleton-comparison
        p.last_queued = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        await db_session.commit()
    cog.dispatcher.reset_mock()
    await cog.playlist_list.callback(cog, fake_context['context'])
    output = cog.dispatcher.send_message.call_args[0][2]
    assert '2024-06-15 12:00:00' in output


@pytest.mark.asyncio
async def test_get_history_playlist_existing(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_history_playlist returns existing id on second call without creating a new one"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    # pylint: disable=protected-access
    playlist_id1 = await cog._Music__get_history_playlist(fake_context['guild'].id)
    playlist_id2 = await cog._Music__get_history_playlist(fake_context['guild'].id)
    assert playlist_id1 == playlist_id2
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(Playlist).where(Playlist.is_history == True))).scalar() == 1  #pylint:disable=singleton-comparison


@pytest.mark.asyncio
async def test_get_playlist_invalid_string_index(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_playlist sends error message and returns (None, False) for non-numeric index"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    # pylint: disable=protected-access
    result = await cog._Music__get_playlist('abc', fake_context['context'])
    assert result == (None, False)
    assert 'Invalid playlist index abc' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_get_playlist_no_playlists_in_database(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_playlist sends 'No playlists in database' when index>0 and DB is empty"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    # pylint: disable=protected-access
    result = await cog._Music__get_playlist(1, fake_context['context'])
    assert result == (None, False)
    assert 'No playlists in database' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_get_playlist_history_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__get_playlist sends 'Invalid playlist index 0' when index=0 and no history playlist"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    # pylint: disable=protected-access
    result = await cog._Music__get_playlist(0, fake_context['context'])
    assert result == (None, False)
    assert 'Invalid playlist index 0' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_remove_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_item_remove returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 1)
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_remove_playlist_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_item_remove returns when __get_playlist returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 1)
    # __get_playlist sends "No playlists in database"; item_remove returns without further message
    assert cog.dispatcher.send_message.call_count == 1
    assert 'No playlists in database' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_remove_invalid_video_index(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_item_remove sends error when video_index cannot be cast to int"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='p')
    cog.dispatcher.reset_mock()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 'abc')
    assert 'Invalid item index abc' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_remove_negative_video_index(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_item_remove sends error when video_index < 1"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='p')
    cog.dispatcher.reset_mock()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 0)
    assert 'Invalid item index 0' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_remove_item_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_item_remove sends 'Unable to find item' when no item at that index"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='p')
    cog.dispatcher.reset_mock()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 1)
    assert 'Unable to find item 1' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_show_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_show returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_show.callback(cog, fake_context['context'], 1)
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_show_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_show returns when __get_playlist returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_show.callback(cog, fake_context['context'], 1)
    assert cog.dispatcher.send_message.call_count == 1
    assert 'No playlists in database' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_delete_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_delete returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_delete.callback(cog, fake_context['context'], 1)
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_delete_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_delete sends explicit 'Cannot delete' message when playlist not found"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_delete.callback(cog, fake_context['context'], 1)
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('Cannot delete playlist' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_rename_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_rename returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_rename.callback(cog, fake_context['context'], 1, playlist_name='new')
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_rename_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_rename returns None when playlist_id not found and not history"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_rename.callback(cog, fake_context['context'], 1, playlist_name='new')
    # __get_playlist sends "No playlists in database", rename itself returns None
    assert cog.dispatcher.send_message.call_count == 1


@pytest.mark.asyncio
async def test_playlist_rename_invalid_name(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_rename rejects names containing PLAYHISTORY_PREFIX"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='my-playlist')
    cog.dispatcher.reset_mock()
    await cog.playlist_rename.callback(cog, fake_context['context'], 1, playlist_name='__playhistory__bad')
    assert 'cannot contain' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_queue_save_create_fails(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save returns early when __playlist_create returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
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
    cog.dispatcher = MagicMock()
    await cog.playlist_queue_save.callback(cog, fake_context['context'], name='save-test')
    assert any('No player connected' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_save_empty_queue(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save sends 'no videos' message when player queue is empty"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mock_player = MagicMock()
    mock_player.get_queue_items.return_value = []
    with patch.object(cog, 'get_player', return_value=mock_player):
        await cog.playlist_queue_save.callback(cog, fake_context['context'], name='save-empty')
    assert any('no videos' in call[0][2].lower() for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_save_max_length(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save stops adding items and sends message when playlist is full"""
    config = music_config({'music': {'playlist': {'server_playlist_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    item1 = MagicMock(webpage_url='https://ex.com/1', title='title1', uploader='up1')
    item2 = MagicMock(webpage_url='https://ex.com/2', title='title2', uploader='up2')
    mock_player = MagicMock()
    mock_player.get_queue_items.return_value = [item1, item2]
    with patch.object(cog, 'get_player', return_value=mock_player):
        await cog.playlist_queue_save.callback(cog, fake_context['context'], name='full-test')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('Cannot add more items' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_queue_save_duplicate(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__playlist_queue_save sends 'likely already exists' for duplicate items"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    item1 = MagicMock(webpage_url='https://ex.com/same', title='title1', uploader='up1')
    item2 = MagicMock(webpage_url='https://ex.com/same', title='title1', uploader='up1')
    mock_player = MagicMock()
    mock_player.get_queue_items.return_value = [item1, item2]
    with patch.object(cog, 'get_player', return_value=mock_player):
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
    with patch.object(cog, 'enqueue_media_requests', return_value=True):
        # pylint: disable=protected-access
        await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 1, True, 0, False)
    # No exception means shuffle path executed
    assert cog.dispatcher.send_message.call_count == 0


@pytest.mark.asyncio
async def test_playlist_queue_internal_max_num_negative(fake_context):  #pylint:disable=redefined-outer-name
    """__playlist_queue sends error and returns when max_num < 0"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    attach_playlist_store(cog, playlist=PlaylistEntry(id=1, name='Playlist'),
                          items=fake_playlist_items(1, start=1))
    with patch.object(cog, 'enqueue_media_requests', return_value=True):
        # pylint: disable=protected-access
        await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 1, False, -1, False)
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

    with patch.object(cog, 'enqueue_media_requests', side_effect=capture_enqueue):
        # pylint: disable=protected-access
        await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 1, False, 2, False)
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

    with patch.object(cog, 'enqueue_media_requests', side_effect=capture_enqueue):
        # pylint: disable=protected-access
        await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 1, False, 5, False)
    assert len(captured) == 2


@pytest.mark.asyncio
async def test_playlist_queue_no_voice(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_queue returns early when author is not in a voice channel"""
    # author.voice is None by default — triggers AttributeError in __check_author_voice_chat
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert any('not in voice chat' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_queue_no_db_check(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_queue returns early when db_engine is None (after voice check passes)"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_queue_player_fails(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_queue returns early when __ensure_player returns None"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch.object(cog, '_Music__ensure_player', return_value=None)
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    # __check_database_session and __ensure_player (None) → function returns
    assert cog.dispatcher.send_message.call_count == 0


@pytest.mark.asyncio
async def test_playlist_queue_playlist_not_found_cmd(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_queue returns None when __get_playlist returns no playlist_id"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch.object(cog, '_Music__ensure_player', return_value=MagicMock())
    # index=1 on empty DB → "No playlists in database" → returns None
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert any('No playlists in database' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_merge_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_merge returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_merge.callback(cog, fake_context['context'], '1', '2')
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_merge_p1_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_merge sends 'Cannot find playlist' for p1 when not found"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_merge.callback(cog, fake_context['context'], '1', '2')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('Cannot find playlist' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_merge_p2_not_found(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_merge sends 'Cannot find playlist' for p2 when only p1 exists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    # Patch __get_playlist so p1 is found but p2 is not
    mocker.patch.object(cog, '_Music__get_playlist', new_callable=AsyncMock,
                        side_effect=[(42, False), (None, False)])
    await cog.playlist_merge.callback(cog, fake_context['context'], '1', '2')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('Cannot find playlist' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_merge_max_length(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_merge stops and sends 'already max size' when PlaylistMaxLength raised"""
    config = music_config({'music': {'playlist': {'server_playlist_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
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
async def test_playlist_merge_duplicate(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_merge sends 'likely already exists' for duplicate item across playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    await cog.playlist_create.callback(cog, fake_context['context'], name='p1')
    await cog.playlist_create.callback(cog, fake_context['context'], name='p2')
    async with async_mock_session(fake_engine) as db_session:
        playlists = (await db_session.execute(select(Playlist).where(Playlist.is_history == False))).scalars().all()  #pylint:disable=singleton-comparison
    first_id = playlists[0].id
    second_id = playlists[1].id
    max_size = cog.config.playlist.server_playlist_max_size
    await cog.playlist_store.add_items(
        first_id, [PlaylistItemWrite(video_url='https://ex.com/same', title='Same', uploader='up')], max_size)
    await cog.playlist_store.add_items(
        second_id, [PlaylistItemWrite(video_url='https://ex.com/same', title='Same', uploader='up')], max_size)
    cog.dispatcher.reset_mock()
    await cog.playlist_merge.callback(cog, fake_context['context'], '1', '2')
    messages = [call[0][2] for call in cog.dispatcher.send_message.call_args_list]
    assert any('likely already exists' in m for m in messages)


@pytest.mark.asyncio
async def test_playlist_random_play_no_voice(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns early when author is not in a voice channel"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert any('not in voice chat' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_random_play_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns early when db_engine is None"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_random_play_player_fails(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns early when __ensure_player returns None"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch.object(cog, '_Music__ensure_player', return_value=None)
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert cog.dispatcher.send_message.call_count == 0


@pytest.mark.asyncio
async def test_playlist_random_play_no_history(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_random_play returns None when no history playlist exists"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    mocker.patch.object(cog, '_Music__ensure_player', return_value=MagicMock())
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    assert any('Invalid playlist index 0' in call[0][2] for call in cog.dispatcher.send_message.call_args_list)


@pytest.mark.asyncio
async def test_playlist_item_add_no_db(fake_context):  #pylint:disable=redefined-outer-name
    """playlist_item_add returns early when db_engine is None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.dispatcher = MagicMock()
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='test')
    assert 'database is not enabled' in cog.dispatcher.send_message.call_args[0][2]


@pytest.mark.asyncio
async def test_playlist_item_add_not_found(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """playlist_item_add returns None when __get_playlist returns no playlist_id"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
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
    cog.dispatcher = MagicMock()
    mocker.patch.object(cog, '_Music__ensure_player', return_value=MagicMock())
    # pylint: disable=protected-access
    await cog._Music__get_history_playlist(fake_context['guild'].id)
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', new_callable=AsyncMock)
    await cog.playlist_random_play.callback(cog, fake_context['context'])
    playlist_queue_mock.assert_called_once()
    _args, kwargs = playlist_queue_mock.call_args
    assert kwargs.get('shuffle') is True
    assert kwargs.get('max_num') == 32
    assert kwargs.get('is_history') is True


@pytest.mark.asyncio
async def test_add_playlist_item_marks_failed_when_playlist_full(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__add_playlist_item pushes FAILED with the 'playlist too long' reason
    when __playlist_insert_item raises PlaylistMaxLength."""
    config = music_config({'music': {'playlist': {'server_playlist_max_size': 1}}})
    cog = Music(fake_context['bot'], config, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    push_mock = AsyncMock()
    cog._push_state = push_mock  #pylint:disable=protected-access

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
    await cog._Music__add_playlist_item(request, result)  #pylint:disable=protected-access
    push_mock.assert_awaited_once()
    args, kwargs = push_mock.await_args
    assert args[0] is request
    assert args[1] is LifecycleEvent.FAILED
    assert 'playlist too long' in kwargs['failure_reason']


@pytest.mark.asyncio
async def test_add_playlist_item_marks_failed_when_item_already_exists(fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """__add_playlist_item pushes FAILED with 'already exists' when the item
    URL is already in the playlist (insert returns None)."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
    cog.dispatcher = MagicMock()
    push_mock = AsyncMock()
    cog._push_state = push_mock  #pylint:disable=protected-access

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
    await cog._Music__add_playlist_item(request, result)  #pylint:disable=protected-access
    push_mock.assert_awaited_once()
    args, kwargs = push_mock.await_args
    assert args[0] is request
    assert args[1] is LifecycleEvent.FAILED
    assert 'already exists' in kwargs['failure_reason']


@pytest.mark.asyncio
async def test_post_play_processing_survives_a_deleted_history_playlist(mocker, fake_engine, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A history playlist deleted mid-play drops the item instead of crashing.

    Reachable rather than defensive: the post-play queue carries a playlist id
    across the play, and `!playlist delete` can land in between. The store says
    False and the loop keeps going -- an exception here would take the whole
    post-play consumer down with it.
    """
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'], fake_stores)
            attach_in_process_broker(cog)
            cog.dispatcher = MagicMock()
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            history_playlist_id = cog.players[fake_context['guild'].id].history_playlist_id

            await cog.playlist_store.delete_playlist(history_playlist_id)
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(history_playlist_id, sd))

            await cog.post_play_processing()

            async with async_mock_session(fake_engine) as session:
                items = (await session.execute(select(sql_count()).select_from(PlaylistItem))).scalar()
            assert items == 0


@pytest.mark.asyncio
async def test_public_view_is_none_when_the_playlist_vanishes_mid_lookup(fake_context):  #pylint:disable=redefined-outer-name
    """A playlist deleted between the two store calls resolves to no index.

    The lookup is a fetch followed by a list, and those are two round trips
    once the store is remote -- so a `!playlist delete` landing between them is
    a real interleaving, not a hypothetical. None is the answer; the caller
    prints it as the ID and carries on.
    """
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    store = attach_playlist_store(
        cog, playlist=PlaylistEntry(id=7, name='vanishing',
                                    server_id=fake_context['guild'].id, is_history=False))
    store.list_playlists.return_value = []

    # pylint: disable=protected-access
    assert await cog._Music__get_playlist_public_view(7, fake_context['guild'].id) is None
