import asyncio
from datetime import datetime, timezone, timedelta
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

import pytest

from discord_bot.database import Playlist, PlaylistItem
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.media_download import MediaDownload

from tests.cogs.test_music import BASE_MUSIC_CONFIG, yield_fake_download_client, yield_fake_search_client, yield_download_client_download_exception
from tests.helpers import mock_session, fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import  FakeVoiceClient

@pytest.mark.asyncio
async def test_create_playlist(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count()

@pytest.mark.asyncio
async def test_create_playlist_invalid_name(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='__playhistory__derp')
    with mock_session(fake_engine) as db_session:
        assert not db_session.query(Playlist).count()

@pytest.mark.asyncio
async def test_create_playlist_same_name_twice(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count() == 1

@pytest.mark.asyncio
async def test_create_playlist_message_includes_public_id(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist creation message includes the public playlist ID"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Create first playlist - should get public ID 1
    await cog.playlist_create.callback(cog, fake_context['context'], name='first-playlist')
    first_message = cog.message_queue.get_single_immutable()
    assert first_message[0].function.args[0] == 'Created playlist "first-playlist" with ID 1'

    # Create second playlist - should get public ID 2
    await cog.playlist_create.callback(cog, fake_context['context'], name='second-playlist')
    second_message = cog.message_queue.get_single_immutable()
    assert second_message[0].function.args[0] == 'Created playlist "second-playlist" with ID 2'

    # Verify playlists were actually created in database
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count() == 2

@pytest.mark.asyncio
async def test_create_playlist_message_with_none_public_id(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test playlist creation message handles None public ID gracefully"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)

    # Mock __get_playlist_public_view to return None
    mocker.patch.object(cog, '_Music__get_playlist_public_view', return_value=None)

    await cog.playlist_create.callback(cog, fake_context['context'], name='test-playlist')
    message = cog.message_queue.get_single_immutable()
    assert message[0].function.args[0] == 'Created playlist "test-playlist" with ID None'

@pytest.mark.asyncio
async def test_list_playlist(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list.callback(cog, fake_context['context'])

    _result0 = cog.message_queue.get_single_immutable()
    result1 = cog.message_queue.get_single_immutable()
    assert result1[0].function.args[0] == '```ID || Playlist Name                                                   || Last Queued\n------------------------------------------------------------------------------------\n0  || Channel History                                                 || N/A\n1  || new-playlist                                                    || N/A```'


@pytest.mark.asyncio
async def test_list_playlist_with_history(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list.callback(cog, fake_context['context'])

    _result0 = cog.message_queue.get_single_immutable()
    result1 = cog.message_queue.get_single_immutable()
    assert result1[0].function.args[0] == '```ID || Playlist Name                                                   || Last Queued\n------------------------------------------------------------------------------------\n0  || Channel History                                                 || N/A\n1  || new-playlist                                                    || N/A```'

@pytest.mark.asyncio()
async def test_playlist_add_item_invalid_history(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_item_add.callback(cog, fake_context['context'], 0, search='https://foo.example')
    result0 = cog.message_queue.get_single_immutable()

    assert result0[0].function.args[0] == 'Unable to add "https://foo.example" to history playlist, is reserved and cannot be added to manually'

@pytest.mark.asyncio()
async def test_playlsit_add_item_function(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    # Process search queue if YouTube Music search is enabled
    if cog.enable_youtube_music_search:
        await cog.search_youtube_music()
    await cog.download_files()
    with mock_session(fake_engine) as db_session:
        assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio()
async def test_playlist_remove_item(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    # Process search queue if YouTube Music search is enabled
    if cog.enable_youtube_music_search:
        await cog.search_youtube_music()
    await cog.download_files()
    await cog.playlist_item_remove.callback(cog, fake_context['context'], 1, 1)
    with mock_session(fake_engine) as db_session:
        assert db_session.query(PlaylistItem).count() == 0

@pytest.mark.asyncio()
async def test_playlist_show(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    # Process search queue if YouTube Music search is enabled
    if cog.enable_youtube_music_search:
        await cog.search_youtube_music()
    await cog.download_files()

    await cog.playlist_show.callback(cog, fake_context['context'], 1)
    cog.message_queue.get_next_message()
    cog.message_queue.get_next_message()
    m2 = cog.message_queue.get_next_message()
    assert m2[1][0].function.args[0] == '```Pos|| Title                           || Uploader\n-------------------------------------------------\n1  || foo                             || foobar```'

@pytest.mark.asyncio()
async def test_playlist_delete(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name

    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    # Process search queue if YouTube Music search is enabled
    if cog.enable_youtube_music_search:
        await cog.search_youtube_music()
    await cog.download_files()

    await cog.playlist_delete.callback(cog, fake_context['context'], 1)
    with mock_session(fake_engine) as db_session:
        assert db_session.query(PlaylistItem).count() == 0
        assert db_session.query(Playlist).count() == 0

@pytest.mark.asyncio()
async def test_playlist_delete_history(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name

    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_delete.callback(cog, fake_context['context'], 0)
    result = cog.message_queue.get_single_immutable()
    assert result[0].function.args[0] == 'Cannot delete history playlist, is reserved'



@pytest.mark.asyncio
async def test_playlist_rename(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_rename.callback(cog, fake_context['context'], 1, playlist_name='foo-bar-playlist')
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
    await cog.playlist_rename.callback(cog, fake_context['context'], 0, playlist_name='foo-bar-playlist')
    result = cog.message_queue.get_single_immutable()
    assert result[0].function.args[0] == 'Cannot rename history playlist, is reserved'

@pytest.mark.asyncio
async def test_history_save(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._history.put(sd) #pylint:disable=protected-access

            await cog.playlist_history_save.callback(cog, fake_context['context'], name='foobar')
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            await cog.players[fake_context['guild'].id]._play_queue.put(sd) #pylint:disable=protected-access

            await cog.playlist_queue_save.callback(cog, fake_context['context'], name='foobar')
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 1, search='https://foo.example')
    # Process search queue if YouTube Music search is enabled
    if cog.enable_youtube_music_search:
        await cog.search_youtube_music()
    await cog.download_files()

    await cog.playlist_queue.callback(cog, fake_context['context'], 1)
    assert cog.download_queue.queues[fake_context['guild'].id]


@pytest.mark.asyncio
async def test_playlist_history_queue(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            await cog.playlist_queue.callback(cog, fake_context['context'], 0)
            assert cog.download_queue.queues[fake_context['guild'].id]

@pytest.mark.asyncio
async def test_random_play_deletes_no_existent_video(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_download_client_download_exception())
            cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
            await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
            cog.history_playlist_queue.put_nowait(HistoryPlaylistItem(cog.players[fake_context['guild'].id].history_playlist_id, sd))
            await cog.playlist_history_update()

            await cog.playlist_queue.callback(cog, fake_context['context'], 0)
            # Process search queue if YouTube Music search is enabled
            if cog.enable_youtube_music_search:
                await cog.search_youtube_music()
            await cog.download_files()
            with mock_session(fake_engine) as db_session:
                assert db_session.query(Playlist).count() == 1
                assert db_session.query(PlaylistItem).count() == 0

@pytest.mark.asyncio()
async def test_playlist_merge(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_create.callback(cog, fake_context['context'], name='delete-me')
    await cog.playlist_item_add.callback(cog, fake_context['context'], 2, search='https://foo.example')
    # Process search queue if YouTube Music search is enabled
    if cog.enable_youtube_music_search:
        await cog.search_youtube_music()
    await cog.download_files()
    await cog.playlist_merge.callback(cog, fake_context['context'], 1, 2)
    with mock_session(fake_engine) as db_session:
        assert db_session.query(Playlist).count() == 1
        assert db_session.query(PlaylistItem).count() == 1

@pytest.mark.asyncio()
async def test_playlist_merge_history(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create.callback(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_merge.callback(cog, fake_context['context'], 0, 1)
    cog.message_queue.get_single_immutable()
    result = cog.message_queue.get_single_immutable()
    assert result[0].function.args[0] == 'Cannot merge history playlist, is reserved'

def test_playlist_insert_item_method(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test __playlist_insert_item private method"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with mock_session(fake_engine) as session:
        # Create a playlist first
        playlist = Playlist(
            server_id=fake_context['guild'].id,
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
        assert playlists[0].server_id == fake_context['guild'].id
        assert playlists[0].name.startswith('__playhistory__')
        assert playlists[0].is_history is True

@pytest.mark.asyncio
async def test_playlist_queue_with_shuffle_and_max_num(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test playlist queue with shuffle and max_num arguments in different orders"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

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
async def test_playlist_queue_with_only_shuffle(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test playlist queue with only shuffle argument"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

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
async def test_playlist_queue_with_only_max_num(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test playlist queue with only max_num argument"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

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
async def test_playlist_queue_with_no_arguments(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test playlist queue with no additional arguments"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

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
async def test_playlist_queue_parameter_parsing_edge_cases(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test edge cases for playlist queue parameter parsing"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

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
async def test_playlist_queue_history_playlist_basic_command(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that '!playlist queue 0' works for history playlist - entire playlist, no shuffle, no limit"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Mock the __playlist_queue method to capture arguments
    playlist_queue_mock = mocker.patch.object(cog, '_Music__playlist_queue', return_value=None)

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
async def test_playlist_queue_comprehensive_integration(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Comprehensive integration test for all playlist queue functionality"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
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
async def test_playlist_show_empty_playlist_message_context_fix(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist show on empty playlist creates proper MessageContext (bug fix for 'str' object has no attribute 'function')"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a player to ensure history playlist exists
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Create an empty playlist
    await cog.playlist_create.callback(cog, fake_context['context'], name='empty-playlist')

    # Clear the message queue after playlist creation
    cog.message_queue.get_single_immutable()  # Remove the playlist creation message

    # Show the empty playlist - this should not crash
    await cog.playlist_show.callback(cog, fake_context['context'], 1)

    # Verify message was queued properly (should be MessageContext object, not string)
    messages = cog.message_queue.get_single_immutable()
    assert len(messages) == 1
    assert hasattr(messages[0], 'function'), "Message should be MessageContext object with function attribute"
    assert callable(messages[0].function), "MessageContext.function should be callable"

    # Verify the message content is correct - check the args which should contain our message
    assert 'No items in playlist' in str(messages[0].function.args), \
           f"Message should contain 'No items in playlist', got: {messages[0].function.args}"

@pytest.mark.asyncio
async def test_playlist_queue_empty_playlist_user_feedback(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist queue on empty playlist provides helpful user feedback message"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a player to ensure voice functionality works
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Create an empty playlist
    await cog.playlist_create.callback(cog, fake_context['context'], name='empty-playlist')

    # Clear the message queue after playlist creation
    cog.message_queue.get_single_immutable()  # Remove the playlist creation message

    # Try to queue the empty playlist - should get helpful message
    await cog.playlist_queue.callback(cog, fake_context['context'], 1)

    # Verify user gets helpful feedback message
    messages = cog.message_queue.get_single_immutable()
    assert len(messages) == 1
    assert hasattr(messages[0], 'function'), "Message should be MessageContext object with function attribute"
    assert callable(messages[0].function), "MessageContext.function should be callable"

    # Verify the message content is correct
    message_text = str(messages[0].function.args)
    assert 'contains no items to queue' in message_text, \
           f"Message should contain 'contains no items to queue', got: {message_text}"
    assert 'empty-playlist' in message_text, \
           f"Message should contain playlist name 'empty-playlist', got: {message_text}"

@pytest.mark.asyncio
async def test_playlist_queue_empty_history_playlist_feedback(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist queue on empty history playlist provides helpful feedback with correct name"""
    fake_context['author'].voice = FakeVoiceClient()
    fake_context['author'].voice.channel = fake_context['channel']

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create a player to ensure history playlist exists
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])

    # Try to queue empty history playlist (playlist index 0)
    await cog.playlist_queue.callback(cog, fake_context['context'], 0)

    # Verify user gets helpful feedback message
    messages = cog.message_queue.get_single_immutable()
    assert len(messages) == 1
    assert hasattr(messages[0], 'function'), "Message should be MessageContext object with function attribute"

    # Verify the message content shows "Channel History" (not the database playlist name)
    message_text = str(messages[0].function.args)
    assert 'contains no items to queue' in message_text, \
           f"Message should contain 'contains no items to queue', got: {message_text}"
    assert 'Channel History' in message_text, \
           f"Message should contain 'Channel History', got: {message_text}"

def test_get_playlist_public_view_history_playlist_returns_zero(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that history playlists return public view index 0"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create a test history playlist
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        history_playlist = Playlist(
            name="Channel History",
            server_id=fake_context['guild'].id,
            is_history=True
        )
        db_session.add(history_playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test the function
        result = asyncio.run(cog._Music__get_playlist_public_view(history_playlist.id, fake_context['guild'].id))  #pylint:disable=protected-access

        assert result == 0


def test_get_playlist_public_view_first_playlist_returns_one(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that the first non-history playlist returns public view index 1"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create test playlists
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create the first playlist (should be index 1)
        playlist1 = Playlist(
            name="First Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist1)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test the function
        result = asyncio.run(cog._Music__get_playlist_public_view(playlist1.id, fake_context['guild'].id))  #pylint:disable=protected-access

        assert result == 1


def test_get_playlist_public_view_multiple_playlists_correct_ordering(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that multiple playlists return correct public view indices based on creation order"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create test playlists in specific order
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
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
        db_session.commit()  #pylint:disable=no-member

        # Test each playlist returns correct index
        result1 = asyncio.run(cog._Music__get_playlist_public_view(playlist1.id, fake_context['guild'].id))  #pylint:disable=protected-access
        result2 = asyncio.run(cog._Music__get_playlist_public_view(playlist2.id, fake_context['guild'].id))  #pylint:disable=protected-access
        result3 = asyncio.run(cog._Music__get_playlist_public_view(playlist3.id, fake_context['guild'].id))  #pylint:disable=protected-access

        assert result1 == 1
        assert result2 == 2
        assert result3 == 3


def test_get_playlist_public_view_ignores_history_playlists_in_ordering(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that history playlists don't affect the public view ordering of regular playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
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
        db_session.commit()  #pylint:disable=no-member

        # History playlist should return 0
        history_result = asyncio.run(cog._Music__get_playlist_public_view(history_playlist.id, fake_context['guild'].id))  #pylint:disable=protected-access

        # Regular playlists should be ordered 1, 2 (ignoring history)
        result1 = asyncio.run(cog._Music__get_playlist_public_view(playlist1.id, fake_context['guild'].id))  #pylint:disable=protected-access
        result2 = asyncio.run(cog._Music__get_playlist_public_view(playlist2.id, fake_context['guild'].id))  #pylint:disable=protected-access

        assert history_result == 0
        assert result1 == 1
        assert result2 == 2


def test_get_playlist_public_view_different_servers_isolated(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlists from different servers don't affect each other's public view indices"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create second fake guild for testing
    other_guild_id = fake_context['guild'].id + 1

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
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
        db_session.commit()  #pylint:disable=no-member

        # Server 1 playlists should be ordered 1, 2
        s1_result1 = asyncio.run(cog._Music__get_playlist_public_view(server1_playlist1.id, fake_context['guild'].id))  #pylint:disable=protected-access
        s1_result2 = asyncio.run(cog._Music__get_playlist_public_view(server1_playlist2.id, fake_context['guild'].id))  #pylint:disable=protected-access

        # Server 2 playlist should be index 1 (not affected by server 1)
        s2_result1 = asyncio.run(cog._Music__get_playlist_public_view(server2_playlist1.id, other_guild_id))  #pylint:disable=protected-access

        assert s1_result1 == 1
        assert s1_result2 == 2
        assert s2_result1 == 1


def test_get_playlist_public_view_nonexistent_playlist_returns_none(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that requesting a non-existent playlist returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create a regular playlist for comparison
    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        playlist = Playlist(
            name="Test Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Test with non-existent playlist ID
        nonexistent_id = 99999
        result = asyncio.run(cog._Music__get_playlist_public_view(nonexistent_id, fake_context['guild'].id))  #pylint:disable=protected-access

        assert result is None


def test_get_playlist_public_view_cross_server_playlist_returns_none(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that requesting a playlist from a different server returns None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create second fake guild
    other_guild_id = str(int(fake_context['guild'].id) + 1)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
        # Create playlist for first server
        playlist = Playlist(
            name="Server 1 Playlist",
            server_id=fake_context['guild'].id,
            is_history=False
        )
        db_session.add(playlist)  #pylint:disable=no-member
        db_session.commit()  #pylint:disable=no-member

        # Try to get the playlist's public view from a different server
        result = asyncio.run(cog._Music__get_playlist_public_view(playlist.id, str(other_guild_id)))  #pylint:disable=protected-access

        assert result is None


def test_get_playlist_public_view_ordering_by_creation_time(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlists are ordered by creation_at timestamp (DESC - newest first)"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
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
        db_session.commit()  #pylint:disable=no-member

        # Test that ordering is by creation_at DESC (newest first), not insert order
        oldest_result = asyncio.run(cog._Music__get_playlist_public_view(playlist_oldest.id, fake_context['guild'].id))  #pylint:disable=protected-access
        middle_result = asyncio.run(cog._Music__get_playlist_public_view(playlist_middle.id, fake_context['guild'].id))  #pylint:disable=protected-access
        newest_result = asyncio.run(cog._Music__get_playlist_public_view(playlist_newest.id, fake_context['guild'].id))  #pylint:disable=protected-access

        assert newest_result == 1   # Newest created = index 1
        assert middle_result == 2   # Second newest = index 2
        assert oldest_result == 3   # Oldest created = index 3


def test_get_playlist_public_view_handles_empty_server(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test behavior when server has no playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Try to get public view for non-existent playlist on server with no playlists
    result = asyncio.run(cog._Music__get_playlist_public_view(1, fake_context['guild'].id))  #pylint:disable=protected-access

    assert result is None


def test_get_playlist_public_view_mixed_history_and_regular_complex(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test complex scenario with mixed history and regular playlists"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    with cog.with_db_session() as db_session:  #pylint:disable=no-member
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
        db_session.commit()  #pylint:disable=no-member

        results = []
        for playlist in playlists:
            result = asyncio.run(cog._Music__get_playlist_public_view(playlist.id, fake_context['guild'].id))  #pylint:disable=protected-access
            results.append(result)

        # History playlists should return 0
        # Regular playlists should be ordered by creation time DESC (newest first)
        # Regular 1 (oldest): created at base_time -> index 3
        # Regular 2 (middle): created at base_time+20min -> index 2
        # Regular 3 (newest): created at base_time+40min -> index 1
        expected = [3, 0, 2, 0, 1]  # Regular 1=3, History 1=0, Regular 2=2, History 2=0, Regular 3=1

        assert results == expected

@pytest.mark.asyncio
async def test_playlist_queue_adds_history_playlist_item_id(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist queue operations add history_playlist_item_id to media requests"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Mock database operations
    with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
        # Setup mock database responses
        playlist_name = "Test Playlist"
        mock_playlist_items = [
            MagicMock(id=1, video_url="https://youtube.com/watch?v=123",
                     requester_name="user1", requester_id=456, title="Video 1"),
            MagicMock(id=2, video_url="https://youtube.com/watch?v=456",
                     requester_name="user2", requester_id=789, title="Video 2"),
        ]

        # Mock database calls in order they appear in playlist_queue method
        mock_db.side_effect = [
            playlist_name,  # get_playlist_name
            mock_playlist_items,  # list_playlist_items
            None,  # playlist_update_queued
        ]

        # Mock the enqueue_media_requests method
        captured_requests = []
        async def mock_enqueue(ctx, entries, bundle, player=None):  #pylint:disable=unused-argument
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
async def test_playlist_queue_completion_messaging_simplified(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist queue completion messaging is simplified in new version"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Mock database operations
    with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
        playlist_name = "Test Playlist"
        mock_playlist_items = [
            MagicMock(id=1, video_url="https://youtube.com/watch?v=123",
                     requester_name="user1", requester_id=456, title="Video 1"),
        ]

        mock_db.side_effect = [
            playlist_name,  # get_playlist_name
            mock_playlist_items,  # list_playlist_items
            None,  # playlist_update_queued
        ]

        # Mock message queue to capture messages
        with patch.object(cog.message_queue, 'send_single_immutable') as mock_send:
            with patch.object(cog, 'enqueue_media_requests', return_value=False):  # finished_all = False
                with patch.object(cog, 'get_player', return_value=MagicMock()):
                    # Call the private playlist queue method directly
                    # pylint: disable=protected-access
                    await cog._Music__playlist_queue(fake_context['context'], MagicMock(), 123, False, 0, False)

                    # Verify only failure message is sent (hit limit case)
                    mock_send.assert_called_once()
                    call_args = mock_send.call_args[0][0]
                    assert len(call_args) == 1
                    message_context = call_args[0]

                    # Check that the message is about hitting limit
                    assert callable(message_context.function)
                    # The message should contain the playlist name and indicate limit hit
                    # We can't easily test the exact message without executing the partial function


@pytest.mark.asyncio
async def test_playlist_queue_bundle_creation_with_text_channel(fake_context):  #pylint:disable=redefined-outer-name
    """Test that enqueue_media_requests creates bundles with proper text_channel parameter"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test the method that actually creates bundles - enqueue_media_requests
    entries = [fake_source_dict(fake_context), fake_source_dict(fake_context)]

    # Create a mock player
    mock_player = MagicMock()
    mock_player.guild = fake_context['guild']
    mock_player.text_channel = fake_context['channel']

    # Create a bundle for the test
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    # Register the bundle manually since we're creating it outside the cog
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Call enqueue_media_requests directly to test bundle creation
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle, mock_player)

    # Verify bundle was created with correct text_channel
    assert result is True
    assert len(cog.multirequest_bundles) == 1

    # Verify bundle has correct text_channel
    bundle = list(cog.multirequest_bundles.values())[0]
    assert bundle.text_channel == fake_context['channel']
    assert bundle.guild_id == fake_context['guild'].id
    assert bundle.channel_id == fake_context['channel'].id


@pytest.mark.asyncio
async def test_history_playlist_queue_behavior(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test history playlist queue retains special behavior"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Mock database operations for history playlist
    with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
        mock_playlist_items = [
            MagicMock(id=1, video_url="https://youtube.com/watch?v=123",
                     requester_name="user1", requester_id=456, title="Video 1"),
        ]

        mock_db.side_effect = [
            "Auto-generated History",  # get_playlist_name (this gets overridden)
            mock_playlist_items,  # list_playlist_items
            None,  # playlist_update_queued
        ]

        # Mock message queue to capture messages
        with patch.object(cog.message_queue, 'send_single_immutable') as mock_send:
            with patch.object(cog, 'enqueue_media_requests', return_value=False):  # finished_all = False
                with patch.object(cog, 'get_player', return_value=MagicMock()):
                    # Call history playlist queue (playlist_id = guild_id for history)
                    # pylint: disable=protected-access
                    await cog._Music__playlist_queue(fake_context['context'], MagicMock(), fake_context['guild'].id, False, 0, True)

                    # For history playlists, should still send completion message
                    mock_send.assert_called_once()
                    call_args = mock_send.call_args[0][0]
                    assert len(call_args) == 1

                    # Message should mention "Channel History" not the database playlist name
                    # This is set by the special is_history logic
