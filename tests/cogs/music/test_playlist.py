from datetime import datetime
from tempfile import TemporaryDirectory
import pytest

from discord_bot.database import Playlist, PlaylistItem
from discord_bot.cogs.music import Music

from discord_bot.cogs.music_helpers.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.media_download import MediaDownload

from tests.cogs.test_music import BASE_MUSIC_CONFIG, yield_fake_download_client, yield_fake_search_client, yield_download_client_download_exception
from tests.helpers import mock_session, fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import
from tests.helpers import  FakeVoiceClient

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

    _result0 = cog.message_queue.get_single_immutable()
    result1 = cog.message_queue.get_single_immutable()
    assert result1[0].args[0] == '```ID || Playlist Name                                                   || Last Queued\n---------------------------------------------------------------------------------------------\n0  || History Playlist                                                || N/A\n1  || new-playlist                                                    || N/A```'


@pytest.mark.asyncio
async def test_list_playlist_with_history(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_list(cog, fake_context['context'])

    _result0 = cog.message_queue.get_single_immutable()
    result1 = cog.message_queue.get_single_immutable()
    assert result1[0].args[0] == '```ID || Playlist Name                                                   || Last Queued\n---------------------------------------------------------------------------------------------\n0  || History Playlist                                                || N/A\n1  || new-playlist                                                    || N/A```'

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
    await cog.playlist_item_add(cog, fake_context['context'], 0, search='https://foo.example')
    result0 = cog.message_queue.get_single_immutable()

    assert result0[0].args[0] == 'Unable to add "https://foo.example" to history playlist, is reserved and cannot be added to manually'

@pytest.mark.asyncio()
async def test_playlsit_add_item_function(fake_engine, mocker, fake_context):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context, download_file=False)
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example'}, s)
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_delete(cog, fake_context['context'], 0)
    result = cog.message_queue.get_single_immutable()
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
    result = cog.message_queue.get_single_immutable()
    assert result[0].args[0] == 'Cannot rename history playlist, is reserved'

@pytest.mark.asyncio
async def test_history_save(mocker, fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
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
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
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
    sd = MediaDownload(None, {'webpage_url': 'https://foo.example', 'title': 'foo', 'uploader': 'foobar'}, s)
    mocker.patch('discord_bot.cogs.music.DownloadClient', side_effect=yield_fake_download_client(sd))
    mocker.patch('discord_bot.cogs.music.SearchClient', side_effect=yield_fake_search_client(s))
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    await cog.get_player(fake_context['guild'].id, ctx=fake_context['context'])
    await cog.playlist_create(cog, fake_context['context'], name='new-playlist')
    await cog.playlist_merge(cog, fake_context['context'], 0, 1)
    cog.message_queue.get_single_immutable()
    result = cog.message_queue.get_single_immutable()
    assert result[0].args[0] == 'Cannot merge history playlist, is reserved'

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
