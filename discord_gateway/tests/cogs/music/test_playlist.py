import asyncio
from datetime import datetime
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_core.cogs.music_helpers.common import SearchType
from discord_core.types.download import LifecycleEvent
from discord_core.types.media_download import MediaDownload
from discord_core.types.media_request import MediaRequest
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult
from discord_core.types.playlist import PlaylistItemWrite
from discord_core.utils.media_enqueuer import MediaEnqueuer
from discord_core.types.playlist_add_result import PlaylistAddResult

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer
from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.tests.cogs.test_music import music_config, BASE_MUSIC_CONFIG, yield_fake_download_worker, yield_fake_search_client, yield_download_worker_download_exception

from discord_db.database import Playlist, PlaylistItem

from tests.helpers import async_mock_session, fake_source_dict, fake_media_download
from tests.helpers import fake_engine, fake_context, fake_stores #pylint:disable=unused-import
from tests.helpers import FakeVoiceClient
from tests.helpers import attach_in_process_broker
from tests.helpers import attach_in_process_search
from tests.helpers import attach_in_process_download


async def create_playlist(cog, fake_context, name):  #pylint:disable=redefined-outer-name
    '''Create a playlist the way `!playlist create` does in the commands pod: through the store.'''
    return (await cog.playlist_store.create_playlist(fake_context['guild'].id, name)).id


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
    playlist_id = await create_playlist(cog, fake_context, 'new-playlist')
    # What `!playlist item-add` does in the commands pod once it has found the playlist.
    await cog.enqueue_logic.generate_media_requests_from_search(
        envelope_from_ctx(fake_context['context']), 'https://foo.example', add_to_playlist=playlist_id)
    await search_driver.run_once(cog.bot_shutdown_event)
    await cog.process_search_results()
    await cog.download_client.run(cog.bot_shutdown_event)
    await cog.process_download_results()
    async with async_mock_session(fake_engine) as db_session:
        assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 1

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

            # What `!playlist queue 0` does in the commands pod: one request per history item.
            history_items = await cog.playlist_store.list_items(history_playlist_id)
            bundle_uuid = await cog.broker_client.create_bundle(
                fake_context['guild'].id, fake_context['channel'].id, input_string='Channel History', has_search_banner=True)
            await cog.enqueue_logic.enqueue_media_requests(
                envelope_from_ctx(fake_context['context']),
                [MediaRequest(guild_id=fake_context['guild'].id, channel_id=fake_context['channel'].id,
                              requester_name='tyler', requester_id=1,
                              search_result=SearchResult(search_type=SearchType.YOUTUBE, raw_search_string=item.video_url,
                                                         proper_name=item.title),
                              added_from_history=True, history_playlist_item_id=item.id)
                 for item in history_items],
                bundle_uuid)
            await search_driver.run_once(cog.bot_shutdown_event)
            await cog.process_search_results()
            await cog.download_client.run(cog.bot_shutdown_event)
            await cog.process_download_results()
            async with async_mock_session(fake_engine) as db_session:
                assert (await db_session.execute(select(sql_count()).select_from(Playlist))).scalar() == 1
                assert (await db_session.execute(select(sql_count()).select_from(PlaylistItem))).scalar() == 0

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
@pytest.mark.parametrize('stub,args,kwargs,command,forwarded', [
    ('playlist_create', (), {'name': 'mine'}, 'playlist.create', {'name': 'mine'}),
    ('playlist_list', (), {}, 'playlist.list', {}),
    ('playlist_show', (3,), {}, 'playlist.show', {'playlist_index': 3}),
    ('playlist_delete', (3,), {}, 'playlist.delete', {'playlist_index': 3}),
    ('playlist_rename', (3,), {'playlist_name': 'new'}, 'playlist.rename', {'playlist_index': 3, 'playlist_name': 'new'}),
    ('playlist_merge', ('1', '2'), {}, 'playlist.merge', {'playlist_index_one': '1', 'playlist_index_two': '2'}),
    ('playlist_item_remove', (3, 4), {}, 'playlist.item-remove', {'playlist_index': 3, 'video_index': 4}),
    ('playlist_item_add', ('2',), {'search': 'a song'}, 'playlist.item-add', {'playlist_index': '2', 'search': 'a song'}),
    ('playlist_queue_save', (), {'name': 'mine'}, 'playlist.save-queue', {'name': 'mine'}),
    ('playlist_history_save', (), {'name': 'mine'}, 'playlist.save-history', {'name': 'mine'}),
    ('playlist_queue', (3, 'shuffle', 16), {}, 'playlist.queue', {'playlist_index': 3, 'args': ['shuffle', '16']}),
    ('playlist_queue', (3,), {}, 'playlist.queue', {'playlist_index': 3, 'args': []}),
])
async def test_playlist_stubs_forward_to_the_commands_pod(fake_context, stub, args, kwargs, command, forwarded):  #pylint:disable=redefined-outer-name
    '''The behaviour of these commands is tested in discord_commands; this checks only that each
    stub forwards its command name and parsed arguments, and runs nothing itself.'''
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    attach_in_process_broker(cog)
    cog.commands_client = AsyncMock()
    await getattr(cog, stub).callback(cog, fake_context['context'], *args, **kwargs)
    cog.commands_client.forward.assert_awaited_once()
    forward_args = cog.commands_client.forward.await_args.args
    assert (forward_args[0], forward_args[1], forward_args[2]) == (
        command, envelope_from_ctx(fake_context['context']), forwarded)
