from unittest.mock import MagicMock, AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_core.types.playlist import PlaylistItemWrite

from discord_gateway.cogs.music import Music
from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG

from discord_db.database import Playlist, PlaylistItem

from tests.helpers import async_mock_session
from tests.helpers import fake_engine, fake_context, fake_stores #pylint:disable=unused-import
from tests.helpers import attach_in_process_broker


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
