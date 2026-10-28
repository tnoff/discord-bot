import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_core.types.playlist import PlaylistItemWrite

from discord_db.database import Playlist, PlaylistItem

from discord_commands.command_logic.playlist import PlaylistCommands

from tests.helpers import async_mock_session
from tests.helpers import fake_engine, fake_stores #pylint:disable=unused-import

DELETE_AFTER = 300
MAX_SIZE = 64


def _logic(reply, fake_stores, max_size=MAX_SIZE):  #pylint:disable=redefined-outer-name
    return PlaylistCommands(reply, logging.getLogger('test'), fake_stores.playlist, DELETE_AFTER, max_size,
                            AsyncMock(), AsyncMock(), AsyncMock())


def _item(name):
    return PlaylistItemWrite(video_url=f'https://ex.com/{name}', title=name, uploader='up')


async def _make_playlist(fake_stores, env, name, *item_names):  #pylint:disable=redefined-outer-name
    '''Create a playlist and put the named items in it, returning its db id.'''
    playlist = await fake_stores.playlist.create_playlist(env.guild_id, name)
    if item_names:
        await fake_stores.playlist.add_items(playlist.id, [_item(item) for item in item_names], MAX_SIZE)
    return playlist.id


async def _count(fake_engine, model):  #pylint:disable=redefined-outer-name
    async with async_mock_session(fake_engine) as session:
        return (await session.execute(select(sql_count()).select_from(model))).scalar()


@pytest.mark.asyncio
async def test_create(reply, sent, delete_afters, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    '''create makes the playlist, and the reply carries the index users see and the delete timer'''
    logic = _logic(reply, fake_stores)
    await logic.create(envelope, 'first-playlist')
    await logic.create(envelope, 'second-playlist')
    # Newest first, so the playlist just created is always index 1 and the earlier one shifts down.
    assert [content for _, _, content in sent] == [
        'Created playlist "first-playlist" with ID 1', 'Created playlist "second-playlist" with ID 1']
    assert delete_afters == [DELETE_AFTER, DELETE_AFTER]
    assert await _count(fake_engine, Playlist) == 2


@pytest.mark.asyncio
async def test_create_refuses_a_reserved_name_and_keeps_the_message(reply, sent, delete_afters, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).create(envelope, '__playhistory__derp')
    assert 'cannot contain' in sent[0][2]
    assert delete_afters == [None]
    assert not await _count(fake_engine, Playlist)


@pytest.mark.asyncio
async def test_create_refuses_a_name_twice(reply, sent, delete_afters, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    logic = _logic(reply, fake_stores)
    await logic.create(envelope, 'new-playlist')
    await logic.create(envelope, 'new-playlist')
    assert 'a playlist with that name already exists' in sent[1][2]
    assert delete_afters[1] is None
    assert await _count(fake_engine, Playlist) == 1


@pytest.mark.asyncio
async def test_list_with_history_first(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await fake_stores.playlist.ensure_history_playlist(envelope.guild_id)
    await _make_playlist(fake_stores, envelope, 'new-playlist')
    await _logic(reply, fake_stores).list_playlists(envelope)
    assert sent[0][2] == (
        'Playlist List\n```ID || Playlist Name                                                   || Last Queued\n'
        '------------------------------------------------------------------------------------\n'
        '0  || Channel History                                                 || N/A\n'
        '1  || new-playlist                                                    || N/A```')


@pytest.mark.asyncio
async def test_list_with_nothing_saved(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).list_playlists(envelope)
    assert sent[0][2] == 'No playlists in database'


@pytest.mark.asyncio
async def test_list_shows_when_a_playlist_was_last_queued(reply, sent, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'dated-playlist')
    async with async_mock_session(fake_engine) as session:
        playlist = (await session.execute(select(Playlist).where(Playlist.is_history == False))).scalars().first()  #pylint:disable=singleton-comparison
        playlist.last_queued = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        await session.commit()
    await _logic(reply, fake_stores).list_playlists(envelope)
    assert '2024-06-15 12:00:00' in sent[0][2]


@pytest.mark.asyncio
async def test_show(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await fake_stores.playlist.create_playlist(envelope.guild_id, 'new-playlist')
    playlist = (await fake_stores.playlist.list_playlists(envelope.guild_id))[0]
    await fake_stores.playlist.add_items(
        playlist.id, [PlaylistItemWrite(video_url='https://foo.example', title='foo', uploader='foobar')], MAX_SIZE)
    await _logic(reply, fake_stores).show(envelope, 1)
    assert sent[0][2] == ('Playlist 1 Items\n```Pos|| Title                           || Uploader\n'
                          '-------------------------------------------------\n'
                          '1  || foo                             || foobar```')


@pytest.mark.asyncio
async def test_show_an_empty_playlist(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'empty-playlist')
    await _logic(reply, fake_stores).show(envelope, 1)
    assert len(sent) == 1
    assert 'No items in playlist' in sent[0][2]


@pytest.mark.asyncio
async def test_show_without_any_playlists(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).show(envelope, 1)
    assert [content for _, _, content in sent] == ['No playlists in database']


@pytest.mark.asyncio
async def test_a_playlist_index_past_the_end_is_invalid(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'only')
    await _logic(reply, fake_stores).show(envelope, 9)
    assert [content for _, _, content in sent] == ['Invalid playlist index 9']


@pytest.mark.asyncio
async def test_a_negative_playlist_index_is_invalid(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    '''-1 used to index the list from its end'''
    await _make_playlist(fake_stores, envelope, 'one')
    await _make_playlist(fake_stores, envelope, 'two')
    await _logic(reply, fake_stores).show(envelope, -1)
    assert [content for _, _, content in sent] == ['Invalid playlist index -1']


@pytest.mark.asyncio
async def test_the_history_playlist_is_missing_until_it_exists(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).show(envelope, 0)
    assert [content for _, _, content in sent] == ['Invalid playlist index 0']


@pytest.mark.asyncio
async def test_item_remove(reply, sent, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'new-playlist', 'foo')
    await _logic(reply, fake_stores).item_remove(envelope, 1, 1)
    assert sent[0][2] == 'Removed item "foo" from playlist 1'
    assert not await _count(fake_engine, PlaylistItem)


@pytest.mark.asyncio
async def test_item_remove_for_an_item_that_is_not_there(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'p')
    await _logic(reply, fake_stores).item_remove(envelope, 1, 1)
    assert sent[0][2] == 'Unable to find item 1'


@pytest.mark.asyncio
async def test_item_remove_rejects_an_item_index_below_one(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'p', 'foo')
    await _logic(reply, fake_stores).item_remove(envelope, 1, 0)
    assert sent[0][2] == 'Invalid item index 0'


@pytest.mark.asyncio
async def test_item_remove_stops_when_the_playlist_is_not_found(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).item_remove(envelope, 1, 1)
    assert [content for _, _, content in sent] == ['No playlists in database']


@pytest.mark.asyncio
async def test_delete(reply, sent, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'new-playlist', 'foo')
    await _logic(reply, fake_stores).delete(envelope, 1)
    assert sent[-1][2] == 'Deleted playlist 1'
    assert not await _count(fake_engine, Playlist)
    assert not await _count(fake_engine, PlaylistItem)


@pytest.mark.asyncio
async def test_delete_refuses_the_history_playlist(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await fake_stores.playlist.ensure_history_playlist(envelope.guild_id)
    await _logic(reply, fake_stores).delete(envelope, 0)
    assert sent[0][2] == 'Cannot delete history playlist, is reserved'


@pytest.mark.asyncio
async def test_delete_says_so_when_the_playlist_is_not_found(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).delete(envelope, 1)
    assert any('Cannot delete playlist' in content for _, _, content in sent)


@pytest.mark.asyncio
async def test_rename(reply, sent, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'new-playlist')
    await _logic(reply, fake_stores).rename(envelope, 1, 'foo-bar-playlist')
    assert sent[0][2] == 'Renamed playlist 1 to name "foo-bar-playlist"'
    async with async_mock_session(fake_engine) as session:
        assert (await session.execute(select(Playlist))).scalars().first().name == 'foo-bar-playlist'


@pytest.mark.asyncio
async def test_rename_refuses_the_history_playlist(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await fake_stores.playlist.ensure_history_playlist(envelope.guild_id)
    await _logic(reply, fake_stores).rename(envelope, 0, 'foo-bar-playlist')
    assert sent[0][2] == 'Cannot rename history playlist, is reserved'


@pytest.mark.asyncio
async def test_rename_refuses_a_reserved_name_and_keeps_the_message(reply, sent, delete_afters, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'my-playlist')
    await _logic(reply, fake_stores).rename(envelope, 1, '__playhistory__bad')
    assert 'cannot contain' in sent[0][2]
    assert delete_afters == [None]


@pytest.mark.asyncio
async def test_rename_says_nothing_more_when_the_playlist_is_not_found(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).rename(envelope, 1, 'new')
    assert len(sent) == 1


@pytest.mark.asyncio
async def test_merge_moves_the_items_and_deletes_the_source(reply, sent, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'new-playlist')
    await _make_playlist(fake_stores, envelope, 'delete-me', 'foo')
    # Newest first: 'delete-me' is index 1, 'new-playlist' index 2.
    await _logic(reply, fake_stores).merge(envelope, '2', '1')
    assert sent[0][2] == 'Added item "foo" to playlist 2'
    assert await _count(fake_engine, Playlist) == 1
    assert await _count(fake_engine, PlaylistItem) == 1


@pytest.mark.asyncio
async def test_merge_refuses_the_history_playlist(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await fake_stores.playlist.ensure_history_playlist(envelope.guild_id)
    await _make_playlist(fake_stores, envelope, 'new-playlist')
    await _logic(reply, fake_stores).merge(envelope, '0', '1')
    assert sent[-1][2] == 'Cannot merge history playlist, is reserved'


@pytest.mark.asyncio
async def test_merge_names_the_playlist_it_cannot_find(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'only')
    logic = _logic(reply, fake_stores)
    await logic.merge(envelope, '1', '5')
    await logic.merge(envelope, '5', '1')
    assert [content for _, _, content in sent if 'Cannot find' in content] == [
        'Cannot find playlist 5', 'Cannot find playlist 5']


@pytest.mark.asyncio
async def test_merge_reports_an_item_the_target_already_has(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _make_playlist(fake_stores, envelope, 'p1', 'same')
    await _make_playlist(fake_stores, envelope, 'p2', 'same')
    await _logic(reply, fake_stores).merge(envelope, '1', '2')
    assert any('likely already exists' in content for _, _, content in sent)


@pytest.mark.asyncio
async def test_merge_into_a_full_playlist_keeps_the_source(reply, sent, fake_engine, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    '''A merge that could not take every item must not then delete the source.'''
    target = await fake_stores.playlist.create_playlist(envelope.guild_id, 'p1')
    source = await fake_stores.playlist.create_playlist(envelope.guild_id, 'p2')
    await fake_stores.playlist.add_items(target.id, [_item('a')], 1)
    await fake_stores.playlist.add_items(source.id, [_item('b')], 1)
    # Newest first: p2 is index 1, p1 index 2.
    await _logic(reply, fake_stores, max_size=1).merge(envelope, '2', '1')
    assert any('already max size' in content for _, _, content in sent)
    assert await _count(fake_engine, Playlist) == 2


@pytest.mark.asyncio
async def test_a_non_numeric_index_is_invalid(reply, sent, fake_stores, envelope):  #pylint:disable=redefined-outer-name
    await _logic(reply, fake_stores).merge(envelope, 'abc', '1')
    assert 'Invalid playlist index abc' in [content for _, _, content in sent]
