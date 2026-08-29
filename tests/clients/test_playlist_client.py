from datetime import datetime, timedelta, timezone
from functools import partial

import pytest
from sqlalchemy import select
from sqlalchemy.sql.functions import count as sql_count

from discord_bot.clients.playlist_client import PlaylistClient
from discord_bot.database import Playlist, PlaylistItem
from discord_bot.interfaces.database_protocols import PlaylistStore
from discord_bot.types.playlist import (
    PlaylistEntry,
    PlaylistItemAddStatus,
    PlaylistItemEntry,
    PlaylistItemWrite,
)

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 555
MAX_SIZE = 4


def build_store(fake_engine) -> PlaylistClient:  #pylint:disable=redefined-outer-name
    '''
    Build a PlaylistClient over the test engine.

    fake_engine : Async engine fixture, schema created and truncated
    '''
    return PlaylistClient(partial(async_mock_session, fake_engine))


def writes(*urls):
    '''
    Build item writes for a list of urls.

    urls : Video urls, used as the title too
    '''
    return [PlaylistItemWrite(video_url=url, title=url.upper(), uploader='up') for url in urls]


@pytest.mark.asyncio
async def test_playlist_client_satisfies_the_store_protocol(fake_engine):  #pylint:disable=redefined-outer-name
    '''PlaylistClient is a structural PlaylistStore'''
    assert isinstance(build_store(fake_engine), PlaylistStore)


@pytest.mark.asyncio
async def test_create_and_get_playlist_round_trip(fake_engine):  #pylint:disable=redefined-outer-name
    '''A created playlist is findable by id and by name, and survives serialisation'''
    store = build_store(fake_engine)
    created = await store.create_playlist(GUILD_ID, 'mixtape')

    assert isinstance(created, PlaylistEntry)
    assert created.name == 'mixtape'
    assert created.is_history is False
    assert created.created_at is not None, 'created_at should be stamped by the model default'

    assert await store.get_playlist(created.id) == created
    assert await store.get_playlist_by_name(GUILD_ID, 'mixtape') == created
    assert PlaylistEntry.model_validate(created.model_dump(mode='json')) == created


@pytest.mark.asyncio
async def test_missing_playlists_are_none_not_errors(fake_engine):  #pylint:disable=redefined-outer-name
    '''No such playlist is an answer'''
    store = build_store(fake_engine)
    assert await store.get_playlist(9999) is None
    assert await store.get_playlist_by_name(GUILD_ID, 'nope') is None
    assert await store.get_history_playlist(GUILD_ID) is None
    assert await store.count_playlists(GUILD_ID) == 0


@pytest.mark.asyncio
async def test_list_playlists_is_newest_first_and_excludes_history(fake_engine):  #pylint:disable=redefined-outer-name
    '''List order is the public index users type, and history is not in it.

    Newest first is what servers have always seen. Unlike `playlist_item`,
    whose `created_at` really was NULL on every row, every production
    `playlist` row already carried a distinct timestamp -- so this DESC has
    been in effect the whole time. Shipping `asc` on the assumption that the
    two tables matched reversed the numbering for every guild with more than
    one playlist. The order is a promise, and this is where it is written down.
    '''
    store = build_store(fake_engine)
    for name in ('first', 'second', 'third'):
        await store.create_playlist(GUILD_ID, name)
    await store.ensure_history_playlist(GUILD_ID)

    listed = await store.list_playlists(GUILD_ID)

    assert [entry.name for entry in listed] == ['third', 'second', 'first']
    assert await store.count_playlists(GUILD_ID) == 3


@pytest.mark.asyncio
async def test_list_playlists_is_deterministic_when_created_at_ties(fake_engine):  #pylint:disable=redefined-outer-name
    '''Rows sharing a timestamp still come back in a defined order.

    Without the id tiebreak a tie is heap order, and heap order moves as rows
    are deleted and reinserted.
    '''
    shared = datetime(2024, 6, 1, tzinfo=timezone.utc)
    async with async_mock_session(fake_engine) as session:
        for name in ('alpha', 'beta', 'gamma'):
            session.add(Playlist(name=name, server_id=GUILD_ID, is_history=False, created_at=shared))
        await session.commit()

    listed = await build_store(fake_engine).list_playlists(GUILD_ID)
    assert [entry.name for entry in listed] == ['gamma', 'beta', 'alpha']


@pytest.mark.asyncio
async def test_ensure_history_playlist_is_idempotent(fake_engine):  #pylint:disable=redefined-outer-name
    '''Calling it twice returns the same row rather than creating a second.

    Get-or-create is one call precisely so two players starting at once cannot
    both decide to create, which the table's unique constraint would turn into
    an error on a path with nowhere to report one.
    '''
    store = build_store(fake_engine)
    first = await store.ensure_history_playlist(GUILD_ID)
    second = await store.ensure_history_playlist(GUILD_ID)

    assert first == second
    async with async_mock_session(fake_engine) as session:
        count = (await session.execute(
            select(sql_count()).select_from(Playlist)
            .where(Playlist.is_history == True))).scalar()  # noqa: E712  pylint:disable=singleton-comparison
    assert count == 1
    history = await store.get_history_playlist(GUILD_ID)
    assert history.id == first


@pytest.mark.asyncio
async def test_add_items_reports_added_duplicate_and_full_in_order(fake_engine):  #pylint:disable=redefined-outer-name
    '''One outcome per item attempted, with the reason.

    The three loops that add items each say something different for each
    outcome, and one of them stops the merge from deleting its source. A
    boolean per item could not carry that.
    '''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'mixtape')

    first = await store.add_items(playlist.id, writes('a', 'b'), MAX_SIZE)
    assert [outcome.status for outcome in first] == [
        PlaylistItemAddStatus.ADDED, PlaylistItemAddStatus.ADDED]
    assert all(outcome.item_id is not None for outcome in first)

    second = await store.add_items(playlist.id, writes('a', 'c'), MAX_SIZE)
    assert [outcome.status for outcome in second] == [
        PlaylistItemAddStatus.DUPLICATE, PlaylistItemAddStatus.ADDED]
    assert second[0].item_id is None


@pytest.mark.asyncio
async def test_add_items_stops_at_the_ceiling_and_reports_nothing_after(fake_engine):  #pylint:disable=redefined-outer-name
    '''A full playlist ends the batch, and later items are not attempted.

    Matching the loops this replaced: they broke or returned at the first
    PlaylistMaxLength and said nothing about the rest.
    '''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'small')

    outcomes = await store.add_items(playlist.id, writes('a', 'b', 'c', 'd'), 2)

    assert [outcome.status for outcome in outcomes] == [
        PlaylistItemAddStatus.ADDED, PlaylistItemAddStatus.ADDED,
        PlaylistItemAddStatus.PLAYLIST_FULL]
    assert len(outcomes) == 3, 'items after the ceiling should not be attempted'
    assert await store.get_playlist_size(playlist.id) == 2


@pytest.mark.asyncio
async def test_add_items_truncates_to_the_column_width(fake_engine):  #pylint:disable=redefined-outer-name
    '''Over-long strings are shortened rather than rejected, as before the move'''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'long')

    await store.add_items(playlist.id, [PlaylistItemWrite(
        video_url='https://ex.com/' + 'u' * 400,
        title='t' * 400,
        uploader='p' * 400)], MAX_SIZE)

    items = await store.list_items(playlist.id)
    assert len(items[0].title) <= 256
    assert len(items[0].video_url) <= 256
    assert len(items[0].uploader) <= 256


@pytest.mark.asyncio
async def test_list_items_returns_entries_in_position_order(fake_engine):  #pylint:disable=redefined-outer-name
    '''Items come back detached, in the order the shown position means'''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'ordered')
    await store.add_items(playlist.id, writes('a', 'b', 'c'), MAX_SIZE)

    items = await store.list_items(playlist.id)

    assert [item.video_url for item in items] == ['a', 'b', 'c']
    assert all(isinstance(item, PlaylistItemEntry) for item in items)


@pytest.mark.asyncio
async def test_delete_item_by_index_returns_what_it_deleted(fake_engine):  #pylint:disable=redefined-outer-name
    '''The caller names the deleted item in its message, after the row is gone.

    The entry is built before the delete for that reason; a live row would be
    detached by the time the message is formatted.
    '''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'removable')
    await store.add_items(playlist.id, writes('a', 'b', 'c'), MAX_SIZE)

    removed = await store.delete_item_by_index(playlist.id, 1)

    assert removed.video_url == 'b'
    assert removed.title == 'B'
    assert [item.video_url for item in await store.list_items(playlist.id)] == ['a', 'c']


@pytest.mark.asyncio
async def test_delete_item_by_index_out_of_range_is_none(fake_engine):  #pylint:disable=redefined-outer-name
    '''An index past the end is an answer, and deletes nothing'''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'short')
    await store.add_items(playlist.id, writes('a'), MAX_SIZE)

    assert await store.delete_item_by_index(playlist.id, 5) is None
    assert await store.delete_item_by_index(playlist.id, -1) is None
    assert await store.get_playlist_size(playlist.id) == 1


@pytest.mark.asyncio
async def test_delete_item_reports_whether_it_was_there(fake_engine):  #pylint:disable=redefined-outer-name
    '''Deleting a row that is already gone is False, not an error'''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'orphans')
    outcomes = await store.add_items(playlist.id, writes('a'), MAX_SIZE)
    item_id = outcomes[0].item_id

    assert await store.delete_item(item_id) is True
    assert await store.delete_item(item_id) is False


@pytest.mark.asyncio
async def test_delete_playlist_takes_its_items(fake_engine):  #pylint:disable=redefined-outer-name
    '''Deleting a playlist leaves no orphaned items'''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'doomed')
    await store.add_items(playlist.id, writes('a', 'b'), MAX_SIZE)

    assert await store.delete_playlist(playlist.id) is True

    async with async_mock_session(fake_engine) as session:
        playlists = (await session.execute(select(sql_count()).select_from(Playlist))).scalar()
        items = (await session.execute(select(sql_count()).select_from(PlaylistItem))).scalar()
    assert playlists == 0
    assert items == 0


@pytest.mark.asyncio
async def test_rename_playlist_reports_a_missing_row(fake_engine):  #pylint:disable=redefined-outer-name
    '''Renaming works, and renaming nothing is False'''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'before')

    assert await store.rename_playlist(playlist.id, 'after') is True
    assert (await store.get_playlist(playlist.id)).name == 'after'
    assert await store.rename_playlist(9999, 'ghost') is False


@pytest.mark.asyncio
async def test_mark_queued_persists_last_queued(fake_engine):  #pylint:disable=redefined-outer-name
    '''The queue time lands in the column `!playlist list` reads back.

    Its predecessor assigned an attribute that was not a column, so the commit
    emitted no UPDATE and this stayed NULL for the life of the feature.
    '''
    store = build_store(fake_engine)
    playlist = await store.create_playlist(GUILD_ID, 'queued')

    assert await store.mark_queued(playlist.id) is True

    stored = await store.get_playlist(playlist.id)
    assert stored.last_queued is not None
    assert await store.mark_queued(9999) is False


@pytest.mark.asyncio
async def test_record_history_item_evicts_the_oldest_to_make_room(fake_engine):  #pylint:disable=redefined-outer-name
    '''History stays at its ceiling by dropping the oldest, not the newest.

    Which items get dropped is what `list_items`' order decides, and until
    created_at was populated it decided nothing.
    '''
    store = build_store(fake_engine)
    playlist_id = await store.ensure_history_playlist(GUILD_ID)
    base = datetime(2024, 6, 1, tzinfo=timezone.utc)
    async with async_mock_session(fake_engine) as session:
        for offset, url in enumerate(('oldest', 'middle', 'newest')):
            session.add(PlaylistItem(title=url, video_url=url, uploader='up',
                                     playlist_id=playlist_id,
                                     created_at=base + timedelta(hours=offset)))
        await session.commit()

    assert await store.record_history_item(
        playlist_id, PlaylistItemWrite(video_url='fresh', title='Fresh'), 3) is True

    assert [item.video_url for item in await store.list_items(playlist_id)] == [
        'middle', 'newest', 'fresh']


@pytest.mark.asyncio
async def test_record_history_item_moves_a_repeat_to_the_end(fake_engine):  #pylint:disable=redefined-outer-name
    '''Playing a track again re-records it rather than duplicating or skipping it'''
    store = build_store(fake_engine)
    playlist_id = await store.ensure_history_playlist(GUILD_ID)
    for url in ('a', 'b'):
        await store.record_history_item(playlist_id, PlaylistItemWrite(video_url=url, title=url), 8)

    await store.record_history_item(playlist_id, PlaylistItemWrite(video_url='a', title='a'), 8)

    items = await store.list_items(playlist_id)
    assert [item.video_url for item in items] == ['b', 'a']
    assert len(items) == 2, 'a repeat should not leave two rows for the same url'


@pytest.mark.asyncio
async def test_record_history_item_reports_a_missing_playlist(fake_engine):  #pylint:disable=redefined-outer-name
    '''A history playlist that no longer exists is False, not a crash.

    Reachable: the post-play queue holds an id, and the playlist can be deleted
    between the play and the write.
    '''
    store = build_store(fake_engine)
    assert await store.record_history_item(
        9999, PlaylistItemWrite(video_url='a', title='a'), 8) is False
