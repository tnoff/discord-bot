'''HttpPlaylistStore, tested against the database contract and nothing else.

Served by a `ContractStub` built from `testing/contracts/database.py`: each call sends a body the contract accepts (a
violation fails the test) and every declared reply becomes what the cog expects. The db pod is not needed; the round trip
over a real store is tests/integration/commands_to_db/.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from datetime import datetime, timezone

import pytest

from discord_core.exceptions import DatabaseUnavailable
from discord_core.routes import database as database_routes
from discord_core.types.playlist import (PlaylistEntry, PlaylistItemAddOutcome, PlaylistItemAddStatus,
                                         PlaylistItemEntry, PlaylistItemWrite)

from discord_commands.clients.http_playlist_store import HttpPlaylistStore

from testing.contracts.stub import serving
from testing.db_client_support import answer, answering, failing

GUILD = 5
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
ROUTES = database_routes.PLAYLIST.routes
PLAYLIST = PlaylistEntry(id=3, name='mix', server_id=GUILD, last_queued=NOW, created_at=NOW, is_history=False)
ITEM = PlaylistItemEntry(id=4, title='a', video_url='https://example.com/a', uploader='u', playlist_id=3,
                         created_at=NOW)


def _client(tc) -> HttpPlaylistStore:
    return HttpPlaylistStore(str(tc.make_url('')), session=tc.session)


def _write(name: str) -> PlaylistItemWrite:
    return PlaylistItemWrite(video_url=f'https://example.com/{name}', title=name.upper())


@pytest.mark.asyncio
@pytest.mark.parametrize('method, route, result, expected', [
    ('list_playlists', 'list_playlists', [PLAYLIST], [PLAYLIST]),
    ('count_playlists', 'count_playlists', 2, 2),
    ('get_history_playlist', 'get_history_playlist', PLAYLIST, PLAYLIST),
    ('get_history_playlist', 'get_history_playlist', None, None),
    ('ensure_history_playlist', 'ensure_history_playlist', 3, 3),
])
async def test_guild_routes_send_the_guild_and_read_the_reply(method, route, result, expected):
    '''The five routes keyed by guild alone: the guild in the body, a null reply read as "none".'''
    stub = answering(ROUTES[route], result)
    async with serving(stub) as tc:
        assert await getattr(_client(tc), method)(GUILD) == expected
    assert stub.calls_to(ROUTES[route]) == [{'guild_id': GUILD}]


@pytest.mark.asyncio
@pytest.mark.parametrize('method, route, result, expected', [
    ('get_playlist', 'get_playlist', PLAYLIST, PLAYLIST),
    ('get_playlist', 'get_playlist', None, None),
    ('delete_playlist', 'delete_playlist', True, True),
    ('mark_queued', 'mark_queued', True, True),
    ('list_items', 'list_items', [ITEM], [ITEM]),
])
async def test_playlist_routes_send_the_playlist_and_read_the_reply(method, route, result, expected):
    '''The routes keyed by playlist row id: the id in the body, a null reply read as "none".'''
    stub = answering(ROUTES[route], result)
    async with serving(stub) as tc:
        assert await getattr(_client(tc), method)(3) == expected
    assert stub.calls_to(ROUTES[route]) == [{'playlist_id': 3}]


@pytest.mark.asyncio
async def test_a_playlist_is_found_and_made_by_guild_and_name():
    '''Both routes send the guild and the name, and an unknown name is none rather than an error.'''
    by_name, create = ROUTES['get_playlist_by_name'], ROUTES['create_playlist']
    stub = answering(by_name, None)
    answer(stub, create, PLAYLIST)
    async with serving(stub) as tc:
        assert await _client(tc).get_playlist_by_name(GUILD, 'mix') is None
        assert await _client(tc).create_playlist(GUILD, 'mix') == PLAYLIST
    assert stub.calls_to(by_name) == stub.calls_to(create) == [{'guild_id': GUILD, 'name': 'mix'}]


@pytest.mark.asyncio
async def test_a_rename_sends_the_playlist_and_the_new_name():
    '''Both fields cross, and the pod's yes/no is returned.'''
    stub = answering(ROUTES['rename_playlist'], True)
    async with serving(stub) as tc:
        assert await _client(tc).rename_playlist(3, 'new') is True
    assert stub.calls_to(ROUTES['rename_playlist']) == [{'playlist_id': 3, 'name': 'new'}]


@pytest.mark.asyncio
async def test_a_batch_of_items_is_one_request_with_its_ceiling():
    '''The whole batch crosses in a single call, in order, with the playlist's size limit, and one outcome comes back
    per item the pod attempted.'''
    outcomes = [PlaylistItemAddOutcome(video_url=f'https://example.com/{name}', title=name.upper(),
                                       status=PlaylistItemAddStatus.ADDED, item_id=index)
                for index, name in enumerate('ab', start=1)]
    stub = answering(ROUTES['add_items'], outcomes)
    async with serving(stub) as tc:
        assert await _client(tc).add_items(3, [_write('a'), _write('b')], 64) == outcomes
    (sent,) = stub.calls_to(ROUTES['add_items'])
    assert [item['video_url'] for item in sent['items']] == ['https://example.com/a', 'https://example.com/b']
    assert (sent['playlist_id'], sent['max_size']) == (3, 64)


@pytest.mark.asyncio
async def test_index_zero_is_sent_as_a_real_position():
    '''Zero is the first item, the position a truthiness check would drop, and what was deleted comes back.'''
    route = ROUTES['delete_item_by_index']
    stub = answering(route, ITEM)
    async with serving(stub) as tc:
        assert await _client(tc).delete_item_by_index(3, 0) == ITEM
    assert stub.calls_to(route) == [{'playlist_id': 3, 'index': 0}]


@pytest.mark.asyncio
async def test_deleting_past_the_end_is_none_not_an_error():
    '''An index with no item is a null result.'''
    stub = answering(ROUTES['delete_item_by_index'], None)
    async with serving(stub) as tc:
        assert await _client(tc).delete_item_by_index(3, 9) is None


@pytest.mark.asyncio
async def test_an_item_is_deleted_by_its_row_id():
    '''The row id crosses and whether it was there comes back.'''
    stub = answering(ROUTES['delete_item'], False)
    async with serving(stub) as tc:
        assert await _client(tc).delete_item(4) is False
    assert stub.calls_to(ROUTES['delete_item']) == [{'item_id': 4}]


@pytest.mark.asyncio
async def test_a_failed_store_is_unavailable_and_asked_once():
    '''The pod's error body is re-raised as DatabaseUnavailable, and 200 means it is not retried.'''
    stub = failing(ROUTES['list_playlists'])
    async with serving(stub) as tc:
        with pytest.raises(DatabaseUnavailable):
            await _client(tc).list_playlists(GUILD)
    assert len(stub.calls_to(ROUTES['list_playlists'])) == 1
