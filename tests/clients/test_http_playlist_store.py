'''Tests for HttpPlaylistStore — against a real DatabaseHttpServer.

Same shape and same reasoning as the guild-analytics and markov HTTP tests: both
halves go through aiohttp's TestServer + TestClient, and the store behind the
server is the real PlaylistClient on real postgres, so what is asserted is that
the two implementations of one Protocol are interchangeable rather than that a
fake agrees with itself.

The properties worth the round trip in this group are the compound calls. Order,
truncation and eviction are decided on the far side of the wire and reported as
data, and each of them is a promise the cog reads back as a user-facing index.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from functools import partial

import pytest
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy.exc import OperationalError

from discord_bot.clients.http_playlist_store import HttpPlaylistStore
from discord_bot.clients.playlist_client import PlaylistClient
from discord_bot.exceptions import DatabaseUnavailable
from discord_bot.interfaces.database_protocols import PlaylistStore
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.types.playlist import (
    PlaylistEntry,
    PlaylistItemAddOutcome,
    PlaylistItemAddStatus,
    PlaylistItemEntry,
    PlaylistItemWrite,
)

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 808
MAX_SIZE = 3


class _RecordingStore:
    '''PlaylistStore stand-in that records calls and can fail on demand.'''

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def _record(self, name, *args):
        self.calls.append((name, *args))
        if self.error:
            raise self.error

    async def list_playlists(self, guild_id):
        self._record('list_playlists', guild_id)
        return []

    async def count_playlists(self, guild_id):
        self._record('count_playlists', guild_id)
        return 0

    async def get_playlist(self, playlist_id):
        self._record('get_playlist', playlist_id)
        return None

    async def get_playlist_by_name(self, guild_id, name):
        self._record('get_playlist_by_name', guild_id, name)
        return None

    async def get_history_playlist(self, guild_id):
        self._record('get_history_playlist', guild_id)
        return None

    async def ensure_history_playlist(self, guild_id):
        self._record('ensure_history_playlist', guild_id)
        return 1

    async def create_playlist(self, guild_id, name):
        self._record('create_playlist', guild_id, name)
        return PlaylistEntry(id=1, name=name, server_id=guild_id)

    async def delete_playlist(self, playlist_id):
        self._record('delete_playlist', playlist_id)
        return True

    async def rename_playlist(self, playlist_id, name):
        self._record('rename_playlist', playlist_id, name)
        return True

    async def mark_queued(self, playlist_id):
        self._record('mark_queued', playlist_id)
        return True

    async def get_playlist_size(self, playlist_id):
        self._record('get_playlist_size', playlist_id)
        return 0

    async def list_items(self, playlist_id):
        self._record('list_items', playlist_id)
        return []

    async def add_items(self, playlist_id, items, max_size):
        self._record('add_items', playlist_id, items, max_size)
        return [PlaylistItemAddOutcome(video_url=item.video_url, title=item.title,
                                       status=PlaylistItemAddStatus.ADDED, item_id=index)
                for index, item in enumerate(items)]

    async def delete_item(self, item_id):
        self._record('delete_item', item_id)
        return True

    async def delete_item_by_index(self, playlist_id, index):
        self._record('delete_item_by_index', playlist_id, index)
        return None

    async def record_history_item(self, playlist_id, item, max_size):
        self._record('record_history_item', playlist_id, item, max_size)
        return True


def _live_store(fake_engine) -> PlaylistClient:  #pylint:disable=redefined-outer-name
    '''Build the real in-process store over the test engine.'''
    return PlaylistClient(partial(async_mock_session, fake_engine))


def _writes(*urls):
    '''Build item writes for a list of urls.'''
    return [PlaylistItemWrite(video_url=url, title=url.upper(), uploader='up') for url in urls]


def test_http_playlist_satisfies_the_protocol():
    '''HttpPlaylistStore is a structural PlaylistStore.'''
    assert isinstance(HttpPlaylistStore('http://db:8085'), PlaylistStore)


@pytest.mark.asyncio
async def test_playlist_lifecycle_over_the_wire(fake_engine):  #pylint:disable=redefined-outer-name
    '''Create, read, rename, count and delete a playlist, all through HTTP.'''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)

        created = await client.create_playlist(GUILD_ID, 'road trip')
        assert isinstance(created, PlaylistEntry)
        assert created.server_id == GUILD_ID

        assert (await client.get_playlist(created.id)).name == 'road trip'
        assert (await client.get_playlist_by_name(GUILD_ID, 'road trip')).id == created.id
        assert await client.count_playlists(GUILD_ID) == 1

        assert await client.rename_playlist(created.id, 'commute') is True
        assert (await client.get_playlist(created.id)).name == 'commute'

        assert await client.mark_queued(created.id) is True
        assert (await client.get_playlist(created.id)).last_queued is not None

        assert await client.delete_playlist(created.id) is True
        assert await client.get_playlist(created.id) is None
        assert await client.count_playlists(GUILD_ID) == 0


@pytest.mark.asyncio
async def test_public_index_stays_newest_first(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_playlists order is the index users type, and JSON array order carries it.

    The order is a promise rather than an observation -- it was reversed once by
    assuming production matched a table it did not -- so it is asserted on this
    side of the wire too. Nothing between the query and the cog may re-sort.
    '''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        for name in ('first', 'second', 'third'):
            await client.create_playlist(GUILD_ID, name)
        names = [entry.name for entry in await client.list_playlists(GUILD_ID)]

    assert names == ['third', 'second', 'first']


@pytest.mark.asyncio
async def test_items_come_back_oldest_first(fake_engine):  #pylint:disable=redefined-outer-name
    '''list_items order is the position delete_item_by_index counts from.'''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        playlist = await client.create_playlist(GUILD_ID, 'ordered')
        await client.add_items(playlist.id, _writes('a', 'b', 'c'), 10)
        items = await client.list_items(playlist.id)
        assert await client.get_playlist_size(playlist.id) == 3

    assert [item.video_url for item in items] == ['a', 'b', 'c']
    assert all(isinstance(item, PlaylistItemEntry) for item in items)
    assert items[0].created_at is not None


@pytest.mark.asyncio
async def test_a_full_playlist_stops_the_batch(fake_engine):  #pylint:disable=redefined-outer-name
    '''The ceiling is enforced on the far side, and reported per item in order.

    Fewer outcomes than items is the wire encoding of "it filled": the item that
    hit the ceiling reports PLAYLIST_FULL and the ones after it were never
    attempted. A boolean per item could not say that, and a count could not say
    which item stopped it.
    '''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        playlist = await client.create_playlist(GUILD_ID, 'small')
        outcomes = await client.add_items(
            playlist.id, _writes('a', 'b', 'a', 'c', 'd'), MAX_SIZE)
        stored = [item.video_url for item in await client.list_items(playlist.id)]

    assert [outcome.status for outcome in outcomes] == [
        PlaylistItemAddStatus.ADDED,
        PlaylistItemAddStatus.ADDED,
        PlaylistItemAddStatus.DUPLICATE,
        PlaylistItemAddStatus.ADDED,
        PlaylistItemAddStatus.PLAYLIST_FULL,
    ]
    assert [outcome.video_url for outcome in outcomes] == ['a', 'b', 'a', 'c', 'd']
    assert outcomes[0].item_id is not None
    assert stored == ['a', 'b', 'c']


@pytest.mark.asyncio
async def test_a_batch_of_items_is_one_request():
    '''The whole batch crosses in a single call, never one round trip per track.'''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(playlist_store=store).build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        outcomes = await client.add_items(7, _writes('a', 'b', 'c'), MAX_SIZE)

    add_calls = [call for call in store.calls if call[0] == 'add_items']
    assert len(add_calls) == 1, 'the batch was split into one request per item'
    assert [write.video_url for write in add_calls[0][2]] == ['a', 'b', 'c']
    assert add_calls[0][3] == MAX_SIZE, 'the ceiling did not cross with the batch'
    assert [outcome.title for outcome in outcomes] == ['A', 'B', 'C']


@pytest.mark.asyncio
async def test_index_zero_is_a_real_position(fake_engine):  #pylint:disable=redefined-outer-name
    '''Deleting item 0 works, and returns what was deleted.

    Zero is the most commonly deleted position and the one a truthiness check on
    the request body would reject as a missing argument. The entry comes back
    because the cog names the track in its reply, and the row is gone by then.
    '''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        playlist = await client.create_playlist(GUILD_ID, 'indexed')
        await client.add_items(playlist.id, _writes('a', 'b'), 10)

        deleted = await client.delete_item_by_index(playlist.id, 0)
        assert deleted.video_url == 'a'
        assert [item.video_url for item in await client.list_items(playlist.id)] == ['b']

        assert await client.delete_item_by_index(playlist.id, 9) is None
        remaining = await client.list_items(playlist.id)
        assert await client.delete_item(remaining[0].id) is True
        assert await client.delete_item(remaining[0].id) is False


@pytest.mark.asyncio
async def test_history_is_ensured_in_one_call(fake_engine):  #pylint:disable=redefined-outer-name
    '''Two ensures return one id, and the second creates nothing.

    The reason the Protocol has `ensure_` rather than a read and a conditional
    write: split in two, any pair of players starting at once races on a table
    with a unique constraint, and the loser gets an error on a path with no way
    to report one.
    '''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        assert await client.get_history_playlist(GUILD_ID) is None

        first = await client.ensure_history_playlist(GUILD_ID)
        second = await client.ensure_history_playlist(GUILD_ID)
        fetched = await client.get_history_playlist(GUILD_ID)
        listed = await client.list_playlists(GUILD_ID)

    assert first == second
    assert fetched.id == first
    assert fetched.is_history is True
    assert not listed, 'the history playlist leaked into the public index'


@pytest.mark.asyncio
async def test_history_eviction_survives_the_wire(fake_engine):  #pylint:disable=redefined-outer-name
    '''One request per played track: dedupe, evict the oldest, insert.

    This is six queries in the post-play loop collapsed into one call, and the
    eviction order is the reason list_items' order is a promise -- "the oldest"
    has to mean something stable.
    '''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        playlist_id = await client.ensure_history_playlist(GUILD_ID)
        for url in ('a', 'b', 'c'):
            assert await client.record_history_item(
                playlist_id, _writes(url)[0], MAX_SIZE) is True
        # Replaying an existing track moves it to the end rather than duplicating.
        await client.record_history_item(playlist_id, _writes('a')[0], MAX_SIZE)
        # And the fifth track evicts the oldest survivor.
        await client.record_history_item(playlist_id, _writes('d')[0], MAX_SIZE)
        stored = [item.video_url for item in await client.list_items(playlist_id)]
        missing = await client.record_history_item(999, _writes('e')[0], MAX_SIZE)

    assert stored == ['c', 'a', 'd']
    assert missing is False


@pytest.mark.asyncio
async def test_a_missing_playlist_is_not_an_error(fake_engine):  #pylint:disable=redefined-outer-name
    '''"No such row" arrives as a 200 with a null or false result.

    The rule the whole envelope exists for: a 404 here would be retried three
    times to be told the same thing, and its body would be discarded by
    raise_for_status().
    '''
    server = DatabaseHttpServer(playlist_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        assert await client.get_playlist(999) is None
        assert await client.get_playlist_by_name(GUILD_ID, 'nope') is None
        assert await client.rename_playlist(999, 'nope') is False
        assert await client.mark_queued(999) is False
        # delete_playlist reports True for a playlist that was never there, unlike
        # rename_playlist and delete_item beside it. That asymmetry predates the
        # wire -- the in-process store deletes the items, checks for the row and
        # returns True either way, and its one caller discards the result. Pinned
        # as it stands rather than quietly changed here: this slice is meant to be
        # inert, and a store whose answer differs by implementation would be a
        # worse bug than an uninformative one.
        assert await client.delete_playlist(999) is True
        assert await client.delete_item(999) is False
        assert await client.delete_item_by_index(999, 0) is None
        assert await client.get_playlist_size(999) == 0
        assert not await client.list_items(999)
        assert not await client.list_playlists(GUILD_ID)

        response = await tc.post('/database/playlist/get_playlist',
                                 json={'playlist_id': 999})
        assert response.status == 200
        assert (await response.json())['result'] is None


@pytest.mark.asyncio
async def test_a_playlist_failure_is_unavailable(fake_engine):  #pylint:disable=redefined-outer-name,unused-argument
    '''A store failure crosses typed, and is not retried across the wire.

    Same contract as the two groups before it; asserted per group because the
    envelope is applied per handler and a route that forgot it would only show up
    here.
    '''
    store = _RecordingStore(error=OperationalError('SELECT 1', {}, Exception('gone')))
    async with TestClient(TestServer(DatabaseHttpServer(playlist_store=store).build_app())) as tc:
        client = HttpPlaylistStore(str(tc.make_url('')), session=tc.session)
        with pytest.raises(DatabaseUnavailable):
            await client.list_playlists(GUILD_ID)

    assert len(store.calls) == 1


@pytest.mark.asyncio
async def test_a_malformed_item_is_rejected_up_front():
    '''An item that is not PlaylistItemWrite-shaped gets 422, never the store.

    Not the error envelope: it never reached the database, and re-sending the
    same bytes cannot make it valid. Rejected before the store call rather than
    inside it, so a bad item in a batch fails the request instead of writing
    half of it.
    '''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(playlist_store=store).build_app())) as tc:
        response = await tc.post('/database/playlist/add_items', json={
            'playlist_id': 1, 'max_size': MAX_SIZE,
            'items': [{'video_url': 'a'}, {'title': 'no url'}]})
        assert response.status == 422
        response = await tc.post('/database/playlist/record_history_item', json={
            'playlist_id': 1, 'max_size': MAX_SIZE, 'item': 'not-an-object'})
        assert response.status == 422
        response = await tc.post('/database/playlist/delete_item_by_index',
                                 json={'playlist_id': 1})
        assert response.status == 422

    assert not store.calls


@pytest.mark.asyncio
async def test_playlist_routes_can_be_left_out():
    '''A store the server was not given registers nothing and answers 404.

    The cost of making the stores optional so each slice stays additive. 404 is
    a 4xx, which async_retry_broker_command propagates immediately rather than
    laddering, so a misconfigured pod fails loudly and fast rather than hanging
    for seven seconds per call.
    '''
    async with TestClient(TestServer(DatabaseHttpServer(markov_store=object()).build_app())) as tc:
        assert (await tc.post('/database/playlist/list_playlists',
                              json={'guild_id': GUILD_ID})).status == 404
