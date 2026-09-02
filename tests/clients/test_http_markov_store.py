'''Tests for HttpMarkovStore — against a real DatabaseHttpServer.

Same shape and same reasoning as tests/clients/test_http_guild_analytics_store:
both halves go through aiohttp's TestServer + TestClient, and the store behind
the server is the real MarkovClient on real postgres, so what is asserted is that
the two implementations of one Protocol are interchangeable rather than that a
fake agrees with itself.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from datetime import datetime, timezone
from functools import partial

import pytest
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy.exc import OperationalError

from discord_bot.clients.http_markov_store import HttpMarkovStore
from discord_bot.clients.markov_client import MarkovClient
from discord_bot.exceptions import DatabaseUnavailable
from discord_bot.interfaces.database_protocols import MarkovStore
from discord_bot.servers.database_server import DatabaseHttpServer
from discord_bot.types.markov import MarkovChannelEntry, MarkovMessageWrite

from tests.helpers import fake_engine #pylint:disable=unused-import
from tests.helpers import async_mock_session

GUILD_ID = 606
CHANNEL_ID = 707


class _RecordingStore:
    '''MarkovStore stand-in that records calls and can fail on demand.'''

    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def _record(self, name, *args):
        self.calls.append((name, *args))
        if self.error:
            raise self.error

    async def list_channels(self):
        self._record('list_channels')
        return []

    async def list_guild_channel_ids(self, guild_id):
        self._record('list_guild_channel_ids', guild_id)
        return []

    async def get_channel(self, guild_id, channel_id):
        self._record('get_channel', guild_id, channel_id)
        return None

    async def add_channel(self, guild_id, channel_id):
        self._record('add_channel', guild_id, channel_id)
        return MarkovChannelEntry(id=1, channel_id=channel_id, server_id=guild_id,
                                  last_message_id=None)

    async def remove_channel(self, guild_id, channel_id):
        self._record('remove_channel', guild_id, channel_id)
        return True

    async def reset_channel(self, guild_id, channel_id):
        self._record('reset_channel', guild_id, channel_id)
        return True

    async def save_messages(self, guild_id, channel_id, messages):
        self._record('save_messages', guild_id, channel_id, messages)
        return len(messages)

    async def generate_words(self, guild_id, count, first_word=None):
        self._record('generate_words', guild_id, count, first_word)
        return []

    async def prune_relations_before(self, cutoff):
        self._record('prune_relations_before', cutoff)
        return True


def _live_store(fake_engine) -> MarkovClient:  #pylint:disable=redefined-outer-name
    '''Build the real in-process store over the test engine.'''
    return MarkovClient(partial(async_mock_session, fake_engine))


def _message(pairs, message_id, hour=0) -> MarkovMessageWrite:
    '''Build one message write.'''
    return MarkovMessageWrite(
        word_pairs=pairs, last_message_id=message_id,
        message_timestamp=datetime(2026, 6, 1, hour, tzinfo=timezone.utc))


def test_http_markov_satisfies_the_protocol():
    '''HttpMarkovStore is a structural MarkovStore.'''
    assert isinstance(HttpMarkovStore('http://db:8085'), MarkovStore)


@pytest.mark.asyncio
async def test_channel_lifecycle_over_the_wire(fake_engine):  #pylint:disable=redefined-outer-name
    '''Add, read, list and remove a channel, all through HTTP against real rows.'''
    server = DatabaseHttpServer(markov_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)

        added = await client.add_channel(GUILD_ID, CHANNEL_ID)
        assert isinstance(added, MarkovChannelEntry)
        assert added.channel_id == CHANNEL_ID
        assert added.server_id == GUILD_ID

        fetched = await client.get_channel(GUILD_ID, CHANNEL_ID)
        assert fetched.id == added.id
        assert await client.list_guild_channel_ids(GUILD_ID) == [CHANNEL_ID]
        assert [entry.channel_id for entry in await client.list_channels()] == [CHANNEL_ID]

        assert await client.remove_channel(GUILD_ID, CHANNEL_ID) is True
        assert await client.get_channel(GUILD_ID, CHANNEL_ID) is None
        assert await client.remove_channel(GUILD_ID, CHANNEL_ID) is False


@pytest.mark.asyncio
async def test_an_untracked_channel_is_none_not_an_error(fake_engine):  #pylint:disable=redefined-outer-name
    '''"Not tracked" is an answer, and it arrives as a 200 with a null result.

    The rule the whole envelope exists for: a 404 here would be retried three
    times to be told the same thing, and its body would be discarded by
    raise_for_status().
    '''
    server = DatabaseHttpServer(markov_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        assert await client.get_channel(GUILD_ID, CHANNEL_ID) is None
        assert await client.reset_channel(GUILD_ID, CHANNEL_ID) is False
        assert await client.save_messages(GUILD_ID, CHANNEL_ID, []) is None
        assert not await client.list_channels()

        response = await tc.post('/database/markov/get_channel',
                                 json={'guild_id': GUILD_ID, 'channel_id': CHANNEL_ID})
        assert response.status == 200
        assert (await response.json())['result'] is None


@pytest.mark.asyncio
async def test_word_pairs_survive_the_json_round_trip(fake_engine):  #pylint:disable=redefined-outer-name
    '''Pairs are tuples in python and arrays in JSON, and must come back usable.

    The one shape in this group with a real wire hazard: `List[Tuple[str, str]]`
    serialises to a list of two-element lists, and a chain built from lists that
    were meant to be pairs would generate silently wrong sentences rather than
    fail.
    '''
    server = DatabaseHttpServer(markov_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        await client.add_channel(GUILD_ID, CHANNEL_ID)
        saved = await client.save_messages(GUILD_ID, CHANNEL_ID, [
            _message([('hello', 'world'), ('world', 'again')], 11),
        ])
        assert saved == 1
        words = await client.generate_words(GUILD_ID, 5, first_word='hello')

    assert words[:2] == ['hello', 'world']


@pytest.mark.asyncio
async def test_a_batch_is_one_request_not_one_per_message():
    '''The whole cycle crosses in a single call.

    This is what MarkovMessageWrite was shaped for, and the property is only
    observable once there is a wire: a per-message signature would be one round
    trip each, the same cost !267 removed a layer down.
    '''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(markov_store=store).build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        batch = [_message([('a', 'b')], 1), _message([('b', 'c')], 2, hour=1),
                 _message([], 3, hour=2)]
        assert await client.save_messages(GUILD_ID, CHANNEL_ID, batch) == 3

    save_calls = [call for call in store.calls if call[0] == 'save_messages']
    assert len(save_calls) == 1, 'the batch was split into one request per message'
    assert [write.last_message_id for write in save_calls[0][3]] == [1, 2, 3]
    assert save_calls[0][3][2].word_pairs == [], (
        'a message that contributed no pairs was dropped; it still has to advance '
        'last_message_id or the same messages are re-read every cycle'
    )


@pytest.mark.asyncio
async def test_a_seedless_sentence_sends_an_explicit_null():
    '''`!markov speak` with no seed reaches the store as first_word=None.

    first_word is the one optional argument in the group, so it is read with
    .get rather than required -- and this is the test that keeps the 422 guard
    from being tightened onto it by accident.
    '''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(markov_store=store).build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        assert not await client.generate_words(GUILD_ID, 32)
        assert not await client.generate_words(GUILD_ID, 32, first_word='seed')

    assert store.calls == [('generate_words', GUILD_ID, 32, None),
                           ('generate_words', GUILD_ID, 32, 'seed')]


@pytest.mark.asyncio
async def test_the_prune_cutoff_keeps_its_utc_offset(fake_engine):  #pylint:disable=redefined-outer-name
    '''A tz-aware cutoff crosses as ISO text and is compared as tz-aware.

    A naive datetime on the pod side would compare against tz-aware column values
    and raise, or worse, silently prune by the wrong offset.
    '''
    server = DatabaseHttpServer(markov_store=_live_store(fake_engine))
    async with TestClient(TestServer(server.build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        await client.add_channel(GUILD_ID, CHANNEL_ID)
        await client.save_messages(GUILD_ID, CHANNEL_ID, [
            _message([('old', 'pair')], 1),
            _message([('new', 'pair')], 2, hour=12),
        ])
        assert await client.prune_relations_before(
            datetime(2026, 6, 1, 6, tzinfo=timezone.utc)) is True
        words = await client.generate_words(GUILD_ID, 5, first_word='old')
        survivors = await client.generate_words(GUILD_ID, 5, first_word='new')

    assert not words
    assert survivors[:1] == ['new']


@pytest.mark.asyncio
async def test_markov_failures_raise_unavailable_once():
    '''A store failure crosses typed, and is not retried across the wire.

    Same contract as the guild-analytics group; asserted per group because the
    envelope is applied per handler and a route that forgot it would only show up
    here.
    '''
    store = _RecordingStore(error=OperationalError('SELECT 1', {}, Exception('gone')))
    async with TestClient(TestServer(DatabaseHttpServer(markov_store=store).build_app())) as tc:
        client = HttpMarkovStore(str(tc.make_url('')), session=tc.session)
        with pytest.raises(DatabaseUnavailable):
            await client.list_channels()

    assert len(store.calls) == 1


@pytest.mark.asyncio
async def test_a_malformed_batch_is_rejected_up_front():
    '''A batch that is not MarkovMessageWrite-shaped gets 422, never the store.

    Not the error envelope: it never reached the database, and re-sending the
    same bytes cannot make it valid.
    '''
    store = _RecordingStore()
    async with TestClient(TestServer(DatabaseHttpServer(markov_store=store).build_app())) as tc:
        response = await tc.post('/database/markov/save_messages', json={
            'guild_id': GUILD_ID, 'channel_id': CHANNEL_ID,
            'messages': [{'word_pairs': [['a', 'b']]}]})
        assert response.status == 422
        response = await tc.post('/database/markov/prune_relations_before',
                                 json={'cutoff': 'not-a-timestamp'})
        assert response.status == 422

    assert not store.calls


@pytest.mark.asyncio
async def test_an_unconfigured_group_serves_no_routes():
    '''A store the server was not given registers nothing and answers 404.

    The cost of making the stores optional so each slice stays additive. 404 is
    a 4xx, which async_retry_broker_command propagates immediately rather than
    laddering, so a misconfigured pod fails loudly and fast rather than hanging
    for seven seconds per call.
    '''
    async with TestClient(TestServer(DatabaseHttpServer(markov_store=_RecordingStore()).build_app())) as tc:
        assert (await tc.post('/database/markov/list_channels', json={})).status == 200
        assert (await tc.post('/database/guild_analytics/get_analytics',
                              json={'guild_id': 1})).status == 404
