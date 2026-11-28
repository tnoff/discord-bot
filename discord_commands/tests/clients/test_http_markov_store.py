'''HttpMarkovStore, tested against the database contract and nothing else.

Served by a `ContractStub` built from `testing/contracts/database.py`: each call sends a body the contract accepts (a
violation fails the test) and every declared reply becomes what the cog expects. The db pod is not needed; the round trip
over a real store is tests/integration/commands_to_db/.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails pr-check:secrets.
from datetime import datetime, timedelta, timezone

import pytest

from discord_core.exceptions import DatabaseUnavailable
from discord_core.routes import database as database_routes
from discord_core.types.markov import MarkovChannelEntry, MarkovMessageWrite

from discord_commands.clients.http_markov_store import HttpMarkovStore

from testing.contracts.stub import serving
from testing.db_client_support import answering, failing

GUILD = 11
CHANNEL = 22
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
ROUTES = database_routes.MARKOV.routes
TRACKED = MarkovChannelEntry(id=1, channel_id=CHANNEL, server_id=GUILD, last_message_id=9)


def _message(last_message_id: int, pairs=(), hour: int = 0) -> MarkovMessageWrite:
    return MarkovMessageWrite(word_pairs=list(pairs), message_timestamp=NOW + timedelta(hours=hour),
                              last_message_id=last_message_id)


def _client(tc) -> HttpMarkovStore:
    return HttpMarkovStore(str(tc.make_url('')), session=tc.session)


@pytest.mark.asyncio
async def test_every_tracked_channel_is_listed():
    '''No arguments, and the entries come back as the pod described them.'''
    stub = answering(ROUTES['list_channels'], [TRACKED])
    async with serving(stub) as tc:
        assert await _client(tc).list_channels() == [TRACKED]


@pytest.mark.asyncio
async def test_a_guilds_channel_ids_are_asked_by_guild():
    '''The guild goes in the body and the ids come back as they are.'''
    stub = answering(ROUTES['list_guild_channel_ids'], [CHANNEL, 33])
    async with serving(stub) as tc:
        assert await _client(tc).list_guild_channel_ids(GUILD) == [CHANNEL, 33]
    assert stub.calls_to(ROUTES['list_guild_channel_ids']) == [{'guild_id': GUILD}]


@pytest.mark.asyncio
@pytest.mark.parametrize('answer, expected', [(TRACKED, TRACKED), (None, None)])
async def test_an_untracked_channel_is_none_not_an_error(answer, expected):
    '''A null result is "not tracked", which the cog tells apart from a failure.'''
    stub = answering(ROUTES['get_channel'], answer)
    async with serving(stub) as tc:
        assert await _client(tc).get_channel(GUILD, CHANNEL) == expected
    assert stub.calls_to(ROUTES['get_channel']) == [{'guild_id': GUILD, 'channel_id': CHANNEL}]


@pytest.mark.asyncio
async def test_a_channel_is_added_by_guild_and_channel():
    '''The row the pod made comes back.'''
    stub = answering(ROUTES['add_channel'], TRACKED)
    async with serving(stub) as tc:
        assert await _client(tc).add_channel(GUILD, CHANNEL) == TRACKED
    assert stub.calls_to(ROUTES['add_channel']) == [{'guild_id': GUILD, 'channel_id': CHANNEL}]


@pytest.mark.asyncio
@pytest.mark.parametrize('method, found', [
    ('remove_channel', True), ('remove_channel', False), ('reset_channel', True), ('reset_channel', False)])
async def test_removing_or_resetting_reports_whether_it_was_tracked(method, found):
    '''The boolean the pod answered is the one the cog gets.'''
    stub = answering(ROUTES[method], found)
    async with serving(stub) as tc:
        assert await getattr(_client(tc), method)(GUILD, CHANNEL) is found
    assert stub.calls_to(ROUTES[method]) == [{'guild_id': GUILD, 'channel_id': CHANNEL}]


@pytest.mark.asyncio
async def test_a_batch_is_one_request_and_keeps_messages_with_no_pairs():
    '''The cycle crosses in one call, in order, and a message that added no pairs is still sent: it has to advance
    last_message_id or the same messages are read again every cycle.'''
    stub = answering(ROUTES['save_messages'], 3)
    batch = [_message(1, [('a', 'b')]), _message(2, [('b', 'c')], hour=1), _message(3, hour=2)]
    async with serving(stub) as tc:
        assert await _client(tc).save_messages(GUILD, CHANNEL, batch) == 3
    (sent,) = stub.calls_to(ROUTES['save_messages'])
    assert [message['last_message_id'] for message in sent['messages']] == [1, 2, 3]
    assert sent['messages'][2]['word_pairs'] == []
    assert sent['messages'][0]['word_pairs'] == [['a', 'b']]


@pytest.mark.asyncio
async def test_saving_to_a_channel_that_is_gone_is_none():
    '''A null count means the channel is no longer tracked, not that nothing was written.'''
    stub = answering(ROUTES['save_messages'], None)
    async with serving(stub) as tc:
        assert await _client(tc).save_messages(GUILD, CHANNEL, [_message(1)]) is None


@pytest.mark.asyncio
async def test_a_sentence_with_no_seed_sends_an_explicit_null():
    '''`!markov speak` with no seed is first_word null, and a seed is sent as given.'''
    stub = answering(ROUTES['generate_words'], ['a', 'b'])
    async with serving(stub) as tc:
        assert await _client(tc).generate_words(GUILD, 32) == ['a', 'b']
        await _client(tc).generate_words(GUILD, 32, first_word='seed')
    assert stub.calls_to(ROUTES['generate_words']) == [
        {'guild_id': GUILD, 'count': 32, 'first_word': None},
        {'guild_id': GUILD, 'count': 32, 'first_word': 'seed'}]


@pytest.mark.asyncio
async def test_the_prune_cutoff_keeps_its_utc_offset():
    '''A cutoff with an offset arrives at the same instant, not shifted to the pod's local time.'''
    cutoff = datetime(2026, 3, 1, 12, 0, tzinfo=timezone(timedelta(hours=-8)))
    stub = answering(ROUTES['prune_relations_before'], True)
    async with serving(stub) as tc:
        assert await _client(tc).prune_relations_before(cutoff) is True
    (sent,) = stub.calls_to(ROUTES['prune_relations_before'])
    assert datetime.fromisoformat(sent['cutoff']) == cutoff


@pytest.mark.asyncio
async def test_a_failed_store_is_unavailable_and_asked_once():
    '''The pod's error body is re-raised as DatabaseUnavailable, and 200 means it is not retried.'''
    stub = failing(ROUTES['add_channel'])
    async with serving(stub) as tc:
        with pytest.raises(DatabaseUnavailable):
            await _client(tc).add_channel(GUILD, CHANNEL)
    assert len(stub.calls_to(ROUTES['add_channel'])) == 1
