import logging
from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from discord_db.database import MarkovChannel, MarkovRelation

from discord_commands.command_logic.markov import MarkovCommands

from tests.helpers import fake_engine, fake_markov_store #pylint:disable=unused-import
from tests.helpers import async_mock_session


def _logic(reply, fake_markov_store, reject_list=None):  #pylint:disable=redefined-outer-name
    return MarkovCommands(reply, logging.getLogger('test'), fake_markov_store, reject_list or [])


async def _add_relations(fake_engine, pairs):  #pylint:disable=redefined-outer-name
    '''Write leader -> follower relations for the (only) tracked channel, which must already be on.'''
    async with async_mock_session(fake_engine) as session:
        channel = (await session.execute(select(MarkovChannel))).scalars().first()
        for leader, follower in pairs:
            session.add(MarkovRelation(channel_id=channel.id, leader_word=leader, follower_word=follower,
                                       created_at=datetime(2024, 11, 30, 0, 0, 0, tzinfo=timezone.utc)))
        await session.commit()


@pytest.mark.asyncio
async def test_turn_on(reply, sent, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''on enables markov for the channel, and says so if it already is'''
    logic = _logic(reply, fake_markov_store)
    assert await logic.on(envelope, True) == 'Markov turned on for channel'
    assert await fake_markov_store.get_channel(envelope.guild_id, envelope.channel_id)
    assert await logic.on(envelope, True) == 'Channel already has markov turned on'
    assert [content for _, _, content in sent] == ['Markov turned on for channel', 'Channel already has markov turned on']


@pytest.mark.asyncio
async def test_turn_on_invalid_channel(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''on rejects a channel the gateway says is not text or voice, and stores nothing'''
    result = await _logic(reply, fake_markov_store).on(envelope, False)
    assert result == 'Not a valid markov channel, cannot turn on markov'
    assert not await fake_markov_store.get_channel(envelope.guild_id, envelope.channel_id)


@pytest.mark.asyncio
async def test_already_on_is_reported_even_for_an_invalid_channel(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''The tracked check comes first, as it did when the channel was fetched'''
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    assert await logic.on(envelope, False) == 'Channel already has markov turned on'


@pytest.mark.asyncio
async def test_server_reject_list(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''on and speak are blocked for servers in the reject list'''
    logic = _logic(reply, fake_markov_store, [envelope.guild_id])
    assert await logic.on(envelope, True) == 'Unable to turn on markov for server, in reject list'
    assert await logic.speak(envelope) == 'Unable to use markov for server, in reject list'


@pytest.mark.asyncio
async def test_turn_off(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''off reports a channel that was not on'''
    assert await _logic(reply, fake_markov_store).off(envelope) == 'Channel does not have markov turned on'


@pytest.mark.asyncio
async def test_turn_on_and_off(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    assert await logic.off(envelope) == 'Markov turned off for channel'
    assert not await fake_markov_store.get_channel(envelope.guild_id, envelope.channel_id)


@pytest.mark.asyncio
async def test_speak_follows_the_corpus(reply, fake_engine, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''Every word has one follower and the corpus loops, so the chain is a fixed cycle from first_word'''
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    await _add_relations(fake_engine, [('alpha', 'bravo'), ('bravo', 'charlie'),
                                                 ('charlie', 'delta'), ('delta', 'alpha')])
    result = await logic.speak(envelope, 'alpha')
    assert result == ' '.join(['alpha', 'bravo', 'charlie', 'delta'] * 8)


@pytest.mark.asyncio
async def test_speak_respects_sentence_length(reply, fake_engine, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    await _add_relations(fake_engine, [('alpha', 'bravo'), ('bravo', 'alpha')])
    assert len((await logic.speak(envelope, sentence_length=5)).split(' ')) == 5


@pytest.mark.asyncio
async def test_speak_with_a_multi_word_prefix_ends_on_the_last_word(reply, fake_engine, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''Earlier words are kept as given, the last one seeds the chain, and the prefix counts toward the length'''
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    await _add_relations(fake_engine, [('alpha', 'bravo'), ('bravo', 'alpha')])
    result = await logic.speak(envelope, 'Hey WHATS alpha', 6)
    assert result.split(' ')[:3] == ['hey', 'whats', 'alpha']
    assert len(result.split(' ')) == 6


@pytest.mark.asyncio
async def test_speak_invalid_first_word(reply, fake_engine, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    await _add_relations(fake_engine, [('alpha', 'bravo')])
    assert await logic.speak(envelope, 'non-existing') == 'No markov word matching "non-existing"'


@pytest.mark.asyncio
async def test_speak_no_words(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    assert await logic.speak(envelope, sentence_length=5) == 'No markov words to pick from'


@pytest.mark.asyncio
async def test_speak_stops_at_a_dead_end_instead_of_raising(reply, fake_engine, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    '''Retention can delete every relation in which a word leads while keeping one where it follows'''
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    await _add_relations(fake_engine, [('alpha', 'omega')])
    assert await logic.speak(envelope, 'alpha', 10) == 'alpha omega'


@pytest.mark.asyncio
async def test_list_channels_none_on(reply, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    result = await _logic(reply, fake_markov_store).list_channels(envelope)
    assert result == 'Markov not enabled for any channels in server'


@pytest.mark.asyncio
async def test_list_channels_shows_enabled_channels_in_a_table(reply, sent, fake_markov_store, envelope):  #pylint:disable=redefined-outer-name
    logic = _logic(reply, fake_markov_store)
    await logic.on(envelope, True)
    sent.clear()
    assert await logic.list_channels(envelope) is True
    assert sent == [(11, 22, f'Channel List \nChannel\n-------\n<#{envelope.channel_id}>')]
