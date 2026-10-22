import logging

import pytest

from discord_core.types.command_args import COMMAND_ARGS
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import parse_args

from discord_commands.handlers import build_handlers

ENVELOPE = CommandEnvelope(guild_id=11, channel_id=22, author_id=33, author_display_name='Tyler')


@pytest.fixture(name='sent')
def fixture_sent():
    return []


@pytest.fixture(name='handlers')
def fixture_handlers(sent):
    async def reply(guild_id, channel_id, content):
        sent.append((guild_id, channel_id, content))
        return content
    return build_handlers(reply, logging.getLogger('test'))


def test_the_general_and_urban_commands_are_served(handlers):
    assert set(handlers) == {'hello', 'roll', 'meta', 'urban'}


def test_every_handler_is_a_command_the_contract_knows(handlers):
    """Guards a typo'd name, which would never be called and never fail."""
    assert set(handlers) <= set(COMMAND_ARGS)


@pytest.mark.asyncio
async def test_hello_and_meta_replies(handlers, sent):
    await handlers['hello'](ENVELOPE, parse_args('hello', {}))
    await handlers['meta'](ENVELOPE, parse_args('meta', {}))
    assert sent == [
        (11, 22, 'Waddup Tyler'),
        (11, 22, '```Server id: 11\nChannel id: 22\nUser id: 33```'),
    ]


@pytest.mark.asyncio
async def test_roll_passes_the_input_value_through(handlers, sent):
    await handlers['roll'](ENVELOPE, parse_args('roll', {'input_value': '1d1'}))
    assert sent == [(11, 22, 'Tyler rolled a 1')]


@pytest.mark.asyncio
async def test_urban_passes_the_word_through(handlers, sent, requests_mock):
    requests_mock.get('https://www.urbandictionary.com/define.php?term=foo', status_code=500)
    await handlers['urban'](ENVELOPE, parse_args('urban', {'word': 'foo'}))
    assert sent == [(11, 22, 'Unable to lookup word "foo"')]
