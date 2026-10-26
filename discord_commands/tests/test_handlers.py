import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.types.command_args import COMMAND_ARGS
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import parse_args

from discord_commands.command_logic.playlist import PlaylistCommands
from discord_commands.handlers import build_handlers

ENVELOPE = CommandEnvelope(guild_id=11, channel_id=22, author_id=33, author_display_name='Tyler')


@pytest.fixture(name='sent')
def fixture_sent():
    return []


class FakeMarkovStore:
    '''Just enough store for the handlers to be seen reaching it; the logic has its own tests.'''
    def __init__(self):
        self.channels = set()
        self.generated = []

    async def get_channel(self, guild_id, channel_id):
        return (guild_id, channel_id) if (guild_id, channel_id) in self.channels else None

    async def add_channel(self, guild_id, channel_id):
        self.channels.add((guild_id, channel_id))

    async def remove_channel(self, guild_id, channel_id):
        removed = (guild_id, channel_id) in self.channels
        self.channels.discard((guild_id, channel_id))
        return removed

    async def list_guild_channel_ids(self, guild_id):
        return [channel for guild, channel in self.channels if guild == guild_id]

    async def generate_words(self, guild_id, length, first_word=None):
        self.generated.append((guild_id, length, first_word))
        return ['one', 'two']


@pytest.fixture(name='store')
def fixture_store():
    return FakeMarkovStore()


@pytest.fixture(name='handlers')
def fixture_handlers(sent, store):
    async def reply(guild_id, channel_id, content):
        sent.append((guild_id, channel_id, content))
        return content
    return build_handlers(reply, logging.getLogger('test'), store, [], MagicMock(), 300, 64)


def test_the_general_urban_and_markov_commands_are_served(handlers):
    assert set(handlers) == {'hello', 'roll', 'meta', 'urban',
                             'markov.on', 'markov.off', 'markov.list-channels', 'markov.speak',
                             'playlist.create', 'playlist.list', 'playlist.item-remove', 'playlist.show',
                             'playlist.delete', 'playlist.rename', 'playlist.merge'}


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


@pytest.mark.asyncio
async def test_markov_on_passes_the_gateways_channel_check_through(handlers, sent, store):
    await handlers['markov.on'](ENVELOPE, parse_args('markov.on', {'channel_is_text_or_voice': False}))
    assert not store.channels
    await handlers['markov.on'](ENVELOPE, parse_args('markov.on', {'channel_is_text_or_voice': True}))
    assert store.channels == {(11, 22)}
    assert [content for _, _, content in sent] == [
        'Not a valid markov channel, cannot turn on markov', 'Markov turned on for channel']


@pytest.mark.asyncio
async def test_markov_off_and_list_channels(handlers, sent, store):
    store.channels.add((11, 22))
    await handlers['markov.list-channels'](ENVELOPE, parse_args('markov.list-channels', {}))
    await handlers['markov.off'](ENVELOPE, parse_args('markov.off', {}))
    assert not store.channels
    assert sent[-1] == (11, 22, 'Markov turned off for channel')
    assert 'Channel List' in sent[0][2]


@pytest.mark.asyncio
async def test_markov_speak_passes_its_arguments_through(handlers, sent, store):
    await handlers['markov.speak'](ENVELOPE, parse_args('markov.speak', {'first_word': 'Hey up', 'sentence_length': 5}))
    assert store.generated == [(11, 4, 'up')]
    assert sent == [(11, 22, 'hey one two')]


@pytest.mark.asyncio
@pytest.mark.parametrize('command,wire,method,call_args', [
    ('playlist.create', {'name': 'mine'}, 'create', ('mine',)),
    ('playlist.list', {}, 'list_playlists', ()),
    ('playlist.item-remove', {'playlist_index': 3, 'video_index': 4}, 'item_remove', (3, 4)),
    ('playlist.show', {'playlist_index': 3}, 'show', (3,)),
    ('playlist.delete', {'playlist_index': 3}, 'delete', (3,)),
    ('playlist.rename', {'playlist_index': 3, 'playlist_name': 'new'}, 'rename', (3, 'new')),
    ('playlist.merge', {'playlist_index_one': '1', 'playlist_index_two': '2'}, 'merge', ('1', '2')),
])
async def test_playlist_commands_pass_their_arguments_through(handlers, mocker, command, wire, method, call_args):
    logic = mocker.patch.object(PlaylistCommands, method, new_callable=AsyncMock)
    await handlers[command](ENVELOPE, parse_args(command, wire))
    logic.assert_awaited_once_with(ENVELOPE, *call_args)
