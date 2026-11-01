from unittest.mock import AsyncMock

import pytest

from discord import ChannelType

from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException

from discord_gateway.cogs.markov import Markov

from tests.helpers import fake_context #pylint:disable=unused-import

CONFIG = {
    'general': {
        'commands_http_url': 'http://commands:8086',
        'include': {
            'markov': True
        }
    },
}


def _cog(fake_context):  #pylint:disable=redefined-outer-name
    cog = Markov(fake_context['bot'], CONFIG, fake_context['dispatcher'])
    cog.commands_client = AsyncMock()
    return cog


def test_markov_no_enabled(fake_context):  #pylint:disable=redefined-outer-name
    with pytest.raises(CogMissingRequiredArg) as exc:
        Markov(fake_context['bot'], {}, fake_context['dispatcher'])
    assert 'Markov cog not enabled' in str(exc.value)


def test_markov_needs_the_commands_pod_url(fake_context):  #pylint:disable=redefined-outer-name
    with pytest.raises(DiscordBotException, match='commands_http_url'):
        Markov(fake_context['bot'], {'general': {'include': {'markov': True}}}, fake_context['dispatcher'])


def test_markov_needs_no_store(fake_context):  #pylint:disable=redefined-outer-name
    assert Markov(fake_context['bot'], CONFIG, fake_context['dispatcher'], None) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('method,command,args', [
    ('off', 'markov.off', {}),
    ('list_channels', 'markov.list-channels', {}),
    ('speak', 'markov.speak', {'first_word': 'hey', 'sentence_length': 8}),
])
async def test_subcommands_forward_to_the_commands_pod(fake_context, method, command, args):  #pylint:disable=redefined-outer-name
    cog = _cog(fake_context)
    await getattr(cog, method)(cog, fake_context['context'], **args)
    cog.commands_client.forward.assert_awaited_once()
    forwarded = cog.commands_client.forward.await_args
    assert forwarded.args[0] == command
    assert forwarded.args[2] == args
    assert forwarded.args[1].guild_id == fake_context['guild'].id


@pytest.mark.asyncio
@pytest.mark.parametrize('channel_type,expected', [
    (ChannelType.text, True),
    (ChannelType.voice, True),
    (ChannelType.forum, False),
])
async def test_on_sends_whether_the_channel_can_run_markov(fake_context, channel_type, expected):  #pylint:disable=redefined-outer-name
    cog = _cog(fake_context)
    fake_context['context'].channel.type = channel_type
    await cog.on(cog, fake_context['context'])
    forwarded = cog.commands_client.forward.await_args
    assert forwarded.args[0] == 'markov.on'
    assert forwarded.args[2] == {'channel_is_text_or_voice': expected}


@pytest.mark.asyncio
async def test_markov_group_no_subcommand(fake_context):  #pylint:disable=redefined-outer-name
    cog = _cog(fake_context)
    fake_context['context'].invoked_subcommand = None
    await cog.markov(cog, fake_context['context'])  #pylint:disable=too-many-function-args
    assert 'Invalid sub command passed...' in fake_context['context'].messages_sent
    cog.commands_client.forward.assert_not_awaited()
