from unittest.mock import AsyncMock

import pytest

from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException

from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.cogs.urban import UrbanDictionary

from tests.helpers import  fake_context #pylint:disable=unused-import

URBAN_CONFIG = {
    'general': {
        'commands_http_url': 'http://commands:8086',
        'include': {
            'urban': True
        }
    }
}


def test_urban_dictionary_startup(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'urban': False
            }
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        UrbanDictionary(fake_context['bot'], config, fake_context['dispatcher'])
    assert 'Urban not enabled' in str(exc.value)


def test_urban_requires_the_commands_url(fake_context):  #pylint:disable=redefined-outer-name
    config = {'general': {'include': {'urban': True}}}
    with pytest.raises(DiscordBotException, match='commands_http_url'):
        UrbanDictionary(fake_context['bot'], config, fake_context['dispatcher'])


@pytest.mark.asyncio
async def test_urban_stub_forwards(fake_context):  #pylint:disable=redefined-outer-name
    '''The stub forwards the parsed word and looks nothing up itself'''
    cog = UrbanDictionary(fake_context['bot'], URBAN_CONFIG, fake_context['dispatcher'])
    cog.commands_client = AsyncMock()
    ctx = fake_context['context']
    await cog.word_lookup(cog, ctx, word='foo bar') #pylint:disable=too-many-function-args
    cog.commands_client.forward.assert_awaited_once()
    assert cog.commands_client.forward.await_args.args[:3] == ('urban', envelope_from_ctx(ctx), {'word': 'foo bar'})
    assert not fake_context['channel'].messages_sent
