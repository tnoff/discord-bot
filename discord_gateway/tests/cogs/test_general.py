from unittest.mock import AsyncMock

import pytest

from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException

from discord_gateway.cogs.common import envelope_from_ctx
from discord_gateway.cogs.general import General

from tests.helpers import fake_context #pylint:disable=unused-import

CONFIG = {'general': {'commands_http_url': 'http://commands:8086'}}


def test_general_startup_not_enabled(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'default': False
            }
        }
    }
    with pytest.raises(CogMissingRequiredArg) as exc:
        General(fake_context['bot'], config, None)
    assert 'Default cog not enabled' in str(exc.value)


def test_general_requires_the_commands_url(fake_context):  #pylint:disable=redefined-outer-name
    with pytest.raises(DiscordBotException, match='commands_http_url'):
        General(fake_context['bot'], {}, fake_context['dispatcher'])


@pytest.mark.asyncio
async def test_general_stubs_forward(fake_context):  #pylint:disable=redefined-outer-name
    '''Each stub forwards its command and parsed arguments, and runs nothing itself'''
    cog = General(fake_context['bot'], CONFIG, fake_context['dispatcher'])
    cog.commands_client = AsyncMock()
    ctx = fake_context['context']
    await cog.hello(cog, ctx) #pylint:disable=too-many-function-args
    await cog.roll(cog, ctx, input_value='3d6') #pylint:disable=too-many-function-args
    await cog.meta(cog, ctx) #pylint:disable=too-many-function-args
    calls = [(c.args[0], c.args[1], c.args[2]) for c in cog.commands_client.forward.await_args_list]
    envelope = envelope_from_ctx(ctx)
    assert calls == [('hello', envelope, {}), ('roll', envelope, {'input_value': '3d6'}), ('meta', envelope, {})]
    assert not fake_context['channel'].messages_sent
