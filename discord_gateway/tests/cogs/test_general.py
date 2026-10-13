import pytest

from discord_core.exceptions import CogMissingRequiredArg

from discord_gateway.cogs.general import General

from tests.helpers import fake_context #pylint:disable=unused-import

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

@pytest.mark.asyncio
async def test_general_stubs_delegate(fake_context):  #pylint:disable=redefined-outer-name
    '''Each stub builds the envelope from ctx and returns what the logic replied'''
    cog = General(fake_context['bot'], {}, fake_context['dispatcher'])
    ctx = fake_context['context']
    assert await cog.hello(cog, ctx) == f'Waddup {fake_context["author"].display_name}' #pylint:disable=too-many-function-args
    assert 'rolled' in await cog.roll(cog, ctx, input_value='3d6') #pylint:disable=too-many-function-args
    assert await cog.meta(cog, ctx) == f'```Server id: {fake_context["guild"].id}\nChannel id: {fake_context["channel"].id}\nUser id: {fake_context["author"].id}```' #pylint:disable=too-many-function-args
