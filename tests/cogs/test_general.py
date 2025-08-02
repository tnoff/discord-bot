import pytest

from discord_bot.cogs.general import General
from discord_bot.exceptions import CogMissingRequiredArg

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
async def test_hello(fake_context):  #pylint:disable=redefined-outer-name
    cog = General(fake_context['bot'], {}, None)
    result = await cog.hello(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert result == f'Waddup {fake_context["author"].display_name}'

@pytest.mark.asyncio
async def test_roll(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = General(fake_context['bot'], {}, None)
    mocker.patch('discord_bot.cogs.general.randint', return_value=3)
    result = await cog.roll(cog, fake_context['context'], input_value='5') #pylint:disable=too-many-function-args
    assert result == f'{fake_context["author"].display_name} rolled a 3'
    result = await cog.roll(cog, fake_context['context'], input_value='d5') #pylint:disable=too-many-function-args
    assert result == f'{fake_context["author"].display_name} rolled a 3'
    result = await cog.roll(cog, fake_context['context'], input_value='3d10') #pylint:disable=too-many-function-args
    assert result == f'{fake_context["author"].display_name} rolled: 3 + 3 + 3 = 9'

@pytest.mark.asyncio
async def test_roll_invalid_input(mocker, fake_context):  #pylint:disable=redefined-outer-name
    cog = General(fake_context['bot'], {}, None)
    mocker.patch('discord_bot.cogs.general.randint', return_value=3)
    result = await cog.roll(cog, fake_context['context'], input_value='foo') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given "foo"'
    result = await cog.roll(cog, fake_context['context'], input_value='21d5') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given, max rolls is 20 but "21" given'
    result = await cog.roll(cog, fake_context['context'], input_value='3d101') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given, max sides is 100 but "101" given'

@pytest.mark.asyncio
async def test_meta(fake_context):  #pylint:disable=redefined-outer-name
    cog = General(fake_context['bot'], {}, None)
    result = await cog.meta(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert result == f'```Server id: {fake_context["guild"].id}\nChannel id: {fake_context["channel"].id}\nUser id: {fake_context["author"].id}```'
