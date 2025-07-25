import pytest

from discord_bot.cogs.general import General
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import generate_fake_context

def test_general_startup_not_enabled():
    config = {
        'general': {
            'include': {
                'default': False
            }
        }
    }
    fakes = generate_fake_context()
    with pytest.raises(CogMissingRequiredArg) as exc:
        General(fakes['bot'], config, None)
    assert 'Default cog not enabled' in str(exc.value)

@pytest.mark.asyncio
async def test_hello():
    fakes = generate_fake_context()
    cog = General(fakes['bot'], {}, None)
    result = await cog.hello(cog, fakes['context']) #pylint:disable=too-many-function-args
    assert result == f'Waddup {fakes["author"].display_name}'

@pytest.mark.asyncio
async def test_roll(mocker):
    fakes = generate_fake_context()
    cog = General(fakes['bot'], {}, None)
    mocker.patch('discord_bot.cogs.general.randint', return_value=3)
    result = await cog.roll(cog, fakes['context'], input_value='5') #pylint:disable=too-many-function-args
    assert result == f'{fakes["author"].display_name} rolled a 3'
    result = await cog.roll(cog, fakes['context'], input_value='d5') #pylint:disable=too-many-function-args
    assert result == f'{fakes["author"].display_name} rolled a 3'
    result = await cog.roll(cog, fakes['context'], input_value='3d10') #pylint:disable=too-many-function-args
    assert result == f'{fakes["author"].display_name} rolled: 3 + 3 + 3 = 9'

@pytest.mark.asyncio
async def test_roll_invalid_input(mocker):
    fakes = generate_fake_context()
    cog = General(fakes['bot'], {}, None)
    mocker.patch('discord_bot.cogs.general.randint', return_value=3)
    result = await cog.roll(cog, fakes['context'], input_value='foo') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given "foo"'
    result = await cog.roll(cog, fakes['context'], input_value='21d5') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given, max rolls is 20 but "21" given'
    result = await cog.roll(cog, fakes['context'], input_value='3d101') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given, max sides is 100 but "101" given'

@pytest.mark.asyncio
async def test_meta():
    fakes = generate_fake_context()
    cog = General(fakes['bot'], {}, None)
    result = await cog.meta(cog, fakes['context']) #pylint:disable=too-many-function-args
    assert result == f'```Server id: {fakes["guild"].id}\nChannel id: {fakes["channel"].id}\nUser id: {fakes["author"].id}```'
