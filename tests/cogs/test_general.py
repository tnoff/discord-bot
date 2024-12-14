import logging
import pytest

from discord_bot.cogs.general import General
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import fake_bot_yielder, FakeGuild

class FakeAuthor():
    def __init__(self):
        self.id = 'fake-user-id-123'
        self.name = 'fake-user-name-123'
        self.display_name = 'fake-display-name-123'

class FakeChannel():
    def __init__(self):
        self.id = 'fake-channel-id-123'

class FakeContext():
    def __init__(self):
        self.author = FakeAuthor()
        self.guild = FakeGuild()
        self.channel = FakeChannel()

    async def send(self, message):
        return message

def test_general_startup_not_enabled():
    config = {
        'include': {
            'default': False
        }
    }
    fake_bot = fake_bot_yielder()
    with pytest.raises(CogMissingRequiredArg) as exc:
        General(fake_bot, logging, config, None)
    assert 'Default cog not enabled' in str(exc.value)

@pytest.mark.asyncio
async def test_hello():
    fake_bot = fake_bot_yielder()
    cog = General(fake_bot, logging, {}, None)
    result = await cog.hello(None, FakeContext()) #pylint:disable=too-many-function-args
    assert result == 'Waddup fake-display-name-123'

@pytest.mark.asyncio
async def test_roll(mocker):
    fake_bot = fake_bot_yielder()
    cog = General(fake_bot, logging, {}, None)
    mocker.patch('discord_bot.cogs.general.randint', return_value=3)
    result = await cog.roll(None, FakeContext(), '5') #pylint:disable=too-many-function-args
    assert result == 'fake-user-name-123 rolled a 3'
    result = await cog.roll(None, FakeContext(), 'd5') #pylint:disable=too-many-function-args
    assert result == 'fake-user-name-123 rolled a 3'
    result = await cog.roll(None, FakeContext(), '3d10') #pylint:disable=too-many-function-args
    assert result == 'fake-user-name-123 rolled: 3 + 3 + 3 = 9'

@pytest.mark.asyncio
async def test_roll_invalid_input(mocker):
    fake_bot = fake_bot_yielder()
    cog = General(fake_bot, logging, {}, None)
    mocker.patch('discord_bot.cogs.general.randint', return_value=3)
    result = await cog.roll(None, FakeContext(), 'foo') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given "foo"'
    result = await cog.roll(None, FakeContext(), '21d5') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given, max rolls is 20 but "21" given'
    result = await cog.roll(None, FakeContext(), '3d101') #pylint:disable=too-many-function-args
    assert result == 'Invalid input given, max sides is 100 but "101" given'

@pytest.mark.asyncio
async def test_meta():
    fake_bot = fake_bot_yielder()
    cog = General(fake_bot, logging, {}, None)
    result = await cog.meta(None, FakeContext()) #pylint:disable=too-many-function-args
    assert result == '```Server id: fake-guild-1234\nChannel id: fake-channel-id-123\nUser id: fake-user-id-123```'
