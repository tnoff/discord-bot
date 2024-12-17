import asyncio
import logging
import pytest

from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.exceptions import CogMissingRequiredArg

from tests.data.urban_data import HTML_DATA
from tests.helpers import fake_bot_yielder, FakeContext

def test_delete_messages_start_failed():
    config = {
        'general': {
            'include': {
                'delete_messages': False
            }
        }
    }
    fake_bot = fake_bot_yielder()()
    with pytest.raises(CogMissingRequiredArg) as exc:
        DeleteMessages(fake_bot, logging, config, None)
    assert 'Delete messages not enabled' in str(exc.value)

def test_delete_messages_start():
    config = {
        'general': {
            'include': {
                'delete_messages': True
            }
        }
    }
    fake_bot = fake_bot_yielder()()
    cog = DeleteMessages(fake_bot, logging, config, None)
    assert cog.loop_sleep_interval is not None
    assert cog.discord_channels == []

def test_delete_messages_start_config():
    config = {
        'general': {
            'include': {
                'delete_messages': True
            }
        },
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {
                    'server_id': 'fake-guild-123',
                    'channel_id': 'fake-channel-123'
                },
            ]
        }
    }
    fake_bot = fake_bot_yielder()()
    cog = DeleteMessages(fake_bot, logging, config, None)
    assert cog.loop_sleep_interval == 5
    assert cog.discord_channels == [{'server_id': 'fake-guild-123', 'channel_id': 'fake-channel-123'}]

@pytest.mark.asyncio
async def test_delete_messages_main_loop():
    config = {
        'general': {
            'include': {
                'delete_messages': True
            }
        },
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {
                    'server_id': 'fake-guild-123',
                    'channel_id': 'fake-channel-123'
                },
            ]
        }
    }
    fake_bot = fake_bot_yielder()()
    print('Fake bot', fake_bot)
    print('Fake bot loop', fake_bot.loop)
    cog = DeleteMessages(fake_bot, logging, config, None)
    await cog.cog_load()
    assert cog._task is not None

    await asyncio.sleep(.01)
    await cog.cog_unload()
    assert True == False
