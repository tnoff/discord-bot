import logging

from freezegun import freeze_time
import pytest

from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import fake_bot_yielder, FakeChannel

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
@freeze_time('2024-12-01 12:00:00', tz_offset=0)
async def test_delete_messages_main_loop(mocker):
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
    fake_channel = FakeChannel()
    fake_bot = fake_bot_yielder(fake_channel=fake_channel)()
    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fake_bot, logging, config, None)
    await cog.delete_messages_loop()
    assert fake_channel.messages[0].deleted is True

@pytest.mark.asyncio
@freeze_time('2024-01-01 12:00:00', tz_offset=0)
async def test_delete_messages_main_loop_no_delete(mocker):
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
                    'channel_id': 'fake-channel-123',
                    'delete_after': 7,
                },
            ]
        }
    }
    fake_channel = FakeChannel()
    fake_bot = fake_bot_yielder(fake_channel=fake_channel)()
    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fake_bot, logging, config, None)
    await cog.delete_messages_loop()
    assert fake_channel.messages[0].deleted is False
