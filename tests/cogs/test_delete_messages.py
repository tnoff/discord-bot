from datetime import datetime, timezone
from freezegun import freeze_time
import pytest

from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import generate_fake_context, FakeMessage

def test_delete_messages_start_failed():
    '''
    Make sure delete messages just doesnt when disabled
    '''
    config = {
        'general': {
            'include': {
                'delete_messages': False
            }
        }
    }
    fakes = generate_fake_context()
    with pytest.raises(CogMissingRequiredArg) as exc:
        DeleteMessages(fakes['bot'], config, None)
    assert 'Delete messages not enabled' in str(exc.value)

def test_delete_messages_requires_config():
    '''
    Test delete message fails when required args not there
    '''
    config = {
        'general': {
            'include': {
                'delete_messages': True
            }
        },
    }
    fakes = generate_fake_context()
    with pytest.raises(CogMissingRequiredArg) as exc:
        DeleteMessages(fakes['bot'], config, None)
    assert 'Invalid config given' in str(exc.value)

def test_delete_messages_start_config():
    '''
    Test basic config starts up
    '''
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
    fakes = generate_fake_context()
    cog = DeleteMessages(fakes['bot'], config, None)
    assert cog.loop_sleep_interval == 5
    assert cog.discord_channels == [{'server_id': 'fake-guild-123', 'channel_id': 'fake-channel-123'}]

@pytest.mark.asyncio
@freeze_time('2025-12-01 12:00:00', tz_offset=0)
async def test_delete_messages_main_loop(mocker):
    fakes = generate_fake_context()
    fake_message = FakeMessage(author=fakes['author'], channel=fakes['channel'], created_at=datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]
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
                    'server_id': fakes['guild'].id,
                    'channel_id': fakes['channel'].id,
                },
            ]
        }
    }

    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fakes['bot'], config, None)
    await cog.delete_messages_loop()
    assert fakes['channel'].messages[0].deleted is True

@pytest.mark.asyncio
@freeze_time('2024-01-01 12:00:00', tz_offset=0)
async def test_delete_messages_main_loop_no_delete(mocker):
    fakes = generate_fake_context()
    fake_message = FakeMessage(channel=fakes['channel'], author=fakes['author'], created_at=datetime(2024, 1, 1, 11, 59, 0, tzinfo=timezone.utc))
    fakes['channel'].messages = [fake_message]
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
                    'server_id': fakes['guild'].id,
                    'channel_id': fakes['channel'].id,
                    'delete_after': 7,
                },
            ]
        }
    }
    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fakes['bot'], config, None)
    await cog.delete_messages_loop()
    assert fakes['channel'].messages[0].deleted is False
