from datetime import datetime, timezone
from freezegun import freeze_time
import pytest

from discord_bot.cogs.delete_messages import DeleteMessages
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import fake_context #pylint:disable=unused-import
from tests.helpers import FakeMessage
BASE_CONFIG = {
    'general': {
        'include': {
            'delete_messages': True
        }
    }
}

def test_delete_messages_start_failed(fake_context):  #pylint:disable=redefined-outer-name
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
    with pytest.raises(CogMissingRequiredArg) as exc:
        DeleteMessages(fake_context['bot'], config, None)
    assert 'Delete messages not enabled' in str(exc.value)

def test_delete_messages_requires_config(fake_context):  #pylint:disable=redefined-outer-name
    '''
    Test delete message fails when required args not there
    '''
    with pytest.raises(CogMissingRequiredArg) as exc:
        DeleteMessages(fake_context['bot'], BASE_CONFIG, None)
    assert 'Invalid config given' in str(exc.value)

def test_delete_messages_start_config(fake_context):  #pylint:disable=redefined-outer-name
    '''
    Test basic config starts up
    '''
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {
                    'server_id': 'fake-guild-123',
                    'channel_id': 'fake-channel-123'
                },
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    assert cog.loop_sleep_interval == 5
    assert cog.discord_channels == [{'server_id': 'fake-guild-123', 'channel_id': 'fake-channel-123'}]

@pytest.mark.asyncio
@freeze_time('2025-12-01 12:00:00', tz_offset=0)
async def test_delete_messages_main_loop(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(author=fake_context['author'], channel=fake_context['channel'], created_at=datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {
                    'server_id': fake_context['guild'].id,
                    'channel_id': fake_context['channel'].id,
                },
            ]
        }
    } | BASE_CONFIG
    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fake_context['bot'], config, None)
    await cog.delete_messages_loop()
    # Message should be deleted and removed from channel
    assert len(fake_context['channel'].messages) == 0
    assert fake_message.deleted is True

@pytest.mark.asyncio
@freeze_time('2024-01-01 12:00:00', tz_offset=0)
async def test_delete_messages_main_loop_no_delete(mocker, fake_context):  #pylint:disable=redefined-outer-name
    fake_message = FakeMessage(channel=fake_context['channel'], author=fake_context['author'], created_at=datetime(2024, 1, 1, 11, 59, 0, tzinfo=timezone.utc))
    fake_context['channel'].messages = [fake_message]
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {
                    'server_id': fake_context['guild'].id,
                    'channel_id': fake_context['channel'].id,
                    'delete_after': 7,
                },
            ]
        }
    } | BASE_CONFIG
    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fake_context['bot'], config, None)
    await cog.delete_messages_loop()
    assert fake_context['channel'].messages[0].deleted is False
