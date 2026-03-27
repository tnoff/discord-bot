import asyncio
from datetime import datetime, timezone
from unittest.mock import MagicMock

from freezegun import freeze_time
import pytest

from discord_bot.cogs.delete_messages import DeleteMessages, DELETE_AFTER_DEFAULT
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.dispatch_result import ChannelHistoryResult
from discord_bot.types.fetched_message import FetchedMessage

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
                    'server_id': 123456789012,
                    'channel_id': 987654321098
                },
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    assert cog.loop_sleep_interval == 5
    assert cog.discord_channels == [{'server_id': 123456789012, 'channel_id': 987654321098, 'delete_after': 7}]


_FULL_CONFIG = {
    'delete_messages': {
        'loop_sleep_interval': 5,
        'discord_channels': [
            {
                'server_id': 123456789012,
                'channel_id': 987654321098
            }
        ]
    }
} | BASE_CONFIG


@pytest.mark.asyncio
@freeze_time('2025-12-01 12:00:00', tz_offset=0)
async def test_delete_messages_process_result_deletes_old(fake_context):  #pylint:disable=redefined-outer-name
    '''_process_delete_result deletes messages older than delete_after days'''
    old_created_at = datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    fake_message = FakeMessage(channel=fake_context['channel'], created_at=old_created_at)
    fake_context['channel'].messages = [fake_message]
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {'server_id': fake_context['guild'].id, 'channel_id': fake_context['channel'].id},
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[FetchedMessage(
            id=fake_message.id,
            content=fake_message.content,
            created_at=old_created_at,
            author_bot=False,
        )],
    )
    await cog._process_delete_result(result)  #pylint:disable=protected-access
    assert fake_message.deleted is True


@pytest.mark.asyncio
@freeze_time('2024-01-01 12:00:00', tz_offset=0)
async def test_delete_messages_process_result_skips_recent(fake_context):  #pylint:disable=redefined-outer-name
    '''_process_delete_result skips messages newer than delete_after days'''
    recent_created_at = datetime(2024, 1, 1, 11, 59, 0, tzinfo=timezone.utc)
    fake_message = FakeMessage(channel=fake_context['channel'], created_at=recent_created_at)
    fake_context['channel'].messages = [fake_message]
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {'server_id': fake_context['guild'].id, 'channel_id': fake_context['channel'].id, 'delete_after': 7},
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[FetchedMessage(
            id=fake_message.id,
            content=fake_message.content,
            created_at=recent_created_at,
            author_bot=False,
        )],
    )
    await cog._process_delete_result(result)  #pylint:disable=protected-access
    assert fake_message.deleted is False


@pytest.mark.asyncio
async def test_delete_messages_process_result_handles_error(fake_context):  #pylint:disable=redefined-outer-name
    '''_process_delete_result logs an error and returns when result has an error'''
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {'server_id': fake_context['guild'].id, 'channel_id': fake_context['channel'].id},
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    result = ChannelHistoryResult(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[],
        error=Exception('test error'),
    )
    # Should not raise
    await cog._process_delete_result(result)  #pylint:disable=protected-access


def test_get_channel_config_returns_config(fake_context):  #pylint:disable=redefined-outer-name
    '''_get_channel_config returns the config dict for a known channel_id'''
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {'server_id': fake_context['guild'].id, 'channel_id': fake_context['channel'].id, 'delete_after': 14},
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    result = cog._get_channel_config(fake_context['channel'].id)  #pylint:disable=protected-access
    assert result['delete_after'] == 14


def test_get_channel_config_returns_empty_for_unknown(fake_context):  #pylint:disable=redefined-outer-name
    '''_get_channel_config returns empty dict for an unknown channel_id'''
    cog = DeleteMessages(fake_context['bot'], _FULL_CONFIG, None)
    result = cog._get_channel_config(999999)  #pylint:disable=protected-access
    assert result == {}
    assert result.get('delete_after', DELETE_AFTER_DEFAULT) == DELETE_AFTER_DEFAULT


def test_loop_active_callback_task_none(fake_context):  #pylint:disable=redefined-outer-name
    '''__loop_active_callback returns 0 when _task is None'''
    cog = DeleteMessages(fake_context['bot'], _FULL_CONFIG, None)
    # _task is None by default after __init__
    result = getattr(cog, '_DeleteMessages__loop_active_callback')(None)
    assert result[0].value == 0


def test_loop_active_callback_task_running(fake_context):  #pylint:disable=redefined-outer-name
    '''__loop_active_callback returns 1 when _task exists and is not done'''
    cog = DeleteMessages(fake_context['bot'], _FULL_CONFIG, None)
    fake_task = MagicMock()
    fake_task.done.return_value = False
    setattr(cog, '_task', fake_task)
    result = getattr(cog, '_DeleteMessages__loop_active_callback')(None)
    assert result[0].value == 1


@pytest.mark.asyncio
async def test_cog_load_creates_task(fake_context):  #pylint:disable=redefined-outer-name
    '''cog_load assigns tasks to self._task and self._result_task'''
    cog = DeleteMessages(fake_context['bot'], _FULL_CONFIG, None)
    fake_task = MagicMock()
    fake_loop = MagicMock()
    fake_loop.create_task.return_value = fake_task
    fake_context['bot'].loop = fake_loop
    await cog.cog_load()
    assert getattr(cog, '_task') is fake_task
    assert getattr(cog, '_result_task') is fake_task


@pytest.mark.asyncio
async def test_cog_unload_cancels_task(fake_context):  #pylint:disable=redefined-outer-name
    '''cog_unload cancels both _task and _result_task when they exist'''
    cog = DeleteMessages(fake_context['bot'], _FULL_CONFIG, None)
    fake_task = MagicMock()
    fake_result_task = MagicMock()
    setattr(cog, '_task', fake_task)
    setattr(cog, '_result_task', fake_result_task)
    await cog.cog_unload()
    fake_task.cancel.assert_called_once()
    fake_result_task.cancel.assert_called_once()


@pytest.mark.asyncio
async def test_cog_unload_handles_none_task(fake_context):  #pylint:disable=redefined-outer-name
    '''cog_unload does not raise when _task is None'''
    cog = DeleteMessages(fake_context['bot'], _FULL_CONFIG, None)
    # _task is already None by default; ensure cog_unload handles it gracefully
    await cog.cog_unload()


# ---------------------------------------------------------------------------
# _delete_request_loop: producer submits requests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_request_loop_submits_history_request(mocker, fake_context):  #pylint:disable=redefined-outer-name
    '''_delete_request_loop dispatches a history fetch for each configured channel'''
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {'server_id': fake_context['guild'].id, 'channel_id': fake_context['channel'].id},
            ]
        }
    } | BASE_CONFIG
    mocker.patch('discord_bot.cogs.delete_messages.sleep', return_value=True)
    cog = DeleteMessages(fake_context['bot'], config, None)
    cog.register_result_queue()
    await cog._delete_request_loop()  #pylint:disable=protected-access
    assert not cog._result_queue.empty()  #pylint:disable=protected-access


# ---------------------------------------------------------------------------
# _delete_result_loop: consumer processes queued results
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_result_loop_deletes_old_message(fake_context):  #pylint:disable=redefined-outer-name
    '''_delete_result_loop reads a result from the queue and deletes old messages'''
    old_created_at = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    fake_message = FakeMessage(channel=fake_context['channel'], created_at=old_created_at)
    fake_context['channel'].messages = [fake_message]
    config = {
        'delete_messages': {
            'loop_sleep_interval': 5,
            'discord_channels': [
                {'server_id': fake_context['guild'].id, 'channel_id': fake_context['channel'].id},
            ]
        }
    } | BASE_CONFIG
    cog = DeleteMessages(fake_context['bot'], config, None)
    cog.register_result_queue()
    cog._result_queue.put_nowait(ChannelHistoryResult(  #pylint:disable=protected-access
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        messages=[FetchedMessage(
            id=fake_message.id,
            content=fake_message.content,
            created_at=old_created_at,
            author_bot=False,
        )],
    ))
    task = asyncio.create_task(cog._delete_result_loop())  #pylint:disable=protected-access
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert fake_message.deleted is True
