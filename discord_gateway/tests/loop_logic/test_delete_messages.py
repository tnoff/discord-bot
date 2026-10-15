from datetime import datetime, timezone
from logging import getLogger

from freezegun import freeze_time
import pytest

from discord_core.types.dispatch_result import ChannelHistoryResult
from discord_core.types.fetched_message import FetchedMessage

from discord_gateway.loop_logic.delete_messages import DELETE_AFTER_DEFAULT, DeleteMessagesLoop

GUILD = 11
CHANNEL = 22


class Recorder:
    '''Stands in for the dispatcher: records the history requests and deletes.'''

    def __init__(self):
        self.history_requests = []
        self.deleted = []

    async def request_history(self, guild_id, channel_id):
        self.history_requests.append((guild_id, channel_id))

    async def delete_message(self, guild_id, channel_id, message_id):
        self.deleted.append((guild_id, channel_id, message_id))


def make_loop(channels=None, recorder=None):
    recorder = recorder or Recorder()
    channels = channels if channels is not None else [{'server_id': GUILD, 'channel_id': CHANNEL, 'delete_after': 7}]
    loop = DeleteMessagesLoop(getLogger('test'), channels, 5, recorder.request_history, recorder.delete_message)
    return loop, recorder


def history(created_at, error=None, message_id=99):
    messages = [] if error else [FetchedMessage(id=message_id, content='hi', created_at=created_at, author_bot=False)]
    return ChannelHistoryResult(guild_id=GUILD, channel_id=CHANNEL, messages=messages, error=error)


@pytest.mark.asyncio
@freeze_time('2025-12-01 12:00:00', tz_offset=0)
async def test_process_result_deletes_old():
    '''Messages older than delete_after days are deleted.'''
    loop, recorder = make_loop()
    await loop.process_result(history(datetime(2024, 12, 31, tzinfo=timezone.utc)))
    assert recorder.deleted == [(GUILD, CHANNEL, 99)]


@pytest.mark.asyncio
@freeze_time('2024-01-01 12:00:00', tz_offset=0)
async def test_process_result_skips_recent():
    '''Messages newer than delete_after days are left alone.'''
    loop, recorder = make_loop()
    await loop.process_result(history(datetime(2024, 1, 1, 11, 59, tzinfo=timezone.utc)))
    assert recorder.deleted == []


@pytest.mark.asyncio
@freeze_time('2024-01-10 12:00:00', tz_offset=0)
async def test_process_result_uses_the_channel_delete_after():
    '''Nine days old is old for a 7 day channel and recent for a 14 day one.'''
    created_at = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    loop, recorder = make_loop()
    await loop.process_result(history(created_at))
    assert len(recorder.deleted) == 1
    loop, recorder = make_loop([{'server_id': GUILD, 'channel_id': CHANNEL, 'delete_after': 14}])
    await loop.process_result(history(created_at))
    assert recorder.deleted == []


@pytest.mark.asyncio
@freeze_time('2024-01-10 12:00:00', tz_offset=0)
async def test_process_result_falls_back_to_the_default_for_an_unconfigured_channel():
    loop, recorder = make_loop([])
    await loop.process_result(history(datetime(2024, 1, 10 - DELETE_AFTER_DEFAULT - 1, tzinfo=timezone.utc)))
    assert len(recorder.deleted) == 1


@pytest.mark.asyncio
async def test_process_result_logs_an_error_result_and_deletes_nothing(mocker):
    loop, recorder = make_loop()
    loop.logger = mocker.MagicMock()
    await loop.process_result(history(None, error=Exception('test error')))
    assert recorder.deleted == []
    assert loop.logger.error.called


@pytest.mark.asyncio
async def test_request_asks_for_each_channel_after_the_sleep(mocker):
    sleep = mocker.patch('discord_gateway.loop_logic.delete_messages.sleep')
    channels = [{'server_id': GUILD, 'channel_id': CHANNEL}, {'server_id': 33, 'channel_id': 44}]
    loop, recorder = make_loop(channels)
    await loop.request()
    sleep.assert_awaited_once_with(5)
    assert recorder.history_requests == [(GUILD, CHANNEL), (33, 44)]


def test_get_channel_config():
    loop, _ = make_loop([{'server_id': GUILD, 'channel_id': CHANNEL, 'delete_after': 14}])
    assert loop.get_channel_config(CHANNEL)['delete_after'] == 14
    assert loop.get_channel_config(999999) == {}
