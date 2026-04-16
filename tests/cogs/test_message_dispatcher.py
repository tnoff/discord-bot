import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock

import fakeredis.aioredis
import pytest

from discord.errors import NotFound

from discord_bot.cogs.message_dispatcher import (
    MessageDispatcher, MessageMutableBundle, MessageContext, DispatchPriority,
    _SendItem, _DeleteItem, _ReadItem, _HistoryReadItem, _EmojiReadItem,
)
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.fetched_message import FetchedMessage
from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest, FetchGuildEmojisRequest, SendRequest, DeleteRequest,
)
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.utils.dispatch_envelope import (
    RequestType, ResultType,
    StreamEnvelope,
)
from discord_bot.utils.dispatch_queue import RedisDispatchQueue
from discord_bot.utils.redis_client import BUNDLE_KEY_PREFIX, save_bundle as redis_save_bundle
from discord_bot.utils.redis_stream_helpers import result_stream_key

from tests.helpers import fake_bot_yielder, FakeChannel, FakeGuild, FakeMessage, FakeResponse, fake_context  # pylint: disable=unused-import
from tests.helpers import generate_fake_context


def make_dispatcher(channels=None):
    '''Return a fresh MessageDispatcher backed by a fake bot.'''
    bot = fake_bot_yielder(channels=channels or [])()
    return MessageDispatcher(bot, {}, None)


async def drain_dispatcher(dispatcher, guild_id, timeout=5.0):
    '''Wait until all currently-queued work for guild_id has been processed.

    Enqueues a LOW-priority fetch_object that resolves only after all
    previously-queued NORMAL items have run (HIGH > NORMAL > LOW ordering).
    '''
    async def _noop():
        return None

    await asyncio.wait_for(dispatcher.fetch_object(guild_id, _noop), timeout=timeout)


# ---------------------------------------------------------------------------
# update_mutable: deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_dedup(fake_context):  # pylint: disable=redefined-outer-name
    '''Rapid-fire update_mutable calls collapse to a single sentinel in the queue.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'play_order-{guild_id}'

    # Call update_mutable three times quickly
    dispatcher.update_mutable(key, guild_id, ['line1'], channel.id)
    dispatcher.update_mutable(key, guild_id, ['line2'], channel.id)
    dispatcher.update_mutable(key, guild_id, ['line3'], channel.id)

    # Only one sentinel should be in the queue
    queue = dispatcher._guilds[guild_id]  # pylint: disable=protected-access
    assert queue.qsize() == 1

    # Latest content wins
    assert dispatcher._pending_mutable[key].content == ['line3']  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_update_mutable_creates_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable lazily creates a MessageMutableBundle.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['msg'], channel.id)

    assert key in dispatcher._bundles  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_update_mutable_empty_content_routes_to_remove(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable with empty content removes the bundle instead of queuing an update.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'play_order-{guild_id}'

    # First create a bundle
    dispatcher.update_mutable(key, guild_id, ['msg'], channel.id)
    assert key in dispatcher._bundles  # pylint: disable=protected-access

    # Calling with empty content should remove it
    dispatcher.update_mutable(key, guild_id, [], channel.id)
    assert key not in dispatcher._bundles  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# delete_message: enqueue at NORMAL priority
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_message_enqueues_item(fake_context):  # pylint: disable=redefined-outer-name
    '''delete_message places a _DeleteItem at NORMAL priority.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    dispatcher.delete_message(guild_id, channel.id, 12345)

    queue = dispatcher._guilds[guild_id]  # pylint: disable=protected-access
    assert queue.qsize() == 1
    priority, _, item = queue.get_nowait()
    assert isinstance(item, _DeleteItem)
    assert priority == DispatchPriority.NORMAL
    assert item.channel_id == channel.id
    assert item.message_id == 12345


@pytest.mark.asyncio
async def test_delete_message_executes_via_worker(fake_context):  # pylint: disable=redefined-outer-name
    '''delete_message causes the worker to delete the message from the channel.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    fake_message = FakeMessage(channel=channel)
    channel.messages = [fake_message]
    dispatcher = make_dispatcher(channels=[channel])

    dispatcher.delete_message(guild_id, channel.id, fake_message.id)
    await drain_dispatcher(dispatcher, guild_id)

    assert fake_message.deleted is True
    assert fake_message not in channel.messages


# ---------------------------------------------------------------------------
# fetch_object: LOW priority, resolves future
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_object_returns_result(fake_context):  # pylint: disable=redefined-outer-name
    '''fetch_object awaits the function in the worker and returns its result.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    async def my_fetch():
        return 'hello'

    result = await dispatcher.fetch_object(guild_id, my_fetch)
    assert result == 'hello'


@pytest.mark.asyncio
async def test_fetch_object_propagates_exception(fake_context):  # pylint: disable=redefined-outer-name
    '''fetch_object propagates exceptions raised by the function.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    async def bad_fetch():
        raise ValueError('boom')

    with pytest.raises(ValueError, match='boom'):
        await dispatcher.fetch_object(guild_id, bad_fetch)


# ---------------------------------------------------------------------------
# Priority ordering: HIGH > NORMAL > LOW
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_priority_ordering(fake_context):  # pylint: disable=redefined-outer-name
    '''A HIGH item queued after a NORMAL item is still dispatched first.'''
    from discord_bot.cogs.message_dispatcher import _MutableSentinel  # pylint: disable=import-outside-toplevel

    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    key = f'play_order-{guild_id}'

    # Submit NORMAL first, then HIGH — HIGH should come out first
    queue = dispatcher._get_queue(guild_id)  # pylint: disable=protected-access
    normal = _SendItem(seq=next(dispatcher._seq), channel_id=0, content='test')  # pylint: disable=protected-access
    high = _MutableSentinel(seq=next(dispatcher._seq), key=key)  # pylint: disable=protected-access
    queue.put_nowait((normal.priority, normal.seq, normal))
    queue.put_nowait((high.priority, high.seq, high))

    # Drain two items — HIGH(0) should come out before NORMAL(1)
    pri1, _, item1 = queue.get_nowait()
    pri2, _, item2 = queue.get_nowait()
    assert pri1 == DispatchPriority.HIGH
    assert pri2 == DispatchPriority.NORMAL
    assert isinstance(item1, _MutableSentinel)
    assert isinstance(item2, _SendItem)


@pytest.mark.asyncio
async def test_priority_ordering_low(fake_context):  # pylint: disable=redefined-outer-name
    '''A LOW (_ReadItem) queued before a NORMAL item is still dispatched after.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    # Submit LOW first, then NORMAL — NORMAL should come out first
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    queue = dispatcher._get_queue(guild_id)  # pylint: disable=protected-access
    low = _ReadItem(seq=next(dispatcher._seq), func=lambda: None, future=future)  # pylint: disable=protected-access
    normal = _SendItem(seq=next(dispatcher._seq), channel_id=0, content='test')  # pylint: disable=protected-access
    queue.put_nowait((low.priority, low.seq, low))
    queue.put_nowait((normal.priority, normal.seq, normal))

    # Drain two items — NORMAL(1) should come out before LOW(2)
    pri1, _, item1 = queue.get_nowait()
    pri2, _, item2 = queue.get_nowait()
    assert pri1 == DispatchPriority.NORMAL
    assert pri2 == DispatchPriority.LOW
    assert isinstance(item1, _SendItem)
    assert isinstance(item2, _ReadItem)
    future.cancel()  # prevent ResourceWarning on the unused future


# ---------------------------------------------------------------------------
# Guild isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_guild_isolation():
    '''Each guild gets its own independent queue.'''
    dispatcher = make_dispatcher()

    guild_a = FakeGuild()
    guild_b = FakeGuild()

    dispatcher.send_message(guild_a.id, 0, 'test')
    dispatcher.send_message(guild_b.id, 0, 'test')

    queue_a = dispatcher._guilds.get(guild_a.id)  # pylint: disable=protected-access
    queue_b = dispatcher._guilds.get(guild_b.id)  # pylint: disable=protected-access

    assert queue_a is not None
    assert queue_b is not None
    assert queue_a is not queue_b
    assert queue_a.qsize() == 1
    assert queue_b.qsize() == 1


# ---------------------------------------------------------------------------
# remove_mutable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_mutable_clears_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''remove_mutable removes the bundle and pending state.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['line'], channel.id)
    assert key in dispatcher._bundles  # pylint: disable=protected-access
    assert key in dispatcher._pending_mutable  # pylint: disable=protected-access

    dispatcher.remove_mutable(key)

    assert key not in dispatcher._bundles  # pylint: disable=protected-access
    assert key not in dispatcher._pending_mutable  # pylint: disable=protected-access
    assert key not in dispatcher._sentinel_in_queue  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_remove_mutable_noop_when_missing():
    '''remove_mutable on an unknown key does not raise.'''
    dispatcher = make_dispatcher()
    dispatcher.remove_mutable('nonexistent-key')  # should not raise


# ---------------------------------------------------------------------------
# update_mutable_channel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_channel_noop_when_no_bundle(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable_channel does nothing if the bundle doesn't exist yet.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    # Should not raise
    dispatcher.update_mutable_channel('missing-key', guild_id, channel.id)


@pytest.mark.asyncio
async def test_update_mutable_channel_requeues_with_new_channel(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable_channel re-queues an update using the new channel.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    old_channel = fake_context['channel']
    new_channel = FakeChannel(guild=old_channel.guild)
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['content'], old_channel.id)
    # Drain the initial sentinel so we can observe the re-queued one
    queue = dispatcher._guilds[guild_id]  # pylint: disable=protected-access
    _ = queue.get_nowait()
    dispatcher._sentinel_in_queue.discard(key)  # pylint: disable=protected-access

    dispatcher.update_mutable_channel(key, guild_id, new_channel.id)

    # A new sentinel should have been queued for the new channel
    assert queue.qsize() == 1
    assert dispatcher._pending_mutable[key].channel_id == new_channel.id  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# cog_unload
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_unload_cancels_workers(fake_context):  # pylint: disable=redefined-outer-name
    '''cog_unload sets shutdown event and cancels all worker and consumer tasks.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    await dispatcher.cog_load()
    # Trigger worker creation via drain (needs a running event loop)
    await drain_dispatcher(dispatcher, guild_id)
    assert guild_id in dispatcher._workers  # pylint: disable=protected-access

    await dispatcher.cog_unload()

    assert dispatcher._shutdown.is_set()  # pylint: disable=protected-access
    assert not dispatcher._workers  # pylint: disable=protected-access
    assert not dispatcher._guilds  # pylint: disable=protected-access
    await asyncio.sleep(0)
    assert dispatcher._cog_consumer_task.done()  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# End-to-end worker dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_message_executes_via_worker(fake_context):  # pylint: disable=redefined-outer-name
    '''send_message causes the worker to deliver the message to the channel.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])

    dispatcher.send_message(guild_id, channel.id, 'world')
    await drain_dispatcher(dispatcher, guild_id)

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'world'


# ---------------------------------------------------------------------------
# Sentinel re-queue after processing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sentinel_requeued_after_processing(fake_context):  # pylint: disable=redefined-outer-name
    '''After a sentinel is consumed by the worker a new update_mutable call re-queues one.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['first'], channel.id)
    await drain_dispatcher(dispatcher, guild_id)

    # key should no longer be in sentinel set; queue should be drained
    assert key not in dispatcher._sentinel_in_queue  # pylint: disable=protected-access

    # A second update should now queue a fresh sentinel
    dispatcher.update_mutable(key, guild_id, ['second'], channel.id)
    assert key in dispatcher._sentinel_in_queue  # pylint: disable=protected-access
    queue = dispatcher._guilds[guild_id]  # pylint: disable=protected-access
    assert queue.qsize() == 1


# ---------------------------------------------------------------------------
# update_mutable: no-channel warning path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_no_channel_on_new_key(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable with channel=None for an unknown key logs a warning and returns.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    key = 'new-key-no-channel'

    dispatcher.update_mutable(key, guild_id, ['content'], None)

    # Bundle must NOT have been created
    assert key not in dispatcher._bundles  # pylint: disable=protected-access
    # Pending content is stored but sentinel cannot be queued without a bundle
    # (implementation discards both when channel is None for a new key)
    assert guild_id not in dispatcher._guilds  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# Long-content truncation in _process_mutable
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_long_content_truncated_to_1900(fake_context):  # pylint: disable=redefined-outer-name
    '''Content longer than 2000 chars is truncated to 1900 before send.'''
    dispatcher = make_dispatcher(channels=[fake_context['channel']])
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'trunc-{guild_id}'

    long_content = 'x' * 2500
    dispatcher.update_mutable(key, guild_id, [long_content], channel.id, sticky=False)
    await drain_dispatcher(dispatcher, guild_id)

    # The channel should have received exactly one message, truncated
    assert len(channel.messages) == 1
    assert len(channel.messages[0].content) == 1900


# ---------------------------------------------------------------------------
# update_mutable end-to-end with FakeChannel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_dispatches_message(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable causes the worker to send a message to the channel.'''
    dispatcher = make_dispatcher(channels=[fake_context['channel']])
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'e2e-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['hello world'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher, guild_id)

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'hello world'


@pytest.mark.asyncio
async def test_ephemeral_bundle_removed_after_dispatch(fake_context):  # pylint: disable=redefined-outer-name
    '''A bundle with delete_after is removed from _bundles after the worker processes it.'''
    dispatcher = make_dispatcher(channels=[fake_context['channel']])
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'ephemeral-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['bye'], channel.id, sticky=False, delete_after=5)
    await drain_dispatcher(dispatcher, guild_id)

    # Bundle should be gone since delete_after was set
    assert key not in dispatcher._bundles  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_message_enqueues_send_item(fake_context):  # pylint: disable=redefined-outer-name
    '''send_message places a _SendItem at NORMAL priority.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    dispatcher.send_message(guild_id, channel.id, 'hello')

    queue = dispatcher._guilds[guild_id]  # pylint: disable=protected-access
    assert queue.qsize() == 1
    priority, _, item = queue.get_nowait()
    assert isinstance(item, _SendItem)
    assert priority == DispatchPriority.NORMAL
    assert item.channel_id == channel.id
    assert item.content == 'hello'


# ---------------------------------------------------------------------------
# fetch_object retry params
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_object_passes_max_retries(fake_context):  # pylint: disable=redefined-outer-name
    '''_ReadItem stores max_retries; fetch_object passes it through end-to-end.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    async def noop():
        return 42

    # Verify the dataclass stores the value correctly
    future = asyncio.get_running_loop().create_future()
    item = _ReadItem(seq=next(dispatcher._seq), func=noop, future=future, max_retries=7)  # pylint: disable=protected-access
    assert item.max_retries == 7
    future.cancel()  # prevent ResourceWarning on the unused future

    # End-to-end: fetch_object with explicit max_retries
    result = await dispatcher.fetch_object(guild_id, noop, max_retries=5)
    assert result == 42


@pytest.mark.asyncio
async def test_fetch_object_with_allow_404(fake_context):  # pylint: disable=redefined-outer-name
    '''_ReadItem stores allow_404; fetch_object passes it through end-to-end.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    async def noop():
        return 'ok'

    # Verify the dataclass stores the value correctly
    future = asyncio.get_running_loop().create_future()
    item = _ReadItem(seq=0, func=noop, future=future, allow_404=True)
    assert item.allow_404 is True
    future.cancel()  # prevent ResourceWarning on the unused future

    # End-to-end: fetch_object with allow_404
    result = await dispatcher.fetch_object(guild_id, noop, allow_404=True)
    assert result == 'ok'


# ---------------------------------------------------------------------------
# cog_load / cog_unload: consumer task lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_load_starts_consumer_task():
    '''cog_load starts the cog consumer task.'''
    dispatcher = make_dispatcher()
    await dispatcher.cog_load()
    assert dispatcher._cog_consumer_task is not None  # pylint: disable=protected-access
    assert not dispatcher._cog_consumer_task.done()  # pylint: disable=protected-access
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access
    await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# __queue_depth_callback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_depth_callback(fake_context):  # pylint: disable=redefined-outer-name
    '''Queue depth callback returns the sum of all pending items across guilds.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    # Enqueue two items so depth is 2
    dispatcher.send_message(guild_id, channel.id, 'a')
    dispatcher.send_message(guild_id, channel.id, 'b')

    result = dispatcher._MessageDispatcher__queue_depth_callback(None)  # pylint: disable=protected-access
    assert len(result) == 1
    assert result[0].value == 2


# ---------------------------------------------------------------------------
# _DeleteItem dispatch: NotFound is silently ignored
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_item_not_found_silently_ignored(fake_context):  # pylint: disable=redefined-outer-name
    '''Deleting a message that is already gone (404) does not crash the worker.'''
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    # Build a channel stub whose partial message raises NotFound on delete
    mock_msg = AsyncMock()
    mock_msg.delete.side_effect = NotFound(FakeResponse(), 'unknown message')
    mock_channel = MagicMock()
    mock_channel.id = channel.id
    mock_channel.get_partial_message.return_value = mock_msg

    dispatcher = make_dispatcher(channels=[mock_channel])
    dispatcher.delete_message(guild_id, channel.id, 99999)
    await drain_dispatcher(dispatcher, guild_id)  # should not raise


# ---------------------------------------------------------------------------
# _process_mutable: bundle removed while sentinel in flight
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_mutable_bundle_removed_during_flight(fake_context):  # pylint: disable=redefined-outer-name
    '''If the bundle is popped between sentinel enqueue and processing, skip silently.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'removed-bundle-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['content'], channel.id)
    # Remove the bundle (but leave pending) before the sentinel is processed
    dispatcher._bundles.pop(key)  # pylint: disable=protected-access

    await drain_dispatcher(dispatcher, guild_id)  # should not raise


# ---------------------------------------------------------------------------
# _process_mutable: channel changed between enqueue and processing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_mutable_channel_changed(fake_context):  # pylint: disable=redefined-outer-name
    '''When pending channel_id differs from bundle's, old messages are cleared and new channel is used.'''
    channel_a = fake_context['channel']
    channel_b = FakeChannel(guild=channel_a.guild)
    guild_id = fake_context['guild'].id
    key = f'channel-change-{guild_id}'

    dispatcher = make_dispatcher(channels=[channel_a, channel_b])

    # First call creates bundle for channel_a and queues the sentinel
    dispatcher.update_mutable(key, guild_id, ['hello'], channel_a.id, sticky=False)
    # Second call updates pending to channel_b — no new sentinel because one is already queued
    dispatcher.update_mutable(key, guild_id, ['hello'], channel_b.id, sticky=False)

    await drain_dispatcher(dispatcher, guild_id)

    # Bundle should have migrated to channel_b, message sent there
    assert dispatcher._bundles[key].channel_id == channel_b.id  # pylint: disable=protected-access
    assert len(channel_b.messages) == 1


# ---------------------------------------------------------------------------
# remove_mutable with live messages: _execute_funcs body is exercised
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_mutable_deletes_tracked_messages(fake_context):  # pylint: disable=redefined-outer-name
    '''remove_mutable schedules deletion of all messages the bundle has sent.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'remove-msgs-{guild_id}'

    dispatcher = make_dispatcher(channels=[channel])

    dispatcher.update_mutable(key, guild_id, ['tracked message'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher, guild_id)

    assert len(channel.messages) == 1

    dispatcher.remove_mutable(key)
    # Give the fire-and-forget delete task time to run
    await asyncio.sleep(0.05)

    assert len(channel.messages) == 0


# ---------------------------------------------------------------------------
# _make_channel_funcs: sticky check exercised end-to-end (check_last_message_func)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_mutable_sticky_check_edits_in_place(fake_context):  # pylint: disable=redefined-outer-name
    '''Second dispatch on a sticky bundle calls check_last_message_func and edits in place.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'sticky-e2e-{guild_id}'

    dispatcher = make_dispatcher(channels=[channel])

    # First dispatch sends the initial message
    dispatcher.update_mutable(key, guild_id, ['first'], channel.id, sticky=True)
    await drain_dispatcher(dispatcher, guild_id)
    assert channel.messages[0].content == 'first'

    # Second dispatch: bundle has one existing context so sticky check runs,
    # our message is still at the end → edit in place rather than re-send
    dispatcher.update_mutable(key, guild_id, ['second'], channel.id, sticky=True)
    await drain_dispatcher(dispatcher, guild_id)

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'second'


@pytest.mark.asyncio
async def test_process_mutable_check_func_channel_not_found(fake_context):  # pylint: disable=redefined-outer-name
    '''check_last_message_func returns [] gracefully when the channel is not in the bot cache.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'check-no-channel-{guild_id}'

    # Dispatcher knows about the channel for sending, then we'll swap it out
    dispatcher = make_dispatcher(channels=[channel])

    # First dispatch establishes a tracked message context
    dispatcher.update_mutable(key, guild_id, ['hello'], channel.id, sticky=True)
    await drain_dispatcher(dispatcher, guild_id)
    assert len(channel.messages) == 1

    # Remove the channel from the bot so check_last_message_func gets None
    dispatcher.bot.channels = []  # pylint: disable=protected-access

    # Second dispatch: sticky check runs, check_last_message_func returns [] → no clear
    dispatcher.update_mutable(key, guild_id, ['world'], channel.id, sticky=True)
    await drain_dispatcher(dispatcher, guild_id)
    # send_function also got None channel, so no new message sent; existing edit also failed
    # The important thing is no crash
    assert key in dispatcher._bundles  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# _cog_consumer: submit_request routing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_request_send_routes_to_guild_queue(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(SendRequest) causes the consumer to enqueue a _SendItem.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.cog_load()

    await dispatcher.submit_request(SendRequest(
        guild_id=guild_id, channel_id=channel.id, content='via consumer',
    ))
    # Yield so consumer processes the SendRequest → _SendItem in guild queue
    await asyncio.sleep(0)
    await drain_dispatcher(dispatcher, guild_id)

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'via consumer'
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_submit_request_delete_routes_to_guild_queue(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(DeleteRequest) causes the consumer to enqueue a _DeleteItem.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.cog_load()

    await dispatcher.submit_request(DeleteRequest(
        guild_id=guild_id, channel_id=channel.id, message_id=msg.id,
    ))
    await asyncio.sleep(0)
    await drain_dispatcher(dispatcher, guild_id)

    assert msg.deleted
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_submit_request_history_delivers_channel_history_result(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(FetchChannelHistoryRequest) delivers ChannelHistoryResult to cog queue.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.cog_load()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access

    assert isinstance(result, ChannelHistoryResult)
    assert len(result.messages) == 1
    assert isinstance(result.messages[0], FetchedMessage)
    assert result.messages[0].id == msg.id


@pytest.mark.asyncio
async def test_submit_request_history_propagates_exception_as_result(fake_context):  # pylint: disable=redefined-outer-name
    '''When the channel fetch fails, a ChannelHistoryResult with error is delivered.'''
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()  # no channels registered
    await dispatcher.cog_load()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=999999, limit=10, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access

    assert isinstance(result, ChannelHistoryResult)
    assert result.error is not None


# ---------------------------------------------------------------------------
# _cog_consumer: deduplication (new per-cog model)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_history_dedup_same_cog_same_channel(fake_context):  # pylint: disable=redefined-outer-name
    '''Two requests from the same cog+channel produce only one _HistoryReadItem.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.cog_load()

    cog_name = 'testcog'
    dispatcher.register_cog_queue(cog_name)

    # Submit both before yielding so the consumer sees them together
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
    ))
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
    ))
    # One yield lets the consumer drain both items from _cog_input
    await asyncio.sleep(0)

    queue = dispatcher._get_queue(guild_id)  # pylint: disable=protected-access
    assert queue.qsize() == 1
    _, _, item = queue.get_nowait()
    assert isinstance(item, _HistoryReadItem)

    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_history_different_cogs_not_deduped(fake_context):  # pylint: disable=redefined-outer-name
    '''Two requests from different cogs for the same channel are NOT deduplicated.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.cog_load()

    dispatcher.register_cog_queue('cog_a')
    dispatcher.register_cog_queue('cog_b')

    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name='cog_a',
    ))
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name='cog_b',
    ))
    await asyncio.sleep(0)

    queue = dispatcher._get_queue(guild_id)  # pylint: disable=protected-access
    assert queue.qsize() == 2

    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_emoji_dedup_single_work_item(fake_context):  # pylint: disable=redefined-outer-name
    '''Two FetchGuildEmojisRequests for the same cog+guild produce only one _EmojiReadItem.'''
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()
    await dispatcher.cog_load()

    cog_name = 'testcog'
    dispatcher.register_cog_queue(cog_name)

    await dispatcher.submit_request(FetchGuildEmojisRequest(guild_id=guild_id, cog_name=cog_name))
    await dispatcher.submit_request(FetchGuildEmojisRequest(guild_id=guild_id, cog_name=cog_name))
    await asyncio.sleep(0)

    queue = dispatcher._get_queue(guild_id)  # pylint: disable=protected-access
    assert queue.qsize() == 1
    _, _, item = queue.get_nowait()
    assert isinstance(item, _EmojiReadItem)

    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_emoji_delivers_guild_emojis_result(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(FetchGuildEmojisRequest) delivers GuildEmojisResult to cog queue.'''
    guild = fake_context['guild']
    guild_id = guild.id
    fake_emoji = MagicMock()
    guild.emojis = [fake_emoji]
    dispatcher = make_dispatcher()
    dispatcher.bot.guilds = [guild]
    await dispatcher.cog_load()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchGuildEmojisRequest(
        guild_id=guild_id, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access

    assert isinstance(result, GuildEmojisResult)
    assert result.emojis == [fake_emoji]


@pytest.mark.asyncio
async def test_submit_request_history_with_after_message_id(fake_context):  # pylint: disable=redefined-outer-name
    '''_fetch_channel_history calls fetch_message when after_message_id is set.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.cog_load()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
        after_message_id=msg.id,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access

    assert isinstance(result, ChannelHistoryResult)
    assert result.after_message_id == msg.id


@pytest.mark.asyncio
async def test_worker_exits_on_shutdown_queue_empty_race(fake_context):  # pylint: disable=redefined-outer-name
    '''Worker returns when shutdown is set and get_nowait raises QueueEmpty (race path).'''
    from asyncio import QueueEmpty  # pylint: disable=import-outside-toplevel

    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()

    queue = dispatcher._get_queue(guild_id)  # pylint: disable=protected-access
    # Put a dummy item so queue.empty() returns False, satisfying the while condition
    queue.put_nowait((99, 0, None))

    # Patch get_nowait to raise QueueEmpty so we hit the shutdown-check branch
    def raise_empty():
        raise QueueEmpty
    queue.get_nowait = raise_empty

    dispatcher._shutdown.set()  # pylint: disable=protected-access

    worker_task = asyncio.create_task(dispatcher._worker(guild_id))  # pylint: disable=protected-access
    await asyncio.wait_for(worker_task, timeout=1.0)
    assert worker_task.done()


@pytest.mark.asyncio
async def test_submit_request_emoji_error_delivers_result(fake_context):  # pylint: disable=redefined-outer-name
    '''When guild emoji fetch fails, GuildEmojisResult with error is delivered to cog queue.'''
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()  # no guilds → fetch_guild returns None → AttributeError
    await dispatcher.cog_load()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchGuildEmojisRequest(
        guild_id=guild_id, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access

    assert isinstance(result, GuildEmojisResult)
    assert result.error is not None


# ---------------------------------------------------------------------------
# Redis persistence
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_bundle_serialization_roundtrip():
    '''to_dict / from_dict preserves guild_id, channel_id, sticky_messages, message_contexts.'''
    ctx = generate_fake_context()
    guild_id = ctx['guild'].id
    channel = ctx['channel']

    bundle = MessageMutableBundle(guild_id, channel.id, sticky_messages=False)
    bundle.message_contexts = [
        MessageContext(guild_id=guild_id, channel_id=channel.id, message_id=12345, message_content='hello'),
    ]

    restored = MessageMutableBundle.from_dict(bundle.to_dict())

    assert restored.guild_id == guild_id
    assert restored.channel_id == channel.id
    assert restored.sticky_messages is False
    assert len(restored.message_contexts) == 1
    assert restored.message_contexts[0].message_id == 12345
    assert restored.message_contexts[0].message_content == 'hello'


@pytest.mark.asyncio
async def test_cog_load_restores_bundles_from_redis():
    '''cog_load populates _bundles from Redis when a client is configured.'''
    ctx = generate_fake_context()
    guild_id = ctx['guild'].id
    channel = ctx['channel']
    key = f'restore-{guild_id}'

    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    bundle = MessageMutableBundle(guild_id, channel.id, sticky_messages=True)
    await redis_save_bundle(fake_redis, key, bundle.to_dict())

    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = fake_redis

    await dispatcher.cog_load()
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access
    await asyncio.sleep(0)

    assert key in dispatcher._bundles  # pylint: disable=protected-access
    restored = dispatcher._bundles[key]  # pylint: disable=protected-access
    assert restored.guild_id == guild_id
    assert restored.channel_id == channel.id


@pytest.mark.asyncio
async def test_process_mutable_saves_bundle_to_redis():
    '''After drain_dispatcher, the bundle is persisted to Redis.'''
    ctx = generate_fake_context()
    channel = ctx['channel']
    guild_id = ctx['guild'].id
    key = f'redis-save-{guild_id}'

    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher = make_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    dispatcher.update_mutable(key, guild_id, ['hello'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher, guild_id)
    await asyncio.sleep(0)  # let fire-and-forget save task run

    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}{key}')
    assert raw is not None


@pytest.mark.asyncio
async def test_remove_mutable_deletes_bundle_from_redis():
    '''remove_mutable schedules deletion of the bundle from Redis.'''
    ctx = generate_fake_context()
    channel = ctx['channel']
    guild_id = ctx['guild'].id
    key = f'redis-remove-{guild_id}'

    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher = make_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    dispatcher.update_mutable(key, guild_id, ['hello'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher, guild_id)
    await asyncio.sleep(0)  # let save task run

    dispatcher.remove_mutable(key)
    await asyncio.sleep(0.05)  # let delete task run

    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}{key}')
    assert raw is None


@pytest.mark.asyncio
async def test_process_mutable_ephemeral_deletes_from_redis():
    '''A bundle with delete_after is removed from Redis, not saved.'''
    ctx = generate_fake_context()
    channel = ctx['channel']
    guild_id = ctx['guild'].id
    key = f'redis-ephemeral-{guild_id}'

    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    # Pre-seed a key so we can verify it gets deleted
    await redis_save_bundle(fake_redis, key, {'guild_id': guild_id, 'channel_id': channel.id, 'sticky_messages': True, 'message_contexts': []})

    dispatcher = make_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis
    # Also pre-populate _bundles so the sentinel processes correctly
    dispatcher._bundles[key] = MessageMutableBundle(guild_id, channel.id)  # pylint: disable=protected-access

    dispatcher.update_mutable(key, guild_id, ['bye'], channel.id, sticky=False, delete_after=5)
    await drain_dispatcher(dispatcher, guild_id)
    await asyncio.sleep(0)  # let delete task run

    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}{key}')
    assert raw is None
    assert key not in dispatcher._bundles  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# MessageContext unit tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_message_context_delete_message_no_message_id():
    '''delete_message returns False when message_id is None.'''
    ctx = MessageContext(guild_id=1, channel_id=100)
    result = await ctx.delete_message(lambda _: MagicMock())
    assert result is False


@pytest.mark.asyncio
async def test_message_context_delete_message_channel_not_found():
    '''delete_message returns False when get_channel returns None.'''
    ctx = MessageContext(guild_id=1, channel_id=100, message_id=999)
    result = await ctx.delete_message(lambda _: None)
    assert result is False


@pytest.mark.asyncio
async def test_message_context_delete_message_not_found_exception():
    '''delete_message swallows NotFound and returns True.'''
    mock_msg = AsyncMock()
    mock_msg.delete.side_effect = NotFound(FakeResponse(), 'unknown message')
    mock_channel = MagicMock()
    mock_channel.get_partial_message.return_value = mock_msg

    ctx = MessageContext(guild_id=1, channel_id=100, message_id=999)
    result = await ctx.delete_message(lambda _: mock_channel)
    assert result is True


@pytest.mark.asyncio
async def test_message_context_edit_message_no_message_id():
    '''edit_message returns False when message_id is None.'''
    ctx = MessageContext(guild_id=1, channel_id=100)
    result = await ctx.edit_message(lambda _: MagicMock(), content='new')
    assert result is False


# ---------------------------------------------------------------------------
# should_clear_messages
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_should_clear_messages_non_sticky_returns_false():
    '''should_clear_messages returns False immediately when sticky_messages is False.'''
    bundle = MessageMutableBundle(1, 100, sticky_messages=False)
    bundle.message_contexts = [MessageContext(guild_id=1, channel_id=100, message_id=10)]
    never_called = MagicMock()
    result = await bundle.should_clear_messages(never_called)
    assert result is False
    never_called.assert_not_called()


@pytest.mark.asyncio
async def test_should_clear_messages_more_history_than_contexts():
    '''index < 0 guard: extra history messages beyond context count do not trigger a clear.'''
    bundle = MessageMutableBundle(1, 100, sticky_messages=True)
    bundle.message_contexts = [MessageContext(guild_id=1, channel_id=100, message_id=42)]

    hist_match = MagicMock()
    hist_match.id = 42   # matches context[0]
    hist_extra = MagicMock()
    hist_extra.id = 99   # index would be -1 → break

    async def check_func(_count):
        return [hist_match, hist_extra]

    result = await bundle.should_clear_messages(check_func)
    assert result is False


@pytest.mark.asyncio
async def test_should_clear_messages_id_mismatch_returns_true():
    '''Mismatched message ID causes should_clear_messages to return True.'''
    bundle = MessageMutableBundle(1, 100, sticky_messages=True)
    bundle.message_contexts = [MessageContext(guild_id=1, channel_id=100, message_id=42)]

    hist = MagicMock()
    hist.id = 999  # different from context.message_id

    async def check_func(_count):
        return [hist]

    result = await bundle.should_clear_messages(check_func)
    assert result is True


# ---------------------------------------------------------------------------
# get_message_dispatch — clear_existing / shrink / equal / grow paths
# ---------------------------------------------------------------------------

def test_get_message_dispatch_clear_existing_with_message_ids():
    '''clear_existing=True appends a delete func only for contexts that have a message_id.'''
    bundle = MessageMutableBundle(1, 100)
    bundle.message_contexts = [
        MessageContext(guild_id=1, channel_id=100, message_id=10, message_content='a'),
        MessageContext(guild_id=1, channel_id=100, message_id=None, message_content='b'),
    ]

    funcs = bundle.get_message_dispatch(['x'], MagicMock(), lambda _: MagicMock(), clear_existing=True)

    # One delete (message_id=10) + one send ('x') = 2
    assert len(funcs) == 2
    assert len(bundle.message_contexts) == 1
    assert bundle.message_contexts[0].message_content == 'x'


def test_get_message_dispatch_shrink_deletes_and_edits():
    '''Shrinking 3 → 2: one content match kept, one deleted, one edited.'''
    bundle = MessageMutableBundle(1, 100)
    bundle.message_contexts = [
        MessageContext(guild_id=1, channel_id=100, message_id=10, message_content='a'),
        MessageContext(guild_id=1, channel_id=100, message_id=20, message_content='b'),
        MessageContext(guild_id=1, channel_id=100, message_id=30, message_content='c'),
    ]

    # 'a' matches existing[0] exactly → keep; 'c' deleted; 'b' edited to 'd'
    funcs = bundle.get_message_dispatch(['a', 'd'], MagicMock(), lambda _: MagicMock())

    # 1 delete + 1 edit = 2 funcs; kept 'a' produces no func
    assert len(funcs) == 2
    assert len(bundle.message_contexts) == 2
    assert {ctx.message_content for ctx in bundle.message_contexts} == {'a', 'd'}


def test_get_message_dispatch_equal_keeps_matching_position():
    '''Same-position content match produces no edit func; mismatched position is edited.'''
    bundle = MessageMutableBundle(1, 100)
    bundle.message_contexts = [
        MessageContext(guild_id=1, channel_id=100, message_id=10, message_content='a'),
        MessageContext(guild_id=1, channel_id=100, message_id=20, message_content='b'),
    ]

    # 'a' at position 0 matches existing[0] → keep; 'c' replaces 'b' → edit
    funcs = bundle.get_message_dispatch(['a', 'c'], MagicMock(), lambda _: MagicMock())

    assert len(funcs) == 1  # only the edit for 'b' → 'c'
    assert bundle.message_contexts[0].message_content == 'a'
    assert bundle.message_contexts[1].message_content == 'c'


def test_get_message_dispatch_grow_appends_send_funcs():
    '''Growing 1 → 3: existing edited, two new messages sent.'''
    bundle = MessageMutableBundle(1, 100)
    bundle.message_contexts = [
        MessageContext(guild_id=1, channel_id=100, message_id=10, message_content='old'),
    ]

    funcs = bundle.get_message_dispatch(['new', 'b', 'c'], MagicMock(), lambda _: MagicMock())

    # 1 edit + 2 sends = 3 funcs; 3 contexts total
    assert len(funcs) == 3
    assert len(bundle.message_contexts) == 3
    assert bundle.message_contexts[0].message_content == 'new'
    assert bundle.message_contexts[1].message_content == 'b'
    assert bundle.message_contexts[2].message_content == 'c'


# ---------------------------------------------------------------------------
# _redis cached_property with URL
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redis_property_returns_client_when_url_configured():
    '''_redis cached_property returns a non-None client when redis_url is set in settings.'''
    settings = {'general': {'redis_url': 'redis://localhost:6379/0'}}
    dispatcher = MessageDispatcher(fake_bot_yielder()(), settings, None)
    client = dispatcher._redis  # pylint: disable=protected-access
    assert client is not None
    await client.aclose()


# ---------------------------------------------------------------------------
# Redis error paths
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restore_bundles_error_is_swallowed(mocker):
    '''_restore_bundles logs and continues when Redis raises.'''
    mocker.patch(
        'discord_bot.cogs.message_dispatcher.load_all_bundles',
        side_effect=Exception('redis down'),
    )
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await dispatcher._restore_bundles()  # pylint: disable=protected-access
    assert not dispatcher._bundles  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_save_bundle_to_redis_error_is_swallowed(mocker):
    '''_save_bundle_to_redis logs and swallows exceptions from Redis.'''
    mocker.patch(
        'discord_bot.cogs.message_dispatcher.redis_save_bundle',
        side_effect=Exception('redis down'),
    )
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await dispatcher._save_bundle_to_redis('key', MessageMutableBundle(1, 100))  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_delete_bundle_from_redis_error_is_swallowed(mocker):
    '''_delete_bundle_from_redis logs and swallows exceptions from Redis.'''
    mocker.patch(
        'discord_bot.cogs.message_dispatcher.redis_delete_bundle',
        side_effect=Exception('redis down'),
    )
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await dispatcher._delete_bundle_from_redis('key')  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# Stream consumer (_stream_consumer / _handle_stream_request)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_consumer_routes_update_mutable():
    '''_handle_stream_request with RequestType.UPDATE_MUTABLE calls update_mutable on the dispatcher.'''
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher = make_dispatcher([FakeChannel(id=100)])
    dispatcher.__dict__['_redis'] = redis_client

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.UPDATE_MUTABLE,
                       {'key': 'key1', 'guild_id': 1, 'content': ['hello'], 'channel_id': 100,
                        'sticky': True, 'delete_after': None},
                       'caller', 'req-1'),
    )

    assert 'key1' in dispatcher._bundles  # pylint: disable=protected-access
    assert dispatcher._pending_mutable['key1'].content == ['hello']  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_stream_consumer_routes_remove_mutable():
    '''_handle_stream_request with RequestType.REMOVE_MUTABLE removes the bundle.'''
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher = make_dispatcher([FakeChannel(id=100)])
    dispatcher.__dict__['_redis'] = redis_client

    dispatcher.update_mutable('key1', 1, ['msg'], 100)

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.REMOVE_MUTABLE, {'key': 'key1'}, 'caller', 'req-2'),
    )

    assert 'key1' not in dispatcher._bundles  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_handle_stream_request_returns_error_on_exception():
    '''_handle_stream_request writes a ResultType.ERROR envelope when the handler raises.'''
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = redis_client

    # RequestType.FETCH_HISTORY will call bot.fetch_channel which raises on the fake bot
    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.FETCH_HISTORY,
                       {'guild_id': 999, 'channel_id': 999, 'limit': 10,
                        'after': None, 'after_message_id': None, 'oldest_first': True},
                       'caller-proc', 'req-err'),
    )

    res_key = result_stream_key('caller-proc')
    msgs = await redis_client.xread({res_key: '0-0'}, count=1)
    assert msgs, 'expected error envelope in result stream'
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['result_type'] == ResultType.ERROR
    assert fields['request_id'] == 'req-err'


# ---------------------------------------------------------------------------
# cog_load / cog_unload — cross-process branch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_load_starts_stream_consumer_when_cross_process(mocker):
    '''cog_load creates _stream_consumer_task when dispatch_cross_process is True.'''
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_cross_process'] = True
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    mocker.patch.object(dispatcher, '_stream_consumer', new=AsyncMock())

    await dispatcher.cog_load()
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access

    assert dispatcher._stream_consumer_task is not None  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_cog_unload_cancels_stream_consumer_task():
    '''cog_unload cancels _stream_consumer_task when it is set.'''
    dispatcher = make_dispatcher()

    async def _noop():
        await asyncio.sleep(10)

    stream_task = asyncio.create_task(_noop())
    cog_task = asyncio.create_task(_noop())
    dispatcher._stream_consumer_task = stream_task  # pylint: disable=protected-access
    dispatcher._cog_consumer_task = cog_task  # pylint: disable=protected-access

    await dispatcher.cog_unload()

    assert stream_task.cancelled() or stream_task.done()


# ---------------------------------------------------------------------------
# _process_id / _shard_id cached properties
# ---------------------------------------------------------------------------

def test_process_id_from_settings():
    '''_process_id returns the configured dispatch_process_id.'''
    bot = fake_bot_yielder()()
    dispatcher = MessageDispatcher(bot, {'general': {'dispatch_process_id': 'my-proc'}}, None)
    assert dispatcher._process_id == 'my-proc'  # pylint: disable=protected-access


def test_shard_id_from_settings():
    '''_shard_id returns the configured dispatch_shard_id as int.'''
    bot = fake_bot_yielder()()
    dispatcher = MessageDispatcher(bot, {'general': {'dispatch_shard_id': 3}}, None)
    assert dispatcher._shard_id == 3  # pylint: disable=protected-access


def test_message_dispatcher_raises_when_disabled():
    '''MessageDispatcher raises CogMissingRequiredArg when include.message_dispatcher is false.'''
    bot = fake_bot_yielder()()
    with pytest.raises(CogMissingRequiredArg, match='MessageDispatcher not enabled'):
        MessageDispatcher(bot, {'general': {'include': {'message_dispatcher': False}}}, None)


@pytest.mark.asyncio
async def test_cog_unload_closes_redis_connection():
    '''cog_unload calls aclose() on the Redis client when one was created.'''
    dispatcher = make_dispatcher()
    mock_redis = AsyncMock()
    dispatcher.__dict__['_redis'] = mock_redis
    await dispatcher.cog_unload()
    mock_redis.aclose.assert_called_once()


@pytest.mark.asyncio
async def test_cog_unload_flushes_bundles_to_redis(fake_context, mocker):  # pylint: disable=redefined-outer-name
    '''cog_unload saves all in-memory bundles to Redis as a fallback flush.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)

    # Inject a bundle directly so we don't depend on worker timing
    bundle = MessageMutableBundle(guild_id=guild_id, channel_id=channel.id)
    dispatcher._bundles['bundle-key'] = bundle  # pylint: disable=protected-access

    save_spy = AsyncMock(wraps=dispatcher._save_bundle_to_redis)  # pylint: disable=protected-access
    mocker.patch.object(dispatcher, '_save_bundle_to_redis', new=save_spy)

    await dispatcher.cog_unload()

    save_spy.assert_called_once_with('bundle-key', bundle)


@pytest.mark.asyncio
async def test_cog_unload_drains_guild_queue(fake_context):  # pylint: disable=redefined-outer-name
    '''cog_unload processes all items already in the guild PriorityQueue before exiting.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.cog_load()
    dispatcher.send_message(guild_id, channel.id, 'msg1')
    dispatcher.send_message(guild_id, channel.id, 'msg2')
    await dispatcher.cog_unload()

    assert len(channel.messages) == 2


@pytest.mark.asyncio
async def test_cog_unload_drains_cog_input(fake_context):  # pylint: disable=redefined-outer-name
    '''cog_unload flushes _cog_input into guild queues and dispatches before exiting.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.cog_load()
    await dispatcher.submit_request(SendRequest(guild_id=guild_id, channel_id=channel.id, content='queued'))
    await dispatcher.cog_unload()

    assert any(m.content == 'queued' for m in channel.messages)


@pytest.mark.asyncio
async def test_cog_unload_awaits_stream_handler_tasks():
    '''cog_unload waits for in-flight _stream_handler_tasks to complete.'''
    dispatcher = make_dispatcher()
    completed = []

    async def slow_handler():
        await asyncio.sleep(0.05)
        completed.append(True)

    task = asyncio.create_task(slow_handler())
    dispatcher._stream_handler_tasks.add(task)  # pylint: disable=protected-access
    task.add_done_callback(dispatcher._stream_handler_tasks.discard)  # pylint: disable=protected-access

    await dispatcher.cog_unload()

    assert completed == [True]


@pytest.mark.asyncio
async def test_cog_unload_timeout_cancels_stuck_worker(fake_context, mocker):  # pylint: disable=redefined-outer-name
    '''cog_unload cancels workers that exceed the drain timeout.'''
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()

    async def _blocking():
        await asyncio.sleep(9999)

    worker_task = asyncio.create_task(_blocking())
    dispatcher._workers[guild_id] = worker_task  # pylint: disable=protected-access
    mocker.patch('discord_bot.cogs.message_dispatcher._DRAIN_TIMEOUT_SECONDS', 0.05)

    await dispatcher.cog_unload()

    assert worker_task.cancelled() or worker_task.done()


@pytest.mark.asyncio
async def test_cog_unload_cancels_timed_out_stream_handler_tasks(mocker):
    '''cog_unload cancels stream handler tasks that exceed the drain timeout.'''
    dispatcher = make_dispatcher()
    mocker.patch('discord_bot.cogs.message_dispatcher._DRAIN_TIMEOUT_SECONDS', 0.05)

    async def _blocking():
        await asyncio.sleep(9999)

    task = asyncio.create_task(_blocking())
    dispatcher._stream_handler_tasks.add(task)  # pylint: disable=protected-access

    await dispatcher.cog_unload()
    await asyncio.sleep(0)  # let cancellation finalise

    assert task.cancelled() or task.done()


@pytest.mark.asyncio
async def test_cog_unload_cog_consumer_timeout(mocker):
    '''cog_unload handles _cog_consumer_task exceeding the drain timeout.'''
    dispatcher = make_dispatcher()
    mocker.patch('discord_bot.cogs.message_dispatcher._DRAIN_TIMEOUT_SECONDS', 0.05)

    async def _blocking():
        await asyncio.sleep(9999)

    dispatcher._cog_consumer_task = asyncio.create_task(_blocking())  # pylint: disable=protected-access

    await dispatcher.cog_unload()  # should not hang or raise

    assert dispatcher._cog_consumer_task.cancelled() or dispatcher._cog_consumer_task.done()  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# _stream_consumer loop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_consumer_dispatches_and_acks(mocker):
    '''_stream_consumer reads a message, tasks _handle_stream_request, and acks it.'''
    dispatcher = make_dispatcher([FakeChannel(id=100)])
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher.__dict__['_process_id'] = 'proc1'
    dispatcher.__dict__['_shard_id'] = 0
    dispatcher.update_mutable('key1', 1, ['hello'], 100)

    call_count = 0

    async def fake_xreadgroup(_client, _stream_key, _consumer, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [('1-0', StreamEnvelope(RequestType.REMOVE_MUTABLE, {'key': 'key1'}, 'c', 'r1').encode())]
        await asyncio.sleep(10)
        return []

    mock_xack = AsyncMock()
    mocker.patch('discord_bot.cogs.message_dispatcher.xreadgroup', side_effect=fake_xreadgroup)
    mocker.patch('discord_bot.cogs.message_dispatcher.xack', new=mock_xack)
    mocker.patch('discord_bot.cogs.message_dispatcher.ensure_consumer_group', new=AsyncMock())
    mocker.patch.object(dispatcher._redis, 'xautoclaim', new=AsyncMock())  # pylint: disable=protected-access

    task = asyncio.create_task(dispatcher._stream_consumer())  # pylint: disable=protected-access
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0)  # let _handle_stream_request complete

    assert 'key1' not in dispatcher._bundles  # pylint: disable=protected-access
    mock_xack.assert_called_once()


@pytest.mark.asyncio
async def test_stream_consumer_xautoclaim_on_startup(mocker):
    '''_stream_consumer calls xautoclaim after ensure_consumer_group to recover pending messages.'''
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher.__dict__['_process_id'] = 'proc1'
    dispatcher.__dict__['_shard_id'] = 0

    mock_xautoclaim = AsyncMock(return_value=(b'0-0', [], []))
    mocker.patch('discord_bot.cogs.message_dispatcher.ensure_consumer_group', new=AsyncMock())
    mocker.patch('discord_bot.cogs.message_dispatcher.xreadgroup', new=AsyncMock(side_effect=asyncio.CancelledError))
    mocker.patch.object(dispatcher._redis, 'xautoclaim', new=mock_xautoclaim)  # pylint: disable=protected-access

    task = asyncio.create_task(dispatcher._stream_consumer())  # pylint: disable=protected-access
    with contextlib.suppress(asyncio.CancelledError):
        await task

    mock_xautoclaim.assert_called_once()
    call_kwargs = mock_xautoclaim.call_args
    assert call_kwargs.kwargs.get('min_idle_time') == 60000
    assert call_kwargs.kwargs.get('start_id') == '0'


# ---------------------------------------------------------------------------
# _handle_stream_request — remaining routes
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handle_stream_request_routes_update_mutable_channel():
    '''_handle_stream_request with RequestType.UPDATE_MUTABLE_CHANNEL calls update_mutable_channel.'''
    dispatcher = make_dispatcher([FakeChannel(id=200)])
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher.update_mutable('k', 1, ['hi'], 100)

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.UPDATE_MUTABLE_CHANNEL,
                       {'key': 'k', 'guild_id': 1, 'new_channel_id': 200},
                       'caller', 'req-3'),
    )

    assert dispatcher._bundles['k'].channel_id == 200  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_handle_stream_request_routes_send():
    '''_handle_stream_request with RequestType.SEND enqueues a send item for the guild.'''
    dispatcher = make_dispatcher([FakeChannel(id=50)])
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.SEND,
                       {'guild_id': 1, 'channel_id': 50, 'content': 'hello',
                        'delete_after': None, 'allow_404': False},
                       'caller', 'req-4'),
    )

    assert 1 in dispatcher._guilds  # pylint: disable=protected-access
    assert not dispatcher._guilds[1].empty()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_handle_stream_request_routes_delete():
    '''_handle_stream_request with RequestType.DELETE enqueues a delete item for the guild.'''
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.DELETE,
                       {'guild_id': 2, 'channel_id': 50, 'message_id': 999},
                       'caller', 'req-5'),
    )

    assert 2 in dispatcher._guilds  # pylint: disable=protected-access
    assert not dispatcher._guilds[2].empty()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_handle_stream_request_routes_fetch_emojis():
    '''_handle_stream_request with RequestType.FETCH_EMOJIS writes a ResultType.EMOJIS envelope.'''
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    fake_guild = FakeGuild()
    fake_emoji = MagicMock()
    fake_emoji.id = 1
    fake_emoji.name = 'thumbsup'
    fake_emoji.animated = False
    fake_guild.emojis = [fake_emoji]

    dispatcher = make_dispatcher()
    dispatcher.bot.guilds = [fake_guild]
    dispatcher.__dict__['_redis'] = redis_client

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.FETCH_EMOJIS,
                       {'guild_id': fake_guild.id, 'max_retries': 1},
                       'caller-proc', 'req-6'),
    )

    res_key = result_stream_key('caller-proc')
    msgs = await redis_client.xread({res_key: '0-0'}, count=1)
    assert msgs
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['result_type'] == ResultType.EMOJIS


@pytest.mark.asyncio
async def test_handle_stream_request_routes_fetch_history_success():
    '''_handle_stream_request with RequestType.FETCH_HISTORY writes a RES_HISTORY envelope on success.'''
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    channel = FakeChannel(id=88)
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher = make_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = redis_client

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope(RequestType.FETCH_HISTORY,
                       {'guild_id': channel.guild.id, 'channel_id': channel.id, 'limit': 10,
                        'after': None, 'after_message_id': None, 'oldest_first': True},
                       'caller-proc', 'req-hist-ok'),
    )

    res_key = result_stream_key('caller-proc')
    msgs = await redis_client.xread({res_key: '0-0'}, count=1)
    assert msgs
    _, stream_msgs = msgs[0]
    _, fields = stream_msgs[0]
    assert fields['result_type'] == 'history'


@pytest.mark.asyncio
async def test_handle_stream_request_unknown_type_returns_silently():
    '''_handle_stream_request with an unknown req_type writes nothing and returns.'''
    redis_client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher = make_dispatcher()
    dispatcher.__dict__['_redis'] = redis_client

    await dispatcher._handle_stream_request(  # pylint: disable=protected-access
        StreamEnvelope('unknown_type', {}, 'caller', 'req-7'),
    )

    msgs = await redis_client.xread({result_stream_key('caller'): '0-0'}, count=1)
    assert not msgs


# ---------------------------------------------------------------------------
# _dispatch_history_and_collect — with after datetime
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_history_and_collect_with_after():
    '''_dispatch_history_and_collect parses the after ISO string into a datetime.'''
    from datetime import datetime, timezone  # pylint: disable=import-outside-toplevel

    channel = FakeChannel(id=77)
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]

    dispatcher = make_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)

    after_ts = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()
    result = await dispatcher._dispatch_history_and_collect({  # pylint: disable=protected-access
        'guild_id': channel.guild.id,
        'channel_id': channel.id,
        'limit': 10,
        'after': after_ts,
        'after_message_id': None,
        'oldest_first': True,
    })

    assert result['channel_id'] == channel.id
    assert len(result['messages']) == 1


# ---------------------------------------------------------------------------
# _dispatch_emojis_and_collect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_emojis_and_collect():
    '''_dispatch_emojis_and_collect fetches guild emojis and returns a JSON-safe dict.'''
    fake_guild = FakeGuild()
    fake_emoji = MagicMock()
    fake_emoji.id = 42
    fake_emoji.name = 'wave'
    fake_emoji.animated = True
    fake_guild.emojis = [fake_emoji]

    dispatcher = make_dispatcher()
    dispatcher.bot.guilds = [fake_guild]
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)

    result = await dispatcher._dispatch_emojis_and_collect({  # pylint: disable=protected-access
        'guild_id': fake_guild.id, 'max_retries': 1,
    })

    assert result['guild_id'] == fake_guild.id
    assert result['emojis'] == [{'id': 42, 'name': 'wave', 'animated': True}]


# ---------------------------------------------------------------------------
# HTTP mode — shared helpers
# ---------------------------------------------------------------------------

class FakeHttpRedisQueue:
    '''Minimal RedisDispatchQueue stand-in for HTTP mode unit tests.'''

    def __init__(self):
        self.enqueued: list = []   # (method, member, payload, priority)
        self.results: dict = {}
        self._lock_held: set = set()
        self._dequeue_items: list = []

    async def enqueue(self, member: str, payload: dict, priority: int) -> None:
        '''Record a non-unique enqueue call.'''
        self.enqueued.append(('enqueue', member, payload, priority))

    async def enqueue_unique(self, member: str, payload: dict, priority: int) -> None:
        '''Record a NX enqueue call.'''
        self.enqueued.append(('enqueue_unique', member, payload, priority))

    async def dequeue(self, timeout: float = 1.0):
        '''Return and remove the next pre-seeded item, or None (never blocks past timeout=0).'''
        await asyncio.sleep(min(timeout, 0))  # yield control; clamp to 0 so tests stay fast
        if self._dequeue_items:
            return self._dequeue_items.pop(0)
        return None

    async def acquire_lock(self, key: str) -> bool:
        '''Acquire lock: returns True first time, False if already held.'''
        if key in self._lock_held:
            return False
        self._lock_held.add(key)
        return True

    async def release_lock(self, key: str) -> None:
        '''Release lock.'''
        self._lock_held.discard(key)

    async def store_result(self, request_id: str, result: dict) -> None:
        '''Store a fetch result.'''
        self.results[request_id] = result

    async def get_result(self, request_id: str):
        '''Retrieve a stored fetch result.'''
        return self.results.get(request_id)


def make_http_dispatcher(channels=None):
    '''Return a (dispatcher, fake_queue) pair configured in HTTP mode.'''
    dispatcher = make_dispatcher(channels=channels or [])
    dispatcher.__dict__['_http_mode'] = True
    fake_queue = FakeHttpRedisQueue()
    dispatcher.__dict__['_redis_queue'] = fake_queue
    return dispatcher, fake_queue


# ---------------------------------------------------------------------------
# MessageContext.edit_message — channel not found (line 107)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_message_context_edit_message_channel_not_found():
    '''edit_message returns False when get_channel returns None.'''
    ctx = MessageContext(guild_id=1, channel_id=100, message_id=999)
    result = await ctx.edit_message(lambda _: None, content='new')
    assert result is False


# ---------------------------------------------------------------------------
# _redis_queue and _num_redis_workers cached properties (lines 529, 534)
# ---------------------------------------------------------------------------

def test_redis_queue_property_returns_redis_dispatch_queue():
    '''_redis_queue returns a RedisDispatchQueue when redis is configured.'''
    bot = fake_bot_yielder()()
    settings = {'general': {'dispatch_server': {'port': 8082}}}
    dispatcher = MessageDispatcher(bot, settings, None)
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    queue = dispatcher._redis_queue  # pylint: disable=protected-access
    assert isinstance(queue, RedisDispatchQueue)


def test_num_redis_workers_default_is_4():
    '''_num_redis_workers defaults to 4 when not configured.'''
    assert make_dispatcher()._num_redis_workers == 4  # pylint: disable=protected-access


def test_num_redis_workers_from_settings():
    '''_num_redis_workers reads dispatch_worker_count from settings.'''
    bot = fake_bot_yielder()()
    dispatcher = MessageDispatcher(bot, {'general': {'dispatch_worker_count': 8}}, None)
    assert dispatcher._num_redis_workers == 8  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# HTTP mode — public API fire-and-forget branches (lines 779-922)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_mode_update_mutable_enqueues_unique():
    '''update_mutable in HTTP mode enqueues a unique mutable: item.'''
    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.update_mutable('mykey', 1, ['msg'], 100)
    await asyncio.sleep(0)
    assert any(e[1] == 'mutable:mykey' for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_http_mode_remove_mutable_enqueues_unique():
    '''remove_mutable in HTTP mode enqueues a unique remove: item.'''
    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.remove_mutable('mykey')
    await asyncio.sleep(0)
    assert any(e[1] == 'remove:mykey' for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_http_mode_send_message_enqueues():
    '''send_message in HTTP mode enqueues a send: item.'''
    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.send_message(1, 100, 'hello')
    await asyncio.sleep(0)
    assert any(e[1].startswith('send:') for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_http_mode_delete_message_enqueues():
    '''delete_message in HTTP mode enqueues a delete: item.'''
    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.delete_message(1, 100, 999)
    await asyncio.sleep(0)
    assert any(e[1].startswith('delete:') for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_http_mode_update_mutable_channel_enqueues():
    '''update_mutable_channel in HTTP mode enqueues a unique update_channel: item.'''
    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.update_mutable_channel('mykey', 1, 200)
    await asyncio.sleep(0)
    assert any(e[1] == 'update_channel:mykey' for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_enqueue_fetch_history_adds_to_queue():
    '''enqueue_fetch_history adds a fetch_history: item to the Redis queue.'''
    dispatcher, fake_queue = make_http_dispatcher()
    await dispatcher.enqueue_fetch_history('req-1', 1, 100, 10, None, None, True)
    assert any(e[1] == 'fetch_history:req-1' for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_enqueue_fetch_emojis_adds_to_queue():
    '''enqueue_fetch_emojis adds a fetch_emojis: item to the Redis queue.'''
    dispatcher, fake_queue = make_http_dispatcher()
    await dispatcher.enqueue_fetch_emojis('req-2', 1, 3)
    assert any(e[1] == 'fetch_emojis:req-2' for e in fake_queue.enqueued)


# ---------------------------------------------------------------------------
# cog_load HTTP mode — starts Redis workers + server (lines 413-423)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_load_http_mode_starts_workers_and_server(mocker):
    '''cog_load in HTTP mode creates redis worker tasks and schedules serve().'''
    bot = fake_bot_yielder()()
    settings = {'general': {'dispatch_server': {'host': '0.0.0.0', 'port': 9099}, 'dispatch_worker_count': 2}}
    dispatcher = MessageDispatcher(bot, settings, None)
    dispatcher.__dict__['_redis'] = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher.__dict__['_redis_queue'] = FakeHttpRedisQueue()

    mocker.patch('discord_bot.servers.dispatch_server.DispatchHttpServer.serve', new=AsyncMock())

    await dispatcher.cog_load()

    assert len(dispatcher._redis_worker_tasks) == 2  # pylint: disable=protected-access
    # Cleanup
    dispatcher._shutdown.set()  # pylint: disable=protected-access
    for t in dispatcher._redis_worker_tasks:  # pylint: disable=protected-access
        t.cancel()
    dispatcher._cog_consumer_task.cancel()  # pylint: disable=protected-access
    await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# cog_unload — Redis worker drain paths (lines 433-443)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_unload_drains_redis_worker_tasks():
    '''cog_unload sets shutdown and waits for redis worker tasks to complete.'''
    dispatcher, _ = make_http_dispatcher()

    task = asyncio.create_task(dispatcher._redis_worker())  # pylint: disable=protected-access
    dispatcher._redis_worker_tasks.append(task)  # pylint: disable=protected-access

    await dispatcher.cog_unload()

    assert not dispatcher._redis_worker_tasks  # pylint: disable=protected-access
    assert dispatcher._shutdown.is_set()  # pylint: disable=protected-access
    assert task.done()


@pytest.mark.asyncio
async def test_cog_unload_cancels_redis_workers_on_drain_timeout(mocker):
    '''cog_unload cancels redis worker tasks that exceed the drain timeout.'''
    dispatcher = make_dispatcher()
    mocker.patch('discord_bot.cogs.message_dispatcher._DRAIN_TIMEOUT_SECONDS', 0.05)

    async def _blocking():
        await asyncio.sleep(9999)

    task = asyncio.create_task(_blocking())
    dispatcher._redis_worker_tasks.append(task)  # pylint: disable=protected-access

    await dispatcher.cog_unload()

    assert task.cancelled() or task.done()


# ---------------------------------------------------------------------------
# _redis_worker loop (lines 1166-1171)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_redis_worker_dequeues_and_dispatches_item():
    '''_redis_worker pops an item from the queue and calls _dispatch_redis_item.'''
    channel = FakeChannel(id=100)
    dispatcher, fake_queue = make_http_dispatcher(channels=[channel])

    call_count = 0

    async def _dequeue(timeout=1.0):
        nonlocal call_count
        await asyncio.sleep(min(timeout, 0))
        call_count += 1
        if call_count == 1:
            return ('send:uuid-1', {'guild_id': 1, 'channel_id': 100,
                                    'content': 'hi', 'delete_after': None,
                                    'allow_404': False, 'span_context': None})
        dispatcher._shutdown.set()  # pylint: disable=protected-access
        return None

    fake_queue.dequeue = _dequeue

    await dispatcher._redis_worker()  # pylint: disable=protected-access

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'hi'


# ---------------------------------------------------------------------------
# _dispatch_redis_item routing (lines 1175-1190)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_mutable(mocker):
    '''_dispatch_redis_item with mutable: prefix calls _process_mutable_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_mutable_redis', new=mock_handler)
    await dispatcher._dispatch_redis_item('mutable:mykey', {'key': 'mykey'})  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('mykey', {'key': 'mykey'})


@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_remove(mocker):
    '''_dispatch_redis_item with remove: prefix calls _remove_mutable_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_remove_mutable_redis', new=mock_handler)
    await dispatcher._dispatch_redis_item('remove:mykey', {})  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('mykey')


@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_send(mocker):
    '''_dispatch_redis_item with send: prefix calls _process_send_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_send_redis', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_redis_item('send:uuid', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with(payload)


@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_delete(mocker):
    '''_dispatch_redis_item with delete: prefix calls _process_delete_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_delete_redis', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_redis_item('delete:uuid', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with(payload)


@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_update_channel(mocker):
    '''_dispatch_redis_item with update_channel: prefix calls _process_update_channel_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_update_channel_redis', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_redis_item('update_channel:mykey', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('mykey', payload)


@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_fetch_history(mocker):
    '''_dispatch_redis_item with fetch_history: prefix calls _process_fetch_history_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_fetch_history_redis', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_redis_item('fetch_history:req-1', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('req-1', payload)


@pytest.mark.asyncio
async def test_dispatch_redis_item_routes_fetch_emojis(mocker):
    '''_dispatch_redis_item with fetch_emojis: prefix calls _process_fetch_emojis_redis.'''
    dispatcher, _ = make_http_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_fetch_emojis_redis', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_redis_item('fetch_emojis:req-2', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('req-2', payload)


@pytest.mark.asyncio
async def test_dispatch_redis_item_unknown_prefix_logs_warning():
    '''_dispatch_redis_item with an unknown prefix logs a warning and does nothing.'''
    dispatcher, _ = make_http_dispatcher()
    # Should not raise
    await dispatcher._dispatch_redis_item('unknown:whatever', {})  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# _process_mutable_redis (lines 1194-1261)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_mutable_redis_lock_not_acquired_reenqueues():
    '''_process_mutable_redis re-enqueues when the bundle lock is already held.'''
    dispatcher, fake_queue = make_http_dispatcher()
    fake_queue._lock_held.add('mykey')  # pylint: disable=protected-access

    await dispatcher._process_mutable_redis('mykey', {'key': 'mykey', 'guild_id': 1,  # pylint: disable=protected-access
                                                       'content': ['hi'], 'channel_id': 100})
    assert any(e[1] == 'mutable:mykey' for e in fake_queue.enqueued)


@pytest.mark.asyncio
async def test_process_mutable_redis_no_channel_id_and_no_bundle_returns_early():
    '''_process_mutable_redis returns early when no bundle exists and channel_id is absent.'''
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.__dict__['_redis'] = fake_redis

    # No bundle in Redis, no channel_id in payload → early return, lock released
    await dispatcher._process_mutable_redis('newkey', {'key': 'newkey', 'guild_id': 1,  # pylint: disable=protected-access
                                                        'content': ['hi']})
    assert 'newkey' not in fake_queue._lock_held  # pylint: disable=protected-access  # lock released in finally


@pytest.mark.asyncio
async def test_process_mutable_redis_creates_new_bundle_and_sends():
    '''_process_mutable_redis creates a bundle and sends when none exists in Redis.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': ['hello'], 'channel_id': 100,
                                                   'sticky': False})
    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'hello'


@pytest.mark.asyncio
async def test_process_mutable_redis_loads_existing_bundle():
    '''_process_mutable_redis loads an existing bundle from Redis and sends.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    # Pre-seed a bundle in Redis
    bundle = MessageMutableBundle(guild_id=1, channel_id=100, sticky_messages=False)
    await redis_save_bundle(fake_redis, 'k', bundle.to_dict())

    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': ['world'], 'channel_id': 100})
    assert len(channel.messages) == 1


@pytest.mark.asyncio
async def test_process_mutable_redis_channel_changed_clears_old():
    '''_process_mutable_redis migrates to a new channel when channel_id differs from bundle.'''
    old_channel = FakeChannel(id=100)
    new_channel = FakeChannel(id=200)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[old_channel, new_channel])
    dispatcher.__dict__['_redis'] = fake_redis

    # Bundle exists on old channel
    bundle = MessageMutableBundle(guild_id=1, channel_id=100, sticky_messages=False)
    await redis_save_bundle(fake_redis, 'k', bundle.to_dict())

    # Payload says new channel_id=200
    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': ['new'], 'channel_id': 200})
    assert len(new_channel.messages) == 1


@pytest.mark.asyncio
async def test_process_mutable_redis_truncates_long_content():
    '''_process_mutable_redis truncates messages over 2000 chars to 1900.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    long_content = 'x' * 2500
    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': [long_content], 'channel_id': 100,
                                                   'sticky': False})
    assert len(channel.messages) == 1
    assert len(channel.messages[0].content) == 1900


@pytest.mark.asyncio
async def test_process_mutable_redis_delete_after_removes_bundle():
    '''_process_mutable_redis deletes the bundle from Redis when delete_after is set.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': ['bye'], 'channel_id': 100,
                                                   'sticky': False, 'delete_after': 5})
    # Bundle should not be saved to Redis (no key)
    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}k')
    assert raw is None


@pytest.mark.asyncio
async def test_process_mutable_redis_sticky_check_runs():
    '''_process_mutable_redis runs the sticky check when bundle.sticky_messages is True.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    # Pre-seed a sticky bundle
    bundle = MessageMutableBundle(guild_id=1, channel_id=100, sticky_messages=True)
    await redis_save_bundle(fake_redis, 'k', bundle.to_dict())

    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': ['hi'], 'channel_id': 100})
    assert len(channel.messages) == 1


@pytest.mark.asyncio
async def test_process_mutable_redis_saves_bundle_when_no_delete_after():
    '''_process_mutable_redis saves the bundle to Redis when delete_after is absent.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    await dispatcher._process_mutable_redis('k', {'key': 'k', 'guild_id': 1,  # pylint: disable=protected-access
                                                   'content': ['hi'], 'channel_id': 100,
                                                   'sticky': False})
    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}k')
    assert raw is not None


# ---------------------------------------------------------------------------
# _remove_mutable_redis (lines 1265-1271)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_mutable_redis_clears_messages_and_deletes_key():
    '''_remove_mutable_redis deletes tracked messages and removes the Redis key.'''
    channel = FakeChannel(id=100)
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher(channels=[channel])
    dispatcher.__dict__['_redis'] = fake_redis

    bundle = MessageMutableBundle(guild_id=1, channel_id=100, sticky_messages=False)
    await redis_save_bundle(fake_redis, 'k', bundle.to_dict())

    await dispatcher._remove_mutable_redis('k')  # pylint: disable=protected-access

    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}k')
    assert raw is None


@pytest.mark.asyncio
async def test_remove_mutable_redis_no_bundle_still_deletes_key():
    '''_remove_mutable_redis succeeds even when no bundle exists in Redis.'''
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher()
    dispatcher.__dict__['_redis'] = fake_redis

    await dispatcher._remove_mutable_redis('missing')  # pylint: disable=protected-access  # should not raise


# ---------------------------------------------------------------------------
# _process_send_redis (lines 1275-1280)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_send_redis_sends_to_channel():
    '''_process_send_redis delivers a message to the target channel.'''
    channel = FakeChannel(id=100)
    dispatcher, _ = make_http_dispatcher(channels=[channel])

    await dispatcher._process_send_redis({'guild_id': 1, 'channel_id': 100,  # pylint: disable=protected-access
                                          'content': 'hello', 'delete_after': None,
                                          'allow_404': False, 'span_context': None})
    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'hello'


# ---------------------------------------------------------------------------
# _process_delete_redis (lines 1287-1296)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_delete_redis_deletes_message():
    '''_process_delete_redis calls delete() on the target message.'''
    channel = FakeChannel(id=100)
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher, _ = make_http_dispatcher(channels=[channel])

    await dispatcher._process_delete_redis({'guild_id': 1, 'channel_id': 100,  # pylint: disable=protected-access
                                            'message_id': msg.id, 'span_context': None})
    assert msg.deleted


@pytest.mark.asyncio
async def test_process_delete_redis_not_found_is_ignored():
    '''_process_delete_redis swallows NotFound when the message is already gone.'''
    mock_msg = AsyncMock()
    mock_msg.delete.side_effect = NotFound(FakeResponse(), 'unknown message')
    mock_channel = MagicMock()
    mock_channel.id = 100
    mock_channel.get_partial_message.return_value = mock_msg

    dispatcher, _ = make_http_dispatcher(channels=[mock_channel])

    await dispatcher._process_delete_redis({'guild_id': 1, 'channel_id': 100,  # pylint: disable=protected-access
                                            'message_id': 999, 'span_context': None})
    # Should not raise


# ---------------------------------------------------------------------------
# _process_update_channel_redis (lines 1300-1311)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_update_channel_redis_migrates_bundle():
    '''_process_update_channel_redis moves the bundle to the new channel and saves.'''
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher()
    dispatcher.__dict__['_redis'] = fake_redis

    bundle = MessageMutableBundle(guild_id=1, channel_id=100, sticky_messages=False)
    await redis_save_bundle(fake_redis, 'k', bundle.to_dict())

    await dispatcher._process_update_channel_redis('k', {'guild_id': 1, 'new_channel_id': 200})  # pylint: disable=protected-access

    raw = await fake_redis.get(f'{BUNDLE_KEY_PREFIX}k')
    assert raw is not None
    import json  # pylint: disable=import-outside-toplevel
    saved = json.loads(raw)
    assert saved['channel_id'] == 200


@pytest.mark.asyncio
async def test_process_update_channel_redis_no_bundle_returns():
    '''_process_update_channel_redis returns without error when bundle does not exist.'''
    fake_redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    dispatcher, _ = make_http_dispatcher()
    dispatcher.__dict__['_redis'] = fake_redis

    await dispatcher._process_update_channel_redis('missing', {'guild_id': 1, 'new_channel_id': 200})  # pylint: disable=protected-access
    # Should not raise


# ---------------------------------------------------------------------------
# _process_fetch_history_redis (lines 1315-1325)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_fetch_history_redis_stores_result():
    '''_process_fetch_history_redis stores a successful history result in the queue.'''
    channel = FakeChannel(id=100)
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher, fake_queue = make_http_dispatcher(channels=[channel])

    await dispatcher._process_fetch_history_redis('req-1', {  # pylint: disable=protected-access
        'guild_id': channel.guild.id, 'channel_id': 100,
        'limit': 10, 'after': None, 'after_message_id': None, 'oldest_first': True,
    })

    assert 'req-1' in fake_queue.results
    assert 'messages' in fake_queue.results['req-1']


@pytest.mark.asyncio
async def test_process_fetch_history_redis_stores_error_on_exception():
    '''_process_fetch_history_redis stores an error result when the fetch raises.'''
    dispatcher, fake_queue = make_http_dispatcher()  # no channels → fetch_channel returns None

    await dispatcher._process_fetch_history_redis('req-err', {  # pylint: disable=protected-access
        'guild_id': 1, 'channel_id': 99999,
        'limit': 10, 'after': None, 'after_message_id': None, 'oldest_first': True,
    })

    assert 'req-err' in fake_queue.results
    assert 'error' in fake_queue.results['req-err']


# ---------------------------------------------------------------------------
# _process_fetch_emojis_redis (lines 1329-1337)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_fetch_emojis_redis_stores_result():
    '''_process_fetch_emojis_redis stores a successful emoji result in the queue.'''
    fake_guild = FakeGuild()
    fake_emoji = MagicMock()
    fake_emoji.id = 1
    fake_emoji.name = 'wave'
    fake_emoji.animated = False
    fake_guild.emojis = [fake_emoji]

    dispatcher, fake_queue = make_http_dispatcher()
    dispatcher.bot.guilds = [fake_guild]

    await dispatcher._process_fetch_emojis_redis('req-e1', {'guild_id': fake_guild.id, 'max_retries': 1})  # pylint: disable=protected-access

    assert 'req-e1' in fake_queue.results
    assert 'emojis' in fake_queue.results['req-e1']


@pytest.mark.asyncio
async def test_process_fetch_emojis_redis_stores_error_on_exception():
    '''_process_fetch_emojis_redis stores an error result when the fetch raises.'''
    dispatcher, fake_queue = make_http_dispatcher()  # no guilds → fetch_guild returns None

    await dispatcher._process_fetch_emojis_redis('req-e-err', {'guild_id': 99999, 'max_retries': 1})  # pylint: disable=protected-access

    assert 'req-e-err' in fake_queue.results
    assert 'error' in fake_queue.results['req-e-err']
