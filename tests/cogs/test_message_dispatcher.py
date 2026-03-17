import asyncio

import pytest

from discord_bot.cogs.message_dispatcher import (
    MessageDispatcher, DispatchPriority, _SendItem, _DeleteItem, _ReadItem,
)

from tests.helpers import fake_bot_yielder, FakeChannel, FakeGuild, FakeMessage, fake_context  # pylint: disable=unused-import


def make_dispatcher(channels=None):
    """Return a fresh MessageDispatcher backed by a fake bot."""
    bot = fake_bot_yielder(channels=channels or [])()
    return MessageDispatcher(bot, {}, None)


async def drain_dispatcher(dispatcher, guild_id, timeout=5.0):
    """Wait until all currently-queued work for guild_id has been processed.

    Enqueues a LOW-priority fetch_object that resolves only after all
    previously-queued NORMAL items have run (HIGH > NORMAL > LOW ordering).
    """
    async def _noop():
        return None

    await asyncio.wait_for(dispatcher.fetch_object(guild_id, _noop), timeout=timeout)


# ---------------------------------------------------------------------------
# update_mutable: deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_dedup(fake_context):  # pylint: disable=redefined-outer-name
    """Rapid-fire update_mutable calls collapse to a single sentinel in the queue."""
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
    """update_mutable lazily creates a MessageMutableBundle."""
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['msg'], channel.id)

    assert key in dispatcher._bundles  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# delete_message: enqueue at NORMAL priority
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delete_message_enqueues_item(fake_context):  # pylint: disable=redefined-outer-name
    """delete_message places a _DeleteItem at NORMAL priority."""
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
    """delete_message causes the worker to delete the message from the channel."""
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
    """fetch_object awaits the function in the worker and returns its result."""
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    async def my_fetch():
        return 'hello'

    result = await dispatcher.fetch_object(guild_id, my_fetch)
    assert result == 'hello'


@pytest.mark.asyncio
async def test_fetch_object_propagates_exception(fake_context):  # pylint: disable=redefined-outer-name
    """fetch_object propagates exceptions raised by the function."""
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
    """A HIGH item queued after a NORMAL item is still dispatched first."""
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
    """A LOW (_ReadItem) queued before a NORMAL item is still dispatched after."""
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
    """Each guild gets its own independent queue."""
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
    """remove_mutable removes the bundle and pending state."""
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
    """remove_mutable on an unknown key does not raise."""
    dispatcher = make_dispatcher()
    dispatcher.remove_mutable('nonexistent-key')  # should not raise


# ---------------------------------------------------------------------------
# update_mutable_channel
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_channel_noop_when_no_bundle(fake_context):  # pylint: disable=redefined-outer-name
    """update_mutable_channel does nothing if the bundle doesn't exist yet."""
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    # Should not raise
    dispatcher.update_mutable_channel('missing-key', guild_id, channel.id)


@pytest.mark.asyncio
async def test_update_mutable_channel_requeues_with_new_channel(fake_context):  # pylint: disable=redefined-outer-name
    """update_mutable_channel re-queues an update using the new channel."""
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
    """cog_unload sets shutdown event and cancels all worker tasks."""
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    # Trigger worker creation via drain (needs a running event loop)
    await drain_dispatcher(dispatcher, guild_id)
    assert guild_id in dispatcher._workers  # pylint: disable=protected-access

    await dispatcher.cog_unload()

    assert dispatcher._shutdown.is_set()  # pylint: disable=protected-access
    assert not dispatcher._workers  # pylint: disable=protected-access
    assert not dispatcher._guilds  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# End-to-end worker dispatch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_message_executes_via_worker(fake_context):  # pylint: disable=redefined-outer-name
    """send_message causes the worker to deliver the message to the channel."""
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
    """After a sentinel is consumed by the worker a new update_mutable call re-queues one."""
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
    """update_mutable with channel=None for an unknown key logs a warning and returns."""
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
    """Content longer than 2000 chars is truncated to 1900 before send."""
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
    """update_mutable causes the worker to send a message to the channel."""
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
    """A bundle with delete_after is removed from _bundles after the worker processes it."""
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
    """send_message places a _SendItem at NORMAL priority."""
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
    """_ReadItem stores max_retries; fetch_object passes it through end-to-end."""
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
    """_ReadItem stores allow_404; fetch_object passes it through end-to-end."""
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
# cog_load
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cog_load():
    """cog_load is a no-op but must not raise."""
    dispatcher = make_dispatcher()
    await dispatcher.cog_load()


# ---------------------------------------------------------------------------
# __queue_depth_callback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_queue_depth_callback(fake_context):  # pylint: disable=redefined-outer-name
    """Queue depth callback returns the sum of all pending items across guilds."""
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
    """Deleting a message that is already gone (404) does not crash the worker."""
    from unittest.mock import AsyncMock, MagicMock  # pylint: disable=import-outside-toplevel
    from discord.errors import NotFound  # pylint: disable=import-outside-toplevel
    from tests.helpers import FakeResponse  # pylint: disable=import-outside-toplevel

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
    """If the bundle is popped between sentinel enqueue and processing, skip silently."""
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
    """When pending channel_id differs from bundle's, old messages are cleared and new channel is used."""
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
    """remove_mutable schedules deletion of all messages the bundle has sent."""
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
    """Second dispatch on a sticky bundle calls check_last_message_func and edits in place."""
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
    """check_last_message_func returns [] gracefully when the channel is not in the bot cache."""
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
