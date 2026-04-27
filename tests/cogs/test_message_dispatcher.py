import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord.errors import NotFound

from discord_bot.workers.message_dispatcher import (
    MessageDispatcher, MessageMutableBundle, MessageContext, DispatchPriority,
)
from discord_bot.workers.asyncio_queues import AsyncioBundleStore, AsyncioWorkQueue
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.fetched_message import FetchedMessage
from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest, FetchGuildEmojisRequest, SendRequest, DeleteRequest,
)
from discord_bot.clients.dispatch_client_base import DispatchRemoteError
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult

from tests.helpers import (
    fake_bot_yielder, FakeChannel, FakeGuild, FakeMessage, FakeResponse,
    generate_fake_context,
)


def make_dispatcher(channels=None, settings=None):
    '''Return a fresh MessageDispatcher backed by AsyncioBundleStore + AsyncioWorkQueue.'''
    bot = fake_bot_yielder(channels=channels or [])()
    return MessageDispatcher(bot, settings or {}, AsyncioBundleStore(), AsyncioWorkQueue())


async def drain_dispatcher(dispatcher, timeout=5.0):
    '''
    Wait until the work queue is empty and all in-flight work has completed.

    First yields once to let any pending asyncio.create_task enqueue calls run,
    then polls until the queue is empty.  After the queue empties, waits briefly
    for any in-flight _dispatch_item that was already dequeued to finish, then
    re-checks before returning.
    '''
    # Let pending enqueue create_tasks run first
    await asyncio.sleep(0)

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        q = dispatcher._work_queue._queue  # pylint: disable=protected-access
        if q.empty():
            # A worker may still be processing the item it already dequeued.
            await asyncio.sleep(0.05)
            if q.empty():
                return
        await asyncio.sleep(0.01)
    raise asyncio.TimeoutError('drain_dispatcher timed out')


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------

def test_message_dispatcher_raises_when_disabled():
    '''MessageDispatcher raises CogMissingRequiredArg when include.message_dispatcher is false.'''
    bot = fake_bot_yielder()()
    with pytest.raises(CogMissingRequiredArg, match='MessageDispatcher not enabled'):
        MessageDispatcher(
            bot,
            {'general': {'include': {'message_dispatcher': False}}},
            AsyncioBundleStore(),
            AsyncioWorkQueue(),
        )


def test_num_workers_default_is_4():
    '''_num_workers defaults to 4 when not configured.'''
    assert make_dispatcher()._num_workers == 4  # pylint: disable=protected-access


def test_num_workers_from_settings():
    '''_num_workers reads dispatch_worker_count from settings.'''
    d = make_dispatcher(settings={'general': {'dispatch_worker_count': 8}})
    assert d._num_workers == 8  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# start() / stop() lifecycle
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_start_spawns_worker_tasks():
    '''start() creates _num_workers running tasks.'''
    dispatcher = make_dispatcher(settings={'general': {'dispatch_worker_count': 2}})
    await dispatcher.start()
    assert len(dispatcher._worker_tasks) == 2  # pylint: disable=protected-access
    assert all(not t.done() for t in dispatcher._worker_tasks)  # pylint: disable=protected-access
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_stop_drains_and_clears_tasks():
    '''stop() waits for workers to finish and clears the task list.'''
    dispatcher = make_dispatcher()
    await dispatcher.start()
    await dispatcher.stop()
    assert not dispatcher._worker_tasks  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_stop_cancels_stuck_workers(mocker):
    '''stop() cancels workers that exceed the drain timeout.'''
    dispatcher = make_dispatcher()
    mocker.patch('discord_bot.workers.message_dispatcher._DRAIN_TIMEOUT_SECONDS', 0.05)

    async def _blocking():
        await asyncio.sleep(9999)

    task = asyncio.create_task(_blocking())
    dispatcher._worker_tasks.append(task)  # pylint: disable=protected-access

    await dispatcher.stop()

    assert task.cancelled() or task.done()


# ---------------------------------------------------------------------------
# send_message / delete_message enqueue to work_queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_message_enqueues_to_work_queue(fake_context):  # pylint: disable=redefined-outer-name
    '''send_message puts a send: item into the work queue at NORMAL priority.'''
    dispatcher = make_dispatcher()
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id

    dispatcher.send_message(guild_id, channel.id, 'hello')
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    priority, _seq, member, payload = q.get_nowait()
    assert priority == DispatchPriority.NORMAL
    assert member.startswith('send:')
    assert payload['content'] == 'hello'
    assert payload['channel_id'] == channel.id


@pytest.mark.asyncio
async def test_delete_message_enqueues_to_work_queue(fake_context):  # pylint: disable=redefined-outer-name
    '''delete_message puts a delete: item into the work queue at NORMAL priority.'''
    dispatcher = make_dispatcher()
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id

    dispatcher.delete_message(guild_id, channel.id, 12345)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    priority, _seq, member, payload = q.get_nowait()
    assert priority == DispatchPriority.NORMAL
    assert member.startswith('delete:')
    assert payload['message_id'] == 12345


@pytest.mark.asyncio
async def test_update_mutable_enqueues_unique_high_priority(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable puts a mutable: item into the queue at HIGH priority.'''
    dispatcher = make_dispatcher()
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['line1'], channel.id)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    priority, _seq, member, payload = q.get_nowait()
    assert priority == DispatchPriority.HIGH
    assert member == f'mutable:{key}'
    assert payload['content'] == ['line1']


@pytest.mark.asyncio
async def test_update_mutable_dedup_same_key(fake_context):  # pylint: disable=redefined-outer-name
    '''Rapid-fire update_mutable calls for the same key collapse to one queue entry.'''
    dispatcher = make_dispatcher()
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, ['line1'], channel.id)
    dispatcher.update_mutable(key, guild_id, ['line2'], channel.id)
    dispatcher.update_mutable(key, guild_id, ['line3'], channel.id)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert q.qsize() == 1


@pytest.mark.asyncio
async def test_update_mutable_empty_content_enqueues_remove(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable with empty content enqueues a remove: item instead.'''
    dispatcher = make_dispatcher()
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'play_order-{guild_id}'

    dispatcher.update_mutable(key, guild_id, [], channel.id)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    _priority, _seq, member, _payload = q.get_nowait()
    assert member.startswith('remove:')


@pytest.mark.asyncio
async def test_remove_mutable_enqueues_remove_item(fake_context):  # pylint: disable=redefined-outer-name
    '''remove_mutable enqueues a remove: item at HIGH priority.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    key = f'play_order-{guild_id}'

    dispatcher.remove_mutable(key)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    priority, _seq, member, _payload = q.get_nowait()
    assert priority == DispatchPriority.HIGH
    assert member == f'remove:{key}'


@pytest.mark.asyncio
async def test_update_mutable_channel_enqueues_unique(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable_channel enqueues a unique update_channel: item.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id
    key = f'key-{guild_id}'

    dispatcher.update_mutable_channel(key, guild_id, 999)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    _priority, _seq, member, payload = q.get_nowait()
    assert member == f'update_channel:{key}'
    assert payload['new_channel_id'] == 999


# ---------------------------------------------------------------------------
# Priority ordering
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_priority_ordering_high_before_normal(fake_context):  # pylint: disable=redefined-outer-name
    '''A HIGH item queued after a NORMAL item still comes out first.'''
    dispatcher = make_dispatcher()
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'key-{guild_id}'

    # NORMAL first, then HIGH
    dispatcher.send_message(guild_id, channel.id, 'normal')
    dispatcher.update_mutable(key, guild_id, ['high'], channel.id)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    pri1, _, _, _ = q.get_nowait()
    pri2, _, _, _ = q.get_nowait()
    assert pri1 == DispatchPriority.HIGH
    assert pri2 == DispatchPriority.NORMAL


# ---------------------------------------------------------------------------
# fetch_object — runs directly, no queue
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_object_returns_result(fake_context):  # pylint: disable=redefined-outer-name
    '''fetch_object awaits the function and returns its result directly.'''
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


@pytest.mark.asyncio
async def test_fetch_object_passes_max_retries(fake_context):  # pylint: disable=redefined-outer-name
    '''fetch_object accepts and respects max_retries.'''
    dispatcher = make_dispatcher()
    guild_id = fake_context['guild'].id

    async def noop():
        return 42

    result = await dispatcher.fetch_object(guild_id, noop, max_retries=5)
    assert result == 42


# ---------------------------------------------------------------------------
# submit_request routing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_request_send_enqueues(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(SendRequest) enqueues a send: item.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.submit_request(SendRequest(
        guild_id=guild_id, channel_id=channel.id, content='via submit',
    ))
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    _pri, _seq, member, payload = q.get_nowait()
    assert member.startswith('send:')
    assert payload['content'] == 'via submit'


@pytest.mark.asyncio
async def test_submit_request_delete_enqueues(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(DeleteRequest) enqueues a delete: item.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    msg = FakeMessage(channel=channel)
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.submit_request(DeleteRequest(
        guild_id=guild_id, channel_id=channel.id, message_id=msg.id,
    ))
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    _pri, _seq, member, payload = q.get_nowait()
    assert member.startswith('delete:')
    assert payload['message_id'] == msg.id


@pytest.mark.asyncio
async def test_submit_request_history_creates_task(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(FetchChannelHistoryRequest) starts a background task.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])
    cog_name = 'testcog'
    dispatcher.register_cog_queue(cog_name)

    # Just verify it does not raise and creates a task
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
    ))
    await asyncio.sleep(0)  # let task start


# ---------------------------------------------------------------------------
# End-to-end: workers process items
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_send_message_executes_via_worker(fake_context):  # pylint: disable=redefined-outer-name
    '''send_message causes the worker to deliver the message to the channel.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.send_message(guild_id, channel.id, 'world')
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'world'


@pytest.mark.asyncio
async def test_delete_message_executes_via_worker(fake_context):  # pylint: disable=redefined-outer-name
    '''delete_message causes the worker to delete the message from the channel.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    fake_message = FakeMessage(channel=channel)
    channel.messages = [fake_message]
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.delete_message(guild_id, channel.id, fake_message.id)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert fake_message.deleted is True
    assert fake_message not in channel.messages


@pytest.mark.asyncio
async def test_delete_item_not_found_silently_ignored(fake_context):  # pylint: disable=redefined-outer-name
    '''Deleting a message that is already gone (404) does not crash the worker.'''
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']

    mock_msg = AsyncMock()
    mock_msg.delete.side_effect = NotFound(FakeResponse(), 'unknown message')
    mock_channel = MagicMock()
    mock_channel.id = channel.id
    mock_channel.get_partial_message.return_value = mock_msg

    dispatcher = make_dispatcher(channels=[mock_channel])
    await dispatcher.start()
    dispatcher.delete_message(guild_id, channel.id, 99999)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_update_mutable_dispatches_message(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable causes the worker to send a message to the channel.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'e2e-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['hello world'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'hello world'


@pytest.mark.asyncio
async def test_long_content_truncated_to_1900(fake_context):  # pylint: disable=redefined-outer-name
    '''Content longer than 2000 chars is truncated to 1900 before send.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'trunc-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['x' * 2500], channel.id, sticky=False)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel.messages) == 1
    assert len(channel.messages[0].content) == 1900


@pytest.mark.asyncio
async def test_ephemeral_bundle_removed_after_dispatch(fake_context):  # pylint: disable=redefined-outer-name
    '''A bundle with delete_after is removed from the bundle_store after processing.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'ephemeral-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['bye'], channel.id, sticky=False, delete_after=5)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    # Bundle removed from the store since delete_after was set
    bundle_dict = await dispatcher._bundle_store.load(key)  # pylint: disable=protected-access
    assert bundle_dict is None


@pytest.mark.asyncio
async def test_remove_mutable_deletes_tracked_messages(fake_context):  # pylint: disable=redefined-outer-name
    '''remove_mutable schedules deletion of all messages the bundle has sent.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'remove-msgs-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['tracked message'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher)
    assert len(channel.messages) == 1

    dispatcher.remove_mutable(key)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel.messages) == 0


@pytest.mark.asyncio
async def test_process_mutable_sticky_check_edits_in_place(fake_context):  # pylint: disable=redefined-outer-name
    '''Second dispatch on a sticky bundle edits in place rather than re-sending.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'sticky-e2e-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['first'], channel.id, sticky=True)
    await drain_dispatcher(dispatcher)
    assert channel.messages[0].content == 'first'

    dispatcher.update_mutable(key, guild_id, ['second'], channel.id, sticky=True)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'second'


@pytest.mark.asyncio
async def test_process_mutable_migrates_channel_when_payload_has_new_channel_id(fake_context):  # pylint: disable=redefined-outer-name
    '''_process_mutable migrates the bundle when payload carries a different channel_id.'''
    channel_a = fake_context['channel']
    channel_b = FakeChannel(guild=channel_a.guild)
    guild_id = fake_context['guild'].id
    key = f'inline-migrate-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel_a, channel_b])

    await dispatcher.start()
    # Establish bundle on channel_a
    dispatcher.update_mutable(key, guild_id, ['v1'], channel_a.id, sticky=False)
    await drain_dispatcher(dispatcher)
    assert len(channel_a.messages) == 1

    # update_mutable with a different channel_id triggers the migration branch inside _process_mutable
    dispatcher.update_mutable(key, guild_id, ['v2'], channel_b.id, sticky=False)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel_b.messages) == 1


@pytest.mark.asyncio
async def test_process_update_channel_no_bundle_returns_early(fake_context):  # pylint: disable=redefined-outer-name
    '''_process_update_channel returns early when no bundle exists for the key.'''
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    # Call update_mutable_channel on a key that has no bundle — should not raise
    dispatcher.update_mutable_channel('nonexistent-key', guild_id, channel.id)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()
    # No exception and nothing was sent
    assert len(channel.messages) == 0


@pytest.mark.asyncio
async def test_update_mutable_channel_migrates_to_new_channel(fake_context):  # pylint: disable=redefined-outer-name
    '''update_mutable_channel moves the bundle and sends to the new channel.'''
    channel_a = fake_context['channel']
    channel_b = FakeChannel(guild=channel_a.guild)
    guild_id = fake_context['guild'].id
    key = f'channel-change-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel_a, channel_b])

    await dispatcher.start()
    # First send establishes the bundle on channel_a
    dispatcher.update_mutable(key, guild_id, ['hello'], channel_a.id, sticky=False)
    await drain_dispatcher(dispatcher)
    assert len(channel_a.messages) == 1

    # Now move to channel_b
    dispatcher.update_mutable_channel(key, guild_id, channel_b.id)
    await drain_dispatcher(dispatcher)

    # Send to channel_b
    dispatcher.update_mutable(key, guild_id, ['world'], channel_b.id, sticky=False)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    assert len(channel_b.messages) == 1


# ---------------------------------------------------------------------------
# Bundle persistence via bundle_store
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_mutable_saves_bundle_to_store(fake_context):  # pylint: disable=redefined-outer-name
    '''After drain, the bundle is persisted to bundle_store.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'store-save-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['hello'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    bundle_dict = await dispatcher._bundle_store.load(key)  # pylint: disable=protected-access
    assert bundle_dict is not None
    assert bundle_dict['channel_id'] == channel.id


@pytest.mark.asyncio
async def test_remove_mutable_deletes_bundle_from_store(fake_context):  # pylint: disable=redefined-outer-name
    '''remove_mutable removes the bundle from the store.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    key = f'store-remove-{guild_id}'
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher.start()
    dispatcher.update_mutable(key, guild_id, ['hello'], channel.id, sticky=False)
    await drain_dispatcher(dispatcher)

    dispatcher.remove_mutable(key)
    await drain_dispatcher(dispatcher)
    await dispatcher.stop()

    bundle_dict = await dispatcher._bundle_store.load(key)  # pylint: disable=protected-access
    assert bundle_dict is None


@pytest.mark.asyncio
async def test_save_bundle_to_store_error_is_swallowed(mocker):
    '''_save_bundle_to_store logs and swallows exceptions from the store.'''
    dispatcher = make_dispatcher()
    mocker.patch.object(
        dispatcher._bundle_store, 'save',  # pylint: disable=protected-access
        side_effect=Exception('store down'),
    )
    await dispatcher._save_bundle_to_store('key', MessageMutableBundle(1, 100))  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_delete_bundle_from_store_error_is_swallowed(mocker):
    '''_delete_bundle_from_store logs and swallows exceptions from the store.'''
    dispatcher = make_dispatcher()
    mocker.patch.object(
        dispatcher._bundle_store, 'delete',  # pylint: disable=protected-access
        side_effect=Exception('store down'),
    )
    await dispatcher._delete_bundle_from_store('key')  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# _dispatch_item routing (unit tests using mocker)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_item_routes_mutable(mocker):
    '''_dispatch_item with mutable: prefix calls _process_mutable.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_mutable', new=mock_handler)
    await dispatcher._dispatch_item('mutable:mykey', {'key': 'mykey'})  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('mykey', {'key': 'mykey'})


@pytest.mark.asyncio
async def test_dispatch_item_routes_remove(mocker):
    '''_dispatch_item with remove: prefix calls _remove_mutable.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_remove_mutable', new=mock_handler)
    await dispatcher._dispatch_item('remove:mykey', {})  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('mykey')


@pytest.mark.asyncio
async def test_dispatch_item_routes_send(mocker):
    '''_dispatch_item with send: prefix calls _process_send.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_send', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_item('send:uuid', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with(payload)


@pytest.mark.asyncio
async def test_dispatch_item_routes_delete(mocker):
    '''_dispatch_item with delete: prefix calls _process_delete.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_delete', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_item('delete:uuid', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with(payload)


@pytest.mark.asyncio
async def test_dispatch_item_routes_update_channel(mocker):
    '''_dispatch_item with update_channel: prefix calls _process_update_channel.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_update_channel', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_item('update_channel:mykey', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('mykey', payload)


@pytest.mark.asyncio
async def test_dispatch_item_routes_fetch_history(mocker):
    '''_dispatch_item with fetch_history: prefix calls _process_fetch_history.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_fetch_history', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_item('fetch_history:req-1', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('req-1', payload)


@pytest.mark.asyncio
async def test_dispatch_item_routes_fetch_emojis(mocker):
    '''_dispatch_item with fetch_emojis: prefix calls _process_fetch_emojis.'''
    dispatcher = make_dispatcher()
    mock_handler = AsyncMock()
    mocker.patch.object(dispatcher, '_process_fetch_emojis', new=mock_handler)
    payload = {'guild_id': 1}
    await dispatcher._dispatch_item('fetch_emojis:req-2', payload)  # pylint: disable=protected-access
    mock_handler.assert_called_once_with('req-2', payload)


@pytest.mark.asyncio
async def test_dispatch_item_unknown_prefix_logs_warning():
    '''_dispatch_item with an unknown prefix logs a warning and does not raise.'''
    dispatcher = make_dispatcher()
    await dispatcher._dispatch_item('unknown:whatever', {})  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# _process_mutable: lock contention
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_mutable_lock_not_acquired_reenqueues(mocker):
    '''_process_mutable re-enqueues when acquire_lock returns False (e.g. Redis contention).'''
    dispatcher = make_dispatcher()
    mocker.patch.object(dispatcher._work_queue, 'acquire_lock', return_value=False)  # pylint: disable=protected-access

    await dispatcher._process_mutable('mykey', {  # pylint: disable=protected-access
        'key': 'mykey', 'guild_id': 1, 'content': ['hi'], 'channel_id': 100,
    })

    # The item should have been re-enqueued
    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    _pri, _seq, member, _payload = q.get_nowait()
    assert member == 'mutable:mykey'


@pytest.mark.asyncio
async def test_process_mutable_no_channel_id_no_bundle_returns_early():
    '''_process_mutable returns early when no bundle exists and channel_id absent.'''
    dispatcher = make_dispatcher()

    # No bundle in store, no channel_id → early return, lock must be released
    await dispatcher._process_mutable('newkey', {  # pylint: disable=protected-access
        'key': 'newkey', 'guild_id': 1, 'content': ['hi'],
    })

    # Queue is empty (no re-enqueue), lock was released
    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert q.empty()


@pytest.mark.asyncio
async def test_process_mutable_creates_new_bundle_and_sends():
    '''_process_mutable creates a bundle and sends when none exists.'''
    channel = FakeChannel(id=100)
    dispatcher = make_dispatcher(channels=[channel])

    await dispatcher._process_mutable('k', {  # pylint: disable=protected-access
        'key': 'k', 'guild_id': 1, 'content': ['hello'], 'channel_id': 100, 'sticky': False,
    })

    assert len(channel.messages) == 1
    assert channel.messages[0].content == 'hello'


@pytest.mark.asyncio
async def test_process_mutable_loads_existing_bundle():
    '''_process_mutable loads an existing bundle from the store and sends.'''
    channel = FakeChannel(id=100)
    dispatcher = make_dispatcher(channels=[channel])

    # Pre-seed a bundle in the store
    bundle = MessageMutableBundle(guild_id=1, channel_id=100, sticky_messages=False)
    await dispatcher._bundle_store.save('k', bundle.to_dict())  # pylint: disable=protected-access

    await dispatcher._process_mutable('k', {  # pylint: disable=protected-access
        'key': 'k', 'guild_id': 1, 'content': ['world'], 'channel_id': 100,
    })

    assert len(channel.messages) == 1


# ---------------------------------------------------------------------------
# In-process fetch: history and emojis via asyncio.Event
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_request_history_delivers_channel_history_result(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(FetchChannelHistoryRequest) delivers ChannelHistoryResult to cog queue.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.start()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    await dispatcher.stop()

    assert isinstance(result, ChannelHistoryResult)
    assert len(result.messages) == 1
    assert isinstance(result.messages[0], FetchedMessage)
    assert result.messages[0].id == msg.id


@pytest.mark.asyncio
async def test_submit_request_history_propagates_exception_as_result(fake_context):  # pylint: disable=redefined-outer-name
    '''When the channel fetch fails, a ChannelHistoryResult with error is delivered.'''
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()  # no channels → fetch_channel returns None → AttributeError
    await dispatcher.start()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=999999, limit=10, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    await dispatcher.stop()

    assert isinstance(result, ChannelHistoryResult)
    assert result.error is not None


@pytest.mark.asyncio
async def test_submit_request_history_with_after_message_id(fake_context):  # pylint: disable=redefined-outer-name
    '''FetchChannelHistoryRequest with after_message_id is handled correctly.'''
    channel = fake_context['channel']
    guild_id = fake_context['guild'].id
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]
    dispatcher = make_dispatcher(channels=[channel])
    await dispatcher.start()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchChannelHistoryRequest(
        guild_id=guild_id, channel_id=channel.id, limit=10, cog_name=cog_name,
        after_message_id=msg.id,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    await dispatcher.stop()

    assert isinstance(result, ChannelHistoryResult)
    assert result.after_message_id == msg.id


@pytest.mark.asyncio
async def test_emoji_delivers_guild_emojis_result(fake_context):  # pylint: disable=redefined-outer-name
    '''submit_request(FetchGuildEmojisRequest) delivers GuildEmojisResult to cog queue.'''
    guild = fake_context['guild']
    guild_id = guild.id
    fake_emoji = MagicMock()
    fake_emoji.id = 1
    fake_emoji.name = 'wave'
    fake_emoji.animated = False
    guild.emojis = [fake_emoji]
    dispatcher = make_dispatcher()
    dispatcher.bot.guilds = [guild]
    await dispatcher.start()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchGuildEmojisRequest(
        guild_id=guild_id, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    await dispatcher.stop()

    assert isinstance(result, GuildEmojisResult)
    assert len(result.emojis) == 1


@pytest.mark.asyncio
async def test_submit_request_emoji_error_delivers_result(fake_context):  # pylint: disable=redefined-outer-name
    '''When guild emoji fetch fails, GuildEmojisResult with error is delivered.'''
    guild_id = fake_context['guild'].id
    dispatcher = make_dispatcher()  # no guilds → fetch_guild returns None → AttributeError
    await dispatcher.start()

    cog_name = 'testcog'
    result_queue = dispatcher.register_cog_queue(cog_name)
    await dispatcher.submit_request(FetchGuildEmojisRequest(
        guild_id=guild_id, cog_name=cog_name,
    ))
    result = await asyncio.wait_for(result_queue.get(), timeout=5.0)
    await dispatcher.stop()

    assert isinstance(result, GuildEmojisResult)
    assert result.error is not None


# ---------------------------------------------------------------------------
# _do_fetch_history / _do_fetch_emojis — None result guard
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_do_fetch_history_raises_when_get_result_returns_none():
    '''_do_fetch_history raises DispatchRemoteError when work_queue.get_result returns None.'''
    dispatcher = make_dispatcher()

    # Patch get_result to return None, then manually trigger the event after enqueueing.
    async def _fake_get_result(_request_id):
        return None

    with patch.object(dispatcher._work_queue, 'get_result', side_effect=_fake_get_result):  # pylint: disable=protected-access
        # Drive _do_fetch_history: it enqueues and waits for the event.
        # We set the event manually so the coroutine proceeds to get_result.
        async def _trigger_event_after_enqueue():
            await asyncio.sleep(0)  # yield so _do_fetch_history registers its event first
            for event in list(dispatcher._result_events.values()):  # pylint: disable=protected-access
                event.set()

        asyncio.create_task(_trigger_event_after_enqueue())
        with pytest.raises(DispatchRemoteError, match='no result stored'):
            await dispatcher._do_fetch_history(  # pylint: disable=protected-access
                {'guild_id': 1, 'channel_id': 2, 'limit': 10}
            )


@pytest.mark.asyncio
async def test_do_fetch_emojis_raises_when_get_result_returns_none():
    '''_do_fetch_emojis raises DispatchRemoteError when work_queue.get_result returns None.'''
    dispatcher = make_dispatcher()

    async def _fake_get_result(_request_id):
        return None

    with patch.object(dispatcher._work_queue, 'get_result', side_effect=_fake_get_result):  # pylint: disable=protected-access
        async def _trigger_event_after_enqueue():
            await asyncio.sleep(0)
            for event in list(dispatcher._result_events.values()):  # pylint: disable=protected-access
                event.set()

        asyncio.create_task(_trigger_event_after_enqueue())
        with pytest.raises(DispatchRemoteError, match='no result stored'):
            await dispatcher._do_fetch_emojis(  # pylint: disable=protected-access
                {'guild_id': 1}
            )


# ---------------------------------------------------------------------------
# enqueue_fetch_history / enqueue_fetch_emojis direct API
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_enqueue_fetch_history_adds_to_queue():
    '''enqueue_fetch_history adds a fetch_history: item to the work queue.'''
    dispatcher = make_dispatcher()
    await dispatcher.enqueue_fetch_history('req-1', 1, 100, 10, None, None, True)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    _pri, _seq, member, _payload = q.get_nowait()
    assert member == 'fetch_history:req-1'


@pytest.mark.asyncio
async def test_enqueue_fetch_emojis_adds_to_queue():
    '''enqueue_fetch_emojis adds a fetch_emojis: item to the work queue.'''
    dispatcher = make_dispatcher()
    await dispatcher.enqueue_fetch_emojis('req-2', 1, 3)
    await asyncio.sleep(0)

    q = dispatcher._work_queue._queue  # pylint: disable=protected-access
    assert not q.empty()
    _pri, _seq, member, _payload = q.get_nowait()
    assert member == 'fetch_emojis:req-2'


# ---------------------------------------------------------------------------
# _dispatch_history_and_collect / _dispatch_emojis_and_collect
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_history_and_collect_basic():
    '''_dispatch_history_and_collect returns messages from the channel.'''
    channel = FakeChannel(id=77)
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]

    dispatcher = make_dispatcher(channels=[channel])
    result = await dispatcher._dispatch_history_and_collect({  # pylint: disable=protected-access
        'guild_id': channel.guild.id,
        'channel_id': channel.id,
        'limit': 10,
        'after': None,
        'after_message_id': None,
        'oldest_first': True,
    })

    assert result['channel_id'] == channel.id
    assert len(result['messages']) == 1


@pytest.mark.asyncio
async def test_dispatch_history_and_collect_with_after():
    '''_dispatch_history_and_collect parses the after ISO string into a datetime.'''
    channel = FakeChannel(id=77)
    msg = FakeMessage(channel=channel)
    channel.messages = [msg]

    dispatcher = make_dispatcher(channels=[channel])
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

    result = await dispatcher._dispatch_emojis_and_collect({  # pylint: disable=protected-access
        'guild_id': fake_guild.id, 'max_retries': 1,
    })

    assert result['guild_id'] == fake_guild.id
    assert result['emojis'] == [{'id': 42, 'name': 'wave', 'animated': True}]


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


@pytest.mark.asyncio
async def test_message_context_edit_message_channel_not_found():
    '''edit_message returns False when get_channel returns None.'''
    ctx = MessageContext(guild_id=1, channel_id=100, message_id=999)
    result = await ctx.edit_message(lambda _: None, content='new')
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
    hist_match.id = 42
    hist_extra = MagicMock()
    hist_extra.id = 99

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
    hist.id = 999

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

    assert len(funcs) == 2  # 1 delete (message_id=10) + 1 send ('x')
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

    funcs = bundle.get_message_dispatch(['a', 'd'], MagicMock(), lambda _: MagicMock())

    assert len(funcs) == 2  # 1 delete + 1 edit
    assert len(bundle.message_contexts) == 2
    assert {ctx.message_content for ctx in bundle.message_contexts} == {'a', 'd'}


def test_get_message_dispatch_equal_keeps_matching_position():
    '''Same-position content match produces no edit func; mismatched position is edited.'''
    bundle = MessageMutableBundle(1, 100)
    bundle.message_contexts = [
        MessageContext(guild_id=1, channel_id=100, message_id=10, message_content='a'),
        MessageContext(guild_id=1, channel_id=100, message_id=20, message_content='b'),
    ]

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

    assert len(funcs) == 3  # 1 edit + 2 sends
    assert len(bundle.message_contexts) == 3
    assert bundle.message_contexts[0].message_content == 'new'
    assert bundle.message_contexts[1].message_content == 'b'
    assert bundle.message_contexts[2].message_content == 'c'


# ---------------------------------------------------------------------------
# MessageMutableBundle serialization
# ---------------------------------------------------------------------------

def test_bundle_serialization_roundtrip():
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
