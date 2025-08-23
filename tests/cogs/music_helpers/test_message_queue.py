from datetime import datetime, timezone, timedelta
from functools import partial

import pytest

from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageLifecycleStage, MessageType

from tests.helpers import FakeContext, fake_source_dict, generate_fake_context


def update_message_references(bundle, messages):
    """Helper function to update message references in tests"""
    for i, message in enumerate(messages):
        if i < len(bundle.message_contexts) and message and hasattr(message, 'id'):
            bundle.message_contexts[i].set_message(message)


def test_message_send_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND


def test_message_send_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.DELETE, x.message_context.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None


def test_message_send_to_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.DELETE, x.message_context.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None


def test_message_edit_to_edit_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content', delete_after=5)
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Second edited content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Second edited content'


def test_message_edit_to_delete_override():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Original message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, x.message_context.edit_message, 'Edited message content', delete_after=5)
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.DELETE, x.message_context.delete_message, '')
    result = mq.get_next_single_mutable()
    assert result is None


def test_single_message():
    mq = MessageQueue()
    c = FakeContext()
    func = partial(c.send, 'Sending test message')
    mq.send_single_immutable(func)
    result = mq.get_single_immutable()
    assert result == func


def test_multiple_send_messages_return_order():
    mq = MessageQueue()
    fake_context = generate_fake_context()
    x = fake_source_dict(fake_context)
    y = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    message2 = MessageContext(fake_context['guild'], fake_context['channel'])
    y.message_context = message2
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'First message content', delete_after=5)
    mq.update_single_mutable(y.message_context, MessageLifecycleStage.SEND, partial(fake_context['context'].send), 'Second message content')
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.EDIT, partial(x.message_context.edit_message), 'Edited message content')
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Edited message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Second message content'
    assert result.lifecycle_stage == MessageLifecycleStage.SEND


def test_multiple_mutable_bundle_order():
    """Test that multiple mutable bundles are processed in chronological order"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create first bundle
    mq.update_multiple_mutable('bundle-1234', fake_context['channel'])
    # Create second bundle
    mq.update_multiple_mutable('bundle-2345', fake_context['channel'])
    # Update first bundle again (should queue it for processing again)
    mq.update_multiple_mutable('bundle-1234', fake_context['channel'])

    # Should return bundle-1234 first (oldest created_at, never sent)
    assert 'bundle-1234' == mq.get_next_multiple_mutable()
    # Then bundle-2345 (next oldest created_at, never sent)
    assert 'bundle-2345' == mq.get_next_multiple_mutable()
    # Then nothing
    assert mq.get_next_multiple_mutable() is None


def test_return_order():
    """Test the priority order of get_next_message"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Add multiple mutable bundle
    mq.update_multiple_mutable('bundle-1234', fake_context['channel'])

    # Add single mutable message
    x = fake_source_dict(fake_context)
    message = MessageContext(fake_context['guild'], fake_context['channel'])
    x.message_context = message
    mq.update_single_mutable(x.message_context, MessageLifecycleStage.SEND,
                            partial(fake_context['context'].send), 'First message content', delete_after=5)

    # Add single immutable message
    func = partial(fake_context['context'].send, 'Sending test message')
    mq.send_single_immutable([func])

    # Should prioritize multiple mutable first
    assert mq.get_next_message() == (MessageType.MULTIPLE_MUTABLE, 'bundle-1234')

    # Then single mutable
    msg_type, result = mq.get_next_message()
    assert msg_type == MessageType.SINGLE_MUTABLE
    assert result.message_content == 'First message content'

    # Then single immutable
    msg_type, funcs = mq.get_next_message()
    assert msg_type == MessageType.SINGLE_IMMUTABLE
    assert funcs == [func]

    # Then nothing
    msg_type, item = mq.get_next_message()
    assert msg_type is None
    assert item is None


def test_mutable_bundle_persistence():
    """Test that mutable bundles persist and can be reused"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "play-order-12345"

    # Register via update_multiple_mutable
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Verify it exists by checking if we can get it as next message
    result = mq.get_next_multiple_mutable()
    assert result == index_name

    # Register again - should reuse the same bundle
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Should be available again
    result = mq.get_next_multiple_mutable()
    assert result == index_name


@pytest.mark.asyncio
async def test_update_mutable_bundle_content():
    """Test updating mutable bundle content"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "play-order-12345"

    # Register bundle
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Update with initial content
    content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Should return dispatch functions
    assert len(dispatch_functions) == 2

    # Execute functions
    for func in dispatch_functions:
        await func()

    # Check messages were created
    assert len(fake_context['channel'].messages) == 2


@pytest.mark.asyncio
async def test_update_nonexistent_mutable_bundle():
    """Test updating a bundle that doesn't exist"""
    mq = MessageQueue()

    content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content("non-existent", content)

    # Should return empty list
    assert len(dispatch_functions) == 0


@pytest.mark.asyncio
async def test_update_mutable_bundle_channel():
    """Test updating the channel for a mutable bundle"""
    mq = MessageQueue()
    fake_context1 = generate_fake_context()
    fake_context2 = generate_fake_context()

    index_name = "play-order-12345"

    # Register bundle with first channel and add some content
    mq.update_multiple_mutable(index_name, fake_context1['channel'])
    content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute functions to create messages in first channel
    for func in dispatch_functions:
        await func()

    assert len(fake_context1['channel'].messages) == 2
    assert len(fake_context2['channel'].messages) == 0

    # Update to second channel
    result = await mq.update_mutable_bundle_channel(index_name, fake_context2['channel'])
    assert result is True

    # Original channel should still have messages (they were "deleted" but our mock doesn't actually remove)
    # But the bundle should now be configured for the new channel

    # Add new content - should go to second channel
    new_content = ["New Message 1"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    for func in dispatch_functions:
        await func()

    # New message should be in second channel
    assert len(fake_context2['channel'].messages) == 1


@pytest.mark.asyncio
async def test_update_mutable_bundle_channel_nonexistent():
    """Test updating channel for non-existent bundle"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    result = await mq.update_mutable_bundle_channel("non-existent", fake_context['channel'])
    assert result is False


@pytest.mark.asyncio
async def test_sticky_messages_clear_scenario():
    """Test sticky message clearing when messages are not at end of channel"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-sticky-bundle"

    # Register bundle and add content
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Sticky Message 1", "Sticky Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute functions to create messages
    for func in dispatch_functions:
        await func()

    # Simulate messages being displaced by other messages
    await fake_context['channel'].send("Interrupting message")

    # Update content again - should trigger clearing logic
    new_content = ["New Message 1"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    # Should have returned dispatch functions despite clearing
    assert len(dispatch_functions) > 0


def test_send_single_immutable_empty_list():
    """Test sending empty function list returns True early"""
    mq = MessageQueue()

    result = mq.send_single_immutable([])
    assert result is True

    # Queue should still be empty
    assert mq.get_single_immutable() is None


def test_multiple_bundles_timestamp_comparison():
    """Test that multiple bundles are ordered correctly by timestamp"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create bundles with different timestamps by manipulating the created_at
    mq.update_multiple_mutable('bundle-new', fake_context['channel'])
    mq.update_multiple_mutable('bundle-old', fake_context['channel'])

    # Manually set different created_at times to force timestamp comparison
    old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    new_time = datetime.now(timezone.utc) - timedelta(minutes=5)

    mq.mutable_bundles['bundle-old'].created_at = old_time
    mq.mutable_bundles['bundle-new'].created_at = new_time

    # Should return older bundle first
    assert mq.get_next_multiple_mutable() == 'bundle-old'
    # Then newer bundle
    assert mq.get_next_multiple_mutable() == 'bundle-new'


def test_single_mutable_edit_lifecycle():
    """Test EDIT lifecycle stage logic in update_single_mutable"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    message = MessageContext(fake_context['guild'], fake_context['channel'])

    # Start with SEND
    mq.update_single_mutable(message, MessageLifecycleStage.SEND,
                           partial(fake_context['context'].send), 'Original message')

    # Change to EDIT stage manually to test edit logic
    message.lifecycle_stage = MessageLifecycleStage.EDIT
    mq.single_mutable_queue[str(message.uuid)] = message

    # Update with another EDIT - should trigger edit-to-edit logic
    result = mq.update_single_mutable(message, MessageLifecycleStage.EDIT,
                                    message.edit_message, 'Edited again')
    assert result is True
    assert mq.single_mutable_queue[str(message.uuid)].message_content == 'Edited again'


def test_single_mutable_edit_to_delete_lifecycle():
    """Test EDIT to DELETE lifecycle transition"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    message = MessageContext(fake_context['guild'], fake_context['channel'])

    # Start with EDIT
    message.lifecycle_stage = MessageLifecycleStage.EDIT
    mq.single_mutable_queue[str(message.uuid)] = message

    # Update with DELETE - should trigger edit-to-delete logic
    result = mq.update_single_mutable(message, MessageLifecycleStage.DELETE,
                                    message.delete_message, '')
    assert result is True

    stored_message = mq.single_mutable_queue[str(message.uuid)]
    assert stored_message.lifecycle_stage == MessageLifecycleStage.DELETE
    assert stored_message.message_content is None


def test_single_mutable_timestamp_comparison():
    """Test single mutable queue timestamp ordering with multiple items"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create multiple messages with different timestamps
    message1 = MessageContext(fake_context['guild'], fake_context['channel'])
    message2 = MessageContext(fake_context['guild'], fake_context['channel'])

    # Manually set different created_at times
    old_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    new_time = datetime.now(timezone.utc) - timedelta(minutes=5)

    message1.created_at = new_time  # Newer
    message2.created_at = old_time  # Older

    # Add to queue
    mq.update_single_mutable(message1, MessageLifecycleStage.SEND,
                           partial(fake_context['context'].send), 'New message')
    mq.update_single_mutable(message2, MessageLifecycleStage.SEND,
                           partial(fake_context['context'].send), 'Old message')

    # Should return older message first
    result = mq.get_next_single_mutable()
    assert result.message_content == 'Old message'

    # Then newer message
    result = mq.get_next_single_mutable()
    assert result.message_content == 'New message'


@pytest.mark.asyncio
async def test_update_mutable_bundle_channel_with_messages():
    """Test updating bundle channel when bundle has existing messages"""
    mq = MessageQueue()
    fake_context1 = generate_fake_context()
    fake_context2 = generate_fake_context()

    index_name = "test-channel-update"

    # Create bundle with messages
    mq.update_multiple_mutable(index_name, fake_context1['channel'])
    content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute to create messages
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Update bundle to track message references
    bundle = mq.mutable_bundles[index_name]
    update_message_references(bundle, results)

    # Now update channel - should trigger delete function execution
    result = await mq.update_mutable_bundle_channel(index_name, fake_context2['channel'])
    assert result is True


@pytest.mark.asyncio
async def test_sticky_clear_with_messages():
    """Test sticky clear scenario that actually executes clear functions"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-clear-execution"

    # Create bundle with messages
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute to create messages and set references
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    bundle = mq.mutable_bundles[index_name]
    update_message_references(bundle, results)

    # Add an interrupting message to trigger sticky clearing
    await fake_context['channel'].send("Interrupting message")

    # Update content - should trigger clear function execution (line 91)
    new_content = ["New Message"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)
    assert len(dispatch_functions) > 0


def test_single_mutable_update_failure():
    """Test the failure case in update_single_mutable that returns False"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    message = MessageContext(fake_context['guild'], fake_context['channel'])

    # Create a scenario that should return False
    # Set up message with DELETE lifecycle stage
    message.lifecycle_stage = MessageLifecycleStage.DELETE
    mq.single_mutable_queue[str(message.uuid)] = message

    # Try to update with SEND stage - should return False (line 199)
    result = mq.update_single_mutable(message, MessageLifecycleStage.SEND,
                                    partial(fake_context['context'].send), 'New message')
    assert result is False


def test_message_queue_creates_sticky_bundles_by_default():
    """Test that MessageQueue creates bundles with sticky_messages=True by default"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-sticky-default"

    # Register bundle
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Bundle should be created with sticky_messages=True by default
    bundle = mq.mutable_bundles[index_name]
    assert bundle.sticky_messages is True


@pytest.mark.asyncio
async def test_message_queue_sticky_deletion_integration():
    """Test that MessageQueue properly handles sticky deletion through update_mutable_bundle_content"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-sticky-deletion"

    # Register bundle
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Send initial content
    initial_content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, initial_content)

    # Execute functions to create messages
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Update bundle to track message references
    bundle = mq.mutable_bundles[index_name]
    update_message_references(bundle, results)

    # Verify initial state
    assert len(fake_context['channel'].messages) == 2
    original_message_ids = [msg.id for msg in fake_context['channel'].messages]

    # Add interrupting message to trigger sticky behavior
    await fake_context['channel'].send("Interrupting message")
    assert len(fake_context['channel'].messages) == 3

    # Send new content - should trigger deletion and new sends
    new_content = ["New Message 1", "New Message 2"]
    new_dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    # Execute new dispatch functions
    new_results = []
    for func in new_dispatch_functions:
        result = await func()
        if result and hasattr(result, 'id'):
            new_results.append(result)

    # Update message references for new messages
    update_message_references(bundle, new_results)

    # Verify that original messages were deleted and new ones exist
    current_messages = fake_context['channel'].messages
    current_message_ids = [msg.id for msg in current_messages]

    # Original messages should be gone
    for original_id in original_message_ids:
        assert original_id not in current_message_ids, f"Original message {original_id} should have been deleted"

    # New content should exist
    new_message_contents = [msg.content for msg in current_messages if msg.content in new_content]
    assert "New Message 1" in new_message_contents
    assert "New Message 2" in new_message_contents


@pytest.mark.asyncio
async def test_message_queue_update_references():
    """Test that update_mutable_bundle_references properly sets message references"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-references"

    # Register bundle and add content
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Message 1", "Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute functions to create messages
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Update references using the new method (filter to only Message objects)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    success = await mq.update_mutable_bundle_references(index_name, message_results)
    assert success

    # Verify that message references were set
    bundle = mq.mutable_bundles[index_name]
    assert len(bundle.message_contexts) == 2
    assert bundle.message_contexts[0].message is not None
    assert bundle.message_contexts[1].message is not None
    assert bundle.message_contexts[0].message.content == "Message 1"
    assert bundle.message_contexts[1].message.content == "Message 2"


@pytest.mark.asyncio
async def test_message_queue_update_references_nonexistent():
    """Test that update_mutable_bundle_references handles nonexistent bundles"""
    mq = MessageQueue()

    success = await mq.update_mutable_bundle_references("nonexistent", [])
    assert success is False


@pytest.mark.asyncio
async def test_message_queue_full_workflow_with_sticky():
    """Test the complete real-world workflow including sticky message handling"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-full-workflow"

    # STEP 1: Initial message send (simulating first queue display)
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Song 1: Artist - Title", "Song 2: Another Artist - Another Title"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute and update references (simulating Music.cog workflow)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    # Verify initial state
    bundle = mq.mutable_bundles[index_name]
    assert len(fake_context['channel'].messages) == 2
    assert bundle.message_contexts[0].message is not None
    assert bundle.message_contexts[1].message is not None
    original_message_ids = [msg.id for msg in fake_context['channel'].messages]

    # STEP 2: User sends a message, displacing our queue messages
    await fake_context['channel'].send("User typed !skip command")
    assert len(fake_context['channel'].messages) == 3

    # STEP 3: Queue changes, triggering message update (simulating after !skip)
    new_content = ["Song 2: Another Artist - Another Title", "Song 3: Third Artist - Third Title"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    # Execute and update references
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    # STEP 4: Verify sticky behavior worked correctly
    current_messages = fake_context['channel'].messages
    current_message_ids = [msg.id for msg in current_messages]

    # Original queue messages should be deleted
    for original_id in original_message_ids:
        assert original_id not in current_message_ids, f"Original message {original_id} should have been deleted"

    # New messages should exist with updated content
    queue_messages = [msg for msg in current_messages if "Song" in msg.content]
    assert len(queue_messages) == 2
    assert "Song 2: Another Artist - Another Title" in [msg.content for msg in queue_messages]
    assert "Song 3: Third Artist - Third Title" in [msg.content for msg in queue_messages]

    # Bundle should have proper message references for future sticky checks
    assert len(bundle.message_contexts) == 2
    assert bundle.message_contexts[0].message is not None
    assert bundle.message_contexts[1].message is not None


@pytest.mark.asyncio
async def test_message_queue_sticky_behavior_integration():
    """Test sticky behavior through the message queue interface"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-sticky-integration"

    # Register bundle and add content
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Sticky Message 1", "Sticky Message 2"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Execute functions to create messages
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Update bundle to track message references (using new method)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    # Add interrupting message to displace our messages
    await fake_context['channel'].send("Interrupting message")

    # Now our messages are no longer at end - sticky check should return True
    bundle = mq.mutable_bundles[index_name]
    should_clear = await bundle.should_clear_messages()
    assert should_clear

    # Update content - should trigger clearing and resending due to sticky behavior
    new_content = ["New Sticky Message"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)
    assert len(dispatch_functions) > 0  # Should have dispatch functions to send new content


@pytest.mark.asyncio
async def test_message_queue_small_bundle_no_sticky_clearing():
    """Test that 1-2 message bundles don't trigger sticky clearing, confirming the bug doesn't occur"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-small-bundle"

    # STEP 1: Create bundle with only 1 message
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Single message"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    bundle = mq.mutable_bundles[index_name]
    assert len(bundle.message_contexts) == 1
    assert bundle.message_contexts[0].message is not None
    original_message_id = bundle.message_contexts[0].message.id

    # STEP 2: Add user message to displace our message
    await fake_context['channel'].send("User interruption")

    # STEP 3: Verify sticky clearing is NOT triggered (this is why the bug doesn't manifest with <3 messages)
    should_clear = await bundle.should_clear_messages()
    assert not should_clear, "Single message should NOT trigger sticky clearing - confirming bug doesn't occur with <3 messages"

    # STEP 4: Update bundle content (should use edit, not clearing)
    new_content = ["Updated single message"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    # STEP 5: Verify message reference is the same (edited in place, not recreated)
    assert len(bundle.message_contexts) == 1
    assert bundle.message_contexts[0].message is not None
    new_message_id = bundle.message_contexts[0].message.id
    assert new_message_id == original_message_id  # Should be the same message (edited)
    assert bundle.message_contexts[0].message.content == "Updated single message"


@pytest.mark.asyncio
async def test_message_queue_large_bundle_reference_bug_prevention():
    """Test that 3+ message bundles have proper reference mapping after fix"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-large-bundle"

    # STEP 1: Create bundle with 4 messages (ensures 3+ threshold)
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Message 1", "Message 2", "Message 3", "Message 4"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    bundle = mq.mutable_bundles[index_name]
    assert len(bundle.message_contexts) == 4
    original_message_ids = [ctx.message.id for ctx in bundle.message_contexts]
    original_contents = [ctx.message.content for ctx in bundle.message_contexts]

    # Verify initial setup
    assert "Message 1" in original_contents
    assert "Message 4" in original_contents

    # STEP 2: Add multiple user messages to displace our messages
    await fake_context['channel'].send("User message 1")
    await fake_context['channel'].send("User message 2")
    await fake_context['channel'].send("User message 3")

    # STEP 3: Update bundle content (should trigger sticky clearing and recreate all messages)
    new_content = ["New Message A", "New Message B", "New Message C", "New Message D"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    # Track what operations are happening
    delete_count = 0
    send_count = 0
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
        if isinstance(result, bool):
            delete_count += 1
        elif hasattr(result, 'id'):
            send_count += 1

    # Should have deleted 4 old messages and sent 4 new ones
    assert delete_count == 4, f"Expected 4 delete operations, got {delete_count}"
    assert send_count == 4, f"Expected 4 send operations, got {send_count}"

    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    # STEP 4: Verify all message references are properly mapped (this is where the bug would manifest)
    assert len(bundle.message_contexts) == 4
    new_message_ids = [ctx.message.id for ctx in bundle.message_contexts if ctx.message]
    new_contents = [ctx.message.content for ctx in bundle.message_contexts if ctx.message]

    # All contexts should have new message references (none should be None)
    assert len(new_message_ids) == 4, "All contexts should have message references after sticky clearing"

    # No old message IDs should persist
    for old_id in original_message_ids:
        assert old_id not in new_message_ids, f"Old message ID {old_id} should not persist after sticky clearing"

    # All new content should be present
    assert "New Message A" in new_contents
    assert "New Message B" in new_contents
    assert "New Message C" in new_contents
    assert "New Message D" in new_contents

    # Original content should not persist (this would be the bug symptom)
    for old_content in original_contents:
        assert old_content not in new_contents, f"Old content '{old_content}' should not persist"


@pytest.mark.asyncio
async def test_message_queue_mixed_results_reference_mapping():
    """Test that mixed delete/send results are properly handled in reference mapping"""
    mq = MessageQueue()
    fake_context = generate_fake_context()
    index_name = "test-mixed-results"

    # STEP 1: Create bundle with 3 messages
    mq.update_multiple_mutable(index_name, fake_context['channel'])
    content = ["Initial 1", "Initial 2", "Initial 3"]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    bundle = mq.mutable_bundles[index_name]

    # STEP 2: Interrupt with user message to trigger sticky behavior
    await fake_context['channel'].send("Interrupting message")

    # STEP 3: Update with different number of messages to create mixed operations
    new_content = ["Final A", "Final B"]  # Fewer messages = some deletes, some sends
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, new_content)

    # Manually verify we get expected mixed results
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Should have: [delete_result1, delete_result2, delete_result3, send_message1, send_message2]
    delete_results = [r for r in results if isinstance(r, bool) and r]
    send_results = [r for r in results if hasattr(r, 'id')]

    assert len(delete_results) == 3, "Should delete 3 original messages"
    assert len(send_results) == 2, "Should send 2 new messages"

    # STEP 4: Apply reference mapping and verify correctness
    message_results = [r for r in results if r and hasattr(r, 'id')]
    await mq.update_mutable_bundle_references(index_name, message_results)

    # Should have exactly 2 contexts with proper message references
    assert len(bundle.message_contexts) == 2
    assert all(ctx.message is not None for ctx in bundle.message_contexts)

    # Verify content matches what we sent
    actual_contents = [ctx.message.content for ctx in bundle.message_contexts]
    assert "Final A" in actual_contents
    assert "Final B" in actual_contents
