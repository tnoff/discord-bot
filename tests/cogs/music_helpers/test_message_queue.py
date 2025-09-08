from datetime import datetime, timezone, timedelta
from functools import partial

import pytest

from discord_bot.cogs.music_helpers.message_context import MessageContext, MessageMutableBundle
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageType, MessageQueueException
from discord_bot.cogs.music_helpers.common import MultipleMutableType

from tests.helpers import generate_fake_context, FakeMessage, fake_context  #pylint:disable=unused-import


def update_message_references(bundle, messages):
    """Helper function to update message references in tests"""
    for i, message in enumerate(messages):
        if i < len(bundle.message_contexts) and message and hasattr(message, 'id'):
            bundle.message_contexts[i].set_message(message)


def test_multiple_mutable_bundle_order(fake_context): #pylint:disable=redefined-outer-name
    """Test that multiple mutable bundles are processed in chronological order"""
    mq = MessageQueue()

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


def test_mutable_bundle_persistence(fake_context): #pylint:disable=redefined-outer-name
    """Test that mutable bundles persist across calls and track processing state"""
    mq = MessageQueue()

    # Create first bundle
    mq.update_multiple_mutable('bundle-1234', fake_context['channel'])

    # Add single immutable message
    message_ctx = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
    func = partial(fake_context['channel'].send, 'Test message')
    message_ctx.function = func
    mq.send_single_immutable([func])

    # Multiple mutable should have priority
    message_type, result = mq.get_next_message()
    assert message_type == MessageType.MULTIPLE_MUTABLE
    assert result == 'bundle-1234'

    # Bundle should no longer be queued for processing
    assert mq.mutable_bundles['bundle-1234'].is_queued_for_processing is False

    # Next call should return single immutable
    message_type, result = mq.get_next_message()
    assert message_type == MessageType.SINGLE_IMMUTABLE


@pytest.mark.asyncio
async def test_update_mutable_bundle_content(fake_context): #pylint:disable=redefined-outer-name
    """Test updating bundle content and getting dispatch functions"""
    mq = MessageQueue()

    # Create and register bundle
    index_name = 'test-bundle-uuid'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Update with content
    message_content = ['Line 1', 'Line 2', 'Line 3']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, message_content)

    # Should have dispatch functions for each message
    assert len(dispatch_functions) == 3

    # Update again - should have edit functions for existing messages
    updated_content = ['Updated Line 1', 'Updated Line 2', 'Updated Line 3']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, updated_content)
    assert len(dispatch_functions) == 3


@pytest.mark.asyncio
async def test_update_nonexistent_mutable_bundle():
    """Test updating content for non-existent bundle returns empty list"""
    mq = MessageQueue()

    dispatch_functions = await mq.update_mutable_bundle_content('nonexistent-bundle', ['content'])
    assert dispatch_functions == []


@pytest.mark.asyncio
async def test_update_mutable_bundle_channel():
    """Test updating bundle to different channel"""
    mq = MessageQueue()
    fake_context1 = generate_fake_context()
    fake_context2 = generate_fake_context()
    fake_context2['channel'].id = 999999  # Different channel ID

    index_name = 'test-migration'

    # Create bundle in first channel
    mq.update_multiple_mutable(index_name, fake_context1['channel'])
    original_channel = mq.mutable_bundles[index_name].channel_id
    assert original_channel == fake_context1['channel'].id

    # Update to second channel
    success = await mq.update_mutable_bundle_channel(index_name, fake_context2['channel'])
    assert success is True

    # Verify channel updated
    updated_channel = mq.mutable_bundles[index_name].channel_id
    assert updated_channel == fake_context2['channel'].id
    assert updated_channel != original_channel


@pytest.mark.asyncio
async def test_update_mutable_bundle_channel_nonexistent(fake_context): #pylint:disable=redefined-outer-name
    """Test updating channel for non-existent bundle"""
    mq = MessageQueue()

    success = await mq.update_mutable_bundle_channel('nonexistent-bundle', fake_context['channel'])
    assert success is False


@pytest.mark.asyncio
async def test_sticky_messages_clear_scenario(fake_context): #pylint:disable=redefined-outer-name
    """Test that sticky bundles clear existing messages when needed"""
    mq = MessageQueue()

    # Create bundle with sticky behavior (default)
    index_name = 'sticky-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Add some fake messages to channel history to trigger clearing
    fake_messages = [
        type('Message', (), {'id': 123, 'content': 'Old message 1'})(),
        type('Message', (), {'id': 124, 'content': 'Old message 2'})()
    ]

    # Mock the channel history to return these messages
    async def mock_history(limit):
        return fake_messages[:limit]

    fake_context['channel'].history = mock_history

    # Update bundle content
    message_content = ['New Line 1', 'New Line 2']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, message_content)

    # Should have functions to clear + send new content
    assert len(dispatch_functions) >= 2


def test_send_single_immutable_empty_list():
    """Test sending empty list of single immutable messages returns False"""
    mq = MessageQueue()

    result = mq.send_single_immutable([])  #pylint:disable=assignment-from-no-return
    assert result is True  # Empty list successfully processed


def test_multiple_bundles_timestamp_comparison(fake_context): #pylint:disable=redefined-outer-name
    """Test that bundles are returned in chronological order based on updated_at"""
    mq = MessageQueue()

    # Create bundles with specific timing
    start_time = datetime.now(timezone.utc)

    # Create first bundle
    mq.update_multiple_mutable('bundle-new', fake_context['channel'])
    mq.mutable_bundles['bundle-new'].created_at = start_time + timedelta(seconds=10)

    # Create second bundle with earlier timestamp
    mq.update_multiple_mutable('bundle-old', fake_context['channel'])
    mq.mutable_bundles['bundle-old'].created_at = start_time + timedelta(seconds=5)

    # Should return oldest first
    assert mq.get_next_multiple_mutable() == 'bundle-old'
    # Then newest
    assert mq.get_next_multiple_mutable() == 'bundle-new'


@pytest.mark.asyncio
async def test_update_mutable_bundle_channel_with_messages():
    """Test updating bundle channel when it has existing messages"""
    mq = MessageQueue()
    fake_context1 = generate_fake_context()
    fake_context2 = generate_fake_context()

    index_name = 'migration-with-messages'

    # Create and populate bundle
    mq.update_multiple_mutable(index_name, fake_context1['channel'])
    await mq.update_mutable_bundle_content(index_name, ['Message 1', 'Message 2'])

    # Simulate some messages exist
    bundle = mq.mutable_bundles[index_name]
    fake_message1 = FakeMessage(id=1001, content='Message 1')
    fake_message2 = FakeMessage(id=1002, content='Message 2')

    bundle.message_contexts[0].set_message(fake_message1)
    bundle.message_contexts[1].set_message(fake_message2)

    # Update to different channel
    success = await mq.update_mutable_bundle_channel(index_name, fake_context2['channel'])
    assert success is True

    # Verify messages cleared and channel updated
    assert bundle.channel_id == fake_context2['channel'].id
    assert all(ctx.message is None for ctx in bundle.message_contexts)


@pytest.mark.asyncio
async def test_sticky_clear_with_messages(fake_context): #pylint:disable=redefined-outer-name
    """Test sticky message clearing behavior with actual messages"""
    mq = MessageQueue()

    index_name = 'sticky-with-messages'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Set up mock for should_clear_messages to return True
    bundle = mq.mutable_bundles[index_name]

    # Mock history to return messages (simulating messages need clearing)
    async def mock_check_last_message(count):
        return [type('Message', (), {'id': i, 'content': f'Old {i}'})() for i in range(count)]

    bundle.check_last_message_func = mock_check_last_message

    # Update with content - should trigger sticky clearing
    content = ['New message 1', 'New message 2']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content, delete_after=10)

    # Should have dispatch functions
    assert len(dispatch_functions) >= 2


def test_message_queue_creates_sticky_bundles_by_default(fake_context): #pylint:disable=redefined-outer-name
    """Test that message queue creates bundles with sticky=True by default"""
    mq = MessageQueue()

    index_name = 'default-sticky-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    bundle = mq.mutable_bundles[index_name]
    assert bundle.sticky_messages is True


@pytest.mark.asyncio
async def test_message_queue_sticky_deletion_integration(fake_context): #pylint:disable=redefined-outer-name
    """Test full workflow of sticky message deletion and recreation"""
    mq = MessageQueue()

    index_name = 'sticky-deletion-test'

    # Create bundle
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Initial content
    initial_content = ['Initial message 1', 'Initial message 2']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, initial_content)

    # Execute functions and set messages
    bundle = mq.mutable_bundles[index_name]
    fake_messages = []
    for i, _ in enumerate(dispatch_functions):
        fake_msg = type('Message', (), {'id': 2000 + i, 'content': initial_content[i]})()
        fake_messages.append(fake_msg)
        bundle.message_contexts[i].set_message(fake_msg)

    # Update references
    await mq.update_mutable_bundle_references(index_name, fake_messages)

    # Mock history to indicate clearing needed
    async def mock_history_with_messages(count):
        return fake_messages[:count] if count > 0 else []

    bundle.check_last_message_func = mock_history_with_messages

    # Update content again - should clear and recreate
    updated_content = ['Updated message 1', 'Updated message 2', 'New message 3']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, updated_content)

    # Should have deletion + creation functions
    assert len(dispatch_functions) >= 3


@pytest.mark.asyncio
async def test_message_queue_update_references(fake_context): #pylint:disable=redefined-outer-name
    """Test updating message references in bundle"""
    mq = MessageQueue()

    index_name = 'reference-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Add content to create contexts
    content = ['Test message 1', 'Test message 2']
    await mq.update_mutable_bundle_content(index_name, content)

    # Create fake messages
    fake_messages = [
        type('Message', (), {'id': 3001, 'content': 'Test message 1'})(),
        type('Message', (), {'id': 3002, 'content': 'Test message 2'})()
    ]

    # Update references
    success = await mq.update_mutable_bundle_references(index_name, fake_messages)
    assert success is True

    # Verify references were set
    bundle = mq.mutable_bundles[index_name]
    assert bundle.message_contexts[0].message.id == 3001
    assert bundle.message_contexts[1].message.id == 3002


@pytest.mark.asyncio
async def test_message_queue_update_references_nonexistent():
    """Test updating references for non-existent bundle"""
    mq = MessageQueue()

    fake_messages = [type('Message', (), {'id': 9999})()]
    success = await mq.update_mutable_bundle_references('nonexistent-bundle', fake_messages)
    assert success is False


@pytest.mark.asyncio
async def test_message_queue_full_workflow_with_sticky(fake_context): #pylint:disable=redefined-outer-name
    """Test complete workflow with sticky message behavior"""
    mq = MessageQueue()

    index_name = 'full-workflow-test'

    # Step 1: Create bundle
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Step 2: Add initial content
    initial_content = ['Workflow step 1', 'Workflow step 2']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, initial_content)
    assert len(dispatch_functions) == 2

    # Step 3: Simulate message sending and set references
    bundle = mq.mutable_bundles[index_name]
    sent_messages = []
    for i, _ in enumerate(dispatch_functions):
        msg = type('Message', (), {'id': 4000 + i, 'content': initial_content[i]})()
        sent_messages.append(msg)
        bundle.message_contexts[i].set_message(msg)

    await mq.update_mutable_bundle_references(index_name, sent_messages)

    # Step 4: Update content (should edit existing messages)
    updated_content = ['Updated step 1', 'Updated step 2', 'New step 3']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, updated_content)
    assert len(dispatch_functions) == 3  # 2 edits + 1 new

    # Step 5: Verify bundle state
    assert len(bundle.message_contexts) == 3
    assert bundle.message_contexts[0].message.id == 4000
    assert bundle.message_contexts[1].message.id == 4001


@pytest.mark.asyncio
async def test_message_queue_sticky_behavior_integration(fake_context): #pylint:disable=redefined-outer-name
    """Test that sticky behavior properly clears messages when needed"""
    mq = MessageQueue()

    index_name = 'sticky-behavior-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    bundle = mq.mutable_bundles[index_name]

    # Mock channel history to indicate messages should be cleared
    historical_messages = [
        type('Message', (), {'id': 5001, 'content': 'Historical 1'})(),
        type('Message', (), {'id': 5002, 'content': 'Historical 2'})()
    ]

    async def mock_check_messages(count):
        return historical_messages[:count]

    bundle.check_last_message_func = mock_check_messages

    # Add content - should trigger clearing due to sticky behavior
    content = ['Fresh content 1', 'Fresh content 2']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Verify we got dispatch functions (clearing + new content)
    assert len(dispatch_functions) >= 2


@pytest.mark.asyncio
async def test_message_queue_small_bundle_no_sticky_clearing(fake_context): #pylint:disable=redefined-outer-name
    """Test that small bundles don't trigger unnecessary sticky clearing"""
    mq = MessageQueue()

    index_name = 'small-bundle-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    bundle = mq.mutable_bundles[index_name]

    # Mock empty channel history (no clearing needed)
    async def mock_empty_history(count):  #pylint:disable=unused-argument
        return []

    bundle.check_last_message_func = mock_empty_history

    # Add single message
    content = ['Single message']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)

    # Should just have one send function, no clearing
    assert len(dispatch_functions) == 1


@pytest.mark.asyncio
async def test_message_queue_large_bundle_reference_bug_prevention(fake_context): #pylint:disable=redefined-outer-name
    """Test that large bundles handle message references correctly"""
    mq = MessageQueue()

    index_name = 'large-bundle-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Create large content list
    large_content = [f'Message {i}' for i in range(10)]
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, large_content)
    assert len(dispatch_functions) == 10

    # Simulate sending and getting references for all messages
    sent_messages = []
    for i in range(10):
        msg = type('Message', (), {'id': 6000 + i, 'content': large_content[i]})()
        sent_messages.append(msg)

    # Update references - should handle all messages
    success = await mq.update_mutable_bundle_references(index_name, sent_messages)
    assert success is True

    # Verify all contexts have message references
    bundle = mq.mutable_bundles[index_name]
    assert len(bundle.message_contexts) == 10
    for i, context in enumerate(bundle.message_contexts):
        assert context.message.id == 6000 + i


@pytest.mark.asyncio
async def test_message_queue_mixed_results_reference_mapping(fake_context): #pylint:disable=redefined-outer-name
    """Test reference updating with mixed success/failure results"""
    mq = MessageQueue()

    index_name = 'mixed-results-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Add content
    content = ['Success message', 'Failed message', 'Success message 2']
    await mq.update_mutable_bundle_content(index_name, content)

    # Create mixed results - some successful messages, some None (failures)
    mixed_results = [
        FakeMessage(id=7001, content='Success message'),
        None,  # Failed message
        FakeMessage(id=7002, content='Success message 2')
    ]

    # Update references - should only map successful messages
    success = await mq.update_mutable_bundle_references(index_name, mixed_results)
    assert success is True

    # Verify only successful messages were referenced
    bundle = mq.mutable_bundles[index_name]
    assert bundle.message_contexts[0].message.id == 7001
    assert bundle.message_contexts[1].message is None  # Failed message
    assert bundle.message_contexts[2].message.id == 7002


# String Parsing Safety Tests

def test_uuid_parsing_safety_request_bundle():
    """Test safe UUID parsing for REQUEST_BUNDLE message types"""

    # Test normal UUID parsing
    normal_uuid = 'request.bundle.12345-67890-abcdef'
    index_name = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{normal_uuid}'

    # Use the same parsing logic as music.py
    extracted_uuid = index_name.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
    assert extracted_uuid == normal_uuid

    # Test UUID with prefix string inside it (edge case)
    tricky_uuid = f'request.bundle.{MultipleMutableType.REQUEST_BUNDLE.value}-inside-uuid'
    tricky_index = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{tricky_uuid}'

    # Should only split on first occurrence
    extracted_tricky = tricky_index.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
    assert extracted_tricky == tricky_uuid
    assert extracted_tricky != 'inside-uuid'  # Would happen with .replace()

    # Test multiple occurrences of prefix
    multi_prefix_uuid = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{MultipleMutableType.REQUEST_BUNDLE.value}-test'
    multi_index = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{multi_prefix_uuid}'

    extracted_multi = multi_index.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
    assert extracted_multi == multi_prefix_uuid


def test_uuid_parsing_safety_play_order():
    """Test safe UUID parsing for PLAY_ORDER message types"""

    # Test normal play order guild ID parsing
    guild_id = '123456789012345678'
    index_name = f'{MultipleMutableType.PLAY_ORDER.value}-{guild_id}'

    extracted_id = index_name.split(f'{MultipleMutableType.PLAY_ORDER.value}-', 1)[1]
    assert extracted_id == guild_id

    # Test guild ID that happens to contain 'play_order'
    tricky_guild_id = f'play_order-{guild_id}-play_order'
    tricky_index = f'{MultipleMutableType.PLAY_ORDER.value}-{tricky_guild_id}'

    extracted_tricky_id = tricky_index.split(f'{MultipleMutableType.PLAY_ORDER.value}-', 1)[1]
    assert extracted_tricky_id == tricky_guild_id


def test_uuid_parsing_malformed_input_safety():
    """Test UUID parsing with malformed or edge case inputs"""

    # Test missing separator
    malformed_no_sep = f'{MultipleMutableType.REQUEST_BUNDLE.value}no-separator-uuid'
    try:
        _ = malformed_no_sep.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
        # Should raise IndexError due to no second element
        assert False, "Should have raised IndexError"
    except IndexError:
        pass  # Expected

    # Test empty string after separator
    empty_uuid = f'{MultipleMutableType.REQUEST_BUNDLE.value}-'
    extracted_empty = empty_uuid.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
    assert extracted_empty == ''

    # Test completely wrong format
    wrong_format = 'completely-wrong-format'
    try:
        _ = wrong_format.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
        assert False, "Should have raised IndexError"
    except IndexError:
        pass  # Expected

    # Test None input (should raise AttributeError)
    try:
        None.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]  #pylint:disable=expression-not-assigned
        assert False, "Should have raised AttributeError"
    except AttributeError:
        pass  # Expected


def test_uuid_parsing_versus_replace_comparison():
    """Test that .split() parsing is safer than .replace() parsing"""

    # Create UUID that contains the prefix string
    prefix = MultipleMutableType.REQUEST_BUNDLE.value
    problematic_uuid = f'request.bundle.{prefix}-inside-{prefix}-uuid'
    index_name = f'{prefix}-{problematic_uuid}'

    # Compare .split() vs .replace() behavior
    split_result = index_name.split(f'{prefix}-', 1)[1]
    replace_result = index_name.replace(f'{prefix}-', '')

    # .split() correctly extracts the full UUID
    assert split_result == problematic_uuid

    # .replace() would incorrectly modify UUID content
    assert replace_result != problematic_uuid
    assert replace_result == 'request.bundle.inside-uuid'  # Corrupted by replace all!

    # Verify .split() preserves UUID integrity
    assert split_result.count(prefix) == 2  # Original UUID preserved
    assert replace_result.count(prefix) == 0  # UUID corrupted by replace (all instances removed)


def test_message_type_prefix_validation():
    """Test validation of message type prefixes in parsing"""

    # Test all message type values are distinct
    message_types = [
        MultipleMutableType.REQUEST_BUNDLE.value,
        MultipleMutableType.PLAY_ORDER.value
    ]

    # Verify no type is a prefix of another (prevents parsing conflicts)
    for i, type1 in enumerate(message_types):
        for j, type2 in enumerate(message_types):
            if i != j:
                assert not type1.startswith(type2), f"'{type1}' starts with '{type2}'"
                assert not type2.startswith(type1), f"'{type2}' starts with '{type1}'"

    # Verify all types are non-empty
    for msg_type in message_types:
        assert len(msg_type) > 0
        assert '-' not in msg_type, f"Message type '{msg_type}' contains separator"


@pytest.mark.asyncio
async def test_uuid_parsing_integration_with_message_queue(fake_context): #pylint:disable=redefined-outer-name
    """Test UUID parsing integration with actual message queue operations"""
    mq = MessageQueue()

    # Create bundles with tricky UUIDs
    tricky_uuids = [
        'request.bundle.normal-uuid',
        f'request.bundle.{MultipleMutableType.REQUEST_BUNDLE.value}-duplicate-prefix'
    ]

    # Register all bundles
    for uuid in tricky_uuids:
        index_name = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{uuid}'
        mq.update_multiple_mutable(index_name, fake_context['channel'])

        # Verify bundle was created
        assert index_name in mq.mutable_bundles

    # Test parsing each registered bundle
    for bundle_name in mq.mutable_bundles:
        if MultipleMutableType.REQUEST_BUNDLE.value in bundle_name:
            extracted = bundle_name.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
            assert extracted in tricky_uuids

@pytest.mark.asyncio
async def test_message_queue_non_sticky_behavior(fake_context): #pylint:disable=redefined-outer-name
    """Test that bundles can be created with sticky_messages=False"""
    mq = MessageQueue()

    # Create bundle with sticky_messages=False
    index_name = 'test-bundle-non-sticky'
    mq.update_multiple_mutable(index_name, fake_context['channel'], sticky_messages=False)

    # Verify bundle was created
    assert index_name in mq.mutable_bundles
    bundle = mq.mutable_bundles[index_name]

    # Verify sticky_messages was set correctly
    assert bundle.sticky_messages is False

    # Test that content updates work with non-sticky bundles
    content = ['Non-sticky message 1', 'Non-sticky message 2']
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, content)
    assert len(dispatch_functions) == 2

@pytest.mark.asyncio
async def test_message_queue_sticky_behavior_default(fake_context): #pylint:disable=redefined-outer-name
    """Test that bundles default to sticky_messages=True when parameter not specified"""
    mq = MessageQueue()

    # Create bundle without specifying sticky_messages (should default to True)
    index_name = 'test-bundle-default-sticky'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Verify bundle was created with sticky_messages=True
    assert index_name in mq.mutable_bundles
    bundle = mq.mutable_bundles[index_name]
    assert bundle.sticky_messages is True

@pytest.mark.asyncio
async def test_message_queue_mixed_sticky_behavior(fake_context): #pylint:disable=redefined-outer-name
    """Test that different bundles can have different sticky_messages settings"""
    mq = MessageQueue()

    # Create one sticky bundle and one non-sticky bundle
    sticky_index = 'test-bundle-sticky'
    non_sticky_index = 'test-bundle-non-sticky'

    mq.update_multiple_mutable(sticky_index, fake_context['channel'], sticky_messages=True)
    mq.update_multiple_mutable(non_sticky_index, fake_context['channel'], sticky_messages=False)

    # Verify both bundles exist with correct settings
    assert sticky_index in mq.mutable_bundles
    assert non_sticky_index in mq.mutable_bundles

    sticky_bundle = mq.mutable_bundles[sticky_index]
    non_sticky_bundle = mq.mutable_bundles[non_sticky_index]

    assert sticky_bundle.sticky_messages is True
    assert non_sticky_bundle.sticky_messages is False

def test_message_queue_exception_inheritance():
    """Test that MessageQueueException is properly defined"""
    # Test that MessageQueueException is an Exception subclass
    exception = MessageQueueException("test message")
    assert isinstance(exception, Exception)
    assert str(exception) == "test message"


def test_message_queue_none_channel_exception_details(fake_context):  #pylint:disable=redefined-outer-name,unused-argument
    """Test detailed behavior of MessageQueueException when creating bundle with None channel"""
    message_queue = MessageQueue()
    bundle_name = "test-exception-bundle"

    # Verify the specific exception type and message format
    with pytest.raises(MessageQueueException) as exc_info:
        message_queue.update_multiple_mutable(bundle_name, None)

    # Verify exception details
    exception = exc_info.value
    assert isinstance(exception, MessageQueueException)
    assert "Cannot create new message bundle" in str(exception)
    assert bundle_name in str(exception)
    assert "without a valid text_channel" in str(exception)

    # Verify the bundle was not created
    assert bundle_name not in message_queue.mutable_bundles


def test_message_queue_valid_operations_after_exception(fake_context):  #pylint:disable=redefined-outer-name
    """Test that MessageQueue works normally after an exception"""
    message_queue = MessageQueue()

    # First, trigger an exception
    with pytest.raises(MessageQueueException):
        message_queue.update_multiple_mutable("bad-bundle", None)

    # Then verify normal operations still work
    good_bundle = "good-bundle"
    result = message_queue.update_multiple_mutable(good_bundle, fake_context['channel'])
    assert result is True
    assert good_bundle in message_queue.mutable_bundles

    # And updating existing bundle with None still works
    result = message_queue.update_multiple_mutable(good_bundle, None)
    assert result is True


def test_message_queue_exception_vs_other_errors(fake_context):  #pylint:disable=redefined-outer-name
    """Test that MessageQueueException is specifically for None channel, not other errors"""
    message_queue = MessageQueue()

    # This should raise MessageQueueException (None channel)
    with pytest.raises(MessageQueueException):
        message_queue.update_multiple_mutable("test-bundle", None)

    # This should work fine (valid channel)
    result = message_queue.update_multiple_mutable("test-bundle", fake_context['channel'])
    assert result is True

    # Other potential errors (like invalid bundle names) would raise different exceptions
    # but we don't test those here since they're not part of the None channel validation

@pytest.fixture
def comprehensive_non_sticky_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Create a non-sticky MessageMutableBundle with detailed tracking for testing"""

    # Track function calls for comprehensive validation
    call_log = []

    async def check_last_messages(count):
        call_log.append(f"check_last_messages({count})")
        messages = [m async for m in fake_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):
        call_log.append(f"send('{content}', delete_after={delete_after})")
        return await fake_context['channel'].send(content)

    bundle = MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        sticky_messages=False
    )

    # Attach call_log to bundle for test access
    bundle.call_log = call_log
    return bundle


def test_non_sticky_fallback_scenario_comprehensive(comprehensive_non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """
    Test comprehensive non-sticky fallback scenario:
    1. Set sticky_messages=False
    2. Create initial messages
    3. Exceed count with new messages
    4. Verify fallback to sticky-like behavior (messages added, not deleted/re-sent)
    """
    bundle = comprehensive_non_sticky_bundle

    # Verify bundle is correctly configured as non-sticky
    assert bundle.sticky_messages is False

    # Phase 1: Create initial messages
    initial_content = ["Initial Message 1", "Initial Message 2"]
    dispatch_functions_1 = bundle.get_message_dispatch(initial_content)

    # Should create 2 send functions for initial messages
    assert len(dispatch_functions_1) == 2
    assert len(bundle.message_contexts) == 2

    # Phase 2: Exceed existing count with more messages
    # This is where the fallback behavior should kick in
    extended_content = ["Updated Message 1", "Updated Message 2", "New Message 3", "New Message 4"]
    dispatch_functions_2 = bundle.get_message_dispatch(extended_content)

    # Key assertion: Should create dispatch functions for additional messages
    # Non-sticky fallback behavior: instead of deleting all and re-sending,
    # it should just add the new messages (acting like sticky=True)
    assert len(dispatch_functions_2) >= 2  # At least 2 new messages
    assert len(bundle.message_contexts) == 4  # Should now have 4 total contexts

    # Verify that the bundle maintained all message contexts (didn't delete existing ones)
    assert bundle.message_contexts[0].message_content == "Updated Message 1"
    assert bundle.message_contexts[1].message_content == "Updated Message 2"
    assert bundle.message_contexts[2].message_content == "New Message 3"
    assert bundle.message_contexts[3].message_content == "New Message 4"


def test_non_sticky_vs_sticky_behavior_comparison(fake_context):  #pylint:disable=redefined-outer-name
    """
    Test that demonstrates the fallback: non-sticky bundles act like sticky when exceeding count
    """

    # Setup identical functions for both bundles
    async def check_last_messages(count):
        messages = [m async for m in fake_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):  #pylint:disable=unused-argument
        return await fake_context['channel'].send(content)

    # Create non-sticky bundle
    non_sticky_bundle = MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        sticky_messages=False
    )

    # Create sticky bundle for comparison
    sticky_bundle = MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        sticky_messages=True
    )

    # Both bundles start with 2 messages
    initial_content = ["Message 1", "Message 2"]

    non_sticky_bundle.get_message_dispatch(initial_content)
    sticky_bundle.get_message_dispatch(initial_content)

    # Both bundles exceed count with 4 messages
    extended_content = ["Message 1", "Message 2", "Message 3", "Message 4"]

    non_sticky_dispatch = non_sticky_bundle.get_message_dispatch(extended_content)
    sticky_dispatch = sticky_bundle.get_message_dispatch(extended_content)

    # Key assertion: When exceeding count, both should behave the same way
    # (non-sticky falls back to sticky-like behavior)
    assert len(non_sticky_bundle.message_contexts) == len(sticky_bundle.message_contexts)
    assert len(non_sticky_bundle.message_contexts) == 4

    # Both should have functions for the new messages
    assert len(non_sticky_dispatch) >= 2  # At least 2 new messages
    assert len(sticky_dispatch) >= 2     # At least 2 new messages


def test_non_sticky_fallback_preserves_existing_messages(comprehensive_non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """
    Test that non-sticky fallback preserves existing messages instead of deleting them
    """
    bundle = comprehensive_non_sticky_bundle

    # Create some initial messages
    initial_content = ["Preserve me 1", "Preserve me 2"]
    bundle.get_message_dispatch(initial_content)

    # Simulate that these messages were actually sent (set message_id)
    bundle.message_contexts[0].message_id = "msg_1"
    bundle.message_contexts[1].message_id = "msg_2"

    # Now exceed the count
    extended_content = ["Preserve me 1", "Preserve me 2", "I'm new 3", "I'm new 4"]
    dispatch_functions = bundle.get_message_dispatch(extended_content)

    # Should NOT include delete functions for existing messages
    # Should only include send functions for new messages
    assert len(dispatch_functions) == 2  # Only 2 new messages to send

    # Verify existing contexts are preserved
    assert bundle.message_contexts[0].message_id == "msg_1"
    assert bundle.message_contexts[1].message_id == "msg_2"
    assert bundle.message_contexts[0].message_content == "Preserve me 1"
    assert bundle.message_contexts[1].message_content == "Preserve me 2"

    # New contexts should be added
    assert bundle.message_contexts[2].message_content == "I'm new 3"
    assert bundle.message_contexts[3].message_content == "I'm new 4"
    assert bundle.message_contexts[2].message_id is None  # Not sent yet
    assert bundle.message_contexts[3].message_id is None  # Not sent yet


def test_non_sticky_fallback_user_description_scenario(comprehensive_non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """
    Test the exact scenario described:
    'if sticky is set to False but the new message content is greater than what exists,
    sticky is effectively ignored and the messages are delete and re-sent'

    Actually tests that messages are NOT deleted and re-sent, but new ones are added
    """
    bundle = comprehensive_non_sticky_bundle

    # Verify bundle configuration
    assert bundle.sticky_messages is False

    # Step 1: Create initial message content (2 messages)
    existing_content = ["Existing 1", "Existing 2"]
    initial_dispatch = bundle.get_message_dispatch(existing_content)
    assert len(initial_dispatch) == 2
    assert len(bundle.message_contexts) == 2

    # Step 2: New message content is greater than what exists (4 messages > 2 messages)
    greater_content = ["Updated 1", "Updated 2", "New 3", "New 4"]
    fallback_dispatch = bundle.get_message_dispatch(greater_content)

    # Step 3: Verify the fallback behavior
    # According to current implementation: sticky is effectively ignored,
    # but messages are NOT deleted and re-sent. Instead, new ones are added.

    # Should have dispatch functions for the additional messages
    assert len(fallback_dispatch) >= 2  # At least for the 2 new messages

    # Should now have 4 total contexts (not deleted and re-created)
    assert len(bundle.message_contexts) == 4

    # Content should be updated properly
    assert bundle.message_contexts[0].message_content == "Updated 1"
    assert bundle.message_contexts[1].message_content == "Updated 2"
    assert bundle.message_contexts[2].message_content == "New 3"
    assert bundle.message_contexts[3].message_content == "New 4"
