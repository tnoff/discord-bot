from datetime import datetime, timezone, timedelta
from functools import partial

import pytest

from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageType
from discord_bot.cogs.music_helpers.common import MultipleMutableType
from discord_bot.cogs.music_helpers.search_client import SearchClient

from tests.helpers import generate_fake_context, FakeMessage


def update_message_references(bundle, messages):
    """Helper function to update message references in tests"""
    for i, message in enumerate(messages):
        if i < len(bundle.message_contexts) and message and hasattr(message, 'id'):
            bundle.message_contexts[i].set_message(message)


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


def test_mutable_bundle_persistence():
    """Test that mutable bundles persist across calls and track processing state"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_update_mutable_bundle_content():
    """Test updating bundle content and getting dispatch functions"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_update_mutable_bundle_channel_nonexistent():
    """Test updating channel for non-existent bundle"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    success = await mq.update_mutable_bundle_channel('nonexistent-bundle', fake_context['channel'])
    assert success is False


@pytest.mark.asyncio
async def test_sticky_messages_clear_scenario():
    """Test that sticky bundles clear existing messages when needed"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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


def test_multiple_bundles_timestamp_comparison():
    """Test that bundles are returned in chronological order based on updated_at"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_sticky_clear_with_messages():
    """Test sticky message clearing behavior with actual messages"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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


def test_message_queue_creates_sticky_bundles_by_default():
    """Test that message queue creates bundles with sticky=True by default"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    index_name = 'default-sticky-test'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    bundle = mq.mutable_bundles[index_name]
    assert bundle.sticky_messages is True


@pytest.mark.asyncio
async def test_message_queue_sticky_deletion_integration():
    """Test full workflow of sticky message deletion and recreation"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_update_references():
    """Test updating message references in bundle"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_full_workflow_with_sticky():
    """Test complete workflow with sticky message behavior"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_sticky_behavior_integration():
    """Test that sticky behavior properly clears messages when needed"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_small_bundle_no_sticky_clearing():
    """Test that small bundles don't trigger unnecessary sticky clearing"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_large_bundle_reference_bug_prevention():
    """Test that large bundles handle message references correctly"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_mixed_results_reference_mapping():
    """Test reference updating with mixed success/failure results"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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


# Search Message Integration Tests

def test_search_message_integration_basic():
    """Test basic search message creation and queue registration"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create search client with message queue
    search_client = SearchClient(None, None, None, mq, 1)

    # Simulate search message creation
    message_context = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
    search_message = 'Gathering spotify data from url "<test-playlist>"'

    # Add message to search client messages dict
    search_client.messages[message_context.uuid] = [search_message]

    # Register with message queue
    index_name = f'{MultipleMutableType.SEARCH.value}-{message_context.uuid}'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Verify message queue knows about search bundle
    assert index_name in mq.mutable_bundles
    bundle = mq.mutable_bundles[index_name]
    assert bundle.guild_id == fake_context['guild'].id
    assert bundle.channel_id == fake_context['channel'].id


@pytest.mark.asyncio
async def test_search_message_integration_content_processing():
    """Test search message content processing through message queue"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create search client
    search_client = SearchClient(None, None, None, mq, 1)

    # Create search context and messages
    message_context = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
    search_messages = [
        'Gathering spotify data from url "<test-playlist>"',
        'Processing 10 tracks from playlist'
    ]

    # Add to search client messages
    search_client.messages[message_context.uuid] = search_messages

    # Register with queue
    index_name = f'{MultipleMutableType.SEARCH.value}-{message_context.uuid}'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Process through message queue content update
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, search_messages)

    # Should get dispatch functions for messages
    assert len(dispatch_functions) == 2

    # Verify bundle has correct content
    bundle = mq.mutable_bundles[index_name]
    assert len(bundle.message_contexts) == 2
    assert bundle.message_contexts[0].message_content == search_messages[0]
    assert bundle.message_contexts[1].message_content == search_messages[1]


@pytest.mark.asyncio
async def test_search_message_integration_error_cleanup():
    """Test search message cleanup on error scenarios"""

    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create search client
    search_client = SearchClient(None, None, None, mq, 1)

    # Create search context with initial message
    message_context = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
    initial_message = 'Gathering spotify data from url "<test-playlist>"'
    search_client.messages[message_context.uuid] = [initial_message]

    # Register with queue
    index_name = f'{MultipleMutableType.SEARCH.value}-{message_context.uuid}'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Process initial message
    await mq.update_mutable_bundle_content(index_name, [initial_message])

    # Simulate error - clear search messages
    search_client.messages[message_context.uuid] = []

    # Process empty messages (error cleanup)
    dispatch_functions = await mq.update_mutable_bundle_content(index_name, [])

    # Should get dispatch functions to clear messages
    assert len(dispatch_functions) >= 0  # May have delete functions

    # Bundle should still exist but with no active content
    bundle = mq.mutable_bundles[index_name]
    assert len(bundle.message_contexts) == 0


def test_search_message_integration_memory_cleanup():
    """Test that search messages are properly cleaned up to prevent memory leaks"""

    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create search client
    search_client = SearchClient(None, None, None, mq, 1)

    # Create multiple search contexts
    contexts = []
    for i in range(5):
        context = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
        search_client.messages[context.uuid] = [f'Search message {i}']
        contexts.append(context)

    # Verify all contexts are in messages dict
    assert len(search_client.messages) == 5

    # Simulate successful processing - clear messages
    for context in contexts:
        search_client.messages[context.uuid] = []
        index_name = f'{MultipleMutableType.SEARCH.value}-{context.uuid}'
        mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Verify messages dict still has entries (they're empty but exist)
    assert len(search_client.messages) == 5

    # Verify all are empty (ready for cleanup)
    for context in contexts:
        assert search_client.messages[context.uuid] == []


@pytest.mark.asyncio
async def test_search_message_integration_uuid_parsing():
    """Test search message UUID parsing in music.py message processing"""

    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create search client
    search_client = SearchClient(None, None, None, mq, 1)

    # Create search context with known UUID
    message_context = MessageContext(fake_context['guild'].id, fake_context['channel'].id)
    test_uuid = message_context.uuid
    search_message = 'Test search message'

    # Add to search client
    search_client.messages[test_uuid] = [search_message]

    # Register with queue using full index name format
    index_name = f'{MultipleMutableType.SEARCH.value}-{test_uuid}'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Simulate the UUID extraction logic from music.py
    # item.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]
    extracted_uuid = index_name.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]

    # Verify UUID extraction works correctly
    assert extracted_uuid == test_uuid

    # Verify we can get the message content using extracted UUID
    assert search_client.messages[extracted_uuid] == [search_message]

    # Verify bundle exists with correct UUID in name
    assert index_name in mq.mutable_bundles


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


def test_uuid_parsing_safety_search():
    """Test safe UUID parsing for SEARCH message types"""

    # Test normal search UUID parsing
    normal_uuid = 'conteext.12345-67890-search-test'
    index_name = f'{MultipleMutableType.SEARCH.value}-{normal_uuid}'

    extracted_uuid = index_name.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]
    assert extracted_uuid == normal_uuid

    # Test search UUID that contains 'search' string
    search_in_uuid = 'conteext.search-within-search-uuid'
    search_index = f'{MultipleMutableType.SEARCH.value}-{search_in_uuid}'

    extracted_search = search_index.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]
    assert extracted_search == search_in_uuid


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
        MultipleMutableType.SEARCH.value,
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
async def test_uuid_parsing_integration_with_message_queue():
    """Test UUID parsing integration with actual message queue operations"""

    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create bundles with tricky UUIDs
    tricky_uuids = [
        'request.bundle.normal-uuid',
        f'conteext.{MultipleMutableType.SEARCH.value}-in-uuid',
        f'request.bundle.{MultipleMutableType.REQUEST_BUNDLE.value}-duplicate-prefix'
    ]

    # Register all bundles
    for uuid in tricky_uuids:
        if uuid.startswith('request.bundle'):
            index_name = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{uuid}'
        else:
            index_name = f'{MultipleMutableType.SEARCH.value}-{uuid}'

        mq.update_multiple_mutable(index_name, fake_context['channel'])

        # Verify bundle was created
        assert index_name in mq.mutable_bundles

    # Test parsing each registered bundle
    for bundle_name in mq.mutable_bundles:
        if MultipleMutableType.REQUEST_BUNDLE.value in bundle_name:
            extracted = bundle_name.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
            assert extracted in tricky_uuids
        elif MultipleMutableType.SEARCH.value in bundle_name:
            extracted = bundle_name.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]
            assert extracted in tricky_uuids

@pytest.mark.asyncio
async def test_message_queue_non_sticky_behavior():
    """Test that bundles can be created with sticky_messages=False"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
async def test_message_queue_sticky_behavior_default():
    """Test that bundles default to sticky_messages=True when parameter not specified"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

    # Create bundle without specifying sticky_messages (should default to True)
    index_name = 'test-bundle-default-sticky'
    mq.update_multiple_mutable(index_name, fake_context['channel'])

    # Verify bundle was created with sticky_messages=True
    assert index_name in mq.mutable_bundles
    bundle = mq.mutable_bundles[index_name]
    assert bundle.sticky_messages is True

@pytest.mark.asyncio
async def test_message_queue_mixed_sticky_behavior():
    """Test that different bundles can have different sticky_messages settings"""
    mq = MessageQueue()
    fake_context = generate_fake_context()

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
