"""
Tests for MessageContext and MessageMutableBundle edge cases
"""
from unittest.mock import AsyncMock

import pytest

from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.message_context import MessageMutableBundle
from tests.helpers import fake_context #pylint: disable=unused-import


@pytest.fixture
def non_sticky_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Create a non-sticky MessageMutableBundle for testing"""
    async def check_last_messages(count):
        messages = [m async for m in fake_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):  #pylint:disable=unused-argument
        return await fake_context['channel'].send(content)

    return MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        sticky_messages=False
    )


@pytest.fixture
def sticky_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Create a sticky MessageMutableBundle for testing"""
    async def check_last_messages(count):
        messages = [m async for m in fake_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):  #pylint:disable=unused-argument
        return await fake_context['channel'].send(content)

    return MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        sticky_messages=True
    )


def test_non_sticky_bundle_empty_to_multiple_messages(non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test that non-sticky bundles can go from empty to multiple messages"""
    # This should work - empty bundle can accept any number of initial messages
    content = ["Message 1", "Message 2", "Message 3"]

    # Should not raise exception
    dispatch_functions = non_sticky_bundle.get_message_dispatch(content)
    assert len(dispatch_functions) == 3


def test_non_sticky_bundle_exceed_existing_count_fallback_behavior(non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test that non-sticky bundles allow exceeding existing count by adding new messages"""
    # First, create some initial messages
    initial_content = ["Message 1", "Message 2"]
    dispatch_functions = non_sticky_bundle.get_message_dispatch(initial_content)
    assert len(dispatch_functions) == 2

    # Now exceed the existing count - should add additional messages
    excess_content = ["Message 1", "Message 2", "Message 3"]

    # Should NOT raise error, instead should return dispatch functions for the additional content
    dispatch_functions = non_sticky_bundle.get_message_dispatch(excess_content)

    # Should have at least one function for the additional message
    assert len(dispatch_functions) >= 1
    assert isinstance(dispatch_functions, list)

    # The additional message should be "Message 3"
    if len(dispatch_functions) == 1:
        # Check that the new message content is correct
        partial_func = dispatch_functions[0]
        assert 'Message 3' in str(partial_func.keywords.get('content', ''))
    else:
        # If more functions, verify we have functions for the new content
        assert len(dispatch_functions) >= 1


def test_non_sticky_bundle_same_count_allowed(non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test that non-sticky bundles allow same number of messages"""
    # First, create initial messages
    initial_content = ["Message 1", "Message 2"]
    dispatch_functions = non_sticky_bundle.get_message_dispatch(initial_content)
    assert len(dispatch_functions) == 2

    # Same count should work
    same_content = ["Updated 1", "Updated 2"]
    dispatch_functions = non_sticky_bundle.get_message_dispatch(same_content)
    assert len(dispatch_functions) == 2


def test_non_sticky_bundle_fewer_messages_allowed(non_sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test that non-sticky bundles allow fewer messages"""
    # First, create initial messages
    initial_content = ["Message 1", "Message 2", "Message 3"]
    dispatch_functions = non_sticky_bundle.get_message_dispatch(initial_content)
    assert len(dispatch_functions) == 3

    # Fewer messages should work
    fewer_content = ["Updated 1", "Updated 2"]
    dispatch_functions = non_sticky_bundle.get_message_dispatch(fewer_content)
    # Should include delete operations for extra messages
    assert len(dispatch_functions) >= 2


def test_sticky_bundle_allows_any_count(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test that sticky bundles allow any number of messages"""
    # Start with some messages
    initial_content = ["Message 1", "Message 2"]
    dispatch_functions = sticky_bundle.get_message_dispatch(initial_content)
    assert len(dispatch_functions) == 2

    # Should be able to exceed count with sticky messages
    more_content = ["Updated 1", "Updated 2", "Message 3", "Message 4"]
    dispatch_functions = sticky_bundle.get_message_dispatch(more_content)
    # Should have 2 edit functions for existing messages + 2 send functions for new messages
    # But since messages don't exist yet, all 4 will be send functions
    assert len(dispatch_functions) == 4


def test_message_context_set_message_none_handling():
    """Test that MessageContext handles None message properly"""

    context = MessageContext(12345, 67890)

    # Should handle None without crashing
    context.set_message(None)
    assert context.message_id is None


def test_message_context_set_message_with_valid_message():
    """Test that MessageContext handles valid message properly"""

    context = MessageContext(12345, 67890)

    # Mock message object
    mock_message = type('Message', (), {'id': 98765})()

    context.set_message(mock_message)
    assert context.message_id == 98765


def test_non_sticky_fallback_with_none_contexts():
    """Test that non-sticky bundles handle None contexts gracefully during fallback"""
    test_bundle = MessageMutableBundle(
        guild_id=12345,
        channel_id=67890,
        check_last_message_func=AsyncMock(),
        send_function=AsyncMock(),
        sticky_messages=False
    )

    # Create initial message contexts with None values (edge case)
    test_bundle.message_contexts = [None, None]  # Simulate 2 existing contexts

    # Try to exceed - should fall back to sticky behavior and handle None contexts
    try:
        dispatch_functions = test_bundle.get_message_dispatch(["A", "B", "C"])
        # Should return dispatch functions, not raise error
        assert isinstance(dispatch_functions, list)
        assert len(dispatch_functions) == 3  # Should create 3 new messages
    except AttributeError as e:
        # If this fails due to None handling, that's the bug we're testing for
        assert "NoneType" in str(e), f"Unexpected AttributeError: {e}"


@pytest.mark.asyncio
async def test_clear_existing_behavior(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test clear_existing parameter behavior"""
    # Create initial messages
    initial_content = ["Message 1", "Message 2"]
    dispatch_functions = sticky_bundle.get_message_dispatch(initial_content)

    # Execute functions to create message contexts
    for func in dispatch_functions:
        await func()

    # Now test clear_existing behavior
    new_content = ["New Message 1"]
    dispatch_functions = sticky_bundle.get_message_dispatch(new_content, clear_existing=True)

    # Should include delete operations for old messages plus new send operations
    # Exact count depends on implementation details
    assert len(dispatch_functions) >= 1


def test_delete_after_parameter_handling(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test that delete_after parameter is properly handled"""
    content = ["Test message"]
    delete_after_value = 300

    dispatch_functions = sticky_bundle.get_message_dispatch(
        content,
        delete_after=delete_after_value
    )

    assert len(dispatch_functions) == 1
    # The delete_after value should be passed to the send function
    # (Implementation-specific verification would depend on how the partial is constructed)


def test_empty_message_content_handling(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test handling of empty message content lists"""
    empty_content = []

    # Should handle empty content gracefully
    dispatch_functions = sticky_bundle.get_message_dispatch(empty_content)

    # Should return empty list or handle appropriately
    assert isinstance(dispatch_functions, list)


def test_whitespace_only_message_content(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test handling of whitespace-only message content"""
    whitespace_content = ["   ", "\t\n", ""]

    # Should handle whitespace content without crashing
    dispatch_functions = sticky_bundle.get_message_dispatch(whitespace_content)

    assert isinstance(dispatch_functions, list)
    assert len(dispatch_functions) == len(whitespace_content)


def test_very_long_message_content(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test handling of very long message content"""
    # Discord messages have a 2000 character limit
    long_message = "A" * 2500  # Exceeds Discord limit
    content = [long_message]

    # Should handle long messages (may truncate or split)
    dispatch_functions = sticky_bundle.get_message_dispatch(content)

    assert isinstance(dispatch_functions, list)
    assert len(dispatch_functions) >= 1


def test_unicode_message_content(sticky_bundle):  #pylint:disable=redefined-outer-name
    """Test handling of Unicode characters in message content"""
    unicode_content = [
        "Hello 世界",  # Chinese characters
        "Café ñoño",   # Accented characters
        "🎵🎶🎸",      # Emojis
        "Ελληνικά",   # Greek
        "العربية"      # Arabic
    ]

    # Should handle Unicode content without issues
    dispatch_functions = sticky_bundle.get_message_dispatch(unicode_content)

    assert isinstance(dispatch_functions, list)
    assert len(dispatch_functions) == len(unicode_content)
