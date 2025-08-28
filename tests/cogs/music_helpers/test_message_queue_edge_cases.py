"""
Test edge cases for MessageQueue functionality
"""
import pytest

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageQueueException
from tests.helpers import fake_context  #pylint:disable=unused-import


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
