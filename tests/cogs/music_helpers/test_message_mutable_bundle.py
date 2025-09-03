import pytest

from discord_bot.cogs.music_helpers.message_context import MessageMutableBundle

from tests.helpers import fake_context, generate_fake_context  # pylint: disable=unused-import


def update_message_references(bundle, messages):
    """Helper function to update message references in tests"""
    for i, message in enumerate(messages):
        if i < len(bundle.message_contexts) and message and hasattr(message, 'id'):
            bundle.message_contexts[i].set_message(message)


@pytest.fixture
def message_bundle(fake_context):  #pylint: disable=redefined-outer-name
    """Fixture providing a MessageMutableBundle instance"""
    async def check_last_messages(count):
        messages = [m async for m in fake_context['channel'].history(limit=count)]
        return list(reversed(messages))  # Return newest first like Discord

    async def send_function_wrapper(content: str, delete_after: int = None):  # pylint: disable=unused-argument
        return await fake_context['channel'].send(content)

    bundle = MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        # delete_after=None
    )
    return bundle


@pytest.mark.asyncio
async def test_message_bundle_initial_empty_state(message_bundle):  #pylint: disable=redefined-outer-name
    """Test that a new MessageMutableBundle starts empty"""
    assert message_bundle.get_message_count() == 0
    assert not message_bundle.has_messages()


@pytest.mark.asyncio
async def test_message_bundle_first_send(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test sending initial messages"""
    content = ["Message 1", "Message 2", "Message 3"]

    dispatch_functions = message_bundle.get_message_dispatch(content)

    # Should return send functions for all messages
    assert len(dispatch_functions) == 3
    assert message_bundle.get_message_count() == 3

    # Execute the dispatch functions
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Update message references
    update_message_references(message_bundle, results)

    # Check that messages were sent
    assert len(fake_context['channel'].messages) == 3
    assert fake_context['channel'].messages[0].content == "Message 1"
    assert fake_context['channel'].messages[1].content == "Message 2"
    assert fake_context['channel'].messages[2].content == "Message 3"


@pytest.mark.asyncio
async def test_message_bundle_no_op_same_content(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test that identical content returns no dispatch functions (no-op)"""
    content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Second dispatch with same content
    dispatch_functions = message_bundle.get_message_dispatch(content)

    # Should return empty list (no-op)
    assert len(dispatch_functions) == 0

    # Message count should remain the same
    assert len(fake_context['channel'].messages) == 2


@pytest.mark.asyncio
async def test_message_bundle_edit_existing_content(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test editing existing messages when content changes"""
    initial_content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Modified content
    modified_content = ["Modified Message 1", "Message 2"]
    dispatch_functions = message_bundle.get_message_dispatch(modified_content)

    # Should return one edit function for the changed message
    assert len(dispatch_functions) == 1

    # Execute the edit
    await dispatch_functions[0]()

    # Check that the message was edited
    assert fake_context['channel'].messages[0].content == "Modified Message 1"
    assert fake_context['channel'].messages[1].content == "Message 2"


@pytest.mark.asyncio
async def test_message_bundle_delete_extra_messages(message_bundle, fake_context):  #pylint: disable=redefined-outer-name,unused-argument
    """Test deleting extra messages when fewer items provided"""
    initial_content = ["Message 1", "Message 2", "Message 3"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Reduced content
    reduced_content = ["Message 1", "Modified Message 2"]
    dispatch_functions = message_bundle.get_message_dispatch(reduced_content)

    # Should return edit function for modified message and delete function for extra
    assert len(dispatch_functions) == 2  # One edit, one delete

    # Execute dispatch functions
    for func in dispatch_functions:
        await func()

    # Check message count was reduced
    assert message_bundle.get_message_count() == 2


@pytest.mark.asyncio
async def test_message_bundle_add_new_messages(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test adding new messages when more items provided"""
    initial_content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Expanded content
    expanded_content = ["Message 1", "Message 2", "Message 3", "Message 4"]
    dispatch_functions = message_bundle.get_message_dispatch(expanded_content)

    # Should return send functions for new messages
    assert len(dispatch_functions) == 2  # Two new messages

    # Execute dispatch functions
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Update message references for new messages
    update_message_references(message_bundle,
        [None, None] + results  # First two are unchanged, last two are new
    )

    # Check that new messages were added
    assert len(fake_context['channel'].messages) == 4
    assert message_bundle.get_message_count() == 4


@pytest.mark.asyncio
async def test_message_bundle_complex_update(message_bundle, fake_context):  #pylint: disable=redefined-outer-name,unused-argument
    """Test complex update with edit, delete, and add operations"""
    initial_content = ["Keep", "Edit", "Delete", "Also Delete"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Complex update: keep first, edit second, delete last two, add new ones
    updated_content = ["Keep", "Edited", "New 1", "New 2"]
    dispatch_functions = message_bundle.get_message_dispatch(updated_content)

    # Should have edit function for second message, delete functions for extras,
    # and send functions for new messages
    assert len(dispatch_functions) >= 2  # At least edit and some new messages

    # Execute all dispatch functions
    new_results = []
    for func in dispatch_functions:
        result = await func()
        new_results.append(result)

    # Final message count should be 4
    assert message_bundle.get_message_count() == 4


@pytest.mark.asyncio
async def test_message_bundle_clear_all_messages(message_bundle, fake_context):  #pylint: disable=redefined-outer-name,unused-argument
    """Test clearing all managed messages"""
    content = ["Message 1", "Message 2", "Message 3"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Clear all messages
    delete_functions = message_bundle.clear_all_messages()

    # Should return delete functions for all messages
    assert len(delete_functions) == 3

    # Execute delete functions
    for func in delete_functions:
        await func()

    # Bundle should be empty
    assert message_bundle.get_message_count() == 0
    assert not message_bundle.has_messages()


@pytest.mark.asyncio
async def test_message_bundle_sticky_check_same_order(message_bundle, fake_context):  #pylint: disable=redefined-outer-name,unused-argument
    """Test sticky check when messages are in the same order (should not clear)"""
    content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Check if messages should be cleared (they shouldn't)
    should_clear = await message_bundle.should_clear_messages()
    assert not should_clear


@pytest.mark.asyncio
async def test_message_bundle_sticky_check_different_order(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test sticky check when other messages were sent after ours (should clear)"""
    content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Simulate another message being sent to the channel after ours
    await fake_context['channel'].send("Interrupting message")

    # Check if messages should be cleared (they should)
    should_clear = await message_bundle.should_clear_messages()
    assert should_clear


@pytest.mark.asyncio
async def test_message_bundle_empty_content_list(message_bundle):  #pylint: disable=redefined-outer-name
    """Test handling empty content list"""
    # Empty content should return empty dispatch list
    dispatch_functions = message_bundle.get_message_dispatch([])
    assert len(dispatch_functions) == 0
    assert message_bundle.get_message_count() == 0


@pytest.mark.asyncio
async def test_message_bundle_update_message_references(message_bundle, fake_context):  #pylint: disable=redefined-outer-name,unused-argument
    """Test updating message references after operations"""
    content = ["Message 1", "Message 2"]

    dispatch_functions = message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)

    # Before updating references, contexts should not have messages
    for context in message_bundle.message_contexts:
        assert context.message is None

    # Update message references
    update_message_references(message_bundle, results)

    # After updating, contexts should have message references
    for i, context in enumerate(message_bundle.message_contexts):
        assert context.message is not None
        assert context.message.content == content[i]


@pytest.mark.asyncio
async def test_message_bundle_partial_results_handling(message_bundle, fake_context):  #pylint: disable=redefined-outer-name,unused-argument
    """Test handling partial results (some operations succeed, others fail)"""
    content = ["Message 1", "Message 2", "Message 3"]

    dispatch_functions = message_bundle.get_message_dispatch(content)

    # Simulate partial success (first succeeds, second fails, third succeeds)
    results = [
        await dispatch_functions[0](),  # Success
        None,                           # Failure
        await dispatch_functions[2]()   # Success
    ]

    # Update with partial results
    update_message_references(message_bundle, results)

    # Should handle None results gracefully
    assert message_bundle.message_contexts[0].message is not None
    assert message_bundle.message_contexts[1].message is None
    assert message_bundle.message_contexts[2].message is not None


@pytest.fixture
def non_sticky_message_bundle(fake_context):  #pylint: disable=redefined-outer-name
    """Fixture providing a MessageMutableBundle instance with sticky_messages=False"""
    async def check_last_messages(count):
        messages = [m async for m in fake_context['channel'].history(limit=count)]
        return list(reversed(messages))  # Return newest first like Discord

    async def send_function_wrapper(content: str, delete_after: int = None):  # pylint: disable=unused-argument
        return await fake_context['channel'].send(content)

    bundle = MessageMutableBundle(
        guild_id=fake_context['guild'].id,
        channel_id=fake_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        # delete_after=None,
        sticky_messages=False
    )
    return bundle


@pytest.mark.asyncio
async def test_sticky_messages_enabled_behavior(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test sticky messages behavior when sticky_messages=True (default)"""
    content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # When messages are at the end of channel, should not clear
    should_clear = await message_bundle.should_clear_messages()
    assert not should_clear

    # Add an interrupting message to displace our messages
    await fake_context['channel'].send("Interrupting message")

    # Now our messages are no longer at the end, should clear
    should_clear = await message_bundle.should_clear_messages()
    assert should_clear


@pytest.mark.asyncio
async def test_sticky_messages_disabled_behavior(non_sticky_message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test sticky messages behavior when sticky_messages=False"""
    content = ["Message 1", "Message 2"]

    # First dispatch
    dispatch_functions = non_sticky_message_bundle.get_message_dispatch(content)
    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(non_sticky_message_bundle, results)

    # Even when messages are at the end of channel, should not clear (sticky disabled)
    should_clear = await non_sticky_message_bundle.should_clear_messages()
    assert not should_clear

    # Add an interrupting message to displace our messages
    await fake_context['channel'].send("Interrupting message")

    # Even when messages are no longer at the end, should still not clear (sticky disabled)
    should_clear = await non_sticky_message_bundle.should_clear_messages()
    assert not should_clear


@pytest.mark.asyncio
async def test_sticky_messages_empty_contexts(message_bundle):  #pylint: disable=redefined-outer-name
    """Test that empty message contexts always return False regardless of sticky_messages setting"""
    # With no message contexts, should always return False
    should_clear = await message_bundle.should_clear_messages()
    assert not should_clear


@pytest.mark.asyncio
async def test_sticky_messages_empty_contexts_non_sticky(non_sticky_message_bundle):  #pylint: disable=redefined-outer-name
    """Test that empty message contexts always return False with sticky_messages=False"""
    # With no message contexts, should always return False
    should_clear = await non_sticky_message_bundle.should_clear_messages()
    assert not should_clear


@pytest.mark.asyncio
async def test_sticky_messages_initialization_defaults():
    """Test that MessageMutableBundle defaults to sticky_messages=True"""
    test_context = generate_fake_context()

    async def check_last_messages(count):
        messages = [m async for m in test_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):  # pylint: disable=unused-argument
        return await test_context['channel'].send(content)

    # Create bundle without specifying sticky_messages parameter
    bundle = MessageMutableBundle(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        # delete_after=None
    )

    # Should default to True
    assert bundle.sticky_messages is True


@pytest.mark.asyncio
async def test_sticky_messages_explicit_false():
    """Test that MessageMutableBundle respects sticky_messages=False when explicitly set"""
    test_context = generate_fake_context()

    async def check_last_messages(count):
        messages = [m async for m in test_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):  # pylint: disable=unused-argument
        return await test_context['channel'].send(content)

    # Create bundle with sticky_messages=False
    bundle = MessageMutableBundle(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        # delete_after=None,
        sticky_messages=False
    )

    # Should be False as specified
    assert bundle.sticky_messages is False


@pytest.mark.asyncio
async def test_sticky_messages_explicit_true():
    """Test that MessageMutableBundle respects sticky_messages=True when explicitly set"""
    test_context = generate_fake_context()

    async def check_last_messages(count):
        messages = [m async for m in test_context['channel'].history(limit=count)]
        return list(reversed(messages))

    async def send_function_wrapper(content: str, delete_after: int = None):  # pylint: disable=unused-argument
        return await test_context['channel'].send(content)

    # Create bundle with sticky_messages=True
    bundle = MessageMutableBundle(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        check_last_message_func=check_last_messages,
        send_function=send_function_wrapper,
        # delete_after=None,
        sticky_messages=True
    )

    # Should be True as specified
    assert bundle.sticky_messages is True


@pytest.mark.asyncio
async def test_sticky_messages_deletion_before_new_content(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """Test that sticky messages are properly deleted before new content is sent"""
    # Send initial content
    initial_content = ["Message 1", "Message 2"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Verify initial messages exist
    assert len(fake_context['channel'].messages) == 2
    assert message_bundle.get_message_count() == 2
    original_message_ids = [msg.id for msg in fake_context['channel'].messages]

    # Add interrupting message to trigger sticky behavior
    await fake_context['channel'].send("Interrupting message")
    assert len(fake_context['channel'].messages) == 3

    # Verify sticky check returns True (messages should be cleared)
    should_clear = await message_bundle.should_clear_messages()
    assert should_clear

    # Send new content with clear_existing=True - this should delete old messages and send new ones
    new_content = ["New Message 1", "New Message 2"]
    new_dispatch_functions = message_bundle.get_message_dispatch(new_content, clear_existing=True)

    # The dispatch functions should include delete functions first, then send functions
    # Before executing any dispatch functions, our original messages should still exist
    assert len(fake_context['channel'].messages) == 3

    # Execute all dispatch functions (deletes first, then sends)
    new_results = []
    for func in new_dispatch_functions:
        result = await func()
        # Only collect non-boolean results (send operations return Message objects, delete returns True/False)
        if result and hasattr(result, 'id'):
            new_results.append(result)

    # Update message references only for the new messages
    update_message_references(message_bundle, new_results)

    # Verify that:
    # 1. Original messages are gone (deleted)
    # 2. New messages exist with new content
    # 3. Total message count reflects the changes
    current_messages = fake_context['channel'].messages

    # Check that original message IDs are no longer in the channel
    # (In real Discord, deleted messages disappear. Our mock should reflect this)
    current_message_ids = [msg.id for msg in current_messages]
    for original_id in original_message_ids:
        assert original_id not in current_message_ids, f"Original message {original_id} should have been deleted"

    # Verify new content exists
    new_message_contents = [msg.content for msg in current_messages if msg.content in new_content]
    assert "New Message 1" in new_message_contents
    assert "New Message 2" in new_message_contents
