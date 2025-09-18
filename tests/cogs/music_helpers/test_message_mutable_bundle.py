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


@pytest.mark.asyncio
async def test_content_aware_diffing_optimization_scenario(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """
    Test the optimization scenario: existing [A, B, C, D] → new [A, B, D]

    Current behavior (suboptimal):
    - Deletes D (position 3)
    - Edits C to show D content (position 2)

    Desired behavior (optimal):
    - Only deletes C (content that was actually removed)
    - Leaves D untouched in its original position

    This test documents the current behavior and will be used to validate
    the optimization when implemented.
    """
    # Setup initial messages: [A, B, C, D]
    initial_content = ["A", "B", "C", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Verify initial state
    assert message_bundle.get_message_count() == 4
    assert len(fake_context['channel'].messages) == 4

    # Update to new content: [A, B, D] (removing C)
    new_content = ["A", "B", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(new_content)

    # Analyze what operations are planned
    delete_operations = []
    edit_operations = []
    send_operations = []

    # Execute and categorize operations
    for func in dispatch_functions:
        # Check if this is a delete operation (bound to delete_message method)
        if hasattr(func, 'func') and func.func.__name__ == 'delete_message':
            delete_operations.append(func)
        # Check if this is an edit operation (bound to edit_message method)
        elif hasattr(func, 'func') and func.func.__name__ == 'edit_message':
            edit_operations.append(func)
        # Otherwise assume it's a send operation
        else:
            send_operations.append(func)

        # Execute the operation
        await func()

    # DESIRED BEHAVIOR ASSERTIONS (what should happen after optimization)
    # These assertions describe the optimal behavior we want to achieve
    # They will FAIL with the current implementation but should pass after optimization

    # Optimal behavior: 1 delete operation (only deletes message C)
    assert len(delete_operations) == 1, f"Expected 1 delete operation, got {len(delete_operations)}"

    # Optimal behavior: 0 edit operations (D message should be left alone)
    assert len(edit_operations) == 0, f"Expected 0 edit operations, got {len(edit_operations)}"

    # Optimal behavior: 0 send operations (no new messages needed)
    assert len(send_operations) == 0, f"Expected 0 send operations, got {len(send_operations)}"

    # Verify final state
    assert message_bundle.get_message_count() == 3

    # Verify content correctness regardless of implementation
    message_contents = [msg.content for msg in fake_context['channel'].messages if not msg.deleted]
    assert "A" in message_contents
    assert "B" in message_contents
    assert "D" in message_contents
    assert "C" not in message_contents


@pytest.mark.asyncio
async def test_content_aware_diffing_edit_plus_removal(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """
    Test optimization scenario: existing [A, B, C, D] → new [A', B, D]
    (Edit first message + remove middle message)

    Current behavior (suboptimal):
    - Edits A to A'
    - Edits B to D (wrong!)
    - Deletes C and D

    Desired behavior (optimal):
    - Edits A to A'
    - Deletes C only
    - Leaves B and D untouched
    """
    # Setup initial messages: [A, B, C, D]
    initial_content = ["A", "B", "C", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Update to: [A', B, D] (edit A, remove C)
    new_content = ["A'", "B", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(new_content)

    # Categorize operations
    delete_operations = []
    edit_operations = []

    for func in dispatch_functions:
        if hasattr(func, 'func') and func.func.__name__ == 'delete_message':
            delete_operations.append(func)
        elif hasattr(func, 'func') and func.func.__name__ == 'edit_message':
            edit_operations.append(func)
        await func()

    # DESIRED BEHAVIOR (will fail with current implementation)
    # Optimal: 1 delete (C only), 1 edit (A→A' only)
    assert len(delete_operations) == 1, f"Expected 1 delete operation, got {len(delete_operations)}"
    assert len(edit_operations) == 1, f"Expected 1 edit operation, got {len(edit_operations)}"

    # Verify final content
    message_contents = [msg.content for msg in fake_context['channel'].messages if not msg.deleted]
    assert "A'" in message_contents
    assert "B" in message_contents
    assert "D" in message_contents
    assert "C" not in message_contents


@pytest.mark.asyncio
async def test_content_aware_diffing_multiple_removals(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """
    Test optimization scenario: existing [A, B, C, D, E, F] → new [A, B, F]
    (Remove multiple middle messages)

    Current behavior (suboptimal):
    - Edits C to F (wrong!)
    - Deletes D, E, F

    Desired behavior (optimal):
    - Deletes C, D, E only
    - Leaves A, B, F untouched
    """
    # Setup initial messages: [A, B, C, D, E, F]
    initial_content = ["A", "B", "C", "D", "E", "F"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Update to: [A, B, F] (remove C, D, E)
    new_content = ["A", "B", "F"]
    dispatch_functions = message_bundle.get_message_dispatch(new_content)

    # Categorize operations
    delete_operations = []
    edit_operations = []

    for func in dispatch_functions:
        if hasattr(func, 'func') and func.func.__name__ == 'delete_message':
            delete_operations.append(func)
        elif hasattr(func, 'func') and func.func.__name__ == 'edit_message':
            edit_operations.append(func)
        await func()

    # DESIRED BEHAVIOR (will fail with current implementation)
    # Optimal: 3 deletes (C, D, E), 0 edits
    assert len(delete_operations) == 3, f"Expected 3 delete operations, got {len(delete_operations)}"
    assert len(edit_operations) == 0, f"Expected 0 edit operations, got {len(edit_operations)}"

    # Verify final content
    message_contents = [msg.content for msg in fake_context['channel'].messages if not msg.deleted]
    assert "A" in message_contents
    assert "B" in message_contents
    assert "F" in message_contents
    assert "C" not in message_contents
    assert "D" not in message_contents
    assert "E" not in message_contents


@pytest.mark.asyncio
async def test_content_aware_diffing_multiple_edits_plus_keep_last(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """
    Test optimization scenario: existing [A, B, C, D] → new [A', B', C', D]
    (Edit multiple messages but keep the last one untouched)

    Current behavior (suboptimal):
    - Edits A to A'
    - Edits B to B'
    - Edits C to C'
    - Keeps D (this part is optimal already)

    Desired behavior (optimal):
    - Edits A to A'
    - Edits B to B'
    - Edits C to C'
    - Leaves D completely untouched (no operations on it)

    This test verifies that when content already matches, no operations are generated
    for that message, even when other messages are being edited.
    """
    # Setup initial messages: [A, B, C, D]
    initial_content = ["A", "B", "C", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Store original D message for tracking
    original_message_d = fake_context['channel'].messages[3]

    # Update to: [A', B', C', D] (edit first three, keep D)
    new_content = ["A'", "B'", "C'", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(new_content)

    # Categorize operations
    delete_operations = []
    edit_operations = []

    for func in dispatch_functions:
        if hasattr(func, 'func') and func.func.__name__ == 'delete_message':
            delete_operations.append(func)
        elif hasattr(func, 'func') and func.func.__name__ == 'edit_message':
            edit_operations.append(func)
        await func()

    # DESIRED BEHAVIOR (should already work with current implementation)
    # Optimal: 0 deletes, 3 edits (A→A', B→B', C→C'), D untouched
    assert len(delete_operations) == 0, f"Expected 0 delete operations, got {len(delete_operations)}"
    assert len(edit_operations) == 3, f"Expected 3 edit operations, got {len(edit_operations)}"

    # Verify D message was not modified (same object reference)
    current_message_d = fake_context['channel'].messages[3]
    assert current_message_d.id == original_message_d.id, "D message should not have been recreated"
    assert current_message_d.content == "D", "D message content should remain unchanged"

    # Verify final content
    message_contents = [msg.content for msg in fake_context['channel'].messages if not msg.deleted]
    assert "A'" in message_contents
    assert "B'" in message_contents
    assert "C'" in message_contents
    assert "D" in message_contents
    assert message_bundle.get_message_count() == 4


@pytest.mark.asyncio
async def test_content_aware_diffing_duplicate_content_matching(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """
    Test edge case: existing [A, B, B, C] → new [A, B, C]
    (Multiple messages with identical content)

    The _match_existing_message_content method uses first-match-only logic:
    for each new message, it finds the first existing message with matching content.

    This could lead to suboptimal behavior:
    - New message B (index 1) matches existing B (index 1) ✓ correct
    - New message C (index 2) might match existing B (index 2) instead of C (index 3) ✗ wrong

    This test verifies the actual behavior and documents potential issues.
    """
    # Setup initial messages: [A, B, B, C] (duplicate B content)
    initial_content = ["A", "B", "B", "C"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Verify initial state
    assert message_bundle.get_message_count() == 4
    assert len(fake_context['channel'].messages) == 4

    # Update to: [A, B, C] (remove one B)
    new_content = ["A", "B", "C"]
    dispatch_functions = message_bundle.get_message_dispatch(new_content)

    # Categorize operations
    delete_operations = []
    edit_operations = []

    for func in dispatch_functions:
        if hasattr(func, 'func') and func.func.__name__ == 'delete_message':
            delete_operations.append(func)
        elif hasattr(func, 'func') and func.func.__name__ == 'edit_message':
            edit_operations.append(func)
        await func()

    # DOCUMENT ACTUAL BEHAVIOR
    # Due to first-match-only logic, we expect:
    # - A matches A (position 0) ✓
    # - B matches first B (position 1) ✓
    # - C might incorrectly match second B (position 2) instead of C (position 3) ✗

    # Current implementation might edit the second B to C and delete the actual C
    # Optimal would be: delete the second B only, leave C untouched


    # Verify final state has correct content regardless of implementation efficiency
    assert message_bundle.get_message_count() == 3
    message_contents = [msg.content for msg in fake_context['channel'].messages if not msg.deleted]
    assert "A" in message_contents
    assert "B" in message_contents
    assert "C" in message_contents

    # Count occurrences to ensure we only have one B now
    b_count = sum(1 for content in message_contents if content == "B")
    assert b_count == 1, f"Expected exactly 1 'B' message, got {b_count}"


@pytest.mark.asyncio
async def test_content_aware_diffing_duplicate_content_middle_removal(message_bundle, fake_context):  #pylint: disable=redefined-outer-name
    """
    Test edge case: existing [A, B, C, B, D] → new [A, B, C, D]
    (Duplicate content with middle removal)

    This tests a more complex scenario where:
    - B appears twice (positions 1 and 3)
    - We want to remove the second B (position 3)
    - Keep everything else in place

    The first-match-only logic might cause issues:
    - New A (index 0) matches existing A (index 0) ✓
    - New B (index 1) matches existing B (index 1) ✓
    - New C (index 2) matches existing C (index 2) ✓
    - New D (index 3) might match existing B (index 3) instead of D (index 4) ✗

    This could result in editing B→D and deleting the actual D message.
    """
    # Setup initial messages: [A, B, C, B, D]
    initial_content = ["A", "B", "C", "B", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(initial_content)

    results = []
    for func in dispatch_functions:
        result = await func()
        results.append(result)
    update_message_references(message_bundle, results)

    # Verify initial state
    assert message_bundle.get_message_count() == 5
    assert len(fake_context['channel'].messages) == 5

    # Store original D message for tracking

    # Update to: [A, B, C, D] (remove the second B)
    new_content = ["A", "B", "C", "D"]
    dispatch_functions = message_bundle.get_message_dispatch(new_content)

    # Categorize operations
    delete_operations = []
    edit_operations = []

    for func in dispatch_functions:
        if hasattr(func, 'func') and func.func.__name__ == 'delete_message':
            delete_operations.append(func)
        elif hasattr(func, 'func') and func.func.__name__ == 'edit_message':
            edit_operations.append(func)
        await func()

    # Verify final state has correct content
    assert message_bundle.get_message_count() == 4
    message_contents = [msg.content for msg in fake_context['channel'].messages if not msg.deleted]
    assert "A" in message_contents
    assert "B" in message_contents
    assert "C" in message_contents
    assert "D" in message_contents

    # Ensure we only have one B now
    b_count = sum(1 for content in message_contents if content == "B")
    assert b_count == 1, f"Expected exactly 1 'B' message, got {b_count}"

    # Key question: Was the original D message preserved or was it recreated?
    # Optimal behavior: original D message should be untouched
    # Suboptimal behavior: D message was edited from the second B
