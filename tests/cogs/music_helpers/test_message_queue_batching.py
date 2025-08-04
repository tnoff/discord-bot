from unittest.mock import MagicMock

from discord_bot.cogs.music_helpers.message_queue import MessageQueue, MessageType, SourceLifecycleStage
from discord_bot.cogs.music_helpers.batched_message import ItemStatus

from tests.helpers import generate_fake_context, fake_source_dict
from tests.helpers import fake_engine #pylint:disable=unused-import


def test_message_queue_batching_init():
    """Test MessageQueue initialization with batching parameters"""
    queue = MessageQueue(batch_size=10, batch_timeout=60)

    assert queue.batch_size == 10
    assert queue.batch_timeout == 60
    assert len(queue.pending_batches) == 0
    assert len(queue.active_batches) == 0


def test_should_batch_items():
    """Test logic for determining when to batch items"""
    queue = MessageQueue(batch_size=15, batch_timeout=30)
    guild_id = 12345

    # Should batch when we have 2 or more items (new threshold)
    assert queue.should_batch_items(guild_id, 20) is True
    assert queue.should_batch_items(guild_id, 15) is True
    assert queue.should_batch_items(guild_id, 10) is True
    assert queue.should_batch_items(guild_id, 2) is True
    assert queue.should_batch_items(guild_id, 1) is False


def test_add_items_to_batch_large_batch():
    """Test adding large number of items creates batch immediately"""
    queue = MessageQueue(batch_size=5, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    # Create mock send function
    mock_send = MagicMock()

    # Create multiple source dicts
    source_dicts = []
    for i in range(8):
        source_dict = fake_source_dict(fake_context)
        source_dict.search_string = f"Song {i+1}"
        source_dicts.append(source_dict)

    # Add items to batch
    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # With new threshold of 2, all 8 items should be finalized in batches
    # Should create 2 active batches (5 items + 3 items)
    assert len(queue.active_batches) == 2
    assert guild_id not in queue.pending_batches  # All batches finalized

    # Check that both batches exist
    first_batch = queue.active_batches[batch_id]
    assert len(first_batch.source_dicts) == 5  # First batch full
    assert first_batch.total_items == 5

    # Should have queued batch for sending
    message_type, item = queue.get_next_message()
    assert message_type == MessageType.BATCHED_MESSAGE
    assert item.batch_id == batch_id
    assert item.lifecycle_stage == SourceLifecycleStage.SEND


def test_add_items_to_batch_single_item():
    """Test adding single item creates pending batch (below threshold)"""
    queue = MessageQueue(batch_size=15, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    # Create single source dict (below batch threshold of 2)
    source_dicts = []
    for _ in range(1):
        source_dict = fake_source_dict(fake_context)
        source_dicts.append(source_dict)

    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # Should create pending batch (not finalized yet since 1 < 2)
    assert guild_id in queue.pending_batches
    assert batch_id not in queue.active_batches

    pending_batch = queue.pending_batches[guild_id]
    assert len(pending_batch.source_dicts) == 1
    assert pending_batch.batch_id == batch_id


def test_update_batch_item():
    """Test updating individual item status in batch"""
    queue = MessageQueue(batch_size=5, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    # Create batch with items
    source_dicts = []
    for _ in range(5):
        source_dict = fake_source_dict(fake_context)
        source_dicts.append(source_dict)

    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # Clear initial send from queue
    queue.get_next_message()

    # Update item status
    source_uuid = str(source_dicts[0].uuid)
    result = queue.update_batch_item(batch_id, source_uuid, ItemStatus.DOWNLOADING)

    assert result is True

    # Should queue batch for update
    message_type, item = queue.get_next_message()
    assert message_type == MessageType.BATCHED_MESSAGE
    assert item.batch_id == batch_id
    assert item.lifecycle_stage == SourceLifecycleStage.EDIT

    # Check status was updated
    batch = queue.active_batches[batch_id]
    assert batch.status_map[source_uuid] == ItemStatus.DOWNLOADING


def test_update_batch_item_with_error():
    """Test updating batch item with error message"""
    queue = MessageQueue(batch_size=5, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    source_dicts = [fake_source_dict(fake_context) for _ in range(5)]
    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # Update with error
    source_uuid = str(source_dicts[0].uuid)
    error_msg = "video unavailable"
    result = queue.update_batch_item(batch_id, source_uuid, ItemStatus.FAILED, error_msg)

    assert result is True

    # Check error was stored
    batch = queue.active_batches[batch_id]
    assert batch.status_map[source_uuid] == ItemStatus.FAILED
    assert batch.error_map[source_uuid] == error_msg


def test_update_nonexistent_batch():
    """Test updating non-existent batch returns False"""
    queue = MessageQueue()

    result = queue.update_batch_item("fake-batch-id", "fake-uuid", ItemStatus.COMPLETED)
    assert result is False


def test_cleanup_completed_batch():
    """Test cleaning up completed batches"""
    queue = MessageQueue(batch_size=2, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    # Create small batch
    source_dicts = [fake_source_dict(fake_context) for _ in range(2)]
    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # Mark all items as completed
    for source_dict in source_dicts:
        queue.update_batch_item(batch_id, str(source_dict.uuid), ItemStatus.COMPLETED)

    # Should be able to cleanup
    result = queue.cleanup_completed_batch(batch_id)
    assert result is True
    assert batch_id not in queue.active_batches

    # Try to cleanup again (should fail)
    result = queue.cleanup_completed_batch(batch_id)
    assert result is False


def test_get_batch_update():
    """Test getting batch updates from queue"""
    queue = MessageQueue(batch_size=3, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    # Initially no updates
    result = queue.get_batch_update()
    assert result is None

    # Create batch
    source_dicts = [fake_source_dict(fake_context) for _ in range(3)]
    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # Should have initial send queued
    result = queue.get_batch_update()
    assert result is not None
    assert result.batch_id == batch_id
    assert result.lifecycle_stage == SourceLifecycleStage.SEND

    # Update item to trigger edit
    queue.update_batch_item(batch_id, str(source_dicts[0].uuid), ItemStatus.DOWNLOADING)

    # Should have edit queued
    result = queue.get_batch_update()
    assert result is not None
    assert result.batch_id == batch_id
    assert result.lifecycle_stage == SourceLifecycleStage.EDIT


def test_message_queue_priority():
    """Test that batched messages have correct priority in get_next_message"""
    queue = MessageQueue(batch_size=2, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    # Add some regular messages
    queue.iterate_play_order(guild_id)
    queue.iterate_single_message([lambda: None])

    # Add batched message
    source_dicts = [fake_source_dict(fake_context) for _ in range(2)]
    queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # Play order should come first
    message_type, _ = queue.get_next_message()
    assert message_type == MessageType.PLAY_ORDER

    # Batched message should come second (before source lifecycle and single message)
    message_type, _ = queue.get_next_message()
    assert message_type == MessageType.BATCHED_MESSAGE

    # Single message should come last
    message_type, _ = queue.get_next_message()
    assert message_type == MessageType.SINGLE_MESSAGE


def test_batch_overflow():
    """Test handling when batch overflows to create multiple batches"""
    queue = MessageQueue(batch_size=3, batch_timeout=30)
    fake_context = generate_fake_context()
    guild_id = fake_context['guild'].id

    mock_send = MagicMock()

    # Create more items than batch size
    source_dicts = [fake_source_dict(fake_context) for _ in range(7)]
    batch_id = queue.add_items_to_batch(guild_id, source_dicts, mock_send)

    # First batch should be finalized and active
    assert batch_id in queue.active_batches
    first_batch = queue.active_batches[batch_id]
    assert len(first_batch.source_dicts) == 3

    # Should have 2 active batches (3 items each) and 1 pending batch (1 item)
    assert len(queue.active_batches) == 2
    assert guild_id in queue.pending_batches

    pending_batch = queue.pending_batches[guild_id]
    assert len(pending_batch.source_dicts) == 1  # Remaining item
    assert pending_batch.batch_id != batch_id  # Different batch
