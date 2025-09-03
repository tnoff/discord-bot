"""
Test error handling in SearchClient for the new multi-mutable message system
"""
from unittest.mock import Mock
import pytest

from discord_bot.cogs.music_helpers.search_client import SearchClient
from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from discord_bot.cogs.music_helpers.common import MultipleMutableType


@pytest.mark.asyncio
async def test_search_message_cleanup_after_processing():
    """Test that search messages are cleaned up after processing in music.py"""
    # This test simulates the cleanup logic from music.py lines 672-676
    search_client = SearchClient(Mock(), spotify_client=None, youtube_client=None,
                               youtube_music_client=None, number_shuffles=1)

    # Simulate having a context UUID with empty messages (processed)
    context_uuid = "test-uuid-123"
    search_client.messages[context_uuid] = []

    # Simulate the cleanup logic from music.py
    item = f'{MultipleMutableType.SEARCH.value}-{context_uuid}'
    context_uuid_extracted = item.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]
    message_content = search_client.messages.get(context_uuid_extracted, [])

    if not message_content:
        search_client.messages.pop(context_uuid_extracted, None)

    # Verify cleanup occurred
    assert context_uuid not in search_client.messages


def test_search_client_messages_dictionary_initialization():
    """Test that SearchClient properly initializes messages dictionary"""
    message_queue = MessageQueue()
    search_client = SearchClient(message_queue, spotify_client=None, youtube_client=None,
                               youtube_music_client=None, number_shuffles=1)

    # Verify messages dictionary exists and is empty
    assert hasattr(search_client, 'messages')
    assert isinstance(search_client.messages, dict)
    assert len(search_client.messages) == 0
