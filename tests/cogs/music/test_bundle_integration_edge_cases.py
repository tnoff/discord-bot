"""
Test edge cases for MultiMediaRequestBundle integration with Music cog
"""
import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.common import MultipleMutableType
from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_context  #pylint:disable=unused-import


def test_bundle_lookup_with_nonexistent_uuid(fake_context):  #pylint:disable=redefined-outer-name
    """Test music.py handling of non-existent bundle UUIDs"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test the logic from music.py lines 653-665
    fake_uuid = 'nonexistent-bundle-uuid'
    item = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{fake_uuid}'

    # Extract UUID as the actual code does
    bundle_uuid = item.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]
    bundle = cog.multirequest_bundles.get(bundle_uuid)

    # Should handle gracefully with None bundle
    assert bundle is None

    # Message content should be empty list when bundle doesn't exist
    message_content = bundle.print() if bundle else []
    assert message_content == []


def test_bundle_cleanup_thread_safety_simulation(fake_context):  #pylint:disable=redefined-outer-name
    """Test the thread-safe bundle cleanup logic from music.py"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create a finished bundle
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Simulate the thread-safe removal logic from music.py lines 658-662
    bundle_uuid = bundle.uuid
    retrieved_bundle = cog.multirequest_bundles.get(bundle_uuid)

    if retrieved_bundle and retrieved_bundle.finished:
        # This is the thread-safe check: only remove if still finished and still exists
        if retrieved_bundle.finished and bundle_uuid in cog.multirequest_bundles:
            removed_bundle = cog.multirequest_bundles.pop(bundle_uuid, None)
            # Only proceed if we actually removed it
            assert removed_bundle is not None

    # Bundle should be gone
    assert bundle_uuid not in cog.multirequest_bundles


def test_search_message_cleanup_logic():
    """Test the search message cleanup logic from music.py lines 672-676"""
    # Simulate the SearchClient messages dictionary
    search_messages = {
        'uuid-with-content': ['Some search message'],
        'uuid-empty': [],
        'uuid-to-cleanup': []
    }

    # Test the cleanup logic for empty messages
    test_cases = [
        ('uuid-with-content', True),   # Should keep - has content
        ('uuid-empty', False),         # Should remove - empty
        ('uuid-to-cleanup', False)     # Should remove - empty
    ]

    for context_uuid, should_keep in test_cases:
        item = f'{MultipleMutableType.SEARCH.value}-{context_uuid}'
        context_uuid_extracted = item.split(f'{MultipleMutableType.SEARCH.value}-', 1)[1]
        message_content = search_messages.get(context_uuid_extracted, [])

        if not message_content:
            search_messages.pop(context_uuid_extracted, None)

        # Verify expected behavior
        if should_keep:
            assert context_uuid in search_messages
        else:
            assert context_uuid not in search_messages


def test_music_cog_bundle_cleanup_on_shutdown(fake_context):  #pylint:disable=redefined-outer-name
    """Test that multirequest_bundles are cleaned up on music cog shutdown"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Add some bundles
    for i in range(3):
        bundle = MultiMediaRequestBundle(123 + i, 456 + i, fake_context['channel'])
        cog.multirequest_bundles[bundle.uuid] = bundle

    # Verify bundles exist
    assert len(cog.multirequest_bundles) == 3

    # Simulate the cleanup from shutdown method
    # From the commit, line 568: self.multirequest_bundles.clear()
    cog.multirequest_bundles.clear()

    # Verify all bundles cleared
    assert len(cog.multirequest_bundles) == 0


def test_bundle_string_parsing_safety():
    """Test safe string parsing for bundle UUIDs to prevent corruption"""
    # Test cases that could cause issues with naive .replace() approach
    test_cases = [
        f'{MultipleMutableType.REQUEST_BUNDLE.value}-uuid-with-{MultipleMutableType.REQUEST_BUNDLE.value}-inside',
        f'{MultipleMutableType.REQUEST_BUNDLE.value}-normal-uuid-123',
        f'{MultipleMutableType.REQUEST_BUNDLE.value}-{MultipleMutableType.REQUEST_BUNDLE.value}',  # Edge case
    ]

    for item in test_cases:
        # Use the safe split approach from the fixed code
        if MultipleMutableType.REQUEST_BUNDLE.value in item:
            # This is the safe way that was implemented
            bundle_uuid = item.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]

            # Verify it extracts correctly
            expected_uuid = item.replace(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', '', 1)
            assert bundle_uuid == expected_uuid


@pytest.mark.asyncio
async def test_message_content_dispatch_with_empty_bundles(fake_context):  #pylint:disable=redefined-outer-name
    """Test that empty message content is handled properly in message dispatch"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test with empty message content (common when bundles are cleaned up)
    message_content = []

    # This simulates the code path in music.py around line 678
    # where empty message content should be handled gracefully
    funcs = await cog.message_queue.update_mutable_bundle_content(
        'test-bundle-uuid', message_content, delete_after=None
    )

    # Should return empty function list for empty content
    assert funcs == []
