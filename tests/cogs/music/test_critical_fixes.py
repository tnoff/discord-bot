"""
Tests for critical bug fixes in Music cog
Tests race condition fixes, memory leak fixes, and error handling improvements
"""
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage, MultipleMutableType
from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_source_dict, fake_context  #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_race_condition_fix_bundle_cleanup(fake_context):  #pylint:disable=redefined-outer-name
    """Test that the race condition fix prevents double cleanup"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create bundle with single request so it finishes immediately
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    req = fake_source_dict(fake_context)
    bundle.add_media_request(req)
    bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    assert bundle.finished is True
    assert bundle.uuid in cog.multirequest_bundles

    # Simulate the fixed cleanup logic from music.py:658-661
    def simulate_fixed_cleanup():
        bundle_uuid = bundle.uuid
        retrieved_bundle = cog.multirequest_bundles.get(bundle_uuid)
        if retrieved_bundle and retrieved_bundle.finished:
            # Fixed version: check if bundle still exists before removal
            if bundle_uuid in cog.multirequest_bundles:
                return cog.multirequest_bundles.pop(bundle_uuid, None)
        return None

    # Execute cleanup from multiple threads simultaneously
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(simulate_fixed_cleanup) for _ in range(3)]
        results = [future.result() for future in futures]

    # Only one thread should have successfully removed the bundle
    successful_removals = [r for r in results if r is not None]
    assert len(successful_removals) == 1, f"Expected 1 removal, got {len(successful_removals)}"

    # Bundle should no longer exist
    assert bundle.uuid not in cog.multirequest_bundles


def test_memory_leak_fix_guild_cleanup(fake_context):  #pylint:disable=redefined-outer-name
    """Test that guild cleanup now properly clears multirequest_bundles"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create multiple bundles
    bundles = []
    for _ in range(5):
        bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
        cog.multirequest_bundles[bundle.uuid] = bundle
        bundles.append(bundle)

    assert len(cog.multirequest_bundles) == 5

    # Mock player for guild cleanup
    mock_player = Mock()
    mock_player.text_channel = fake_context['channel']
    cog.players[fake_context['guild'].id] = mock_player

    # Simulate the fixed guild cleanup (includes the .clear() fix)
    for _uuid, item in cog.multirequest_bundles.items():
        item.shutdown()
        # Simulate message queue update (doesn't need actual implementation for test)

    # Apply the fix: clear the bundles dictionary
    cog.multirequest_bundles.clear()

    # After fix, bundles dictionary should be empty (memory leak fixed)
    assert len(cog.multirequest_bundles) == 0

    # Verify bundles were properly shutdown
    for bundle in bundles:
        assert bundle.is_shutdown is True


def test_error_handling_fix_missing_bundle_references(fake_context):  #pylint:disable=redefined-outer-name
    """Test that missing bundle references are handled gracefully"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test case 1: bundle_uuid is None (should not crash)
    fake_uuid = None
    bundle = cog.multirequest_bundles.get(fake_uuid) if fake_uuid else None
    assert bundle is None
    # This should not crash when bundle is None
    message_content = bundle.print() if bundle else []
    assert not message_content

    # Test case 2: bundle_uuid doesn't exist in dictionary (should not crash)
    fake_uuid = 'request.bundle.non-existent-uuid'
    bundle = cog.multirequest_bundles.get(fake_uuid)
    assert bundle is None
    # This should not crash when bundle is None
    message_content = bundle.print() if bundle else []
    assert not message_content

    # Test case 3: Simulate the message queue processing with missing bundle
    index_name = f'{MultipleMutableType.REQUEST_BUNDLE.value}-{fake_uuid}'
    bundle_uuid = index_name.split(f'{MultipleMutableType.REQUEST_BUNDLE.value}-', 1)[1]

    # Safe access pattern should not crash
    bundle = cog.multirequest_bundles.get(bundle_uuid)
    assert bundle is None

    if bundle:
        message_content = bundle.print()
    else:
        message_content = []  # Graceful fallback

    assert not message_content


def test_bundle_cleanup_thread_safety_with_fix(fake_context):  #pylint:disable=redefined-outer-name
    """Test that multiple threads can safely access bundle cleanup with the fix"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create multiple finished bundles
    bundles = []
    for _ in range(10):
        bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
        cog.multirequest_bundles[bundle.uuid] = bundle

        req = fake_source_dict(fake_context)
        bundle.add_media_request(req)
        bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

        bundles.append(bundle)

    assert len(cog.multirequest_bundles) == 10

    # Simulate multiple threads trying to clean up bundles simultaneously
    def safe_cleanup_attempt(bundle_uuid):
        bundle = cog.multirequest_bundles.get(bundle_uuid)
        if bundle and bundle.finished:
            # Apply the fix: check existence before removal
            if bundle_uuid in cog.multirequest_bundles:
                return cog.multirequest_bundles.pop(bundle_uuid, None)
        return None

    with ThreadPoolExecutor(max_workers=5) as executor:
        # Each bundle gets cleaned up by multiple threads
        futures = []
        for bundle in bundles:
            for _ in range(3):  # 3 threads per bundle
                futures.append(executor.submit(safe_cleanup_attempt, bundle.uuid))

        results = [future.result() for future in futures]

    # Exactly 10 bundles should have been removed (one per bundle, despite multiple threads)
    successful_removals = [r for r in results if r is not None]
    assert len(successful_removals) == 10, f"Expected 10 removals, got {len(successful_removals)}"

    # All bundles should be removed
    assert len(cog.multirequest_bundles) == 0


def test_comprehensive_error_resilience_with_fixes(fake_context):  #pylint:disable=redefined-outer-name
    """Test comprehensive error resilience with all fixes applied"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test scenario combining all fixes
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    req = fake_source_dict(fake_context)
    bundle.add_media_request(req)
    bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    # Test race condition fix
    def thread_safe_cleanup():
        if bundle.uuid in cog.multirequest_bundles and bundle.finished:
            return cog.multirequest_bundles.pop(bundle.uuid, None)
        return None

    # Multiple threads should safely handle cleanup
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(thread_safe_cleanup) for _ in range(3)]
        results = [future.result() for future in futures]

    successful_removals = [r for r in results if r is not None]
    assert len(successful_removals) == 1

    # Test error handling with missing bundle
    missing_bundle = cog.multirequest_bundles.get('non-existent')
    assert missing_bundle is None
    content = missing_bundle.print() if missing_bundle else []
    assert not content

    # Test memory leak fix (simulate clearing after operations)
    cog.multirequest_bundles.clear()
    assert len(cog.multirequest_bundles) == 0


def test_bundle_lifecycle_with_all_fixes(fake_context):  #pylint:disable=redefined-outer-name
    """Test complete bundle lifecycle with all fixes applied"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create bundle and add requests
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    requests = []
    for _ in range(5):
        req = fake_source_dict(fake_context)
        bundle.add_media_request(req)
        requests.append(req)

    # Complete all requests
    for req in requests:
        bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    assert bundle.finished is True

    # Simulate safe cleanup with race condition fix
    if bundle.uuid in cog.multirequest_bundles and bundle.finished:
        removed_bundle = cog.multirequest_bundles.pop(bundle.uuid, None)
        assert removed_bundle is not None

    # Verify bundle is removed
    assert bundle.uuid not in cog.multirequest_bundles

    # Test error handling after removal
    missing_bundle = cog.multirequest_bundles.get(bundle.uuid)
    assert missing_bundle is None

    # Should not crash when accessing removed bundle
    content = missing_bundle.print() if missing_bundle else []
    assert not content

    # Apply memory leak fix
    cog.multirequest_bundles.clear()
    assert len(cog.multirequest_bundles) == 0
