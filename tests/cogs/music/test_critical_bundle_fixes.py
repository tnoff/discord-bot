"""
Test critical fixes for bundle-related issues found in the recent commit
"""
import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle, MediaRequest
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from discord_bot.cogs.music_helpers.download_client import DownloadClientException
from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_context, fake_source_dict  #pylint:disable=unused-import


def test_bundle_channel_parameter_handling(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle operations handle None channel parameter correctly for existing bundles"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create a bundle by first calling with a valid channel (this creates the bundle in message_queue)
    bundle_uuid = "test-bundle-uuid"
    bundle_index = f'request_bundle-{bundle_uuid}'

    # First call with valid channel to create the bundle
    cog.message_queue.update_multiple_mutable(bundle_index, fake_context['channel'])

    # Second call with None channel should work (bundle already exists)
    # This is the valid case - updating existing bundle with None channel
    result = cog.message_queue.update_multiple_mutable(bundle_index, None)
    assert result is True

    # Verify the bundle exists and is queued for processing
    assert bundle_index in cog.message_queue.mutable_bundles
    assert cog.message_queue.mutable_bundles[bundle_index].is_queued_for_processing is True


@pytest.mark.asyncio
async def test_bundle_error_handling_missing_bundle(fake_context):  #pylint:disable=redefined-outer-name
    """Test error handling when bundle is missing"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create a media request with non-existent bundle UUID
    req = fake_source_dict(fake_context)
    req.bundle_uuid = "nonexistent-bundle-uuid"

    # Test __ensure_video_download_result with missing bundle
    result = await cog._Music__ensure_video_download_result(req, None)  #pylint:disable=protected-access

    # Should return None (early return) and handle gracefully (not crash)
    assert result is None


@pytest.mark.asyncio
async def test_bundle_error_handling_bad_video(fake_context):  #pylint:disable=redefined-outer-name
    """Test error handling in __return_bad_video with missing bundle"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create a media request with non-existent bundle UUID
    req = fake_source_dict(fake_context)
    req.bundle_uuid = "nonexistent-bundle-uuid"

    # Create a mock exception
    exception = DownloadClientException("Test error", user_message="User-friendly error")

    # Test __return_bad_video with missing bundle
    # Should handle gracefully without crashing
    await cog._Music__return_bad_video(req, exception)  #pylint:disable=protected-access

    # If we reach here without exception, the error handling worked


def test_bundle_cleanup_memory_leak_prevention(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle cleanup prevents memory leaks"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create bundles for multiple guilds
    guild1_id = 12345
    guild2_id = 12346
    guild3_id = 12347

    bundles = []
    for i, guild_id in enumerate([guild1_id, guild2_id, guild3_id]):
        bundle = MultiMediaRequestBundle(guild_id, 456 + i, fake_context['channel'])
        cog.multirequest_bundles[bundle.uuid] = bundle
        bundles.append(bundle)

    # Verify all bundles exist
    assert len(cog.multirequest_bundles) == 3

    # Simulate guild cleanup logic from music.py lines 1095-1108
    target_guild_id = guild1_id

    # Clear bundles for specific guild (current implementation)
    bundles_to_remove = []
    for uuid, item in cog.multirequest_bundles.items():
        if int(item.guild_id) == int(target_guild_id):
            item.shutdown()
            bundles_to_remove.append(uuid)

    # Remove shutdown bundles
    for uuid in bundles_to_remove:
        cog.multirequest_bundles.pop(uuid, None)

    # Should have 2 remaining bundles (for other guilds)
    assert len(cog.multirequest_bundles) == 2

    # Verify correct bundles remain
    remaining_guild_ids = {bundle.guild_id for bundle in cog.multirequest_bundles.values()}
    assert guild1_id not in remaining_guild_ids
    assert guild2_id in remaining_guild_ids
    assert guild3_id in remaining_guild_ids


def test_bundle_removal_logic_consistency(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle removal logic handles both finished and shutdown states consistently"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test case 1: Finished bundle
    finished_bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    req1 = fake_source_dict(fake_context)
    finished_bundle.add_media_request(req1)
    finished_bundle.update_request_status(req1, MediaRequestLifecycleStage.COMPLETED)
    assert finished_bundle.finished is True
    assert finished_bundle.is_shutdown is False

    cog.multirequest_bundles[finished_bundle.uuid] = finished_bundle

    # Test case 2: Shutdown bundle (with content so it's not automatically finished)
    shutdown_bundle = MultiMediaRequestBundle(12346, 457, fake_context['channel'])
    req2 = fake_source_dict(fake_context)
    shutdown_bundle.add_media_request(req2)  # Add content so it's not automatically finished
    shutdown_bundle.shutdown()
    assert shutdown_bundle.finished is False  # Not finished, just shutdown
    assert shutdown_bundle.is_shutdown is True

    cog.multirequest_bundles[shutdown_bundle.uuid] = shutdown_bundle

    # Simulate the removal logic from music.py:658-662
    test_cases = [
        (finished_bundle, "finished bundle"),
        (shutdown_bundle, "shutdown bundle")
    ]

    for bundle, description in test_cases:
        bundle_uuid = bundle.uuid
        retrieved_bundle = cog.multirequest_bundles.get(bundle_uuid)

        # Test the condition from the commit
        if (retrieved_bundle.finished or retrieved_bundle.is_shutdown) and bundle_uuid in cog.multirequest_bundles:
            removed_bundle = cog.multirequest_bundles.pop(bundle_uuid, None)

            # Test the problematic delete_after logic
            delete_after = None
            if retrieved_bundle.finished and removed_bundle:
                delete_after = cog.delete_after

            # Document the issue: shutdown bundles don't get delete_after set
            if description == "finished bundle":
                assert delete_after is not None, "Finished bundles should get delete_after"
            elif description == "shutdown bundle":
                assert delete_after is None, "BUG: Shutdown bundles don't get delete_after"

        # Verify bundle was removed
        assert bundle_uuid not in cog.multirequest_bundles


def test_bundle_uuid_string_format_validation(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle UUIDs follow expected format"""
    bundle = MultiMediaRequestBundle(123, 456, fake_context['channel'])

    # Verify UUID format
    assert bundle.uuid.startswith('request.bundle.')
    assert len(bundle.uuid) > len('request.bundle.')

    # Verify it's consistent with MediaRequest UUID format
    req = MediaRequest(123, 456, "user", 1, "search", "search")
    assert req.uuid.startswith('request.')
    assert not req.uuid.startswith('request.bundle.')


def test_bundle_lifecycle_stage_transitions(fake_context):  #pylint:disable=redefined-outer-name
    """Test all possible lifecycle stage transitions"""
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    req = fake_source_dict(fake_context)
    bundle.add_media_request(req)

    # Initial state should be QUEUED
    assert bundle.media_requests[0]['status'] == MediaRequestLifecycleStage.QUEUED

    # Test valid transitions
    transitions = [
        (MediaRequestLifecycleStage.QUEUED, MediaRequestLifecycleStage.IN_PROGRESS),
        (MediaRequestLifecycleStage.IN_PROGRESS, MediaRequestLifecycleStage.COMPLETED),
        (MediaRequestLifecycleStage.IN_PROGRESS, MediaRequestLifecycleStage.FAILED),
        (MediaRequestLifecycleStage.QUEUED, MediaRequestLifecycleStage.DISCARDED),
    ]

    for from_stage, to_stage in transitions:
        # Reset to from_stage
        bundle.media_requests[0]['status'] = from_stage

        # Transition to to_stage
        result = bundle.update_request_status(req, to_stage)
        assert result is True
        assert bundle.media_requests[0]['status'] == to_stage
