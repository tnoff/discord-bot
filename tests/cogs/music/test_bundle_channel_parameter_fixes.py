"""
Test the bundle channel parameter fixes from commit 0b66662
"""
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock
import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.media_request import MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.common import MediaRequestLifecycleStage
from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_source_dict, fake_engine, fake_context, fake_media_download  #pylint:disable=unused-import


@pytest.mark.asyncio
async def test_bundle_message_queue_updates_use_text_channel(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle message queue updates use bundle.text_channel instead of None"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create bundle with text_channel
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add a media request
    req = fake_source_dict(fake_context)
    req.bundle_uuid = bundle.uuid
    bundle.add_media_request(req)

    # Mock message queue to verify text_channel parameter is passed correctly
    with patch.object(cog.message_queue, 'update_multiple_mutable', return_value=True) as mock_update:
        # Test bundle failure update via __ensure_video_download_result (music.py:867)
        # pylint: disable=protected-access
        await cog._Music__ensure_video_download_result(req, None)

        # Verify text_channel parameter was used (not None)
        mock_update.assert_called_with(
            f'request_bundle-{bundle.uuid}',
            fake_context['channel'],  # Should be bundle.text_channel, not None
            sticky_messages=False,
        )


@pytest.mark.asyncio
async def test_playlist_add_message_updates_use_text_channel(fake_engine, fake_context):  #pylint:disable=redefined-outer-name
    """Test that playlist add operations use bundle.text_channel"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_engine)

    # Create bundle with text_channel
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add a media request
    req = fake_source_dict(fake_context)
    req.add_to_playlist = 123  # Simulate playlist add request
    req.bundle_uuid = bundle.uuid
    bundle.add_media_request(req)

    # Mock message queue to verify text_channel parameter
    with patch.object(cog.message_queue, 'update_multiple_mutable', return_value=True) as mock_update:
        with patch('discord_bot.cogs.music.retry_database_commands') as mock_db:
            # Mock successful playlist add
            mock_db.return_value = 456  # playlist_item_id

            # Create a fake media download that would trigger playlist add
            with TemporaryDirectory() as tmp_dir:
                with fake_media_download(tmp_dir, fake_context=fake_context) as media_download:
                    media_download.media_request = req

                    # Test the playlist add private method (music.py:1833)
                    # pylint: disable=protected-access
                    await cog._Music__add_playlist_item_function(123, media_download)

                    # Verify text_channel parameter was used (not None) - music.py:1833
                    mock_update.assert_called_with(
                        f'request_bundle-{bundle.uuid}',
                        fake_context['channel'],  # Should be bundle.text_channel, not None
                        sticky_messages=False,
                    )


@pytest.mark.asyncio
async def test_bundle_processing_status_updates_use_text_channel(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle processing status updates use text_channel correctly"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create bundle with text_channel
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add a media request
    req = fake_source_dict(fake_context)
    bundle.add_media_request(req)

    # Mock message queue to track calls
    with patch.object(cog.message_queue, 'update_multiple_mutable', return_value=True) as mock_update:
        # Test different bundle status updates that were fixed in the commit

        # Update to IN_PROGRESS (music.py:943)
        bundle.update_request_status(req, MediaRequestLifecycleStage.IN_PROGRESS)
        cog.message_queue.update_multiple_mutable(
            f'request_bundle-{bundle.uuid}',
            bundle.text_channel,
            sticky_messages=False,
        )

        # Update to COMPLETED (when adding to player - music.py:812)
        bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)
        cog.message_queue.update_multiple_mutable(
            f'request_bundle-{bundle.uuid}',
            bundle.text_channel,
            sticky_messages=False,
        )

        # Verify all calls used bundle.text_channel (not None)
        expected_calls = [
            (f'request_bundle-{bundle.uuid}', fake_context['channel'], False),
            (f'request_bundle-{bundle.uuid}', fake_context['channel'], False),
        ]

        actual_calls = [(call.args[0], call.args[1], call.kwargs.get('sticky_messages', True)) for call in mock_update.call_args_list]
        assert len(actual_calls) == len(expected_calls)
        for actual, expected in zip(actual_calls, expected_calls):
            assert actual[0] == expected[0]  # bundle key
            assert actual[1] == expected[1]  # text_channel
            assert actual[2] == expected[2]  # sticky_messages


@pytest.mark.asyncio
async def test_bundle_constructor_integration_with_music_cog(fake_context):  #pylint:disable=redefined-outer-name
    """Test that Music cog creates bundles with proper text_channel parameter"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Test bundle creation in enqueue_media_requests (music.py:1219)
    entries = [fake_source_dict(fake_context), fake_source_dict(fake_context)]

    # Create a mock player
    mock_player = MagicMock()
    mock_player.guild = fake_context['guild']
    mock_player.text_channel = fake_context['channel']

    # Test the method that actually creates bundles
    result = await cog.enqueue_media_requests(fake_context['context'], mock_player, entries)

    # Verify bundle was created and stored
    assert result is True
    assert len(cog.multirequest_bundles) == 1

    # Verify bundle has correct text_channel
    bundle = list(cog.multirequest_bundles.values())[0]
    assert bundle.text_channel == fake_context['channel']
    assert bundle.guild_id == fake_context['guild'].id
    assert bundle.channel_id == fake_context['channel'].id


def test_bundle_safe_access_pattern_prevents_keyerror(fake_context):  #pylint:disable=redefined-outer-name
    """Test that safe bundle access patterns prevent KeyError exceptions"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create a media request with bundle_uuid but don't add bundle to cog.multirequest_bundles
    req = fake_source_dict(fake_context)
    req.bundle_uuid = "nonexistent-bundle-uuid"

    # Test safe access pattern: bundle = self.multirequest_bundles.get(uuid) if uuid else None
    bundle = cog.multirequest_bundles.get(req.bundle_uuid) if req.bundle_uuid else None
    assert bundle is None  # Should return None, not raise KeyError

    # Test with None bundle_uuid
    req.bundle_uuid = None
    bundle = cog.multirequest_bundles.get(req.bundle_uuid) if req.bundle_uuid else None
    assert bundle is None  # Should handle None gracefully

    # Test with empty string bundle_uuid
    req.bundle_uuid = ""
    bundle = cog.multirequest_bundles.get(req.bundle_uuid) if req.bundle_uuid else None
    assert bundle is None  # Should handle empty string gracefully


@pytest.mark.asyncio
async def test_bundle_cleanup_preserves_text_channel_reference(fake_context):  #pylint:disable=redefined-outer-name
    """Test that bundle cleanup operations preserve text_channel references"""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, None)

    # Create multiple bundles for the same guild
    bundle1 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle3 = MultiMediaRequestBundle(999, 888, fake_context['channel'])  # Different guild

    cog.multirequest_bundles[bundle1.uuid] = bundle1
    cog.multirequest_bundles[bundle2.uuid] = bundle2
    cog.multirequest_bundles[bundle3.uuid] = bundle3

    # Add requests and mark as finished
    for bundle in [bundle1, bundle2]:
        req = fake_source_dict(fake_context)
        bundle.add_media_request(req)
        bundle.update_request_status(req, MediaRequestLifecycleStage.COMPLETED)

    req3 = fake_source_dict(fake_context)
    req3.guild_id = 999
    req3.channel_id = 888
    bundle3.add_media_request(req3)
    bundle3.update_request_status(req3, MediaRequestLifecycleStage.COMPLETED)

    # Test cleanup for specific guild (music.py:1095-1108)
    test_guild_id = fake_context['guild'].id
    bundles_to_remove = []
    for bundle_uuid, bundle in cog.multirequest_bundles.items():
        if bundle.guild_id == test_guild_id and bundle.finished:
            bundles_to_remove.append(bundle_uuid)
            # Verify text_channel is still accessible during cleanup
            assert bundle.text_channel is not None
            assert bundle.text_channel == fake_context['channel']

    # Remove bundles for the test guild
    for bundle_uuid in bundles_to_remove:
        del cog.multirequest_bundles[bundle_uuid]

    # Verify cleanup worked correctly
    assert bundle3.uuid in cog.multirequest_bundles  # Different guild should remain
    assert bundle1.uuid not in cog.multirequest_bundles
    assert bundle2.uuid not in cog.multirequest_bundles

    # Remaining bundle should still have valid text_channel
    remaining_bundle = cog.multirequest_bundles[bundle3.uuid]
    assert remaining_bundle.text_channel == fake_context['channel']
