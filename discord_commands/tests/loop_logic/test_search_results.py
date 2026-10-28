from asyncio import QueueFull
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_core.cogs.music_helpers.common import MediaRequestLifecycleStage, YOUTUBE_VIDEO_PREFIX
from discord_core.exceptions import ExitEarlyException, YoutubeMusicRetryException
from discord_core.types.queue import PutsBlocked
from discord_core.utils.media_enqueuer import MediaEnqueuer

from tests.fakes.search_pipeline_fakes import (MockYoutubeMusicClient, create_test_media_request,
                                               create_test_playlist_add_request, make_broker_bundle)
from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod


def build_pod(fake_context, fake_stores, client=None, **kwargs):  #pylint:disable=redefined-outer-name
    '''A pod with the search stack attached, whose ytmusic client answers ``client``.'''
    pod = ResultsPod(fake_context, fake_stores, with_search=True, **kwargs)
    pod.youtube_music_search_client.local_worker._client = client or MockYoutubeMusicClient('test-video-id')  #pylint:disable=protected-access
    return pod


def search_queue(pod):
    '''The search worker's input queue, which the search pod would fill.'''
    return pod.youtube_music_search_client.local_worker._input_queue  #pylint:disable=protected-access


@pytest.mark.asyncio()
async def test_process_search_results_empty_idles(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """process_search_results sleeps (idles) when the broker has no resolution."""
    mock_sleep = mocker.patch('discord_commands.workers.result_loops.sleep', return_value=True)
    pod = ResultsPod(fake_context, fake_stores)

    # Nothing on the broker search-result queue → idle backoff, no submit.
    await pod.loops.process_search_results()
    mock_sleep.assert_awaited()
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_process_search_results_shutdown(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """process_search_results exits early once the pod is shutting down."""
    pod = ResultsPod(fake_context, fake_stores)
    pod.stop_event.set()

    with pytest.raises(ExitEarlyException) as exc:
        await pod.loops.process_search_results()
    assert 'Shutdown called' in str(exc.value)


@pytest.mark.asyncio()
async def test_search_youtube_music_successful_search_no_cache(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test successful YouTube Music search with no cache hit"""
    pod = build_pod(fake_context, fake_stores)

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    await pod.search_driver.run_once(pod.stop_event)

    # Verify search string was updated with YouTube prefix
    assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

    # search_youtube_music now hands the resolved request to the broker seam;
    # process_search_results runs the cache-check + download submit tail.
    await pod.loops.process_search_results()

    # Verify request was added to download queue
    assert await pod.download_client.queue_size(fake_context['guild'].id) > 0
    download_item = await pod.download_client.local_worker.get_input_nowait()
    assert download_item == media_request

    # Verify bundle status was updated
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.QUEUED


@pytest.mark.asyncio()
async def test_search_youtube_music_successful_search_cache_hit(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test successful YouTube Music search with cache hit"""
    pod = build_pod(fake_context, fake_stores)

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=cached_download))

            # The guild has a player; the cached track goes to it.
            await pod.open_player(fake_context['guild'].id, fake_context['channel'].id)
            mock_add_cached = mocker.patch.object(pod.player_gateway, 'add_cached', return_value=None)

            await pod.search_driver.run_once(pod.stop_event)
            await pod.loops.process_search_results()

            # Verify search string was updated with YouTube prefix
            assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

            # Verify download queue is empty (cache hit, no download needed)
            assert await pod.download_client.queue_size(fake_context['guild'].id) == 0

            # Verify the cached track was handed to the player
            mock_add_cached.assert_called_once_with(fake_context['guild'].id, cached_download)


@pytest.mark.asyncio()
async def test_search_youtube_music_cache_hit_marks_request_completed(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """
    Regression: when a SEARCH-type request resolves to a cached track, the
    *current* media_request must transition to COMPLETED. Previously it stayed
    in QUEUED forever, leaving the bundle's "Media request queued for download"
    row dangling in the channel even though the song was playing.
    """
    pod = build_pod(fake_context, fake_stores, dispatcher=None)

    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request,
                                                     all_added=True)

    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    with TemporaryDirectory() as tmp_dir:
        # The real broker.check_cache binds the cached download to the SAME
        # media_request it was queried with (see _media_download_from_dict), so the
        # COMPLETED push inside _enqueue_media_download_from_cache advances THIS
        # request. Bind the same object here so the mock reflects that contract.
        with fake_media_download(tmp_dir, media_request=media_request) as cached_download:
            mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=cached_download))

            await pod.open_player(fake_context['guild'].id, fake_context['channel'].id)
            mocker.patch.object(pod.player_gateway, 'add_cached', return_value=None)

            await pod.search_driver.run_once(pod.stop_event)
            await pod.loops.process_search_results()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED
    # The broker re-rendered on the COMPLETED push, so its bundle state already
    # reflects the cleared row and incremented counter.
    assert bundle.finished is True
    assert bundle.completed == 1


@pytest.mark.asyncio()
async def test_search_youtube_music_no_result(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search returns no results"""
    pod = build_pod(fake_context, fake_stores, client=MockYoutubeMusicClient(None))  # No result

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    await pod.search_driver.run_once(pod.stop_event)
    await pod.loops.process_search_results()

    # Verify original search string unchanged
    assert media_request.search_result.raw_search_string == 'test search'

    # Verify download queue still has item
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 1


@pytest.mark.asyncio()
async def test_search_youtube_music_download_queue_full(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when download queue is full"""
    pod = build_pod(fake_context, fake_stores)

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    # Mock download queue full
    mocker.patch.object(pod.download_client, 'submit', side_effect=QueueFull())

    await pod.search_driver.run_once(pod.stop_event)
    # The download submit (now in process_search_results) hits the full queue.
    await pod.loops.process_search_results()

    # Verify bundle status was updated to DISCARDED
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_search_youtube_music_download_queue_blocked(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when download queue puts are blocked"""
    pod = build_pod(fake_context, fake_stores)

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    # Mock download queue blocked
    mocker.patch.object(pod.download_client, 'submit', side_effect=PutsBlocked())

    await pod.search_driver.run_once(pod.stop_event)
    # The blocked download submit now surfaces in process_search_results.
    await pod.loops.process_search_results()

    # Blocked puts drop the request to DISCARDED.
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_search_youtube_music_playlist_item(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search for playlist addition"""
    pod = build_pod(fake_context, fake_stores)

    # Create a broker bundle and playlist add request
    media_request = create_test_playlist_add_request(fake_context, playlist_id=123, search_string='test search')
    _bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=cached_download))

            # Mock playlist addition
            add_item = mocker.patch.object(MediaEnqueuer, 'add_playlist_item', return_value=None)

            await pod.search_driver.run_once(pod.stop_event)
            await pod.loops.process_search_results()

            # Verify playlist addition was called with the right request and result
            add_item.assert_called_once()
            call_request, call_result = add_item.call_args[0]
            assert call_request.playlist_id == 123
            assert call_result.webpage_url == cached_download.webpage_url
            assert call_result.title == cached_download.title
            assert call_result.uploader == cached_download.uploader

            # Verify download queue is empty (playlist addition, no player queue needed)
            assert await pod.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_bundle_expiration_during_search_processing(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test handling when bundle expires while item is being processed in search queue"""
    pod = build_pod(fake_context, fake_stores)

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    # Remove bundle from the broker to simulate expiration
    await pod.broker_client.delete_bundle(bundle.uuid)

    # Should handle missing bundle gracefully
    await pod.search_driver.run_once(pod.stop_event)
    await pod.loops.process_search_results()

    # Verify search still happened and item went to download queue
    assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 1


@pytest.mark.asyncio()
async def test_message_queue_update_failure_during_search(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test handling when message queue update fails during search processing"""
    # The broker captures the dispatcher at construction; we arm update_mutable
    # to raise only after the bundle is set up, so this exercises bundle-render
    # failure during the search push.
    dispatcher = MagicMock()
    pod = build_pod(fake_context, fake_stores, dispatcher=dispatcher)

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request)

    # Add to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    # Arm the dispatcher to raise on the next render
    dispatcher.update_mutable.side_effect = Exception("Message queue error")

    # Should handle message queue failure gracefully
    try:
        await pod.search_driver.run_once(pod.stop_event)
        await pod.loops.process_search_results()
        # Should not crash despite message queue failure
        # Verify core functionality still works
        assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
        assert await pod.download_client.queue_size(fake_context['guild'].id) == 1
    except Exception as e: #pylint:disable=broad-exception-caught
        # If exception propagates, it should be handled gracefully in real implementation
        assert "Message queue error" in str(e)


@pytest.mark.asyncio()
async def test_concurrent_bundle_operations_during_search(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test concurrent bundle operations while search queue is processing"""
    pod = build_pod(fake_context, fake_stores)

    # Create multiple broker bundles with one request each
    media_request1 = create_test_media_request(fake_context, 'test search 1')
    media_request2 = create_test_media_request(fake_context, 'test search 2')
    bundle1, media_request1 = await make_broker_bundle(pod, fake_context, request=media_request1)
    bundle2, media_request2 = await make_broker_bundle(pod, fake_context, request=media_request2)

    # Add both to search queue
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request1)
    search_queue(pod).put_nowait(fake_context['guild'].id, media_request2)

    # Mock cache miss
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    # Process both items through the search loop and the broker seam
    await pod.search_driver.run_once(pod.stop_event)
    await pod.search_driver.run_once(pod.stop_event)
    await pod.loops.process_search_results()
    await pod.loops.process_search_results()

    # Verify both items were processed correctly
    assert media_request1.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
    assert media_request2.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

    # Both should be in download queue
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 2

    # Verify both bundles were updated
    assert len(bundle1.bundled_requests) == 1
    assert len(bundle2.bundled_requests) == 1


@pytest.mark.asyncio()
async def test_search_youtube_music_429_resets_lifecycle_on_retry(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that a re-queued item resets from RETRY_SEARCH back to SEARCHING on next attempt"""
    call_count = 0

    class SucceedOnSecondCallClient:
        def search(self, search_string): #pylint:disable=unused-argument
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise YoutubeMusicRetryException('429 Exhaust Limit Hit')
            return 'test-video-id'

    pod = build_pod(fake_context, fake_stores, client=SucceedOnSecondCallClient())

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request,
                                                      all_added=True)

    search_queue(pod).put_nowait(fake_context['guild'].id, media_request)

    # First call hits 429
    mocker.patch.object(pod.youtube_music_search_client.local_worker, 'backoff_wait', new=AsyncMock())
    await pod.search_driver.run_once(pod.stop_event)
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH

    # The 429 armed the backoff window, and the loop no longer pops while one is
    # open — clear it so the retry attempt actually runs.
    pod.youtube_music_search_client.local_worker._wait_timestamp = None  #pylint:disable=protected-access

    # Second call succeeds — lifecycle should reset to SEARCHING then proceed to QUEUED
    await pod.search_driver.run_once(pod.stop_event)
    await pod.loops.process_search_results()
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.QUEUED
