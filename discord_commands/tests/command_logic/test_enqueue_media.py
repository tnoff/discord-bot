from asyncio import QueueFull
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from discord_core.cogs.music_helpers.common import MediaRequestLifecycleStage, SearchType
from discord_core.types.queue import PutsBlocked
from discord_core.types.search import SearchResult
from discord_commands.utils.media_enqueuer import MediaEnqueuer

from tests.fakes.pipeline_fakes import fake_envelope
from tests.fakes.search_pipeline_fakes import (BrokerBundleProxy, create_test_media_request,
                                               create_test_playlist_add_request, make_broker_bundle)
from tests.helpers import fake_media_download, fake_source_dict
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod, music_settings


def search_queue(pod):
    '''The search worker's input queue, which the search pod would fill.'''
    return pod.youtube_music_search_client.local_worker._input_queue  #pylint:disable=protected-access


async def open_bundle(pod, fake_context):  #pylint:disable=redefined-outer-name
    return await pod.broker_client.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True)


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_cache_miss(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """enqueue_from_cache returns False on a cache miss"""
    pod = ResultsPod(fake_context, fake_stores)
    media_request = create_test_media_request(fake_context)
    await make_broker_bundle(pod, fake_context, request=media_request)
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    assert await pod.enqueuer.enqueue_from_cache(media_request) is False


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_cache_hit_player(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A cache hit is handed to the guild's player"""
    pod = ResultsPod(fake_context, fake_stores)
    media_request = create_test_media_request(fake_context)
    await make_broker_bundle(pod, fake_context, request=media_request)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=cached_download))
            add_cached = mocker.patch.object(pod.player_gateway, 'add_cached', new=AsyncMock(return_value=True))

            assert await pod.enqueuer.enqueue_from_cache(media_request) is True
            add_cached.assert_awaited_once_with(fake_context['guild'].id, cached_download)


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_playlist_addition(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A cache hit for a playlist addition adds the item and skips the player"""
    pod = ResultsPod(fake_context, fake_stores)
    media_request = create_test_playlist_add_request(fake_context, playlist_id=456)
    await make_broker_bundle(pod, fake_context, request=media_request)
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=cached_download))
            add_item = mocker.patch.object(MediaEnqueuer, 'add_playlist_item', return_value=None)

            assert await pod.enqueuer.enqueue_from_cache(media_request) is True
            add_item.assert_called_once()
            call_request, call_result = add_item.call_args[0]
            assert call_request.playlist_id == 456
            assert call_result.webpage_url == cached_download.webpage_url
            assert call_result.title == cached_download.title
            assert call_result.uploader == cached_download.uploader


@pytest.mark.asyncio()
async def test_youtube_search_queue_integration_with_enqueue_media_requests(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Search-type requests go to the search queue, direct ones to the download queue"""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    bundle_uuid = await open_bundle(pod, fake_context)
    bundle = BrokerBundleProxy(pod.broker_client.local_broker, bundle_uuid)
    search_request = create_test_media_request(fake_context, 'search term')
    direct_request = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    result = await pod.enqueuer.enqueue_media_requests(
        fake_envelope(fake_context), [search_request, direct_request], bundle_uuid)

    assert result is True
    assert search_queue(pod).size(fake_context['guild'].id) > 0
    assert search_queue(pod).get_nowait() == search_request
    assert await pod.download_client.queue_size(fake_context['guild'].id) > 0
    assert await pod.download_client.local_worker.get_input_nowait() == direct_request
    assert len(bundle.bundled_requests) == 2


@pytest.mark.asyncio()
async def test_mixed_search_types_routing(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Routing with mixed SearchTypes in the same batch"""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    bundle_uuid = await open_bundle(pod, fake_context)
    search_request = create_test_media_request(fake_context, 'search term')
    spotify_search_request = create_test_media_request(fake_context, 'spotify track', search_type=SearchType.SEARCH)
    direct_request = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)
    youtube_request = create_test_media_request(fake_context, 'https://youtube.com/watch?v=123', search_type=SearchType.YOUTUBE)
    youtube_playlist_request = create_test_media_request(fake_context, 'https://youtube.com/playlist?list=123', search_type=SearchType.YOUTUBE)
    entries = [search_request, spotify_search_request, direct_request, youtube_request, youtube_playlist_request]
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    result = await pod.enqueuer.enqueue_media_requests(fake_envelope(fake_context), entries, bundle_uuid)

    assert result is True
    guild_id = fake_context['guild'].id
    assert search_queue(pod).size(guild_id) == 2
    assert await pod.download_client.queue_size(guild_id) == 3
    search_queue_items = [search_queue(pod).get_nowait() for _ in range(2)]
    download_queue_items = [await pod.download_client.local_worker.get_input_nowait() for _ in range(3)]
    assert search_request in search_queue_items
    assert spotify_search_request in search_queue_items
    assert direct_request in download_queue_items
    assert youtube_request in download_queue_items
    assert youtube_playlist_request in download_queue_items


@pytest.mark.asyncio()
async def test_search_queue_priority_handling(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Server-specific priority is applied to the search queue"""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    test_priority = 50
    pod.server_queue_priority[fake_context['guild'].id] = test_priority
    bundle_uuid = await open_bundle(pod, fake_context)
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    result = await pod.enqueuer.enqueue_media_requests(
        fake_envelope(fake_context), [create_test_media_request(fake_context, 'test search')], bundle_uuid)

    assert result is True
    assert search_queue(pod).size(fake_context['guild'].id) == 1
    assert search_queue(pod).queues[fake_context['guild'].id].priority == test_priority


@pytest.mark.asyncio()
async def test_search_queue_resource_limits(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """The search queue is sized at twice the player queue"""
    pod = ResultsPod(fake_context, fake_stores, with_search=True,
                     music=music_settings({'player': {'queue_max_size': 2}}))
    bundle_uuid = await open_bundle(pod, fake_context)
    search_requests = [create_test_media_request(fake_context, f'search term {i}') for i in range(10)]
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    result = await pod.enqueuer.enqueue_media_requests(fake_envelope(fake_context), search_requests, bundle_uuid)

    assert result is True
    assert search_queue(pod).max_size == 4
    assert search_queue(pod).size(fake_context['guild'].id) == 4
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_enqueue_media_requests_download_queue_blocked_deletes_bundle(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A blocked download queue (shutdown) tears the bundle down and returns False."""
    pod = ResultsPod(fake_context, fake_stores)
    bundle_uuid = await open_bundle(pod, fake_context)
    direct = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)
    mocker.patch.object(MediaEnqueuer, 'enqueue_from_cache', new=AsyncMock(return_value=False))
    mocker.patch.object(pod.download_client, 'submit', side_effect=PutsBlocked())

    result = await pod.enqueuer.enqueue_media_requests(fake_envelope(fake_context), [direct], bundle_uuid)

    assert result is False
    assert pod.broker_client.local_broker.get_bundle_state(bundle_uuid) is None


@pytest.mark.asyncio()
async def test_enqueue_media_requests_download_queue_full_discards_request(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A full download queue discards the request and stops enqueuing (bundle survives)."""
    pod = ResultsPod(fake_context, fake_stores)
    bundle_uuid = await open_bundle(pod, fake_context)
    direct = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)
    mocker.patch.object(MediaEnqueuer, 'enqueue_from_cache', new=AsyncMock(return_value=False))
    mocker.patch.object(pod.download_client, 'submit', side_effect=QueueFull())

    result = await pod.enqueuer.enqueue_media_requests(fake_envelope(fake_context), [direct], bundle_uuid)

    assert result is True
    state = pod.broker_client.local_broker.get_bundle_state(bundle_uuid)
    assert state.bundled_requests[0].media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_enqueue_media_requests_search_queue_blocked_deletes_bundle(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A blocked search queue (shutdown) tears the bundle down and returns False."""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    bundle_uuid = await open_bundle(pod, fake_context)
    search_request = create_test_media_request(fake_context, 'search term', search_type=SearchType.SEARCH)
    mocker.patch.object(search_queue(pod), 'put_nowait', side_effect=PutsBlocked())

    result = await pod.enqueuer.enqueue_media_requests(fake_envelope(fake_context), [search_request], bundle_uuid)

    assert result is False
    assert pod.broker_client.local_broker.get_bundle_state(bundle_uuid) is None


@pytest.mark.asyncio()
async def test_generate_media_requests_collection_creates_multitrack_bundle(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A named collection drops the single-search bundle and opens a multi-track banner bundle."""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    collection = SimpleNamespace(
        collection_name='Mock Album',
        search_results=[SearchResult(search_type=SearchType.SEARCH, raw_search_string='track one')],
    )
    mocker.patch.object(pod.search_client, 'check_source', new=AsyncMock(return_value=collection))

    await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'some album')

    bundles = await pod.broker_client.list_bundles_for_guild(fake_context['guild'].id)
    assert len(bundles) == 1
    assert pod.broker_client.local_broker.get_bundle_state(bundles[0]).has_search_banner is True


@pytest.mark.asyncio()
async def test_playlist_queue_bundle_creation_with_channel_id(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """enqueue_media_requests registers requests against a broker bundle"""
    pod = ResultsPod(fake_context, fake_stores, with_search=True)
    entries = [fake_source_dict(fake_context), fake_source_dict(fake_context)]
    bundle_uuid = await open_bundle(pod, fake_context)

    result = await pod.enqueuer.enqueue_media_requests(fake_envelope(fake_context), entries, bundle_uuid)

    assert result is True
    state = pod.broker_client.local_broker.get_bundle_state(bundle_uuid)
    assert state is not None
    assert state.guild_id == fake_context['guild'].id
    assert state.channel_id == fake_context['channel'].id
    assert len(state.bundled_requests) == 2
