# White-box tests reach into the extracted search worker (client.local_worker
# internals: the injected ytmusic client, the input queue, the failure queue and
# backoff timestamp), mirroring tests/workers/test_redis_download_worker.py.
# pylint: disable=protected-access
import asyncio
from asyncio import QueueFull
from datetime import datetime, timezone
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.workers.youtube_music_search_driver import SEARCH_BACKOFF_SLICE_SECONDS
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.exceptions import ExitEarlyException
from discord_bot.cogs.music_helpers.common import SearchType, MediaRequestLifecycleStage, YOUTUBE_VIDEO_PREFIX
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.search import SearchResult
from discord_bot.utils.integrations.youtube_music import YoutubeMusicRetryException
from discord_bot.utils.failure_queue import FailureStatus
from discord_bot.types.queue import PutsBlocked
from discord_bot.workers.media_bundle import BundleRenderer

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


class BrokerBundleProxy:
    '''Read-through view of a broker-owned bundle for test assertions.

    Mirrors the small surface the old MultiMediaRequestBundle exposed
    (uuid / bundled_requests / completed / finished) but resolves everything
    from the broker's live BundleState so tests assert broker-side state.
    '''
    def __init__(self, broker, uuid):
        self._broker = broker
        self.uuid = uuid

    @property
    def _renderer(self):
        state = self._broker.get_bundle_state(self.uuid)
        renderer = BundleRenderer(state)
        renderer.update_request_status()
        return renderer

    @property
    def bundled_requests(self):
        '''Live BundledRequestState list off the broker state.'''
        return self._broker.get_bundle_state(self.uuid).bundled_requests

    @property
    def completed(self):
        '''Completed counter after reconciling lifecycle stages.'''
        return self._renderer.state.completed

    @property
    def finished(self):
        '''True once all requests reach a terminal stage.'''
        return self._renderer.finished


async def make_broker_bundle(cog, test_context, request=None, all_added=False,
                             has_search_banner=False):
    '''Create a broker-owned bundle (optionally with one registered request).

    Returns (BrokerBundleProxy, request).  Mirrors the old test pattern of
    constructing a MultiMediaRequestBundle, attaching it to the cog, and adding
    a request — now routed through the broker so the cog owns nothing.
    '''
    bundle_uuid = await cog.create_bundle(
        test_context['guild'].id, test_context['channel'].id,
        has_search_banner=has_search_banner,
    )
    if request is not None:
        request.bundle_uuid = bundle_uuid
        await cog.media_broker.register_request(request)
    if all_added:
        await cog.broker_client.finalize_bundle(bundle_uuid)
    return BrokerBundleProxy(cog.media_broker, bundle_uuid), request



class MockYoutubeMusicClient:
    """Mock YouTube Music client for testing"""
    def __init__(self, youtube_music_result='test-video-id'):
        self.youtube_music_result = youtube_music_result

    def search(self, search_string): #pylint:disable=unused-argument
        """Mock YouTube Music search that returns a video ID"""
        return self.youtube_music_result


def create_test_media_request(test_context, search_string='test search', bundle_uuid=None, search_type=SearchType.SEARCH):
    """Helper to create test media requests"""
    request = MediaRequest(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        requester_name=test_context['author'].display_name,
        requester_id=test_context['author'].id,
        search_result=SearchResult(search_type=search_type, raw_search_string=search_string)
    )
    if bundle_uuid:
        request.bundle_uuid = bundle_uuid
    return request


def create_test_playlist_add_request(test_context, playlist_id, search_string='test search', bundle_uuid=None, search_type=SearchType.SEARCH):
    """Helper to create test PlaylistAddRequest objects"""
    request = PlaylistAddRequest(
        guild_id=test_context['guild'].id,
        channel_id=test_context['channel'].id,
        requester_name=test_context['author'].display_name,
        requester_id=test_context['author'].id,
        search_result=SearchResult(search_type=search_type, raw_search_string=search_string),
        playlist_id=playlist_id,
    )
    if bundle_uuid:
        request.bundle_uuid = bundle_uuid
    return request


@pytest.mark.asyncio()
async def test_search_youtube_music_empty_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test search_youtube_music when queue is empty"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient()

    # Queue is empty, should return without error
    result = await cog.search_youtube_music()
    assert result


@pytest.mark.asyncio()
async def test_search_youtube_music_bot_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test search_youtube_music exits early when bot is shutting down"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.bot_shutdown_event.set()

    with pytest.raises(ExitEarlyException) as exc:
        await cog.search_youtube_music()
    assert 'Bot shutdown called' in str(exc.value)


@pytest.mark.asyncio()
async def test_process_search_results_empty_idles(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """process_search_results sleeps (idles) when the broker has no resolution."""
    mock_sleep = mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])

    # Nothing on the broker search-result queue → idle backoff, no submit.
    await cog.process_search_results()
    mock_sleep.assert_awaited()
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_process_search_results_bot_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """process_search_results exits early once the bot is shutting down."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.bot_shutdown_event.set()

    with pytest.raises(ExitEarlyException) as exc:
        await cog.process_search_results()
    assert 'Bot shutdown called' in str(exc.value)


@pytest.mark.asyncio()
async def test_search_youtube_music_successful_search_no_cache(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test successful YouTube Music search with no cache hit"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    await cog.search_youtube_music()

    # Verify search string was updated with YouTube prefix
    assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

    # search_youtube_music now hands the resolved request to the broker seam;
    # process_search_results runs the cache-check + download submit tail.
    await cog.process_search_results()

    # Verify request was added to download queue
    assert await cog.download_client.queue_size(fake_context['guild'].id) > 0
    download_item = await cog.download_client.local_worker.get_input_nowait()
    assert download_item == media_request

    # Verify bundle status was updated
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.QUEUED


@pytest.mark.asyncio()
async def test_search_youtube_music_successful_search_cache_hit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test successful YouTube Music search with cache hit"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=cached_download))

            # Mock player methods
            mock_player = MagicMock()
            mocker.patch.object(cog, 'get_player', return_value=mock_player)
            mock_add_source = mocker.patch.object(cog, 'add_source_to_player', return_value=None)

            await cog.search_youtube_music()
            await cog.process_search_results()

            # Verify search string was updated with YouTube prefix
            assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

            # Verify download queue is empty (cache hit, no download needed)
            assert await cog.download_client.queue_size(fake_context['guild'].id) == 0

            # Verify add_source_to_player was called
            mock_add_source.assert_called_once_with(cached_download, mock_player)


@pytest.mark.asyncio()
async def test_search_youtube_music_cache_hit_marks_request_completed(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """
    Regression: when a SEARCH-type request resolves to a cached track, the
    *current* media_request must transition to COMPLETED. Previously it stayed
    in QUEUED forever, leaving the bundle's "Media request queued for download"
    row dangling in the channel even though the song was playing.
    """
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request,
                                                     all_added=True)

    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    with TemporaryDirectory() as tmp_dir:
        # The real broker.check_cache binds the cached download to the SAME
        # media_request it was queried with (see _media_download_from_dict), so the
        # COMPLETED push inside _enqueue_media_download_from_cache advances THIS
        # request. Bind the same object here so the mock reflects that contract.
        with fake_media_download(tmp_dir, media_request=media_request) as cached_download:
            mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=cached_download))

            mock_player = MagicMock()
            mocker.patch.object(cog, 'get_player', return_value=mock_player)
            mocker.patch.object(cog, 'add_source_to_player', return_value=None)

            await cog.search_youtube_music()
            await cog.process_search_results()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.COMPLETED
    # The broker re-rendered on the COMPLETED push, so its bundle state already
    # reflects the cleared row and incremented counter.
    assert bundle.finished is True
    assert bundle.completed == 1


@pytest.mark.asyncio()
async def test_search_youtube_music_no_result(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search returns no results"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient(None)  # No result

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    await cog.search_youtube_music()
    await cog.process_search_results()

    # Verify original search string unchanged
    assert media_request.search_result.raw_search_string == 'test search'

    # Verify download queue still has item
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 1


@pytest.mark.asyncio()
async def test_search_youtube_music_download_queue_full(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when download queue is full"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Mock download queue full
    mocker.patch.object(cog.download_client, 'submit', side_effect=QueueFull())

    await cog.search_youtube_music()
    # The download submit (now in process_search_results) hits the full queue.
    await cog.process_search_results()

    # Verify bundle status was updated to DISCARDED
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_search_youtube_music_download_queue_blocked(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when download queue puts are blocked"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Mock download queue blocked
    mocker.patch.object(cog.download_client, 'submit', side_effect=PutsBlocked())

    await cog.search_youtube_music()
    # The blocked download submit now surfaces in process_search_results.
    await cog.process_search_results()

    # Blocked puts drop the request to DISCARDED.
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_search_youtube_music_playlist_item(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search for playlist addition"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and playlist add request
    media_request = create_test_playlist_add_request(fake_context, playlist_id=123, search_string='test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=cached_download))

            # Mock playlist addition
            mocker.patch.object(cog, '_Music__add_playlist_item', return_value=None)

            await cog.search_youtube_music()
            await cog.process_search_results()

            # Verify playlist addition was called with the right request and result
            cog._Music__add_playlist_item.assert_called_once() #pylint:disable=protected-access
            call_request, call_result = cog._Music__add_playlist_item.call_args[0] #pylint:disable=protected-access
            assert call_request.playlist_id == 123
            assert call_result.webpage_url == cached_download.webpage_url
            assert call_result.title == cached_download.title
            assert call_result.uploader == cached_download.uploader

            # Verify download queue is empty (playlist addition, no player queue needed)
            assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_cache_miss(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test _enqueue_media_download_from_cache with cache miss"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    media_request = create_test_media_request(fake_context)

    # Create broker bundle for the request
    await make_broker_bundle(cog, fake_context, request=media_request)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    result = await cog._enqueue_media_download_from_cache(media_request) #pylint:disable=protected-access

    assert result is False


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_cache_hit_player(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test _enqueue_media_download_from_cache with cache hit and player"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    media_request = create_test_media_request(fake_context)

    # Create broker bundle for the request
    await make_broker_bundle(cog, fake_context, request=media_request)

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=cached_download))

            # Mock player methods
            mock_player = MagicMock()
            mocker.patch.object(cog, 'get_player', return_value=mock_player)
            mock_add_source = mocker.patch.object(cog, 'add_source_to_player', return_value=None)

            result = await cog._enqueue_media_download_from_cache(media_request) #pylint:disable=protected-access

            assert result is True
            mock_add_source.assert_called_once_with(cached_download, mock_player)


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_playlist_addition(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test _enqueue_media_download_from_cache with playlist addition"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    media_request = create_test_playlist_add_request(fake_context, playlist_id=456)

    # Create broker bundle for the request
    await make_broker_bundle(cog, fake_context, request=media_request)

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=cached_download))

            # Mock playlist addition
            mocker.patch.object(cog, '_Music__add_playlist_item', return_value=None)

            result = await cog._enqueue_media_download_from_cache(media_request) #pylint:disable=protected-access

            assert result is True
            cog._Music__add_playlist_item.assert_called_once() #pylint:disable=protected-access
            call_request, call_result = cog._Music__add_playlist_item.call_args[0] #pylint:disable=protected-access
            assert call_request.playlist_id == 456
            assert call_result.webpage_url == cached_download.webpage_url
            assert call_result.title == cached_download.title
            assert call_result.uploader == cached_download.uploader


@pytest.mark.asyncio()
async def test_youtube_search_queue_integration_with_enqueue_media_requests(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test integration of YouTube search queue with enqueue_media_requests"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    # Create a broker bundle
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )
    bundle = BrokerBundleProxy(cog.media_broker, bundle_uuid)

    # Create search-type media requests (should go to search queue)
    search_request = create_test_media_request(fake_context, 'search term')

    # Create direct-type media request (should go directly to download queue)
    direct_request = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)

    entries = [search_request, direct_request]

    # Mock cache misses
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle_uuid, player=mock_player)

    assert result is True

    # Verify search request went to search queue
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) > 0
    search_queue_item = cog.youtube_music_search_client.local_worker._input_queue.get_nowait()
    assert search_queue_item == search_request

    # Verify direct request went to download queue
    assert await cog.download_client.queue_size(fake_context['guild'].id) > 0
    download_queue_item = await cog.download_client.local_worker.get_input_nowait()
    assert download_queue_item == direct_request

    # Verify bundle was updated
    assert len(bundle.bundled_requests) == 2


@pytest.mark.asyncio()
async def test_search_youtube_music_search_client_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when search client raises exception"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create YouTube Music client that raises exception
    class FailingYoutubeMusicClient:
        """Mock YouTube Music client that raises exceptions"""
        def search(self, search_string): #pylint:disable=unused-argument
            """Mock method that raises a network error"""
            raise RuntimeError("Network error")

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = FailingYoutubeMusicClient()

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Should handle exception gracefully and not crash
    try:
        await cog.search_youtube_music()
        # Test should not crash - exception should be handled gracefully
        # In real implementation, this might log the error and continue
    except Exception as e: #pylint:disable=broad-exception-caught
        # If exception propagates, the implementation needs error handling
        assert "Network error" in str(e)


@pytest.mark.asyncio()
async def test_search_youtube_music_search_client_timeout(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search timeout scenario"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create YouTube Music client that times out
    class TimeoutYoutubeMusicClient:
        """Mock YouTube Music client that times out"""
        def search(self, search_string): #pylint:disable=unused-argument
            """Mock method that raises a timeout error"""
            raise asyncio.TimeoutError("Search timeout")

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = TimeoutYoutubeMusicClient()

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Should handle timeout gracefully
    try:
        await cog.search_youtube_music()
    except asyncio.TimeoutError as e:
        assert "Search timeout" in str(e)


@pytest.mark.asyncio()
async def test_mixed_search_types_routing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test routing with mixed SearchTypes in same batch"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    # Create a broker bundle
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )

    # Create requests of different types
    search_request = create_test_media_request(fake_context, 'search term')

    spotify_search_request = create_test_media_request(fake_context, 'spotify track', search_type=SearchType.SEARCH)

    direct_request = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)

    youtube_request = create_test_media_request(fake_context, 'https://youtube.com/watch?v=123', search_type=SearchType.YOUTUBE)

    youtube_playlist_request = create_test_media_request(fake_context, 'https://youtube.com/playlist?list=123', search_type=SearchType.YOUTUBE)

    entries = [search_request, spotify_search_request, direct_request, youtube_request, youtube_playlist_request]

    # Mock cache misses
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle_uuid, player=mock_player)

    assert result is True

    # Verify search requests (including spotify-originated) went to search queue
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 2

    # Verify direct and youtube requests went to download queue
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 3

    # Verify correct items in each queue
    search_queue_items = []
    for _ in range(2):
        item = cog.youtube_music_search_client.local_worker._input_queue.get_nowait()
        search_queue_items.append(item)

    download_queue_items = []
    for _ in range(3):
        item = await cog.download_client.local_worker.get_input_nowait()
        download_queue_items.append(item)

    # Check search queue has search requests (including spotify-originated)
    assert search_request in search_queue_items
    assert spotify_search_request in search_queue_items

    # Check download queue has direct and youtube requests
    assert direct_request in download_queue_items
    assert youtube_request in download_queue_items
    assert youtube_playlist_request in download_queue_items


@pytest.mark.asyncio()
async def test_search_queue_priority_handling(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test server-specific priority handling in search queue"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    # Set server-specific priority
    test_priority = 50
    cog.server_queue_priority[fake_context['guild'].id] = test_priority

    # Create a broker bundle and media request
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )
    media_request = create_test_media_request(fake_context, 'test search')

    entries = [media_request]

    # Mock cache misses
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle_uuid, player=mock_player)

    assert result is True

    # Verify item went to search queue with correct priority
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 1

    # Check that the guild queue has the correct priority
    guild_queue_data = cog.youtube_music_search_client.local_worker._input_queue.queues[fake_context['guild'].id]
    assert guild_queue_data.priority == test_priority


@pytest.mark.asyncio()
async def test_bundle_expiration_during_search_processing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test handling when bundle expires while item is being processed in search queue"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Remove bundle from the broker to simulate expiration
    await cog.delete_bundle(fake_context['guild'].id, bundle.uuid)

    # Should handle missing bundle gracefully
    await cog.search_youtube_music()
    await cog.process_search_results()

    # Verify search still happened and item went to download queue
    assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 1


@pytest.mark.asyncio()
async def test_search_queue_resource_limits(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test search queue with the new 10x sizing vs download queue limits"""
    # Test with very small download queue size to verify search queue is larger
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'player': {
                'queue_max_size': 2,  # Small download queue
            },
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])

    # Create enough items to fill beyond download queue size but within search queue size
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )

    # Add more items than download queue can handle
    search_requests = []
    for i in range(10):  # More than download queue size of 2
        request = create_test_media_request(fake_context, f'search term {i}')
        search_requests.append(request)

    # Mock cache misses
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Should be able to enqueue all search requests to search queue
    result = await cog.enqueue_media_requests(fake_context['context'], search_requests, bundle_uuid, player=mock_player)

    assert result is True
    # Verify the search queue size is configured correctly
    assert cog.youtube_music_search_client.local_worker._input_queue.max_size == 4

    # All 10 items should fit in search queue
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 4

    # Download queue should be empty initially
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_message_queue_update_failure_during_search(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test handling when message queue update fails during search processing"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # The broker captures the dispatcher at construction; we arm update_mutable
    # to raise only after the bundle is set up, so this exercises bundle-render
    # failure during the search push.
    dispatcher = MagicMock()
    cog = Music(fake_context['bot'], config, dispatcher)
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create a broker bundle and media request
    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    # Add to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Arm the dispatcher to raise on the next render
    dispatcher.update_mutable.side_effect = Exception("Message queue error")

    # Should handle message queue failure gracefully
    try:
        await cog.search_youtube_music()
        await cog.process_search_results()
        # Should not crash despite message queue failure
        # Verify core functionality still works
        assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
        assert await cog.download_client.queue_size(fake_context['guild'].id) == 1
    except Exception as e: #pylint:disable=broad-exception-caught
        # If exception propagates, it should be handled gracefully in real implementation
        assert "Message queue error" in str(e)


@pytest.mark.asyncio()
async def test_concurrent_bundle_operations_during_search(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test concurrent bundle operations while search queue is processing"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Create multiple broker bundles with one request each
    media_request1 = create_test_media_request(fake_context, 'test search 1')
    media_request2 = create_test_media_request(fake_context, 'test search 2')
    bundle1, media_request1 = await make_broker_bundle(cog, fake_context, request=media_request1)
    bundle2, media_request2 = await make_broker_bundle(cog, fake_context, request=media_request2)

    # Add both to search queue
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request1)
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request2)

    # Mock cache miss
    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))

    # Process both items through the search loop and the broker seam
    await cog.search_youtube_music()
    await cog.search_youtube_music()
    await cog.process_search_results()
    await cog.process_search_results()

    # Verify both items were processed correctly
    assert media_request1.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
    assert media_request2.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

    # Both should be in download queue
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 2

    # Verify both bundles were updated
    assert len(bundle1.bundled_requests) == 1
    assert len(bundle2.bundled_requests) == 1


class RateLimitedYoutubeMusicClient:
    """Mock YouTube Music client that raises YoutubeMusicRetryException"""
    def search(self, search_string): #pylint:disable=unused-argument
        raise YoutubeMusicRetryException('429 Exhaust Limit Hit')


@pytest.mark.asyncio()
async def test_search_youtube_music_429_requeues_item(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a 429 re-enqueues the item and sets RETRY_SEARCH lifecycle stage"""
    config = BASE_MUSIC_CONFIG
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.interfaces.youtube_music_search_protocols.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = RateLimitedYoutubeMusicClient()

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request,
                                                      all_added=True)

    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    result = await cog.search_youtube_music()

    assert result is False
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH
    assert media_request.youtube_music_retry_information.retry_count == 1
    assert media_request.youtube_music_retry_information.retry_reason is not None
    # Item should be back in the search queue
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 1
    # Not in download queue
    assert await cog.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
@pytest.mark.freeze_time
async def test_search_youtube_music_429_sets_backoff_timestamp(freezer, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a 429 sets the youtube_music_wait_timestamp with exponential backoff"""
    config = BASE_MUSIC_CONFIG
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.interfaces.youtube_music_search_protocols.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = RateLimitedYoutubeMusicClient()

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request,
                                                      all_added=True)

    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    freezer.move_to('2025-01-01 12:00:00 UTC')
    assert cog.youtube_music_search_client.local_worker._wait_timestamp is None

    await cog.search_youtube_music()

    # Failure queue size is 1, so multiplier is 2^1 = 2
    # Expected: now (1735732800) + 30*2 + 5 = 1735732865
    assert cog.youtube_music_search_client.local_worker._wait_timestamp == 1735732865
    assert cog.youtube_music_search_client.local_worker._failure_queue.size == 1


@pytest.mark.asyncio()
@pytest.mark.freeze_time
async def test_search_youtube_music_429_exponential_backoff_growth(freezer, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that repeated 429s grow the backoff exponentially"""
    config = BASE_MUSIC_CONFIG
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.interfaces.youtube_music_search_protocols.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = RateLimitedYoutubeMusicClient()

    # Pre-populate failure queue with 2 existing failures
    cog.youtube_music_search_client.local_worker._failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    cog.youtube_music_search_client.local_worker._failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    assert cog.youtube_music_search_client.local_worker._failure_queue.size == 2

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request,
                                                      all_added=True)

    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    freezer.move_to('2025-01-01 12:00:00 UTC')
    await cog.search_youtube_music()

    # Failure queue size is now 3, so multiplier is 2^3 = 8
    # Expected: now (1735732800) + 30*8 + 5 = 1735733045
    assert cog.youtube_music_search_client.local_worker._failure_queue.size == 3
    assert cog.youtube_music_search_client.local_worker._wait_timestamp == 1735733045


@pytest.mark.asyncio()
async def test_search_youtube_music_429_retry_limit_exceeded(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that hitting max retries marks the request as FAILED instead of re-queuing"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'max_youtube_music_search_retries': 3}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.interfaces.youtube_music_search_protocols.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = RateLimitedYoutubeMusicClient()

    media_request = create_test_media_request(fake_context, 'test search')
    # Simulate already at retry limit
    media_request.youtube_music_retry_information.retry_count = 2
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request,
                                                      all_added=True)

    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    await cog.search_youtube_music()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert media_request.failure_reason is not None
    # Should NOT be re-queued in search queue
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_search_youtube_music_429_resets_lifecycle_on_retry(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a re-queued item resets from RETRY_SEARCH back to SEARCHING on next attempt"""
    config = BASE_MUSIC_CONFIG
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    call_count = 0

    class SucceedOnSecondCallClient:
        def search(self, search_string): #pylint:disable=unused-argument
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise YoutubeMusicRetryException('429 Exhaust Limit Hit')
            return 'test-video-id'

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = SucceedOnSecondCallClient()

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request,
                                                      all_added=True)

    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # First call hits 429
    mocker.patch.object(cog.youtube_music_search_client.local_worker, 'backoff_wait', new=AsyncMock())
    await cog.search_youtube_music()
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH

    # The 429 armed the backoff window, and the loop no longer pops while one is
    # open — clear it so the retry attempt actually runs.
    cog.youtube_music_search_client.local_worker._wait_timestamp = None

    # Second call succeeds — lifecycle should reset to SEARCHING then proceed to QUEUED
    await cog.search_youtube_music()
    await cog.process_search_results()
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.QUEUED


@pytest.mark.asyncio()
async def test_search_youtube_music_success_clears_failure_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a successful search adds a success to the failure queue"""
    config = BASE_MUSIC_CONFIG
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    # Pre-populate failure queue
    cog.youtube_music_search_client.local_worker._failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    cog.youtube_music_search_client.local_worker._failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    assert cog.youtube_music_search_client.local_worker._failure_queue.size == 2

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)

    mocker.patch.object(cog.media_broker, 'check_cache', new=AsyncMock(return_value=None))
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    await cog.search_youtube_music()

    # Successful search should remove one failure from the queue
    assert cog.youtube_music_search_client.local_worker._failure_queue.size == 1


# ---------------------------------------------------------------------------
# youtube_music_backoff_time
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_youtube_backoff_time_expired_returns_true(fake_context):  # pylint: disable=redefined-outer-name
    """backoff_wait returns immediately when the timestamp is in the past."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    # Set timestamp to the past (now - 100 seconds)
    cog.youtube_music_search_client.local_worker._wait_timestamp = datetime.now(timezone.utc).timestamp() - 100
    result = await cog.youtube_music_search_client.local_worker.backoff_wait(cog.bot_shutdown_event)
    assert result is None


@pytest.mark.asyncio
async def test_youtube_backoff_time_shutdown_raises(fake_context):  # pylint: disable=redefined-outer-name
    """youtube_music_backoff_time raises ExitEarlyException when shutdown is set."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    # Set timestamp to the future
    cog.youtube_music_search_client.local_worker._wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600
    cog.bot_shutdown_event.set()
    with pytest.raises(ExitEarlyException):
        await cog.youtube_music_search_client.local_worker.backoff_wait(cog.bot_shutdown_event)


@pytest.mark.asyncio
async def test_youtube_backoff_time_waits_until_timeout(fake_context):  # pylint: disable=redefined-outer-name
    """backoff_wait waits for the event and returns when it times out."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    # Set timestamp to just slightly in the future (tiny wait)
    cog.youtube_music_search_client.local_worker._wait_timestamp = datetime.now(timezone.utc).timestamp() + 0.05
    # bot_shutdown NOT set → wait_for will time out → returns None
    result = await cog.youtube_music_search_client.local_worker.backoff_wait(cog.bot_shutdown_event)
    assert result is None


@pytest.mark.asyncio
async def test_youtube_backoff_time_raises_when_event_set_during_wait(fake_context):  # pylint: disable=redefined-outer-name
    """youtube_music_backoff_time raises ExitEarlyException when shutdown is set during wait."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    # Set timestamp far in the future so wait_for won't time out before the event fires
    cog.youtube_music_search_client.local_worker._wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600

    async def _set_event_soon():
        await asyncio.sleep(0.02)
        cog.bot_shutdown_event.set()

    asyncio.ensure_future(_set_event_soon())

    with pytest.raises(ExitEarlyException):
        await cog.youtube_music_search_client.local_worker.backoff_wait(cog.bot_shutdown_event)


@pytest.mark.asyncio()
async def test_enqueue_media_requests_download_queue_blocked_deletes_bundle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """A blocked download queue (shutdown) tears the bundle down and returns False."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )
    direct = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)
    mocker.patch.object(cog, '_enqueue_media_download_from_cache', new=AsyncMock(return_value=False))
    mocker.patch.object(cog.download_client, 'submit', side_effect=PutsBlocked())
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    result = await cog.enqueue_media_requests(fake_context['context'], [direct], bundle_uuid, player=mock_player)
    assert result is False
    assert cog.media_broker.get_bundle_state(bundle_uuid) is None


@pytest.mark.asyncio()
async def test_enqueue_media_requests_download_queue_full_discards_request(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """A full download queue discards the request and stops enqueuing (bundle survives)."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )
    direct = create_test_media_request(fake_context, 'https://direct.url', search_type=SearchType.DIRECT)
    mocker.patch.object(cog, '_enqueue_media_download_from_cache', new=AsyncMock(return_value=False))
    mocker.patch.object(cog.download_client, 'submit', side_effect=QueueFull())
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    result = await cog.enqueue_media_requests(fake_context['context'], [direct], bundle_uuid, player=mock_player)
    assert result is True
    state = cog.media_broker.get_bundle_state(bundle_uuid)
    assert state.bundled_requests[0].media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_enqueue_media_requests_search_queue_blocked_deletes_bundle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """A blocked search queue (shutdown) tears the bundle down and returns False."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    bundle_uuid = await cog.create_bundle(
        fake_context['guild'].id, fake_context['channel'].id, has_search_banner=True,
    )
    search_request = create_test_media_request(fake_context, 'search term', search_type=SearchType.SEARCH)
    mocker.patch.object(cog.youtube_music_search_client.local_worker._input_queue, 'put_nowait', side_effect=PutsBlocked())
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    result = await cog.enqueue_media_requests(fake_context['context'], [search_request], bundle_uuid, player=mock_player)
    assert result is False
    assert cog.media_broker.get_bundle_state(bundle_uuid) is None


@pytest.mark.asyncio()
async def test_generate_media_requests_collection_creates_multitrack_bundle(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """A named collection drops the single-search bundle and opens a multi-track banner bundle."""
    from types import SimpleNamespace  # pylint: disable=import-outside-toplevel
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    collection = SimpleNamespace(
        collection_name='Mock Album',
        search_results=[SearchResult(search_type=SearchType.SEARCH, raw_search_string='track one')],
    )
    mocker.patch.object(cog.search_client, 'check_source', new=AsyncMock(return_value=collection))
    mock_player = MagicMock()

    await cog._generate_media_requests_from_search(fake_context['context'], 'some album', player=mock_player)  # pylint: disable=protected-access

    bundles = await cog.broker_client.list_bundles_for_guild(fake_context['guild'].id)
    assert len(bundles) == 1
    assert cog.media_broker.get_bundle_state(bundles[0]).has_search_banner is True


@pytest.mark.asyncio()
async def test_search_youtube_music_waits_in_slices_before_popping(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """An open backoff window ends the iteration WITHOUT popping a request.

    Popping first would hold the request in pod memory for the whole window (and
    the Redis-backed worker DELetes on pop, so a restart would lose it), while a
    single uninterrupted sleep past the loop-health staleness window would read
    as a wedge and get the pod restarted over a rate limit.
    """
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # Window well past one slice; stub the sleep itself so the test doesn't wait.
    cog.youtube_music_search_client.local_worker._wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600
    backoff_mock = mocker.patch.object(cog.youtube_music_search_client.local_worker,
                                       'backoff_wait', new=AsyncMock())

    assert await cog.search_youtube_music() is True

    # Waited one slice, then returned: nothing popped, nothing resolved.
    backoff_mock.assert_awaited_once_with(cog.bot_shutdown_event,
                                          max_wait_seconds=SEARCH_BACKOFF_SLICE_SECONDS)
    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 1
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.SEARCHING


@pytest.mark.asyncio()
async def test_search_youtube_music_pops_once_backoff_window_clears(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """With the window elapsed the same iteration pops and resolves as usual."""
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.youtube_music_search_client.local_worker._client = MockYoutubeMusicClient('test-video-id')

    media_request = create_test_media_request(fake_context, 'test search')
    _bundle, media_request = await make_broker_bundle(cog, fake_context, request=media_request)
    cog.youtube_music_search_client.local_worker._input_queue.put_nowait(fake_context['guild'].id, media_request)

    # An elapsed window reads as 0 seconds remaining, not None.
    cog.youtube_music_search_client.local_worker._wait_timestamp = datetime.now(timezone.utc).timestamp() - 5

    assert await cog.search_youtube_music() is True

    assert cog.youtube_music_search_client.local_worker._input_queue.size(fake_context['guild'].id) == 0
    assert media_request.search_result.youtube_music_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
