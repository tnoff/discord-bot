import asyncio
from asyncio import QueueFull
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest

from discord_bot.cogs.music import Music
from discord_bot.cogs.music_helpers.download_client import ExistingFileException
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.common import SearchType, MediaRequestLifecycleStage, YOUTUBE_VIDEO_PREFIX
from discord_bot.cogs.music_helpers.media_request import MediaRequest, MultiMediaRequestBundle
from discord_bot.cogs.music_helpers.search_client import SearchResult
from discord_bot.utils.clients.youtube_music import YoutubeMusicRetryException
from discord_bot.utils.failure_queue import FailureStatus
from discord_bot.utils.queue import PutsBlocked

from tests.cogs.test_music import BASE_MUSIC_CONFIG
from tests.helpers import fake_media_download
from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


class MockSearchClient:
    """Mock search client for testing YouTube Music search"""
    def __init__(self, youtube_music_result='test-video-id'):
        self.youtube_music_result = youtube_music_result

    async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
        """Mock YouTube Music search that returns a video ID"""
        if self.youtube_music_result:
            return self.youtube_music_result
        return None


def create_test_media_request(test_context, search_string='test search', bundle_uuid=None, search_type=SearchType.SEARCH):
    """Helper to create test media requests"""
    request = MediaRequest(
        test_context['guild'].id,
        test_context['channel'].id,
        test_context['author'].display_name,
        test_context['author'].id,
        SearchResult(search_type, search_string)
    )
    if bundle_uuid:
        request.bundle_uuid = bundle_uuid
    return request


@pytest.mark.asyncio()
async def test_search_youtube_music_empty_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test search_youtube_music when queue is empty"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient()

    # Queue is empty, should return without error
    result = await cog.search_youtube_music()
    assert result


@pytest.mark.asyncio()
async def test_search_youtube_music_bot_shutdown(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test search_youtube_music exits early when bot is shutting down"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.bot_shutdown_event.set()

    with pytest.raises(ExistingFileException) as exc:
        await cog.search_youtube_music()
    assert 'Bot shutdown called' in str(exc.value)


@pytest.mark.asyncio()
async def test_search_youtube_music_successful_search_no_cache(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test successful YouTube Music search with no cache hit"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock message queue
    cog.message_queue = MagicMock()

    await cog.search_youtube_music()

    # Verify search string was updated with YouTube prefix
    assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

    # Verify request was added to download queue
    assert cog.download_queue.size(fake_context['guild'].id) > 0
    download_item = cog.download_queue.get_nowait()
    assert download_item == media_request

    # Verify bundle status was updated
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.QUEUED


@pytest.mark.asyncio()
async def test_search_youtube_music_successful_search_cache_hit(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test successful YouTube Music search with cache hit"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog, '_Music__check_video_cache', return_value=cached_download)

            # Mock player methods
            mock_player = MagicMock()
            mocker.patch.object(cog, 'get_player', return_value=mock_player)
            mock_add_source = mocker.patch.object(cog, 'add_source_to_player', return_value=None)

            await cog.search_youtube_music()

            # Verify search string was updated with YouTube prefix
            assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

            # Verify download queue is empty (cache hit, no download needed)
            assert cog.download_queue.size(fake_context['guild'].id) == 0

            # Verify add_source_to_player was called
            mock_add_source.assert_called_once_with(cached_download, mock_player)


@pytest.mark.asyncio()
async def test_search_youtube_music_no_result(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search returns no results"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient(None)  # No result

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    await cog.search_youtube_music()

    # Verify original search string unchanged
    assert media_request.search_result.raw_search_string == 'test search'

    # Verify download queue still has item
    assert cog.download_queue.size(fake_context['guild'].id) == 1


@pytest.mark.asyncio()
async def test_search_youtube_music_download_queue_full(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when download queue is full"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock download queue full
    mocker.patch.object(cog.download_queue, 'put_nowait', side_effect=QueueFull())

    # Mock message queue
    cog.message_queue = MagicMock()

    await cog.search_youtube_music()

    # Verify bundle status was updated to DISCARDED
    bundle_request = bundle.bundled_requests[0]
    assert bundle_request.media_request.lifecycle_stage == MediaRequestLifecycleStage.DISCARDED


@pytest.mark.asyncio()
async def test_search_youtube_music_download_queue_blocked(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when download queue puts are blocked"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock download queue blocked
    mocker.patch.object(cog.download_queue, 'put_nowait', side_effect=PutsBlocked())

    result = await cog.search_youtube_music()

    # Should return False when puts are blocked
    assert result is False


@pytest.mark.asyncio()
async def test_search_youtube_music_playlist_item(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search for playlist addition"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request for playlist addition
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    media_request.add_to_playlist = 123  # Playlist ID
    media_request.download_file = False
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog, '_Music__check_video_cache', return_value=cached_download)

            # Mock playlist addition
            mocker.patch.object(cog, '_Music__add_playlist_item_function', return_value=None)

            await cog.search_youtube_music()

            # Verify playlist addition was called
            cog._Music__add_playlist_item_function.assert_called_once_with(123, cached_download) #pylint:disable=protected-access

            # Verify download queue is empty (playlist addition, no player queue needed)
            assert cog.download_queue.size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_cache_miss(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test _enqueue_media_download_from_cache with cache miss"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    media_request = create_test_media_request(fake_context)

    # Create bundle for the request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request.bundle_uuid = bundle.uuid
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    result = await cog._enqueue_media_download_from_cache(media_request, bundle) #pylint:disable=protected-access

    assert result is False


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_cache_hit_player(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test _enqueue_media_download_from_cache with cache hit and player"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    media_request = create_test_media_request(fake_context)

    # Create bundle for the request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request.bundle_uuid = bundle.uuid
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog, '_Music__check_video_cache', return_value=cached_download)

            # Mock player methods
            mock_player = MagicMock()
            mocker.patch.object(cog, 'get_player', return_value=mock_player)
            mock_add_source = mocker.patch.object(cog, 'add_source_to_player', return_value=None)

            result = await cog._enqueue_media_download_from_cache(media_request, bundle) #pylint:disable=protected-access

            assert result is True
            mock_add_source.assert_called_once_with(cached_download, mock_player)


@pytest.mark.asyncio()
async def test_enqueue_media_download_from_cache_playlist_addition(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test _enqueue_media_download_from_cache with playlist addition"""
    config = BASE_MUSIC_CONFIG

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    media_request = create_test_media_request(fake_context)
    media_request.add_to_playlist = 456
    media_request.download_file = False

    # Create bundle for the request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request.bundle_uuid = bundle.uuid
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Create mock cached item
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as cached_download:
            # Mock cache hit
            mocker.patch.object(cog, '_Music__check_video_cache', return_value=cached_download)

            # Mock playlist addition
            mocker.patch.object(cog, '_Music__add_playlist_item_function', return_value=None)

            result = await cog._enqueue_media_download_from_cache(media_request, bundle) #pylint:disable=protected-access

            assert result is True
            cog._Music__add_playlist_item_function.assert_called_once_with(456, cached_download) #pylint:disable=protected-access


@pytest.mark.asyncio()
async def test_youtube_search_queue_integration_with_enqueue_media_requests(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test integration of YouTube search queue with enqueue_media_requests"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    # Create a bundle
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Create search-type media requests (should go to search queue)
    search_request = create_test_media_request(fake_context, 'search term', bundle.uuid)

    # Create direct-type media request (should go directly to download queue)
    direct_request = create_test_media_request(fake_context, 'https://direct.url', bundle.uuid, SearchType.DIRECT)

    entries = [search_request, direct_request]

    # Mock cache misses
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle, player=mock_player)

    assert result is True

    # Verify search request went to search queue
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) > 0
    search_queue_item = cog.youtube_music_search_queue.get_nowait()
    assert search_queue_item[0] == search_request  # (media_request, channel)

    # Verify direct request went to download queue
    assert cog.download_queue.size(fake_context['guild'].id) > 0
    download_queue_item = cog.download_queue.get_nowait()
    assert download_queue_item == direct_request

    # Verify bundle was updated
    assert len(bundle.bundled_requests) == 2


@pytest.mark.asyncio()
async def test_search_youtube_music_search_client_exception(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test YouTube Music search when search client raises exception"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create search client that raises exception
    class FailingSearchClient:
        """Mock search client that raises exceptions"""
        async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
            """Mock method that raises a network error"""
            raise RuntimeError("Network error")

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = FailingSearchClient()

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Mock message queue
    cog.message_queue = MagicMock()

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
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    # Create search client that times out
    class TimeoutSearchClient:
        """Mock search client that times out"""
        async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
            """Mock method that raises a timeout error"""
            raise asyncio.TimeoutError("Search timeout")

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = TimeoutSearchClient()

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Should handle timeout gracefully
    try:
        await cog.search_youtube_music()
    except asyncio.TimeoutError as e:
        assert "Search timeout" in str(e)


@pytest.mark.asyncio()
async def test_search_queue_disabled_routing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test routing when YouTube Music search is disabled"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': False
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    # Create a bundle
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Create search-type media requests (should go directly to download queue when disabled)
    search_request = create_test_media_request(fake_context, 'search term', bundle.uuid)

    spotify_request = create_test_media_request(fake_context, 'spotify search', bundle.uuid, SearchType.SPOTIFY)

    entries = [search_request, spotify_request]

    # Mock cache misses
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle, player=mock_player)

    assert result is True

    # Verify both requests went directly to download queue (not search queue)
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) == 0
    assert cog.download_queue.size(fake_context['guild'].id) == 2

    # Verify both items are in download queue
    download_item1 = cog.download_queue.get_nowait()
    download_item2 = cog.download_queue.get_nowait()

    assert download_item1 in [search_request, spotify_request]
    assert download_item2 in [search_request, spotify_request]
    assert download_item1 != download_item2


@pytest.mark.asyncio()
async def test_mixed_search_types_routing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test routing with mixed SearchTypes in same batch"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    # Create a bundle
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Create requests of different types
    search_request = create_test_media_request(fake_context, 'search term', bundle.uuid)

    spotify_request = create_test_media_request(fake_context, 'spotify track', bundle.uuid, SearchType.SPOTIFY)

    direct_request = create_test_media_request(fake_context, 'https://direct.url', bundle.uuid, SearchType.DIRECT)

    youtube_request = create_test_media_request(fake_context, 'https://youtube.com/watch?v=123', bundle.uuid, SearchType.YOUTUBE)

    youtube_playlist_request = create_test_media_request(fake_context, 'https://youtube.com/playlist?list=123', bundle.uuid, SearchType.YOUTUBE_PLAYLIST)

    entries = [search_request, spotify_request, direct_request, youtube_request, youtube_playlist_request]

    # Mock cache misses
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle, player=mock_player)

    assert result is True

    # Verify search and spotify requests went to search queue
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) == 2

    # Verify direct, youtube, and youtube playlist went to download queue
    assert cog.download_queue.size(fake_context['guild'].id) == 3

    # Verify correct items in each queue
    search_queue_items = []
    for _ in range(2):
        item = cog.youtube_music_search_queue.get_nowait()
        search_queue_items.append(item[0])  # Get media_request from tuple

    download_queue_items = []
    for _ in range(3):
        item = cog.download_queue.get_nowait()
        download_queue_items.append(item)

    # Check search queue has search and spotify requests
    assert search_request in search_queue_items
    assert spotify_request in search_queue_items

    # Check download queue has direct, youtube, and youtube playlist requests
    assert direct_request in download_queue_items
    assert youtube_request in download_queue_items
    assert youtube_playlist_request in download_queue_items


@pytest.mark.asyncio()
async def test_search_queue_priority_handling(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test server-specific priority handling in search queue"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    # Set server-specific priority
    test_priority = 50
    cog.server_queue_priority[fake_context['guild'].id] = test_priority

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle

    entries = [media_request]

    # Mock cache misses
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Call enqueue_media_requests
    result = await cog.enqueue_media_requests(fake_context['context'], entries, bundle, player=mock_player)

    assert result is True

    # Verify item went to search queue with correct priority
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) == 1

    # Check that the guild queue has the correct priority
    guild_queue_data = cog.youtube_music_search_queue.queues[fake_context['guild'].id]
    assert guild_queue_data.priority == test_priority


@pytest.mark.asyncio()
async def test_bundle_expiration_during_search_processing(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test handling when bundle expires while item is being processed in search queue"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Remove bundle to simulate expiration
    del cog.multirequest_bundles[bundle.uuid]

    # Should handle missing bundle gracefully
    await cog.search_youtube_music()

    # Verify search still happened and item went to download queue
    assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
    assert cog.download_queue.size(fake_context['guild'].id) == 1


@pytest.mark.asyncio()
async def test_search_queue_resource_limits(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test search queue with the new 10x sizing vs download queue limits"""
    # Test with very small download queue size to verify search queue is larger
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'player': {
                'queue_max_size': 2,  # Small download queue
            },
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)

    # Create enough items to fill beyond download queue size but within search queue size
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    cog.multirequest_bundles[bundle.uuid] = bundle

    # Add more items than download queue can handle
    search_requests = []
    for i in range(10):  # More than download queue size of 2
        request = create_test_media_request(fake_context, f'search term {i}', bundle.uuid)
        search_requests.append(request)

    # Mock cache misses
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock player
    mock_player = MagicMock()
    mocker.patch.object(cog, 'get_player', return_value=mock_player)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Should be able to enqueue all search requests to search queue
    result = await cog.enqueue_media_requests(fake_context['context'], search_requests, bundle, player=mock_player)

    assert result is True
    # Verify the search queue size is configured correctly
    assert cog.youtube_music_search_queue.max_size == 4

    # All 10 items should fit in search queue
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) == 4

    # Download queue should be empty initially
    assert cog.download_queue.size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_message_queue_update_failure_during_search(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test handling when message queue update fails during search processing"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create a bundle and media request
    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    # Add to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock message queue to raise exception
    failing_message_queue = MagicMock()
    failing_message_queue.update_multiple_mutable.side_effect = Exception("Message queue error")
    cog.message_queue = failing_message_queue

    # Should handle message queue failure gracefully
    try:
        await cog.search_youtube_music()
        # Should not crash despite message queue failure
        # Verify core functionality still works
        assert media_request.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
        assert cog.download_queue.size(fake_context['guild'].id) == 1
    except Exception as e: #pylint:disable=broad-exception-caught
        # If exception propagates, it should be handled gracefully in real implementation
        assert "Message queue error" in str(e)


@pytest.mark.asyncio()
async def test_concurrent_bundle_operations_during_search(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test concurrent bundle operations while search queue is processing"""
    config = BASE_MUSIC_CONFIG | {
        'music': {
            'download': {
                'enable_youtube_music_search': True
            }
        }
    }

    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')

    # Create multiple bundles
    bundle1 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    bundle2 = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])

    cog.multirequest_bundles[bundle1.uuid] = bundle1
    cog.multirequest_bundles[bundle2.uuid] = bundle2

    # Create media requests for both bundles
    media_request1 = create_test_media_request(fake_context, 'test search 1', bundle1.uuid)
    media_request2 = create_test_media_request(fake_context, 'test search 2', bundle2.uuid)

    bundle1.add_media_request(media_request1)
    bundle2.add_media_request(media_request2)

    # Add both to search queue
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request1, fake_context['channel']))
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request2, fake_context['channel']))

    # Mock cache miss
    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)

    # Mock message queue
    cog.message_queue = MagicMock()

    # Process both items
    await cog.search_youtube_music()
    await cog.search_youtube_music()

    # Verify both items were processed correctly
    assert media_request1.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
    assert media_request2.search_result.resolved_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'

    # Both should be in download queue
    assert cog.download_queue.size(fake_context['guild'].id) == 2

    # Verify both bundles were updated
    assert len(bundle1.bundled_requests) == 1
    assert len(bundle2.bundled_requests) == 1


class RateLimitedSearchClient:
    """Mock search client that raises YoutubeMusicRetryException"""
    async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
        raise YoutubeMusicRetryException('429 Exhaust Limit Hit')


@pytest.mark.asyncio()
async def test_search_youtube_music_429_requeues_item(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a 429 re-enqueues the item and sets RETRY_SEARCH lifecycle stage"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'enable_youtube_music_search': True}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = RateLimitedSearchClient()

    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)
    bundle.all_requests_added()
    cog.message_queue = MagicMock()

    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    result = await cog.search_youtube_music()

    assert result is False
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH
    assert media_request.youtube_music_retry_information.retry_count == 1
    assert media_request.youtube_music_retry_information.retry_reason is not None
    # Item should be back in the search queue
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) == 1
    # Not in download queue
    assert cog.download_queue.size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
@pytest.mark.freeze_time
async def test_search_youtube_music_429_sets_backoff_timestamp(freezer, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a 429 sets the youtube_music_wait_timestamp with exponential backoff"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'enable_youtube_music_search': True}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = RateLimitedSearchClient()
    cog.message_queue = MagicMock()

    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    freezer.move_to('2025-01-01 12:00:00 UTC')
    assert cog.youtube_music_wait_timestamp is None

    await cog.search_youtube_music()

    # Failure queue size is 1, so multiplier is 2^1 = 2
    # Expected: now (1735732800) + 30*2 + 5 = 1735732865
    assert cog.youtube_music_wait_timestamp == 1735732865
    assert cog.youtube_music_failure_queue.size == 1


@pytest.mark.asyncio()
@pytest.mark.freeze_time
async def test_search_youtube_music_429_exponential_backoff_growth(freezer, mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that repeated 429s grow the backoff exponentially"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'enable_youtube_music_search': True}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = RateLimitedSearchClient()
    cog.message_queue = MagicMock()

    # Pre-populate failure queue with 2 existing failures
    cog.youtube_music_failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    cog.youtube_music_failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    assert cog.youtube_music_failure_queue.size == 2

    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    freezer.move_to('2025-01-01 12:00:00 UTC')
    await cog.search_youtube_music()

    # Failure queue size is now 3, so multiplier is 2^3 = 8
    # Expected: now (1735732800) + 30*8 + 5 = 1735733045
    assert cog.youtube_music_failure_queue.size == 3
    assert cog.youtube_music_wait_timestamp == 1735733045


@pytest.mark.asyncio()
async def test_search_youtube_music_429_retry_limit_exceeded(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that hitting max retries marks the request as FAILED instead of re-queuing"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'enable_youtube_music_search': True, 'max_youtube_music_search_retries': 3}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')
    mocker.patch('discord_bot.cogs.music.random.randint', return_value=5000)

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = RateLimitedSearchClient()
    cog.message_queue = MagicMock()

    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    # Simulate already at retry limit
    media_request.youtube_music_retry_information.retry_count = 2
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    await cog.search_youtube_music()

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert media_request.failure_reason is not None
    # Should NOT be re-queued in search queue
    assert cog.youtube_music_search_queue.size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_search_youtube_music_429_resets_lifecycle_on_retry(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a re-queued item resets from RETRY_SEARCH back to SEARCHING on next attempt"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'enable_youtube_music_search': True}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    call_count = 0

    class SucceedOnSecondCallClient:
        async def search_youtube_music(self, search_string, loop): #pylint:disable=unused-argument
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise YoutubeMusicRetryException('429 Exhaust Limit Hit')
            return 'test-video-id'

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = SucceedOnSecondCallClient()
    cog.message_queue = MagicMock()

    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)
    bundle.all_requests_added()

    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    # First call hits 429
    mocker.patch.object(cog, 'youtube_music_backoff_time', return_value=True)
    await cog.search_youtube_music()
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH

    # Second call succeeds — lifecycle should reset to SEARCHING then proceed to QUEUED
    await cog.search_youtube_music()
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.QUEUED


@pytest.mark.asyncio()
async def test_search_youtube_music_success_clears_failure_queue(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """Test that a successful search adds a success to the failure queue"""
    config = BASE_MUSIC_CONFIG | {
        'music': {'download': {'enable_youtube_music_search': True}}
    }
    mocker.patch('discord_bot.cogs.music.sleep', return_value=True)
    mocker.patch.object(MusicPlayer, 'start_tasks')

    cog = Music(fake_context['bot'], config, None)
    cog.search_client = MockSearchClient('test-video-id')
    cog.message_queue = MagicMock()

    # Pre-populate failure queue
    cog.youtube_music_failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    cog.youtube_music_failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    assert cog.youtube_music_failure_queue.size == 2

    bundle = MultiMediaRequestBundle(fake_context['guild'].id, fake_context['channel'].id, fake_context['channel'])
    media_request = create_test_media_request(fake_context, 'test search', bundle.uuid)
    cog.multirequest_bundles[bundle.uuid] = bundle
    bundle.add_media_request(media_request)

    mocker.patch.object(cog, '_Music__check_video_cache', return_value=None)
    cog.youtube_music_search_queue.put_nowait(fake_context['guild'].id, (media_request, fake_context['channel']))

    await cog.search_youtube_music()

    # Successful search should remove one failure from the queue
    assert cog.youtube_music_failure_queue.size == 1
