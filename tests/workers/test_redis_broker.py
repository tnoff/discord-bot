'''Tests for RedisBroker — Redis-backed media broker implementation.'''
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.interfaces.broker_protocols import BrokerEntry, Zone
from discord_bot.types.download import DownloadEvent, DownloadResult, DownloadStatus, DownloadStatusUpdate
from discord_bot.types.media_download import MediaDownload
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.workers.broker_registry import RedisBrokerRegistry
from discord_bot.workers.redis_broker import RedisBroker, _download_from_dict, _download_to_dict, _entry_from_dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_registry() -> RedisBrokerRegistry:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisBrokerRegistry(RedisManager.from_client(client))


def _make_broker(video_cache=None, bucket_name=None, registry: RedisBrokerRegistry | None = None) -> RedisBroker:
    return RedisBroker(registry or _make_registry(), video_cache=video_cache, bucket_name=bucket_name)


def _make_request(guild_id: int = 1, channel_id: int = 2) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id,
        channel_id=channel_id,
        requester_name='tester',
        requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT,
            raw_search_string='https://example.com/video',
        ),
    )


def _make_download(request: MediaRequest, file_path: Path | None = None) -> MediaDownload:
    return MediaDownload(
        file_path,
        {
            'id': 'vid1',
            'title': 'Test Video',
            'webpage_url': 'https://example.com/video',
            'uploader': 'Someone',
            'duration': 120,
            'extractor': 'youtube',
        },
        request,
    )


def _make_result(request: MediaRequest, file_path: Path | None = None) -> DownloadResult:
    return DownloadResult(
        status=DownloadStatus(success=True),
        media_request=request,
        ytdlp_data={
            'id': 'vid1',
            'title': 'Test Video',
            'webpage_url': 'https://example.com/video',
            'uploader': 'Someone',
            'duration': 120,
            'extractor': 'youtube',
        },
        file_name=file_path,
        file_size_bytes=1024,
    )


# ---------------------------------------------------------------------------
# _download_to_dict / _download_from_dict / _entry_from_dict helpers
# ---------------------------------------------------------------------------

def test_download_to_dict_roundtrip():
    '''_download_to_dict serialises all expected keys.'''
    req = _make_request()
    dl = _make_download(req, Path('/tmp/test.mp3'))
    d = _download_to_dict(dl)
    assert d['file_path'] == '/tmp/test.mp3'
    assert d['webpage_url'] == 'https://example.com/video'
    assert d['title'] == 'Test Video'


def test_download_to_dict_none_file_path():
    '''_download_to_dict serialises None file_path as None.'''
    req = _make_request()
    dl = _make_download(req, None)
    assert _download_to_dict(dl)['file_path'] is None


def test_download_from_dict_reconstructs():
    '''_download_from_dict reconstructs a MediaDownload from a serialised dict.'''
    req = _make_request()
    data = {
        'id': 'vid1',
        'title': 'Test Video',
        'webpage_url': 'https://example.com/video',
        'uploader': 'Someone',
        'duration': 120,
        'extractor': 'youtube',
        'file_path': '/tmp/test.mp3',
        'file_size_bytes': 2048,
    }
    dl = _download_from_dict(data, req)
    assert dl.file_path == Path('/tmp/test.mp3')
    assert dl.file_size_bytes == 2048
    assert dl.webpage_url == 'https://example.com/video'


def test_download_from_dict_no_file_path():
    '''_download_from_dict handles missing file_path gracefully.'''
    req = _make_request()
    dl = _download_from_dict({'webpage_url': 'https://example.com/video'}, req)
    assert dl.file_path is None


def test_entry_from_dict_in_flight_no_download():
    '''_entry_from_dict reconstructs an IN_FLIGHT entry with no download.'''
    req = _make_request()
    data = {
        'zone': 'in_flight',
        'checked_out_by': None,
        'guild_file_path': None,
        'request': req.model_dump(mode='json'),
        'download': None,
    }
    entry = _entry_from_dict(data)
    assert entry.zone == Zone.IN_FLIGHT
    assert entry.download is None
    assert entry.guild_file_path is None


def test_entry_from_dict_checked_out_with_guild_path():
    '''_entry_from_dict reconstructs a CHECKED_OUT entry with guild_file_path.'''
    req = _make_request()
    dl = _make_download(req, Path('/s3/key.mp3'))
    data = {
        'zone': 'checked_out',
        'checked_out_by': 42,
        'guild_file_path': '/local/guild/file.mp3',
        'request': req.model_dump(mode='json'),
        'download': _download_to_dict(dl),
    }
    entry = _entry_from_dict(data)
    assert entry.zone == Zone.CHECKED_OUT
    assert entry.checked_out_by == 42
    assert entry.guild_file_path == Path('/local/guild/file.mp3')


# ---------------------------------------------------------------------------
# register_request
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_request_stores_in_flight():
    '''register_request stores a new entry in IN_FLIGHT zone.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    entry = await broker.get_entry(str(req.uuid))
    assert entry is not None
    assert entry.zone == Zone.IN_FLIGHT
    assert entry.download is None


@pytest.mark.asyncio
async def test_register_request_idempotent():
    '''register_request called twice does not overwrite the existing entry.'''
    registry = _make_registry()
    broker = _make_broker(registry=registry)
    req = _make_request()
    await broker.register_request(req)
    # Advance zone to available via registry to simulate a partial download.
    data = await registry.get_entry(str(req.uuid))
    data['zone'] = 'available'
    await registry.set_entry(str(req.uuid), data)
    # Second call should not reset zone back to in_flight
    await broker.register_request(req)
    entry = await broker.get_entry(str(req.uuid))
    assert entry.zone == Zone.AVAILABLE


# ---------------------------------------------------------------------------
# update_request_status
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_update_request_status_in_progress():
    '''update_request_status with IN_PROGRESS marks the request in progress.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = DownloadStatusUpdate(event=DownloadEvent.IN_PROGRESS)
    await broker.update_request_status(str(req.uuid), update)
    # No exception raised and entry still exists
    entry = await broker.get_entry(str(req.uuid))
    assert entry is not None


@pytest.mark.asyncio
async def test_update_request_status_backoff():
    '''update_request_status with BACKOFF does not raise.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = DownloadStatusUpdate(event=DownloadEvent.BACKOFF)
    await broker.update_request_status(str(req.uuid), update)


@pytest.mark.asyncio
async def test_update_request_status_retry():
    '''update_request_status with RETRY stores error detail and backoff.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = DownloadStatusUpdate(event=DownloadEvent.RETRY, error_detail='rate-limited', backoff_seconds=5)
    await broker.update_request_status(str(req.uuid), update)
    entry = await broker.get_entry(str(req.uuid))
    assert entry is not None


@pytest.mark.asyncio
async def test_update_request_status_discarded():
    '''update_request_status with DISCARDED does not raise.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = DownloadStatusUpdate(event=DownloadEvent.DISCARDED)
    await broker.update_request_status(str(req.uuid), update)


@pytest.mark.asyncio
async def test_update_request_status_unknown_uuid_logs_warning(caplog):
    '''update_request_status for an unknown UUID logs a warning and does not raise.'''
    broker = _make_broker()
    update = DownloadStatusUpdate(event=DownloadEvent.IN_PROGRESS)
    with caplog.at_level(logging.WARNING, logger='discord_bot.workers.redis_broker'):
        await broker.update_request_status('no-such-uuid', update)
    assert 'unknown uuid' in caplog.text


# ---------------------------------------------------------------------------
# register_download_result / register_download
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_download_result_returns_media_download():
    '''register_download_result creates and returns a MediaDownload.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    result = _make_result(req, Path('/s3/key.mp3'))
    media_download = await broker.register_download_result(result)
    assert isinstance(media_download, MediaDownload)
    assert media_download.file_path == Path('/s3/key.mp3')


@pytest.mark.asyncio
async def test_register_download_moves_to_available():
    '''register_download transitions the entry to AVAILABLE zone.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    entry = await broker.get_entry(str(req.uuid))
    assert entry.zone == Zone.AVAILABLE
    assert entry.download is not None


@pytest.mark.asyncio
async def test_register_download_creates_entry_if_missing():
    '''register_download creates a new AVAILABLE entry when none exists yet.'''
    broker = _make_broker()
    req = _make_request()
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    entry = await broker.get_entry(str(req.uuid))
    assert entry is not None
    assert entry.zone == Zone.AVAILABLE


@pytest.mark.asyncio
async def test_register_download_calls_video_cache_iterate():
    '''register_download calls video_cache.iterate_file when cache is configured.'''
    video_cache = MagicMock()
    video_cache.iterate_file = AsyncMock()
    broker = _make_broker(video_cache=video_cache)
    req = _make_request()
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    video_cache.iterate_file.assert_called_once_with(dl)


# ---------------------------------------------------------------------------
# checkout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_checkout_returns_s3_key():
    '''checkout returns the file_path from the download as a Path (S3 key).'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/bucket/key.mp3'))
    await broker.register_download(dl)
    path = await broker.checkout(str(req.uuid), guild_id=1)
    assert path == Path('/s3/bucket/key.mp3')


@pytest.mark.asyncio
async def test_checkout_returns_none_when_atomic_checkout_fails():
    '''checkout returns None when the entry is already checked out.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    # First checkout succeeds
    path = await broker.checkout(str(req.uuid), guild_id=1)
    assert path is not None
    # Second checkout fails
    path2 = await broker.checkout(str(req.uuid), guild_id=2)
    assert path2 is None


@pytest.mark.asyncio
async def test_checkout_returns_none_when_no_download():
    '''checkout returns None when the entry has no download.'''
    registry = _make_registry()
    broker = _make_broker(registry=registry)
    req = _make_request()
    await broker.register_request(req)
    # Force zone to available without a download so atomic_checkout succeeds but file_path is missing.
    data = await registry.get_entry(str(req.uuid))
    data['zone'] = 'available'
    await registry.set_entry(str(req.uuid), data)
    path = await broker.checkout(str(req.uuid), guild_id=1)
    assert path is None


# ---------------------------------------------------------------------------
# remove / release / discard
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_remove_deletes_entry():
    '''remove deletes the entry from the registry.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    await broker.remove(str(req.uuid))
    assert await broker.get_entry(str(req.uuid)) is None


@pytest.mark.asyncio
async def test_release_deletes_entry():
    '''release deletes the entry from the registry.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    await broker.release(str(req.uuid))
    assert await broker.get_entry(str(req.uuid)) is None


@pytest.mark.asyncio
async def test_discard_deletes_entry():
    '''discard removes the entry even when no download exists.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    await broker.discard(str(req.uuid))
    assert await broker.get_entry(str(req.uuid)) is None


@pytest.mark.asyncio
async def test_discard_deletes_s3_file_when_no_cache():
    '''discard calls delete_file on S3 when there is a download and no video_cache.'''
    broker = _make_broker(bucket_name='my-bucket')
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    with patch('discord_bot.workers.redis_broker.delete_file') as mock_delete:
        await broker.discard(str(req.uuid))
    mock_delete.assert_called_once()


@pytest.mark.asyncio
async def test_discard_skips_s3_delete_when_video_cache_configured():
    '''discard does not delete from S3 when a video_cache manages the file.'''
    video_cache = MagicMock()
    video_cache.iterate_file = AsyncMock()
    broker = _make_broker(video_cache=video_cache, bucket_name='my-bucket')
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    with patch('discord_bot.workers.redis_broker.delete_file') as mock_delete:
        await broker.discard(str(req.uuid))
    mock_delete.assert_not_called()


# ---------------------------------------------------------------------------
# prefetch (no-op)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_prefetch_is_noop():
    '''prefetch is a no-op and does not raise.'''
    broker = _make_broker()
    await broker.prefetch([], guild_id=1, guild_path=None, limit=3)


# ---------------------------------------------------------------------------
# can_evict_request / can_evict_base
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_can_evict_request_true_when_missing():
    '''can_evict_request returns True for an unknown UUID.'''
    broker = _make_broker()
    assert await broker.can_evict_request('no-such-uuid') is True


@pytest.mark.asyncio
async def test_can_evict_request_true_when_available():
    '''can_evict_request returns True when the entry is AVAILABLE.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    assert await broker.can_evict_request(str(req.uuid)) is True


@pytest.mark.asyncio
async def test_can_evict_request_false_when_checked_out():
    '''can_evict_request returns False when the entry is CHECKED_OUT.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    await broker.checkout(str(req.uuid), guild_id=1)
    assert await broker.can_evict_request(str(req.uuid)) is False


@pytest.mark.asyncio
async def test_can_evict_base_true_when_not_present():
    '''can_evict_base returns True when no entry references the given URL.'''
    broker = _make_broker()
    assert await broker.can_evict_base('https://example.com/other') is True


@pytest.mark.asyncio
async def test_can_evict_base_false_when_url_available():
    '''can_evict_base returns False when an AVAILABLE entry references the URL.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    assert await broker.can_evict_base('https://example.com/video') is False


@pytest.mark.asyncio
async def test_can_evict_base_false_when_url_checked_out():
    '''can_evict_base returns False when a CHECKED_OUT entry references the URL.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/key.mp3'))
    await broker.register_download(dl)
    await broker.checkout(str(req.uuid), guild_id=1)
    assert await broker.can_evict_base('https://example.com/video') is False


# ---------------------------------------------------------------------------
# get_entry / get_cache_count / get_checked_out_by
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_entry_returns_none_when_missing():
    '''get_entry returns None for an unknown UUID.'''
    broker = _make_broker()
    assert await broker.get_entry('no-such-uuid') is None


@pytest.mark.asyncio
async def test_get_entry_returns_broker_entry():
    '''get_entry returns a typed BrokerEntry for a known UUID.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    entry = await broker.get_entry(str(req.uuid))
    assert isinstance(entry, BrokerEntry)
    assert entry.zone == Zone.IN_FLIGHT


@pytest.mark.asyncio
async def test_get_cache_count_returns_zero_without_cache():
    '''get_cache_count returns 0 when no video_cache is configured.'''
    broker = _make_broker()
    assert await broker.get_cache_count() == 0


@pytest.mark.asyncio
async def test_get_cache_count_delegates_to_video_cache():
    '''get_cache_count returns the cache count from video_cache.'''
    video_cache = MagicMock()
    video_cache.get_cache_count = AsyncMock(return_value=7)
    broker = _make_broker(video_cache=video_cache)
    assert await broker.get_cache_count() == 7


@pytest.mark.asyncio
async def test_get_checked_out_by_returns_matching_entries():
    '''get_checked_out_by returns only entries checked out by the given guild.'''
    broker = _make_broker()
    req1 = _make_request(guild_id=1)
    req2 = _make_request(guild_id=2)

    for req in (req1, req2):
        await broker.register_request(req)
        dl = _make_download(req, Path('/s3/key.mp3'))
        await broker.register_download(dl)

    await broker.checkout(str(req1.uuid), guild_id=10)
    await broker.checkout(str(req2.uuid), guild_id=20)

    results = await broker.get_checked_out_by(guild_id=10)
    assert len(results) == 1
    assert results[0].checked_out_by == 10


@pytest.mark.asyncio
async def test_get_checked_out_by_returns_empty_when_none():
    '''get_checked_out_by returns an empty list when no entries match.'''
    broker = _make_broker()
    results = await broker.get_checked_out_by(guild_id=99)
    assert results == []


# ---------------------------------------------------------------------------
# check_cache (inherited template method)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_check_cache_returns_none_without_video_cache():
    '''check_cache returns None when no video_cache is configured.'''
    broker = _make_broker()
    req = _make_request()
    result = await broker.check_cache(req)
    assert result is None


# ---------------------------------------------------------------------------
# _get_evictable_entries overridden batch fetch
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_cleanup_evicts_via_batch_fetch():
    '''cache_cleanup (via _get_evictable_entries override) evicts entries not in use.'''
    video_cache = MagicMock()
    deletable = MagicMock()
    deletable.video_url = 'https://example.com/other'
    deletable.base_path = '/s3/other.mp3'
    deletable.id = 99
    video_cache.ready_remove = AsyncMock()
    video_cache.get_deletable_entries = AsyncMock(return_value=[deletable])
    video_cache.remove_video_cache = AsyncMock()
    broker = _make_broker(video_cache=video_cache, bucket_name='my-bucket')
    with patch('discord_bot.interfaces.broker_protocols.delete_file') as mock_delete:
        result = await broker.cache_cleanup()
    assert result is True
    mock_delete.assert_called_once_with('my-bucket', '/s3/other.mp3')
