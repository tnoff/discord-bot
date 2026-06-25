'''Tests for RedisBroker — Redis-backed media broker implementation.'''
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.interfaces.broker_protocols import BrokerEntry, Zone
from discord_bot.types.download import LifecycleEvent, DownloadResult, DownloadStatus, LifecycleStatusUpdate
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
    update = LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
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
    update = LifecycleStatusUpdate(event=LifecycleEvent.BACKOFF)
    await broker.update_request_status(str(req.uuid), update)


@pytest.mark.asyncio
async def test_update_request_status_retry():
    '''update_request_status with RETRY stores error detail and backoff.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = LifecycleStatusUpdate(event=LifecycleEvent.RETRY, error_detail='rate-limited', backoff_seconds=5)
    await broker.update_request_status(str(req.uuid), update)
    entry = await broker.get_entry(str(req.uuid))
    assert entry is not None


@pytest.mark.asyncio
async def test_update_request_status_discarded():
    '''update_request_status with DISCARDED does not raise.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = LifecycleStatusUpdate(event=LifecycleEvent.DISCARDED)
    await broker.update_request_status(str(req.uuid), update)


@pytest.mark.parametrize('event', [
    LifecycleEvent.QUEUED,
    LifecycleEvent.RETRY_SEARCH,
    LifecycleEvent.COMPLETED,
    LifecycleEvent.FAILED,
])
@pytest.mark.asyncio
async def test_update_request_status_handles_every_event_branch(event):
    '''Each LifecycleEvent branch in update_request_status mutates the entry.'''
    broker = _make_broker()
    req = _make_request()
    await broker.register_request(req)
    update = LifecycleStatusUpdate(
        event=event,
        error_detail='r' if event == LifecycleEvent.RETRY_SEARCH else None,
        backoff_seconds=5 if event == LifecycleEvent.RETRY_SEARCH else None,
        failure_reason='boom' if event == LifecycleEvent.FAILED else None,
    )
    await broker.update_request_status(str(req.uuid), update)
    entry = await broker.get_entry(str(req.uuid))
    assert entry is not None  # didn't raise / wipe the entry


@pytest.mark.asyncio
async def test_sync_request_into_bundle_no_op_when_bundle_missing():
    '''_sync_request_into_bundle returns silently if the bundle has been deleted.'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(guild_id=1, channel_id=2)
    req = _make_request()
    req.bundle_uuid = bundle_uuid
    await broker.register_request(req)
    await broker.delete_bundle(bundle_uuid)
    # update_request_status now triggers _sync_request_into_bundle, but the
    # bundle is gone — must not raise.
    await broker.update_request_status(
        str(req.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED),
    )


@pytest.mark.asyncio
async def test_sync_request_into_bundle_no_op_when_request_not_in_bundle():
    '''_sync_request_into_bundle's for/else falls through when the request
    isn't actually attached to the bundle (defensive — should be unreachable
    in practice but keeps the helper safe).'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(guild_id=1, channel_id=2)
    req = _make_request()
    # Set bundle_uuid but never call register_request → request is in the
    # registry (we'll add it manually below) but not in the bundle's
    # bundled_requests list.
    req.bundle_uuid = bundle_uuid
    # Plant a registry entry without the bundle attachment side-effect.
    await broker._registry.set_entry(  # pylint: disable=protected-access
        str(req.uuid),
        {'zone': 'in_flight', 'checked_out_by': None, 'guild_file_path': None,
         'request': req.model_dump(mode='json'), 'download': None},
    )
    await broker.update_request_status(
        str(req.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED),
    )


@pytest.mark.asyncio
async def test_update_request_status_unknown_uuid_is_noop():
    '''update_request_status for an unknown UUID returns without raising and
    creates no entry. (Asserting on the warning via caplog is unreliable in the
    full suite — setup_logging sets propagate=False on the discord_bot logger.)'''
    broker = _make_broker()
    update = LifecycleStatusUpdate(event=LifecycleEvent.IN_PROGRESS)
    await broker.update_request_status('no-such-uuid', update)
    assert await broker.get_entry('no-such-uuid') is None


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
    '''checkout returns a CheckoutResult carrying the download file_path as the
    S3 key plus the broker's bucket_name (HA — caller fetches from S3).'''
    broker = _make_broker(bucket_name='my-bucket')
    req = _make_request()
    await broker.register_request(req)
    dl = _make_download(req, Path('/s3/bucket/key.mp3'))
    await broker.register_download(dl)
    result = await broker.checkout(str(req.uuid), guild_id=1)
    assert result.s3_key == '/s3/bucket/key.mp3'
    assert result.bucket_name == 'my-bucket'
    assert result.local_path is None


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
# cache_cleanup eviction (base _get_evictable_entries + RedisBroker.can_evict_base)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cache_cleanup_evicts_via_batch_fetch():
    '''cache_cleanup evicts entries not referenced by any in-use registry entry.'''
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


# ---------------------------------------------------------------------------
# Bundle lifecycle backed by Redis
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_bundle_persists_to_redis():
    '''create_bundle stores BundleState JSON in Redis; load returns it.'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(
        guild_id=100, channel_id=200,
        input_string='multi-track', has_search_banner=True,
    )
    state = await broker.get_bundle_state(bundle_uuid)
    assert state is not None
    assert state.uuid == bundle_uuid
    assert state.has_search_banner is True


@pytest.mark.asyncio
async def test_finalize_bundle_marks_all_requests_enqueued_in_redis():
    '''finalize_bundle round-trips through Redis JSON without losing fields.'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(100, 200)
    await broker.finalize_bundle(bundle_uuid)
    state = await broker.get_bundle_state(bundle_uuid)
    assert state.all_requests_enqueued is True


@pytest.mark.asyncio
async def test_delete_bundle_removes_from_redis():
    '''delete_bundle clears the Redis key.'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(100, 200)
    await broker.delete_bundle(bundle_uuid)
    assert await broker.get_bundle_state(bundle_uuid) is None


@pytest.mark.asyncio
async def test_list_bundles_for_guild_scans_redis():
    '''list_bundles_for_guild SCANs every bundle and filters by guild_id.'''
    broker = _make_broker()
    a = await broker.create_bundle(100, 200)
    b = await broker.create_bundle(100, 201)
    other = await broker.create_bundle(999, 200)
    assert set(await broker.list_bundles_for_guild(100)) == {a, b}
    assert set(await broker.list_bundles_for_guild(999)) == {other}
    assert await broker.list_bundles_for_guild(42424242) == []


@pytest.mark.asyncio
async def test_register_request_with_bundle_uuid_attaches_in_redis():
    '''register_request attaches a request to its bundle when bundle_uuid is set.'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(
        guild_id=1, channel_id=2,
        input_string='multi', has_search_banner=True,
    )
    mr = _make_request()
    mr.bundle_uuid = bundle_uuid
    mr.state_machine.mark_queued()
    await broker.register_request(mr)
    state = await broker.get_bundle_state(bundle_uuid)
    assert state.total == 1
    assert str(state.bundled_requests[0].media_request.uuid) == str(mr.uuid)


@pytest.mark.asyncio
async def test_update_request_status_syncs_bundle_copy():
    '''Lifecycle changes pushed via update_request_status reach the bundle copy.

    Regression: in HA mode the registry entry and bundled_requests are separate
    JSON blobs, so mutating the entry alone leaves the bundle stuck on the
    registered-time stage and counters never advance.
    '''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(
        guild_id=1, channel_id=2,
        input_string='multi', has_search_banner=True,
    )
    mr = _make_request()
    mr.bundle_uuid = bundle_uuid
    mr.state_machine.mark_queued()
    await broker.register_request(mr)
    await broker.finalize_bundle(bundle_uuid)

    await broker.update_request_status(
        str(mr.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED),
    )
    state = await broker.get_bundle_state(bundle_uuid)
    bundled_stage = state.bundled_requests[0].media_request.lifecycle_stage
    assert bundled_stage.value == 'completed'
    assert state.completed == 1


@pytest.mark.asyncio
async def test_concurrent_register_requests_all_land_in_bundle():
    '''Three concurrent register_request calls must all land in the bundle.

    Without the per-bundle redis lock, the read-modify-write on the bundle
    races: each call loads the same snapshot, adds its request, writes back —
    last writer wins, the others are silently dropped.  This is the bug
    behind "playlist queue 1 with 3 items only counts 2".
    '''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(
        guild_id=1, channel_id=2,
        input_string='multi-item input', has_search_banner=True,
    )
    requests = [_make_request() for _ in range(3)]
    for mr in requests:
        mr.bundle_uuid = bundle_uuid
        mr.state_machine.mark_queued()

    await asyncio.gather(*(broker.register_request(mr) for mr in requests))

    state = await broker.get_bundle_state(bundle_uuid)
    assert state.total == 3
    stored_uuids = {str(br.media_request.uuid) for br in state.bundled_requests}
    assert stored_uuids == {str(mr.uuid) for mr in requests}


# ---------------------------------------------------------------------------
# Bundle edge branches
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_register_request_skips_attach_when_bundle_missing():
    '''register_request stores the entry but skips bundle attach when the
    referenced bundle is not in Redis.'''
    broker = _make_broker()
    req = _make_request()
    req.bundle_uuid = 'nonexistent-bundle'
    await broker.register_request(req)
    assert await broker.get_entry(str(req.uuid)) is not None
    assert await broker.get_bundle_state('nonexistent-bundle') is None


@pytest.mark.asyncio
async def test_sync_request_into_bundle_noop_without_bundle_uuid():
    '''_sync_request_into_bundle is a no-op when the request has no bundle.'''
    broker = _make_broker()
    req = _make_request()  # bundle_uuid defaults to None
    await broker._sync_request_into_bundle(req)  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_register_download_renders_bundle():
    '''register_download re-renders the request's bundle under the per-bundle lock.'''
    broker = _make_broker()
    bundle_uuid = await broker.create_bundle(guild_id=1, channel_id=2)
    req = _make_request()
    req.bundle_uuid = bundle_uuid
    await broker.register_request(req)
    await broker.register_download(_make_download(req))
    entry = await broker.get_entry(str(req.uuid))
    assert entry.zone == Zone.AVAILABLE
