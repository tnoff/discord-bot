'''Evicting a cached object a player found does not match the digest in its key.'''
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager
from discord_core.utils.integrations.s3 import ObjectDigestMismatch, ObjectStorageException

from discord_broker.workers.broker_registry import RedisBrokerRegistry
from discord_broker.workers.corrupt_object import evict_corrupt_object
from discord_broker.workers.redis_broker import RedisBroker

from tests.fakes.worker_requests import direct_request


def _make_broker(video_cache=None, bucket_name=None, cache_bucket_name=None) -> RedisBroker:
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisBroker(RedisBrokerRegistry(RedisManager.from_client(client)), video_cache=video_cache,
                       bucket_name=bucket_name, cache_bucket_name=cache_bucket_name)



BAD_KEY = 'cache/youtube.abc.sha256-0123456789abcdef0123456789abcdef.pcm'
CORRUPT = 'discord_broker.workers.corrupt_object'


def _catalog_holding(key: str | None) -> MagicMock:
    """A catalog whose lookup for the request returns a hit on `key` (or a miss for None)."""
    video_cache = MagicMock()
    cached = MagicMock(file_path=Path(key)) if key else None
    video_cache.get_webpage_url_item = AsyncMock(return_value=cached)
    video_cache.remove_video_cache = AsyncMock()
    return video_cache


@pytest.mark.asyncio
async def test_a_corrupt_object_is_evicted_when_the_broker_reads_it_wrong_too():
    """The row goes first, then the object, in the bucket the key lives in."""
    video_cache = _catalog_holding(BAD_KEY)
    # The first lookup finds the row; once it is removed the second finds nothing.
    video_cache.get_webpage_url_item.side_effect = [MagicMock(file_path=Path(BAD_KEY)), None]
    broker = _make_broker(video_cache=video_cache, bucket_name='queue-bucket', cache_bucket_name='cache-bucket')
    calls = []
    video_cache.remove_video_cache.side_effect = lambda *a, **k: calls.append(('row', a, k))
    with patch(f'{CORRUPT}.get_file', side_effect=ObjectDigestMismatch('bad')) as get_file, \
            patch(f'{CORRUPT}.delete_file', side_effect=lambda b, k: calls.append(('object', b, k))):
        assert await evict_corrupt_object(broker, direct_request(), BAD_KEY) is True
    assert get_file.call_args.args[:2] == ('cache-bucket', BAD_KEY)
    assert calls == [('row', ([],), {'base_paths': [BAD_KEY]}), ('object', 'cache-bucket', BAD_KEY)]


@pytest.mark.asyncio
async def test_a_healthy_object_is_kept_when_the_broker_reads_it_fine():
    """One read cut short in transit must not cost a good song."""
    video_cache = _catalog_holding(BAD_KEY)
    broker = _make_broker(video_cache=video_cache, bucket_name='queue-bucket', cache_bucket_name='cache-bucket')
    with patch(f'{CORRUPT}.get_file', return_value=True), \
            patch(f'{CORRUPT}.delete_file') as delete_file:
        assert await evict_corrupt_object(broker, direct_request(), BAD_KEY) is False
    delete_file.assert_not_called()
    video_cache.remove_video_cache.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_object_the_broker_cannot_read_is_left_alone():
    """A failed read says nothing about the bytes, so it is not grounds to delete."""
    video_cache = _catalog_holding(BAD_KEY)
    broker = _make_broker(video_cache=video_cache, bucket_name='queue-bucket', cache_bucket_name='cache-bucket')
    with patch(f'{CORRUPT}.get_file', side_effect=ObjectStorageException('timeout')), \
            patch(f'{CORRUPT}.delete_file') as delete_file:
        assert await evict_corrupt_object(broker, direct_request(), BAD_KEY) is False
    delete_file.assert_not_called()
    video_cache.remove_video_cache.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('held', [None, 'cache/youtube.abc.sha256-ffffffffffffffffffffffffffffffff.pcm'])
async def test_a_report_for_an_object_the_catalog_does_not_hold_is_ignored(held):
    """Already evicted, or replaced by another copy of the video: nothing to do, and no read."""
    video_cache = _catalog_holding(held)
    broker = _make_broker(video_cache=video_cache, bucket_name='queue-bucket', cache_bucket_name='cache-bucket')
    with patch(f'{CORRUPT}.get_file') as get_file, patch(f'{CORRUPT}.delete_file') as delete_file:
        assert await evict_corrupt_object(broker, direct_request(), BAD_KEY) is False
    get_file.assert_not_called()
    delete_file.assert_not_called()


@pytest.mark.asyncio
async def test_a_key_without_a_digest_cannot_be_reported_corrupt():
    """Old keys carry no digest, so there is nothing to have failed."""
    video_cache = _catalog_holding('cache/youtube.abc.pcm')
    broker = _make_broker(video_cache=video_cache, bucket_name='queue-bucket', cache_bucket_name='cache-bucket')
    assert await evict_corrupt_object(broker, direct_request(), 'cache/youtube.abc.pcm') is False
    video_cache.get_webpage_url_item.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_report_without_a_catalog_is_ignored():
    assert await evict_corrupt_object(_make_broker(), direct_request(), BAD_KEY) is False


@pytest.mark.asyncio
async def test_the_object_is_kept_when_the_catalog_did_not_drop_the_row():
    """A db pod that predates base_paths ignores it; deleting the object would strand a cache hit."""
    video_cache = _catalog_holding(BAD_KEY)
    broker = _make_broker(video_cache=video_cache, bucket_name='queue-bucket', cache_bucket_name='cache-bucket')
    with patch(f'{CORRUPT}.get_file', side_effect=ObjectDigestMismatch('bad')), \
            patch(f'{CORRUPT}.delete_file') as delete_file:
        assert await evict_corrupt_object(broker, direct_request(), BAD_KEY) is False
    video_cache.remove_video_cache.assert_awaited_once()
    delete_file.assert_not_called()
