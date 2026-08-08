'''
Unit tests for the in-process YouTube-Music search client + worker passthroughs.

These cover the InMemoryYoutubeMusicSearchClient surface and the worker edge
branches that the cog's search loop does not itself exercise (queue_size,
failure_summary, set_wait_timestamp via the client, and the no-backoff read).
'''
import asyncio

import pytest

from discord_bot.clients.youtube_music_search_client import InMemoryYoutubeMusicSearchClient
from discord_bot.workers.asyncio_youtube_music_search_worker import AsyncioYoutubeMusicSearchWorker
from discord_bot.utils.failure_queue import FailureQueue
from discord_bot.utils.integrations.youtube_music import YoutubeMusicRetryException
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.cogs.music_helpers.common import SearchType


class _StubYoutubeMusicClient:
    '''Minimal stand-in for YoutubeMusicClient: returns a fixed id or raises 429.'''
    def __init__(self, result='vid-1', raise_retry=False):
        self.result = result
        self.raise_retry = raise_retry

    def search(self, search_string):  # pylint: disable=unused-argument
        if self.raise_retry:
            raise YoutubeMusicRetryException('429 Exhaust Limit Hit')
        return self.result


def _request(guild_id=1):
    return MediaRequest(
        guild_id=guild_id,
        channel_id=2,
        requester_name='tester',
        requester_id=3,
        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string='some song'),
    )


def _client(stub=None, wait_minimum=30, wait_variance=10):
    worker = AsyncioYoutubeMusicSearchWorker(
        None,
        stub or _StubYoutubeMusicClient(),
        FailureQueue(max_size=100, max_age_seconds=300),
        wait_minimum,
        wait_variance,
        queue_max_size=10,
    )
    return InMemoryYoutubeMusicSearchClient(worker)


def test_local_worker_exposes_wrapped_engine():
    '''local_worker returns the wrapped worker (single-process escape hatch).'''
    client = _client()
    assert isinstance(client.local_worker, AsyncioYoutubeMusicSearchWorker)


@pytest.mark.asyncio
async def test_submit_get_and_queue_size_passthrough():
    '''submit enqueues, queue_size reflects it, get_input_nowait pops it.'''
    client = _client()
    request = _request()
    assert await client.queue_size(request.guild_id) == 0
    await client.submit(request.guild_id, request)
    assert await client.queue_size(request.guild_id) == 1
    assert await client.get_input_nowait() is request
    assert await client.queue_size(request.guild_id) == 0


@pytest.mark.asyncio
async def test_resolve_success_records_pass_and_returns_id():
    '''resolve returns the videoId and records a passing failure-queue entry.'''
    client = _client(_StubYoutubeMusicClient(result='abc123'))
    assert await client.resolve(_request()) == 'abc123'
    assert client.failure_summary == client.local_worker._failure_queue.get_status_summary()  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_resolve_retry_arms_backoff_and_reraises():
    '''A 429 re-raises and arms the backoff window (readable via the client).'''
    client = _client(_StubYoutubeMusicClient(raise_retry=True))
    assert client.backoff_seconds_remaining is None
    with pytest.raises(YoutubeMusicRetryException):
        await client.resolve(_request())
    assert client.backoff_seconds_remaining is not None
    assert client.backoff_seconds_remaining >= 0


@pytest.mark.asyncio
async def test_set_wait_timestamp_passthrough():
    '''set_wait_timestamp on the client arms the worker's backoff window.'''
    client = _client()
    assert client.backoff_seconds_remaining is None
    client.set_wait_timestamp(backoff_multiplier=2)
    assert client.backoff_seconds_remaining is not None


@pytest.mark.asyncio
async def test_block_and_clear_guild_passthrough():
    '''block_guild reports success once a queue exists; clear returns dropped items.'''
    client = _client()
    request = _request()
    await client.submit(request.guild_id, request)
    assert await client.block_guild(request.guild_id) is True
    result = await client.clear_guild_queue(request.guild_id)
    assert result.dropped == [request]
    assert result.preserved_bundle_uuids == set()


@pytest.mark.asyncio
async def test_backoff_wait_returns_immediately_without_timestamp():
    '''backoff_wait is a no-op when no backoff window is armed.'''
    client = _client()
    assert await client.backoff_wait(asyncio.Event()) is None


@pytest.mark.asyncio
async def test_backoff_wait_forwards_max_wait_seconds():
    '''The client passes the caller's slice cap through to the worker.'''
    client = _client()
    client.set_wait_timestamp()  # 30 s + jitter, far past the slice below

    await asyncio.wait_for(client.backoff_wait(asyncio.Event(), max_wait_seconds=0.01), timeout=5)

    # Slice elapsed but the window is untouched — still counting down.
    assert client.backoff_seconds_remaining > 0
