'''
Tests for the InMemoryDownloadClient wrapper.

The wrapper is a thin delegator around a DownloadWorkerBase engine
(AsyncioDownloadWorker in single-process); these tests assert every method and
property forwards to the wrapped worker, mirroring tests/clients/test_broker_client.py.
'''
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.clients.download_client import InMemoryDownloadClient
from discord_bot.interfaces.download_protocols import ClearGuildResult
from discord_bot.workers.asyncio_download_worker import AsyncioDownloadWorker


def _make_worker() -> AsyncioDownloadWorker:
    return AsyncioDownloadWorker(None, Path('/tmp'))


def test_local_worker_exposes_wrapped_engine():
    worker = _make_worker()
    client = InMemoryDownloadClient(worker)
    assert client.local_worker is worker


@pytest.mark.asyncio
async def test_submit_delegates():
    worker = _make_worker()
    worker.submit = AsyncMock()
    client = InMemoryDownloadClient(worker)
    media_request = MagicMock()
    await client.submit(42, media_request, priority=7)
    worker.submit.assert_called_once_with(42, media_request, priority=7)


@pytest.mark.asyncio
async def test_block_guild_delegates():
    worker = _make_worker()
    worker.block_guild = AsyncMock(return_value=True)
    client = InMemoryDownloadClient(worker)
    assert await client.block_guild(42) is True
    worker.block_guild.assert_called_once_with(42)


@pytest.mark.asyncio
async def test_clear_guild_queue_no_predicate_returns_result_with_empty_preserved():
    worker = _make_worker()
    dropped = [MagicMock()]
    worker.clear_guild_queue = AsyncMock(return_value=dropped)
    client = InMemoryDownloadClient(worker)
    result = await client.clear_guild_queue(42)
    assert isinstance(result, ClearGuildResult)
    assert result.dropped is dropped
    assert result.preserved_bundle_uuids == set()
    # No predicate supplied → worker is called with None (no wrapper).
    worker.clear_guild_queue.assert_called_once_with(42, preserve_predicate=None)


@pytest.mark.asyncio
async def test_clear_guild_queue_collects_preserved_bundle_uuids():
    '''The wrapper records the bundle_uuids of items the predicate keeps, dropping
    the rest — so the result carries preserved uuids even for the in-process worker.'''
    download_item = MagicMock(download_file=True, bundle_uuid='drop-bundle')
    playlist_add = MagicMock(download_file=False, bundle_uuid='keep-bundle')
    no_bundle_add = MagicMock(download_file=False, bundle_uuid=None)

    async def fake_clear(_guild_id, preserve_predicate=None):
        # Mirror a real worker: evaluate the predicate over every queued item and
        # return only the ones it does NOT keep.
        return [it for it in (download_item, playlist_add, no_bundle_add)
                if preserve_predicate is None or not preserve_predicate(it)]

    worker = _make_worker()
    worker.clear_guild_queue = AsyncMock(side_effect=fake_clear)
    client = InMemoryDownloadClient(worker)
    result = await client.clear_guild_queue(42, preserve_predicate=lambda r: not r.download_file)
    assert result.dropped == [download_item]
    assert result.preserved_bundle_uuids == {'keep-bundle'}
    # The worker receives the wrapper, not the raw predicate.
    _, kwargs = worker.clear_guild_queue.call_args
    assert kwargs['preserve_predicate'] is not None


@pytest.mark.asyncio
async def test_queue_size_delegates():
    worker = _make_worker()
    worker.queue_size = AsyncMock(return_value=3)
    client = InMemoryDownloadClient(worker)
    assert await client.queue_size(42) == 3
    worker.queue_size.assert_called_once_with(42)


def test_failure_summary_delegates():
    worker = _make_worker()
    client = InMemoryDownloadClient(worker)
    assert client.failure_summary == worker.failure_summary


def test_backoff_seconds_remaining_delegates():
    worker = _make_worker()
    client = InMemoryDownloadClient(worker)
    assert client.backoff_seconds_remaining is None
    worker.set_wait_timestamp()
    assert client.backoff_seconds_remaining == worker.backoff_seconds_remaining


@pytest.mark.asyncio
async def test_run_delegates():
    worker = _make_worker()
    worker.run = AsyncMock()
    client = InMemoryDownloadClient(worker)
    shutdown_event = MagicMock()
    await client.run(shutdown_event)
    worker.run.assert_awaited_once_with(shutdown_event)
