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
from discord_bot.workers.asyncio_download_worker import AsyncioDownloadWorker


def _make_worker() -> AsyncioDownloadWorker:
    return AsyncioDownloadWorker(None, Path('/tmp'))


def test_local_worker_exposes_wrapped_engine():
    worker = _make_worker()
    client = InMemoryDownloadClient(worker)
    assert client.local_worker is worker


def test_submit_delegates():
    worker = _make_worker()
    worker.submit = MagicMock()
    client = InMemoryDownloadClient(worker)
    media_request = MagicMock()
    client.submit(42, media_request, priority=7)
    worker.submit.assert_called_once_with(42, media_request, priority=7)


def test_block_guild_delegates():
    worker = _make_worker()
    worker.block_guild = MagicMock(return_value=True)
    client = InMemoryDownloadClient(worker)
    assert client.block_guild(42) is True
    worker.block_guild.assert_called_once_with(42)


def test_clear_guild_queue_delegates():
    worker = _make_worker()
    dropped = [MagicMock()]
    worker.clear_guild_queue = MagicMock(return_value=dropped)
    client = InMemoryDownloadClient(worker)
    predicate = MagicMock()
    assert client.clear_guild_queue(42, preserve_predicate=predicate) is dropped
    worker.clear_guild_queue.assert_called_once_with(42, preserve_predicate=predicate)


def test_queue_size_delegates():
    worker = _make_worker()
    worker.queue_size = MagicMock(return_value=3)
    client = InMemoryDownloadClient(worker)
    assert client.queue_size(42) == 3
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
