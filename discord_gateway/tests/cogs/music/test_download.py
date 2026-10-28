import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from discord_downloader.interfaces import download_protocols

from tests.fakes.asyncio_download_worker import AsyncioDownloadWorker


@pytest.mark.asyncio()
async def test_run_idle_empty_queue_backs_off(mocker):
    """run() sleeps the idle backoff (not every iteration) when both input queues
    are empty and no backoff is active — cutting idle busy-loop churn."""
    sleep_mock = mocker.patch('discord_downloader.interfaces.download_protocols.sleep')
    worker = AsyncioDownloadWorker(None, Path('/tmp'))
    assert worker.backoff_seconds_remaining is None  # else-branch (no backoff)
    await worker.run(asyncio.Event())
    sleep_mock.assert_awaited_once_with(download_protocols._IDLE_POLL_BACKOFF_SECONDS)  # pylint: disable=protected-access


@pytest.mark.asyncio()
async def test_run_idle_backoff_active_empty_queue_backs_off(mocker):
    """With backoff active but nothing queued, run() reaches the merged-empty path
    after the backoff wait and applies the idle backoff sleep before returning."""
    sleep_mock = mocker.patch('discord_downloader.interfaces.download_protocols.sleep')
    worker = AsyncioDownloadWorker(None, Path('/tmp'))
    # Future timestamp → backoff_seconds_remaining truthy → backoff branch taken.
    worker.wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600
    assert worker.backoff_seconds_remaining
    # backoff_wait has its own dedicated tests; stub it to "elapsed, no DIRECT item"
    # so control falls through to the real (empty) merged_get_nowait idle path.
    mocker.patch.object(worker, 'backoff_wait', new=AsyncMock(return_value=None))
    await worker.run(asyncio.Event())
    sleep_mock.assert_awaited_once_with(download_protocols._IDLE_POLL_BACKOFF_SECONDS)  # pylint: disable=protected-access
