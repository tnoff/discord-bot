from tempfile import TemporaryDirectory

import pytest

from discord_core.utils.failure_queue import FailureStatus

from tests.fakes.pipeline_fakes import (yield_download_worker_bot_flagged,
                                        yield_download_worker_retryable_exception,
                                        yield_fake_download_worker)
from tests.helpers import fake_media_download, fake_source_dict
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod


@pytest.fixture(autouse=True)
def no_download_sleeps(mocker):
    '''The backoff is computed from the clock; the waits themselves are not under test.'''
    mocker.patch('discord_downloader.interfaces.download_protocols.random.randint', return_value=5000)
    mocker.patch('discord_downloader.interfaces.download_protocols.sleep', return_value=None)


async def fail_one(pod, fake_context):  #pylint:disable=redefined-outer-name
    '''Queue one request for the guild, download it and run one pass of the download-result loop.'''
    guild_id = fake_context['guild'].id
    await pod.open_player(guild_id, fake_context['channel'].id)
    await pod.download_client.submit(guild_id, fake_source_dict(fake_context))
    await pod.download_client.run(pod.stop_event)
    await pod.loops.process_download_results()


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_retryable_exception_adds_failure_to_queue(freezer, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that RetryableException adds a failure to the download failure queue"""
    pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_download_worker_retryable_exception())
    freezer.move_to('2025-01-01 12:00:00 UTC')
    assert pod.download_client.local_worker.failure_queue.size == 0

    await fail_one(pod, fake_context)

    assert pod.download_client.local_worker.failure_queue.size == 1


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_retryable_exception_applies_exponential_backoff(freezer, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that RetryableException applies exponential backoff based on failure queue size"""
    pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_download_worker_retryable_exception())
    freezer.move_to('2025-01-01 12:00:00 UTC')

    # Pre-populate failure queue with 2 failures to test exponential backoff
    failure_queue = pod.download_client.local_worker.failure_queue
    failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
    failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
    assert failure_queue.size == 2

    await fail_one(pod, fake_context)

    # After failure, queue size is now 3
    assert failure_queue.size == 3

    # Backoff multiplier should be 2^3 = 8 (since size is 3 after adding new failure)
    # Expected timestamp: now (1735732800) + 30*8 + 5 = 1735733045
    assert pod.download_client.local_worker.wait_timestamp == 1735733045


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_bot_download_flagged_applies_backoff(freezer, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that BotDownloadFlagged (a RetryableException) applies proper backoff"""
    pod = ResultsPod(fake_context, fake_stores,
                     download_worker_cls=yield_download_worker_bot_flagged('Bot download flagged'))
    freezer.move_to('2025-01-01 12:00:00 UTC')
    assert pod.download_client.local_worker.wait_timestamp is None

    await fail_one(pod, fake_context)

    # After BotDownloadFlagged, failure queue size is 1, so multiplier is 2^1 = 2
    # Expected timestamp: now (1735732800) + 30*2 + 5 = 1735732865
    assert pod.download_client.local_worker.wait_timestamp == 1735732865
    assert pod.download_client.local_worker.failure_queue.size == 1


@pytest.mark.asyncio
@pytest.mark.freeze_time
async def test_successful_download_clears_failure_from_queue(freezer, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Test that successful download removes one item from failure queue"""
    with TemporaryDirectory() as tmp_dir:
        with fake_media_download(tmp_dir, fake_context=fake_context) as sd:
            pod = ResultsPod(fake_context, fake_stores, download_worker_cls=yield_fake_download_worker(sd))
            freezer.move_to('2025-01-01 12:00:00 UTC')

            # Pre-populate failure queue
            failure_queue = pod.download_client.local_worker.failure_queue
            failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
            failure_queue.add_item(FailureStatus(success=False, exception_type='RetryableException'))
            assert failure_queue.size == 2

            guild_id = fake_context['guild'].id
            await pod.open_player(guild_id, fake_context['channel'].id)
            await pod.download_client.submit(guild_id, sd.media_request)
            await pod.download_client.run(pod.stop_event)
            await pod.loops.process_download_results()

            # Successful download should remove one failure from queue
            assert failure_queue.size == 1
