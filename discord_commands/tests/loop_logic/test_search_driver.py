# White-box tests reach into the extracted search worker (worker internals: the injected ytmusic
# client, the input queue, the failure queue and backoff timestamp). The driver is the search pod's;
# the pod harness only supplies the in-process broker and search doubles it runs against.
# pylint: disable=protected-access
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from discord_core.cogs.music_helpers.common import MediaRequestLifecycleStage, YOUTUBE_VIDEO_PREFIX
from discord_core.exceptions import ExitEarlyException, YoutubeMusicRetryException
from discord_core.utils.failure_queue import FailureStatus

from discord_search.workers.youtube_music_search_driver import SEARCH_BACKOFF_SLICE_SECONDS

from tests.fakes.search_pipeline_fakes import MockYoutubeMusicClient, create_test_media_request, make_broker_bundle
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod, music_settings


class RateLimitedYoutubeMusicClient:
    """Mock YouTube Music client that raises YoutubeMusicRetryException"""
    def search(self, search_string): #pylint:disable=unused-argument
        raise YoutubeMusicRetryException('429 Exhaust Limit Hit')


class FailingYoutubeMusicClient:
    """Mock YouTube Music client that raises a network error"""
    def search(self, search_string): #pylint:disable=unused-argument
        raise RuntimeError("Network error")


class TimeoutYoutubeMusicClient:
    """Mock YouTube Music client that times out"""
    def search(self, search_string): #pylint:disable=unused-argument
        raise asyncio.TimeoutError("Search timeout")


def build_pod(fake_context, fake_stores, client=None, music=None):  #pylint:disable=redefined-outer-name
    return ResultsPod(fake_context, fake_stores, music=music, with_search=True,
                      ytm_client=client or MockYoutubeMusicClient())


def worker(pod):
    return pod.youtube_music_search_client.local_worker


async def queue_request(pod, fake_context, all_added=False, retry_count=None):  #pylint:disable=redefined-outer-name
    '''A registered search request, on the search queue.'''
    media_request = create_test_media_request(fake_context, 'test search')
    if retry_count is not None:
        media_request.youtube_music_retry_information.retry_count = retry_count
    _bundle, media_request = await make_broker_bundle(pod, fake_context, request=media_request, all_added=all_added)
    worker(pod)._input_queue.put_nowait(fake_context['guild'].id, media_request)
    return media_request


@pytest.mark.asyncio()
async def test_search_youtube_music_empty_queue(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """The driver returns without error when the queue is empty"""
    pod = build_pod(fake_context, fake_stores)
    assert await pod.search_driver.run_once(pod.stop_event)


@pytest.mark.asyncio()
@pytest.mark.parametrize('client,message', [(FailingYoutubeMusicClient, 'Network error'),
                                            (TimeoutYoutubeMusicClient, 'Search timeout')])
async def test_search_client_failure_is_handled_or_propagates_unchanged(client, message, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A failing ytmusic client does not crash the iteration; if it propagates it is the client's error"""
    pod = build_pod(fake_context, fake_stores, client())
    await queue_request(pod, fake_context)
    try:
        await pod.search_driver.run_once(pod.stop_event)
    except Exception as error:  #pylint:disable=broad-exception-caught
        assert message in str(error)


@pytest.mark.asyncio()
async def test_search_youtube_music_429_requeues_item(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A 429 re-enqueues the item and sets RETRY_SEARCH lifecycle stage"""
    mocker.patch('discord_core.interfaces.youtube_music_search_protocols.randint', return_value=5000)
    pod = build_pod(fake_context, fake_stores, RateLimitedYoutubeMusicClient())
    media_request = await queue_request(pod, fake_context, all_added=True)

    result = await pod.search_driver.run_once(pod.stop_event)

    assert result is False
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.RETRY_SEARCH
    assert media_request.youtube_music_retry_information.retry_count == 1
    assert media_request.youtube_music_retry_information.retry_reason is not None
    assert worker(pod)._input_queue.size(fake_context['guild'].id) == 1
    assert await pod.download_client.queue_size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
@pytest.mark.freeze_time
async def test_search_youtube_music_429_sets_backoff_timestamp(freezer, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A 429 sets the youtube_music_wait_timestamp with exponential backoff"""
    mocker.patch('discord_core.interfaces.youtube_music_search_protocols.randint', return_value=5000)
    pod = build_pod(fake_context, fake_stores, RateLimitedYoutubeMusicClient())
    await queue_request(pod, fake_context, all_added=True)
    freezer.move_to('2025-01-01 12:00:00 UTC')
    assert worker(pod)._wait_timestamp is None

    await pod.search_driver.run_once(pod.stop_event)

    # Failure queue size is 1, so multiplier is 2^1 = 2
    # Expected: now (1735732800) + 30*2 + 5 = 1735732865
    assert worker(pod)._wait_timestamp == 1735732865
    assert worker(pod)._failure_queue.size == 1


@pytest.mark.asyncio()
@pytest.mark.freeze_time
async def test_search_youtube_music_429_exponential_backoff_growth(freezer, mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Repeated 429s grow the backoff exponentially"""
    mocker.patch('discord_core.interfaces.youtube_music_search_protocols.randint', return_value=5000)
    pod = build_pod(fake_context, fake_stores, RateLimitedYoutubeMusicClient())
    for _ in range(2):
        worker(pod)._failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    assert worker(pod)._failure_queue.size == 2
    await queue_request(pod, fake_context, all_added=True)
    freezer.move_to('2025-01-01 12:00:00 UTC')

    await pod.search_driver.run_once(pod.stop_event)

    # Failure queue size is now 3, so multiplier is 2^3 = 8
    # Expected: now (1735732800) + 30*8 + 5 = 1735733045
    assert worker(pod)._failure_queue.size == 3
    assert worker(pod)._wait_timestamp == 1735733045


@pytest.mark.asyncio()
async def test_search_youtube_music_429_retry_limit_exceeded(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """Hitting max retries marks the request as FAILED instead of re-queuing"""
    mocker.patch('discord_core.interfaces.youtube_music_search_protocols.randint', return_value=5000)
    pod = build_pod(fake_context, fake_stores, RateLimitedYoutubeMusicClient(),
                    music_settings({'download': {'max_youtube_music_search_retries': 3}}))
    media_request = await queue_request(pod, fake_context, all_added=True, retry_count=2)

    await pod.search_driver.run_once(pod.stop_event)

    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.FAILED
    assert media_request.failure_reason is not None
    assert worker(pod)._input_queue.size(fake_context['guild'].id) == 0


@pytest.mark.asyncio()
async def test_search_youtube_music_success_clears_failure_queue(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """A successful search adds a success to the failure queue"""
    pod = build_pod(fake_context, fake_stores, MockYoutubeMusicClient('test-video-id'))
    for _ in range(2):
        worker(pod)._failure_queue.add_item(FailureStatus(success=False, exception_type='YoutubeMusicRetryException'))
    assert worker(pod)._failure_queue.size == 2
    await queue_request(pod, fake_context)
    mocker.patch.object(pod.broker_client, 'check_cache', new=AsyncMock(return_value=None))

    await pod.search_driver.run_once(pod.stop_event)

    assert worker(pod)._failure_queue.size == 1


# ---------------------------------------------------------------------------
# backoff_wait
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_youtube_backoff_time_expired_returns_true(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """backoff_wait returns immediately when the timestamp is in the past."""
    pod = build_pod(fake_context, fake_stores)
    worker(pod)._wait_timestamp = datetime.now(timezone.utc).timestamp() - 100
    assert await worker(pod).backoff_wait(pod.stop_event) is None


@pytest.mark.asyncio
async def test_youtube_backoff_time_shutdown_raises(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """backoff_wait raises ExitEarlyException when shutdown is set."""
    pod = build_pod(fake_context, fake_stores)
    worker(pod)._wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600
    pod.stop_event.set()
    with pytest.raises(ExitEarlyException):
        await worker(pod).backoff_wait(pod.stop_event)


@pytest.mark.asyncio
async def test_youtube_backoff_time_waits_until_timeout(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """backoff_wait waits for the event and returns when it times out."""
    pod = build_pod(fake_context, fake_stores)
    worker(pod)._wait_timestamp = datetime.now(timezone.utc).timestamp() + 0.05
    assert await worker(pod).backoff_wait(pod.stop_event) is None


@pytest.mark.asyncio
async def test_youtube_backoff_time_raises_when_event_set_during_wait(fake_context, fake_stores):  # pylint: disable=redefined-outer-name
    """backoff_wait raises ExitEarlyException when shutdown is set during the wait."""
    pod = build_pod(fake_context, fake_stores)
    worker(pod)._wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600

    async def _set_event_soon():
        await asyncio.sleep(0.02)
        pod.stop_event.set()

    stopper = asyncio.ensure_future(_set_event_soon())
    with pytest.raises(ExitEarlyException):
        await worker(pod).backoff_wait(pod.stop_event)
    await stopper


@pytest.mark.asyncio()
async def test_search_youtube_music_waits_in_slices_before_popping(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """An open backoff window ends the iteration WITHOUT popping a request.

    Popping first would hold the request in pod memory for the whole window (and
    the Redis-backed worker DELetes on pop, so a restart would lose it), while a
    single uninterrupted sleep past the loop-health staleness window would read
    as a wedge and get the pod restarted over a rate limit.
    """
    pod = build_pod(fake_context, fake_stores, MockYoutubeMusicClient('test-video-id'))
    media_request = await queue_request(pod, fake_context)
    # Window well past one slice; stub the sleep itself so the test doesn't wait.
    worker(pod)._wait_timestamp = datetime.now(timezone.utc).timestamp() + 3600
    backoff_mock = mocker.patch.object(worker(pod), 'backoff_wait', new=AsyncMock())

    assert await pod.search_driver.run_once(pod.stop_event) is True

    # Waited one slice, then returned: nothing popped, nothing resolved.
    backoff_mock.assert_awaited_once_with(pod.stop_event, max_wait_seconds=SEARCH_BACKOFF_SLICE_SECONDS)
    assert worker(pod)._input_queue.size(fake_context['guild'].id) == 1
    assert media_request.lifecycle_stage == MediaRequestLifecycleStage.SEARCHING


@pytest.mark.asyncio()
async def test_search_youtube_music_pops_once_backoff_window_clears(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    """With the window elapsed the same iteration pops and resolves as usual."""
    pod = build_pod(fake_context, fake_stores, MockYoutubeMusicClient('test-video-id'))
    media_request = await queue_request(pod, fake_context)
    # An elapsed window reads as 0 seconds remaining, not None.
    worker(pod)._wait_timestamp = datetime.now(timezone.utc).timestamp() - 5

    assert await pod.search_driver.run_once(pod.stop_event) is True

    assert worker(pod)._input_queue.size(fake_context['guild'].id) == 0
    assert media_request.search_result.youtube_music_search_string == f'{YOUTUBE_VIDEO_PREFIX}test-video-id'
