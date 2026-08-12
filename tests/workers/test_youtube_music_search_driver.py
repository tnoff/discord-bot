'''
Tests for the shared YouTube-Music search loop body.

The driver is the one copy of the loop the cog and the search pod both run, so
these tests drive it directly against fakes rather than through either caller —
the cog-side integration is covered by tests/cogs/music/test_youtube_search_queue.py
and the pod-side wiring by tests/cli/test_search.py.
'''
import asyncio
from asyncio import QueueEmpty

import pytest

from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.download import LifecycleEvent
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.utils.integrations.common import YOUTUBE_VIDEO_PREFIX
from discord_bot.utils.integrations.youtube_music import YoutubeMusicRetryException
from discord_bot.workers.youtube_music_search_driver import YoutubeMusicSearchDriver


def _media_request(guild_id=1234, search_string='test search') -> MediaRequest:
    '''Minimal MediaRequest for the search path.'''
    return MediaRequest(
        guild_id=guild_id,
        channel_id=4321,
        requester_name='tester',
        requester_id=99,
        search_result=SearchResult(search_type=SearchType.SEARCH,
                                   raw_search_string=search_string),
    )


class FakeSearchClient:
    '''Pop/resolve half of the search surface, scripted per test.'''
    def __init__(self, *, queued=None, resolve_result='video-id', resolve_error=None,
                 backoff_remaining=None, backoff_after_error=None):
        self.queued = list(queued or [])
        self.resolve_result = resolve_result
        self.resolve_error = resolve_error
        # Pre-armed window (a slice left over from an earlier iteration).
        self.backoff_seconds_remaining = backoff_remaining
        # What resolve() arms on a 429, mirroring the real worker: the window is a
        # side effect of the failing resolve, not a precondition of it.
        self.backoff_after_error = backoff_after_error
        self.backoff_waits = []
        self.submitted = []
        self.resolved = []

    async def backoff_wait(self, shutdown_event, max_wait_seconds=None):
        '''Record the slice cap the driver asked for.'''
        self.backoff_waits.append((shutdown_event, max_wait_seconds))

    async def get_input_nowait(self):
        '''Pop the next scripted request, or raise QueueEmpty.'''
        if not self.queued:
            raise QueueEmpty('empty')
        return self.queued.pop(0)

    async def resolve(self, media_request):
        '''Return the scripted videoId, or raise the scripted 429.'''
        self.resolved.append(media_request)
        if self.resolve_error:
            self.backoff_seconds_remaining = self.backoff_after_error
            raise self.resolve_error
        return self.resolve_result

    async def submit(self, guild_id, media_request, priority=None):
        '''Record a re-enqueue.'''
        self.submitted.append((guild_id, media_request, priority))


class FakeBroker:
    '''Captures the lifecycle pushes and the finished resolutions.'''
    def __init__(self):
        self.status_updates = []
        self.search_results = []

    async def update_request_status(self, uuid, update):
        '''Record a lifecycle transition.'''
        self.status_updates.append((uuid, update))

    async def register_search_result(self, resolution):
        '''Record a completed resolution.'''
        self.search_results.append(resolution)


def _driver(search_client, broker, mocker, **kwargs):
    '''Build a driver with a mock logger.'''
    return YoutubeMusicSearchDriver(search_client, broker, mocker.Mock(), **kwargs)


@pytest.mark.asyncio
async def test_run_once_resolves_and_hands_back_to_broker(mocker):
    '''The happy path stamps the videoId and registers a SearchResolution.'''
    request = _media_request()
    client = FakeSearchClient(queued=[request])
    broker = FakeBroker()
    driver = _driver(client, broker, mocker)

    assert await driver.run_once(asyncio.Event()) is True

    assert client.resolved == [request]
    assert len(broker.search_results) == 1
    resolution = broker.search_results[0]
    assert resolution.media_request.uuid == request.uuid
    assert resolution.media_request.search_result.youtube_music_search_string == \
        f'{YOUTUBE_VIDEO_PREFIX}video-id'
    # A resolved search is not a lifecycle transition — the bot-side tail owns
    # what happens next.
    assert not broker.status_updates


@pytest.mark.asyncio
async def test_run_once_handles_no_match(mocker):
    '''A None resolution still hands the request back, without a videoId stamp.'''
    request = _media_request()
    client = FakeSearchClient(queued=[request], resolve_result=None)
    broker = FakeBroker()

    assert await _driver(client, broker, mocker).run_once(asyncio.Event()) is True

    assert len(broker.search_results) == 1
    assert broker.search_results[0].media_request.search_result.youtube_music_search_string is None


@pytest.mark.asyncio
async def test_run_once_waits_a_backoff_slice_before_popping(mocker):
    '''
    An open 429 window returns after ONE slice without popping.

    Popping first would hold the request in this process's memory for the whole
    window, and the Redis queue DELetes on pop — a restart mid-wait loses it.
    '''
    request = _media_request()
    client = FakeSearchClient(queued=[request], backoff_remaining=90)
    broker = FakeBroker()
    driver = _driver(client, broker, mocker, backoff_slice_seconds=30.0)
    stop_event = asyncio.Event()

    assert await driver.run_once(stop_event) is True

    assert client.backoff_waits == [(stop_event, 30.0)]
    # Nothing popped or resolved: the request is still queued.
    assert client.queued == [request]
    assert not client.resolved
    assert not broker.search_results


@pytest.mark.asyncio
async def test_run_once_idle_sleeps_when_queue_empty(mocker):
    '''An empty queue sleeps the idle backoff and reports a completed iteration.'''
    client = FakeSearchClient(queued=[])
    broker = FakeBroker()
    sleep_mock = mocker.patch('discord_bot.workers.youtube_music_search_driver.asyncio.sleep',
                              new=mocker.AsyncMock())
    driver = _driver(client, broker, mocker, idle_sleep_seconds=0.25)

    assert await driver.run_once(asyncio.Event()) is True

    sleep_mock.assert_awaited_once_with(0.25)
    assert not broker.search_results


@pytest.mark.asyncio
async def test_run_once_retries_a_rate_limited_request(mocker):
    '''
    A 429 under the retry budget re-enqueues the request and pushes RETRY_SEARCH.

    The re-enqueue carries the guild's configured priority — a retry that dropped
    to the default bucket would quietly demote a prioritised guild's searches.
    '''
    request = _media_request(guild_id=42)
    client = FakeSearchClient(queued=[request],
                              resolve_error=YoutubeMusicRetryException('rate limited'),
                              backoff_after_error=60)
    broker = FakeBroker()
    driver = _driver(client, broker, mocker, max_retries=3, queue_priority={42: 5})

    assert await driver.run_once(asyncio.Event()) is False

    assert client.submitted == [(42, request, 5)]
    assert request.youtube_music_retry_information.retry_count == 1
    assert len(broker.status_updates) == 1
    _, update = broker.status_updates[0]
    assert update.event == LifecycleEvent.RETRY_SEARCH
    assert update.backoff_seconds == 60
    assert update.retry_count == 1


@pytest.mark.asyncio
async def test_run_once_fails_request_past_retry_budget(mocker):
    '''The last retry fails the request instead of re-enqueueing it forever.'''
    request = _media_request()
    request.youtube_music_retry_information.retry_count = 2
    client = FakeSearchClient(queued=[request],
                              resolve_error=YoutubeMusicRetryException('rate limited'),
                              backoff_after_error=60)
    broker = FakeBroker()
    driver = _driver(client, broker, mocker, max_retries=3)

    assert await driver.run_once(asyncio.Event()) is False

    assert not client.submitted
    _, update = broker.status_updates[0]
    assert update.event == LifecycleEvent.FAILED
    assert 'rate limit exceeded' in update.failure_reason


@pytest.mark.asyncio
async def test_run_once_retry_without_backoff_window(mocker):
    '''A 429 that left no readable window still retries (backoff_seconds unset).'''
    request = _media_request()
    client = FakeSearchClient(queued=[request],
                              resolve_error=YoutubeMusicRetryException('rate limited'),
                              backoff_after_error=None)
    broker = FakeBroker()
    driver = _driver(client, broker, mocker, max_retries=3)

    assert await driver.run_once(asyncio.Event()) is False

    assert len(client.submitted) == 1
    _, update = broker.status_updates[0]
    assert update.event == LifecycleEvent.RETRY_SEARCH


@pytest.mark.asyncio
async def test_run_once_retry_defaults_priority_to_none(mocker):
    '''An unprioritised guild re-enqueues with priority=None (the default bucket).'''
    request = _media_request(guild_id=7)
    client = FakeSearchClient(queued=[request],
                              resolve_error=YoutubeMusicRetryException('rate limited'))
    broker = FakeBroker()
    driver = _driver(client, broker, mocker, max_retries=3, queue_priority={42: 5})

    await driver.run_once(asyncio.Event())

    assert client.submitted == [(7, request, None)]
