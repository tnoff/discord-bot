'''Tests for the YouTube-Music search engine base and its cog-facing Protocols.'''
import asyncio
from datetime import datetime, timezone

import pytest

from discord_core.exceptions import ExitEarlyException, YoutubeMusicRetryException
from discord_core.interfaces.youtube_music_search_protocols import (
    YoutubeMusicSearchClient,
    YoutubeMusicSearchWorkerBase,
)
from discord_core.types.clear_guild_result import ClearGuildResult
from discord_core.types.media_request import MediaRequest
from discord_core.types.search import SearchResult, SearchType
from discord_core.utils.common import LoggingConfig
from discord_core.utils.failure_queue import FailureQueue


class ScriptedYoutubeClient:
    '''A ytmusic client double that returns or raises what it was told to.'''

    def __init__(self, outcomes):
        '''Hold the scripted outcomes and the strings searched for.'''
        self.outcomes = list(outcomes)
        self.searched = []

    def search(self, search_string: str) -> str:
        '''Record the query and return or raise the next outcome.'''
        self.searched.append(search_string)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class ListQueueWorker(YoutubeMusicSearchWorkerBase):
    '''Minimal concrete worker backed by a plain list.'''

    def __init__(self, *args, **kwargs):
        '''Add the list queue to the base state.'''
        super().__init__(*args, **kwargs)
        self.queue = []

    def expire_window(self):
        '''Put the backoff deadline five seconds in the past.'''
        self._wait_timestamp = datetime.now(timezone.utc).timestamp() - 5

    async def _enqueue(self, guild_id, media_request, priority=None):
        '''Append the request with its guild and priority.'''
        self.queue.append((guild_id, media_request, priority))

    async def get_input_nowait(self):
        '''Pop the oldest request or raise QueueEmpty.'''
        if not self.queue:
            raise asyncio.QueueEmpty()
        return self.queue.pop(0)[1]

    async def block_guild(self, guild_id):
        '''Pretend to block.'''
        return guild_id > 0

    async def clear_guild_queue(self, guild_id, preserve_predicate=None):
        '''Drop everything for the guild.'''
        dropped = [item[1] for item in self.queue if item[0] == guild_id]
        self.queue = [item for item in self.queue if item[0] != guild_id]
        return dropped

    async def queue_size(self, guild_id):
        '''Count the guild's pending requests.'''
        return len([item for item in self.queue if item[0] == guild_id])


def make_request(text='some song') -> MediaRequest:
    '''Build a search request for the given text.'''
    return MediaRequest(guild_id=1, channel_id=2, requester_name='n', requester_id=3,
                        search_result=SearchResult(search_type=SearchType.SEARCH, raw_search_string=text))


def make_worker(outcomes, minimum=10, variance=2):
    '''Build a worker around a scripted client; returns (worker, client, failures).'''
    client = ScriptedYoutubeClient(outcomes)
    failures = FailureQueue()
    return ListQueueWorker(LoggingConfig(log_level=10, otlp_only=True), client, failures, minimum, variance), client, failures


@pytest.mark.asyncio
async def test_resolve_returns_the_video_id_and_searches_the_raw_string():
    '''A good lookup returns the id the client found and has no backoff armed'''
    worker, client, failures = make_worker(['vid123'])
    assert await worker.resolve(make_request('daft punk')) == 'vid123'
    assert client.searched == ['daft punk']
    assert worker.backoff_seconds_remaining is None
    assert worker.failure_summary == failures.get_status_summary()


@pytest.mark.asyncio
async def test_resolve_reraises_a_429_and_arms_the_backoff_window():
    '''A retry exception is recorded, backoff is armed, and the error propagates'''
    worker, _, failures = make_worker([YoutubeMusicRetryException('429')])
    with pytest.raises(YoutubeMusicRetryException):
        await worker.resolve(make_request())
    assert failures.size == 1
    # one failure doubles the 10 second minimum, plus 1-2 seconds of jitter
    assert 20 < worker.backoff_seconds_remaining <= 23


@pytest.mark.asyncio
async def test_submit_goes_through_the_enqueue_hook_with_priority():
    '''submit forwards guild, request and priority to the subclass hook'''
    worker, _, _ = make_worker([])
    request = make_request()
    await worker.submit(7, request, priority=2)
    assert worker.queue == [(7, request, 2)]
    assert await worker.queue_size(7) == 1
    assert await worker.get_input_nowait() is request
    with pytest.raises(asyncio.QueueEmpty):
        await worker.get_input_nowait()


@pytest.mark.asyncio
async def test_backoff_wait_returns_at_once_when_no_window_is_armed():
    '''With no wait timestamp the call is a no-op even if shutdown is set'''
    worker, _, _ = make_worker([])
    event = asyncio.Event()
    event.set()
    await worker.backoff_wait(event)
    assert worker.backoff_seconds_remaining is None


@pytest.mark.asyncio
async def test_backoff_wait_raises_when_shutdown_is_already_set():
    '''An armed window with shutdown set exits early instead of sleeping'''
    worker, _, _ = make_worker([])
    worker.set_wait_timestamp()
    event = asyncio.Event()
    event.set()
    with pytest.raises(ExitEarlyException):
        await worker.backoff_wait(event)


@pytest.mark.asyncio
async def test_backoff_wait_returns_when_the_window_has_already_elapsed():
    '''A timestamp in the past means zero sleep and a clean return'''
    worker, _, _ = make_worker([])
    worker.expire_window()
    await worker.backoff_wait(asyncio.Event())
    assert worker.backoff_seconds_remaining == 0


@pytest.mark.asyncio
async def test_backoff_wait_caps_the_sleep_at_max_wait_seconds():
    '''A long window is slept in slices; the slice times out and returns'''
    worker, _, _ = make_worker([])
    worker.set_wait_timestamp(backoff_multiplier=100)
    await worker.backoff_wait(asyncio.Event(), max_wait_seconds=0.01)
    assert worker.backoff_seconds_remaining > 900


@pytest.mark.asyncio
async def test_backoff_wait_exits_early_when_shutdown_fires_mid_sleep():
    '''Setting the event during the sleep surfaces as ExitEarlyException'''
    worker, _, _ = make_worker([])
    worker.set_wait_timestamp()
    event = asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, event.set)
    with pytest.raises(ExitEarlyException):
        await worker.backoff_wait(event)


class ConformingSearchClient:
    '''Implements the whole cog-facing search surface.'''
    failure_summary = ''
    backoff_seconds_remaining = None

    async def submit(self, guild_id, media_request, priority=None):
        '''Accept a request.'''

    async def get_input_nowait(self):
        '''Return nothing useful.'''

    async def resolve(self, media_request):
        '''Resolve to nothing.'''

    async def backoff_wait(self, shutdown_event, max_wait_seconds=None):
        '''Never wait.'''

    def set_wait_timestamp(self, backoff_multiplier=1):
        '''Ignore the window.'''

    async def block_guild(self, guild_id):
        '''Pretend to block.'''
        return guild_id != 0

    async def clear_guild_queue(self, guild_id, preserve_predicate=None):
        '''Return an empty result.'''
        return ClearGuildResult(dropped=[], preserved_bundle_uuids={str(guild_id)} if preserve_predicate else set())

    async def queue_size(self, guild_id):
        '''Report an empty queue.'''
        return guild_id * 0


class MissingMostOfTheSurface:
    '''Only one of the required methods.'''

    async def submit(self, guild_id, media_request, priority=None):
        '''Accept a request.'''


def test_search_client_protocol_accepts_a_full_implementation_only():
    '''isinstance passes for a conforming class and fails for partial ones'''
    assert isinstance(ConformingSearchClient(), YoutubeMusicSearchClient)
    assert not isinstance(MissingMostOfTheSurface(), YoutubeMusicSearchClient)
    assert not isinstance(object(), YoutubeMusicSearchClient)
