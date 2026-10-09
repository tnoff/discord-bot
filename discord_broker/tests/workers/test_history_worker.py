'''Tests for HistoryWorker: recording plays from the Redis list into the db pod.'''
import asyncio
import json
import logging

import aiohttp
import fakeredis.aioredis
import pytest

from discord_core.clients.redis_client import RedisManager
from discord_core.exceptions import DatabaseUnavailable
from discord_core.types.playlist import PlaylistItemWrite
from discord_core.utils.loop_health import LOOP_HEALTH, LoopStatus

from discord_broker.workers import history_worker
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry, HISTORY_EVENTS_KEY
from discord_broker.workers.history_worker import HistoryWorker, Outcome

MAX_SIZE = 7


class FakePlaylists:
    '''Records history writes; can be told to fail the next calls.'''

    def __init__(self):
        self.ensured = []
        self.written = []
        self.errors = []          # raised, one per call, before anything is recorded
        self.record_result = True
        self.closed = False

    async def ensure_history_playlist(self, guild_id):
        if self.errors:
            raise self.errors.pop(0)
        self.ensured.append(guild_id)
        return 1000 + guild_id

    async def record_history_item(self, playlist_id, item, max_size):
        if self.errors:
            raise self.errors.pop(0)
        self.written.append((playlist_id, item, max_size))
        return self.record_result

    async def close(self):
        self.closed = True


class FakeAnalytics:
    '''Records plays; can be told to fail the next calls.'''

    def __init__(self):
        self.plays = []
        self.errors = []
        self.closed = False

    async def record_play(self, guild_id, duration_seconds, cache_hit):
        if self.errors:
            raise self.errors.pop(0)
        self.plays.append((guild_id, duration_seconds, cache_hit))
        return True

    async def close(self):
        self.closed = True


def _event(guild_id=1, title='Song', **overrides) -> dict:
    event = {'uuid': f'u-{title}', 'guild_id': guild_id, 'requester_name': 'Alice', 'requester_id': 9,
             'added_from_history': False, 'webpage_url': f'https://example.com/{title}',
             'title': title, 'uploader': 'Someone', 'duration': 90, 'cache_hit': False}
    event.update(overrides)
    return event


class Env:
    '''A worker over a real (fakeredis) record list and fake db stores.'''

    def __init__(self, **kwargs):
        self.client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        self.queues = GuildQueueRegistry(RedisManager.from_client(self.client))
        self.playlists = FakePlaylists()
        self.analytics = FakeAnalytics()
        self.worker = HistoryWorker(self.queues, self.playlists, self.analytics, MAX_SIZE, **kwargs)

    async def push(self, *events):
        '''Queue records the way `finish` does: at the head, so the worker (taking from the tail)
        sees them oldest first.'''
        for event in events:
            await self.client.lpush(HISTORY_EVENTS_KEY, json.dumps(event))


@pytest.fixture(name='env')
def env_fixture():
    return Env(idle_interval=0.01, error_backoff=0.01)


# ---------------------------------------------------------------------------
# recording
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_nothing_waiting_is_idle(env):
    '''An empty list is not an error.'''
    assert await env.worker.process_one() is Outcome.IDLE


@pytest.mark.asyncio
async def test_a_play_is_counted_and_added_to_the_guilds_history(env):
    '''Both halves of what the gateway's post-play loop did.'''
    await env.push(_event(guild_id=3, title='Song', duration=125, cache_hit=True))
    assert await env.worker.process_one() is Outcome.DONE

    assert env.analytics.plays == [(3, 125, True)]
    [(playlist_id, item, max_size)] = env.playlists.written
    assert playlist_id == 1003
    assert item == PlaylistItemWrite(video_url='https://example.com/Song', title='Song', uploader='Someone')
    assert max_size == MAX_SIZE


@pytest.mark.asyncio
async def test_a_track_replayed_from_history_is_counted_but_not_added_again(env):
    '''History does not feed on itself.'''
    await env.push(_event(added_from_history=True))
    assert await env.worker.process_one() is Outcome.DONE
    assert len(env.analytics.plays) == 1
    assert not env.playlists.written
    assert not env.playlists.ensured


@pytest.mark.asyncio
async def test_a_missing_duration_counts_as_zero(env):
    '''Some sources do not report one.'''
    await env.push(_event(duration=None))
    await env.worker.process_one()
    assert env.analytics.plays == [(1, 0, False)]


@pytest.mark.asyncio
async def test_the_history_playlist_is_asked_for_once_per_guild(env):
    '''The id does not change, so only the first play in a guild looks it up.'''
    await env.push(_event(guild_id=1, title='a'), _event(guild_id=1, title='b'), _event(guild_id=2, title='c'))
    for _ in range(3):
        await env.worker.process_one()
    assert env.playlists.ensured == [1, 2]
    assert [playlist for playlist, _, _ in env.playlists.written] == [1001, 1001, 1002]


@pytest.mark.asyncio
async def test_records_are_taken_oldest_first(env):
    '''Plays are recorded in the order they happened.'''
    await env.push(_event(title='first'), _event(title='second'))
    await env.worker.process_one()
    await env.worker.process_one()
    assert [item.title for _, item, _ in env.playlists.written] == ['first', 'second']


@pytest.mark.asyncio
async def test_a_deleted_history_playlist_is_looked_up_again(env, caplog):
    '''If the db says the playlist is gone, the next play in that guild re-resolves it.'''
    await env.push(_event(title='a'), _event(title='b'))
    env.playlists.record_result = False
    with caplog.at_level(logging.WARNING):
        assert await env.worker.process_one() is Outcome.DONE
    assert 'no longer exists' in caplog.text
    env.playlists.record_result = True
    await env.worker.process_one()
    assert env.playlists.ensured == [1, 1]


# ---------------------------------------------------------------------------
# failures
# ---------------------------------------------------------------------------

TRANSIENT = [DatabaseUnavailable('db down'), aiohttp.ClientConnectionError('reset'), asyncio.TimeoutError()]


@pytest.mark.asyncio
@pytest.mark.parametrize('error', TRANSIENT, ids=lambda error: type(error).__name__)
async def test_a_db_that_cannot_answer_puts_the_record_back(env, error):
    '''Nothing is lost while the db is down; the record waits and is tried again.'''
    await env.push(_event())
    env.analytics.errors = [error]
    assert await env.worker.process_one() is Outcome.RETRY

    assert await env.queues.history_event_depth() == 1
    assert await env.worker.process_one() is Outcome.DONE
    assert len(env.analytics.plays) == 1
    assert len(env.playlists.written) == 1


@pytest.mark.asyncio
async def test_a_retried_record_goes_before_newer_ones(env):
    '''Recording stays in play order across a failure.'''
    await env.push(_event(title='older'), _event(title='newer'))
    env.analytics.errors = [DatabaseUnavailable('db down')]
    assert await env.worker.process_one() is Outcome.RETRY
    await env.worker.process_one()
    await env.worker.process_one()
    assert [item.title for _, item, _ in env.playlists.written] == ['older', 'newer']


@pytest.mark.asyncio
async def test_a_play_is_not_counted_twice_when_only_the_playlist_write_failed(env):
    '''The retry remembers the analytics already succeeded.'''
    await env.push(_event())
    env.playlists.errors = [DatabaseUnavailable('db down')]
    assert await env.worker.process_one() is Outcome.RETRY
    assert len(env.analytics.plays) == 1
    assert await env.worker.process_one() is Outcome.DONE
    assert len(env.analytics.plays) == 1
    assert len(env.playlists.written) == 1


@pytest.mark.asyncio
async def test_a_record_is_given_up_on_after_the_attempt_limit(caplog):
    '''A db that never recovers must not hold up everything behind one record.'''
    env = Env(max_attempts=3)
    await env.push(_event(title='doomed'), _event(title='fine'))
    env.analytics.errors = [DatabaseUnavailable('down')] * 3
    assert await env.worker.process_one() is Outcome.RETRY
    assert await env.worker.process_one() is Outcome.RETRY
    with caplog.at_level(logging.ERROR):
        assert await env.worker.process_one() is Outcome.DONE
    assert 'Giving up' in caplog.text
    assert await env.queues.history_event_depth() == 1

    await env.worker.process_one()
    assert [item.title for _, item, _ in env.playlists.written] == ['fine']


@pytest.mark.asyncio
@pytest.mark.parametrize('overrides', [{'webpage_url': None}, {'webpage_url': ''}])
async def test_a_record_that_can_never_succeed_is_dropped_not_retried(env, overrides, caplog):
    '''No URL to store: retrying would only repeat the same failure forever.'''
    await env.push(_event(**overrides), _event(title='after'))
    with caplog.at_level(logging.ERROR):
        assert await env.worker.process_one() is Outcome.DONE
    assert 'Dropping unrecordable' in caplog.text
    assert await env.queues.history_event_depth() == 1
    await env.worker.process_one()
    assert [item.title for _, item, _ in env.playlists.written] == ['after']


@pytest.mark.asyncio
async def test_a_record_missing_its_guild_is_dropped(env):
    '''Nothing to attribute it to.'''
    broken = _event()
    del broken['guild_id']
    await env.push(broken)
    assert await env.worker.process_one() is Outcome.DONE
    assert not env.analytics.plays
    assert await env.queues.history_event_depth() == 0


# ---------------------------------------------------------------------------
# the loop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_drains_the_backlog_then_stops_when_told(env):
    '''Records are written as they arrive, and a stop is picked up without waiting out an interval.'''
    await env.push(*[_event(title=f't{i}') for i in range(3)])
    stop = asyncio.Event()
    task = asyncio.create_task(env.worker.run(stop))
    for _ in range(100):
        if len(env.playlists.written) == 3:
            break
        await asyncio.sleep(0.01)
    assert len(env.playlists.written) == 3

    await env.push(_event(title='late'))
    for _ in range(100):
        if len(env.playlists.written) == 4:
            break
        await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert len(env.playlists.written) == 4


@pytest.mark.asyncio
async def test_run_registers_a_heartbeat_and_marks_it_stopped_on_the_way_out(env):
    '''The loop is visible to the heartbeat gauge while it runs, and not a false alarm after.'''
    stop = asyncio.Event()
    task = asyncio.create_task(env.worker.run(stop))
    await asyncio.sleep(0.05)
    assert LOOP_HEALTH.get(history_worker.LOOP_HISTORY_WORKER).status == LoopStatus.OK
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert LOOP_HEALTH.get(history_worker.LOOP_HISTORY_WORKER).status == LoopStatus.STOPPED


@pytest.mark.asyncio
async def test_run_survives_an_unexpected_error_and_keeps_going(env, caplog):
    '''Whatever one pass throws, the loop backs off and carries on.'''
    calls = {'count': 0}
    real = env.worker.process_one

    async def flaky():
        calls['count'] += 1
        if calls['count'] == 1:
            raise RuntimeError('boom')
        return await real()

    env.worker.process_one = flaky
    await env.push(_event())
    stop = asyncio.Event()
    with caplog.at_level(logging.ERROR):
        task = asyncio.create_task(env.worker.run(stop))
        for _ in range(100):
            if env.playlists.written:
                break
            await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, timeout=1)
    assert 'pass failed' in caplog.text
    assert len(env.playlists.written) == 1


@pytest.mark.asyncio
async def test_the_backlog_gauge_follows_the_record_list():
    '''Depth climbs while the db is down, which is what an alert watches.'''
    env = Env(idle_interval=0.01, error_backoff=0.01, max_attempts=10_000)  # never give up in this test
    assert env.worker.depth_observations()[0].value == 0
    await env.push(_event(), _event(title='b'))
    env.analytics.errors = [DatabaseUnavailable('down')] * 5000
    stop = asyncio.Event()
    task = asyncio.create_task(env.worker.run(stop))
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    [observation] = env.worker.depth_observations()
    assert observation.value == 2
    assert observation.attributes == {'background_job': 'history_worker'}


@pytest.mark.asyncio
async def test_an_unreadable_backlog_is_logged_not_raised(env, caplog):
    '''A metric read must never take the loop down.'''
    async def broken():
        raise ConnectionError('redis gone')

    env.queues.history_event_depth = broken
    with caplog.at_level(logging.ERROR):
        await env.worker._refresh_depth()  # pylint: disable=protected-access
    assert 'could not read the record backlog' in caplog.text


@pytest.mark.asyncio
async def test_close_closes_both_clients(env):
    '''Their HTTP sessions are released at shutdown.'''
    await env.worker.close()
    assert env.playlists.closed and env.analytics.closed
