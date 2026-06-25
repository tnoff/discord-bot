'''Multi-pod coherency tests for RedisBroker.

These construct TWO independent ``RedisBroker`` instances — each with its own
client and registry — pointed at ONE shared ``fakeredis.FakeServer``, i.e. two
HA broker pods sharing one Redis.  That is the scenario the single-broker tests
in ``test_redis_broker.py`` cannot reach: their "concurrency" is concurrent
asyncio tasks inside one broker, so a broken lock would still pass because the
tasks share in-process Python state.  Here the only thing the two pods share is
Redis, so the registry's ``atomic_checkout`` lock and the per-bundle lock have
to actually serialise the cross-pod read-modify-write.

This is the MR-F merge gate: the BundleState/registry coherency the doc flags
as only surfacing under real Redis + multiple broker pods.
'''
import asyncio

import fakeredis.aioredis
import pytest

from discord_bot.clients.redis_client import RedisManager
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.types.download import LifecycleEvent, LifecycleStatusUpdate
from discord_bot.types.media_request import MediaRequest
from discord_bot.types.search import SearchResult
from discord_bot.workers.broker_registry import RedisBrokerRegistry
from discord_bot.workers.media_bundle import BundleRenderer
from discord_bot.workers.redis_broker import RedisBroker


class _YieldingRedis:
    '''Wrap a fakeredis client so every get/set/delete yields to the event loop
    before executing.

    This is load-bearing for these tests.  fakeredis resolves its awaits
    synchronously — it never suspends the running coroutine — so two coroutines
    under asyncio.gather run to completion one after the other and an emergent
    read-modify-write race can NEVER interleave.  A real Redis round-trip *does*
    suspend at every await; without restoring that suspend point the tests would
    pass even with the bundle lock removed (verified: they do).  The `sleep(0)`
    reinstates the interleaving so the cross-pod RMW actually races and the lock
    has something to serialise.  The wrapped ops stay individually atomic (the
    yield is *before* the single fakeredis operation), exactly like Redis.
    '''

    def __init__(self, inner):
        self._inner = inner

    async def get(self, *args, **kwargs):
        await asyncio.sleep(0)
        return await self._inner.get(*args, **kwargs)

    async def set(self, *args, **kwargs):
        await asyncio.sleep(0)
        return await self._inner.set(*args, **kwargs)

    async def delete(self, *args, **kwargs):
        await asyncio.sleep(0)
        return await self._inner.delete(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _two_pods():
    '''Return (broker_a, reg_a, broker_b, reg_b): two brokers with independent
    clients/registries sharing ONE FakeServer — two pods, one Redis — each
    wrapped so Redis ops yield (see _YieldingRedis) and cross-pod races are real.'''
    server = fakeredis.FakeServer()

    def _pod():
        client = _YieldingRedis(
            fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
        registry = RedisBrokerRegistry(RedisManager.from_client(client))
        return RedisBroker(registry), registry

    (broker_a, reg_a), (broker_b, reg_b) = _pod(), _pod()
    return broker_a, reg_a, broker_b, reg_b


def _make_request(guild_id: int = 1, channel_id: int = 2) -> MediaRequest:
    return MediaRequest(
        guild_id=guild_id,
        channel_id=channel_id,
        requester_name='tester',
        requester_id=9,
        search_result=SearchResult(
            search_type=SearchType.DIRECT,
            raw_search_string='https://example.com/video',
        ),
    )


def _queued_request(bundle_uuid: str) -> MediaRequest:
    mr = _make_request()
    mr.bundle_uuid = bundle_uuid
    mr.state_machine.mark_queued()
    return mr


@pytest.mark.asyncio
async def test_two_pods_atomic_checkout_only_one_wins():
    '''Two separate pods race to check out the same AVAILABLE item; the
    registry's SET NX lock lets exactly one win, and both pods then read the
    same CHECKED_OUT entry from the shared Redis.'''
    _broker_a, reg_a, _broker_b, reg_b = _two_pods()
    await reg_a.set_entry('item-1', {
        'zone': 'available',
        'checked_out_by': None,
        'download': {'file_path': '/tmp/item.mp4'},
        'request': {},
    })

    results = await asyncio.gather(
        reg_a.atomic_checkout('item-1', guild_id=11),
        reg_b.atomic_checkout('item-1', guild_id=22),
    )
    assert results.count(True) == 1
    assert results.count(False) == 1

    entry_a = await reg_a.get_entry('item-1')
    entry_b = await reg_b.get_entry('item-1')
    assert entry_a['zone'] == 'checked_out'
    assert entry_b['zone'] == 'checked_out'
    # The winner stamped checked_out_by; both pods agree on who holds it.
    assert entry_a['checked_out_by'] == entry_b['checked_out_by']
    assert entry_a['checked_out_by'] in (11, 22)


@pytest.mark.asyncio
async def test_two_pods_register_into_one_bundle_no_counter_drift():
    '''Two pods concurrently register requests into the SAME bundle.  Without
    the per-bundle lock the cross-pod read-modify-write races and silently
    drops requests (the "playlist with N items only counts N-1" bug).  Both
    pods must end up seeing all four.'''
    broker_a, _reg_a, broker_b, _reg_b = _two_pods()
    bundle_uuid = await broker_a.create_bundle(
        guild_id=1, channel_id=2, input_string='multi', has_search_banner=True,
    )
    reqs_a = [_queued_request(bundle_uuid) for _ in range(2)]
    reqs_b = [_queued_request(bundle_uuid) for _ in range(2)]

    await asyncio.gather(
        *(broker_a.register_request(mr) for mr in reqs_a),
        *(broker_b.register_request(mr) for mr in reqs_b),
    )

    expected = {str(mr.uuid) for mr in reqs_a + reqs_b}
    for broker in (broker_a, broker_b):
        state = await broker.get_bundle_state(bundle_uuid)
        assert state.total == 4
        assert {str(br.media_request.uuid) for br in state.bundled_requests} == expected


@pytest.mark.asyncio
async def test_cross_pod_status_update_is_visible():
    '''Pod A registers a request; pod B advances it to COMPLETED.  Pod A then
    sees the advanced lifecycle and the incremented counter — the
    _sync_request_into_bundle write-back crossing the pod boundary.'''
    broker_a, _reg_a, broker_b, _reg_b = _two_pods()
    bundle_uuid = await broker_a.create_bundle(
        guild_id=1, channel_id=2, input_string='x', has_search_banner=True,
    )
    mr = _queued_request(bundle_uuid)
    await broker_a.register_request(mr)

    await broker_b.update_request_status(
        str(mr.uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED),
    )

    state = await broker_a.get_bundle_state(bundle_uuid)
    assert state.completed == 1
    assert state.bundled_requests[0].media_request.lifecycle_stage.value == 'completed'


@pytest.mark.asyncio
async def test_full_lifecycle_split_across_pods_finishes_coherently():
    '''A bundle whose registers and per-request completions are driven
    CONCURRENTLY across BOTH pods converges on one finished state.  The
    completions race on the shared `completed` counter — the read-modify-write
    that, unserialised, produces "12/12 album shows 11/12 completed".  Both pods
    must agree on total=3, completed=3, finished.'''
    broker_a, _reg_a, broker_b, _reg_b = _two_pods()
    bundle_uuid = await broker_a.create_bundle(
        guild_id=1, channel_id=2, input_string='album', has_search_banner=True,
    )
    reqs = [_queued_request(bundle_uuid) for _ in range(3)]
    # Register across both pods, concurrently.
    await asyncio.gather(
        broker_a.register_request(reqs[0]),
        broker_b.register_request(reqs[1]),
        broker_a.register_request(reqs[2]),
    )
    await broker_b.finalize_bundle(bundle_uuid)

    # Complete every request concurrently from alternating pods — the counter race.
    await asyncio.gather(
        broker_b.update_request_status(
            str(reqs[0].uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED)),
        broker_a.update_request_status(
            str(reqs[1].uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED)),
        broker_b.update_request_status(
            str(reqs[2].uuid), LifecycleStatusUpdate(event=LifecycleEvent.COMPLETED)),
    )

    for broker in (broker_a, broker_b):
        state = await broker.get_bundle_state(bundle_uuid)
        assert state.total == 3
        assert state.completed == 3
        assert BundleRenderer(state).finished is True
