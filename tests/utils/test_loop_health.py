'''Tests for the per-loop health registry that drives heartbeats and probes.'''
import asyncio

import pytest

from discord_bot.utils.loop_health import (DEFAULT_STALE_AFTER_SECONDS, health_aware_queue_get,
                                           heartbeat_observation_value, LOOP_HEALTH, LoopHealth,
                                           LoopHealthRegistry, LoopStatus)


class FakeClock:
    '''Manually advanced monotonic clock so staleness is tested without sleeping.'''
    def __init__(self, now: float = 1000.0):
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        '''Move the clock forward.'''
        self.now += seconds


def test_starts_healthy_with_a_full_grace_window():
    # Startup grace: a loop that has never run yet is healthy for its whole
    # window. Without this a slow boot (or bot.wait_until_ready) would fail the
    # liveness probe and restart the pod before the loop ever got going.
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    assert health.is_healthy
    clock.advance(59)
    assert health.is_healthy


def test_goes_stalled_only_after_the_window_with_no_success():
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    clock.advance(61)
    assert not health.is_healthy
    assert health.status == LoopStatus.STALLED


def test_errors_alone_do_not_make_a_loop_unhealthy():
    # The whole point of the rework: five quick errors against a peer that is
    # still rolling is a blip, not a wedge. Only time decides.
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    for _ in range(50):
        health.record_error()
        clock.advance(0.1)
    assert health.is_healthy
    assert health.consecutive_errors == 50


def test_success_rearms_the_window_and_clears_the_error_count():
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    health.record_error()
    clock.advance(59)
    health.record_success()
    clock.advance(59)
    assert health.is_healthy  # window restarted from the success
    assert health.consecutive_errors == 0


def test_a_stalled_loop_recovers_when_work_succeeds_again():
    # Self-healing is the behaviour the old give-up rule made impossible.
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    clock.advance(61)
    assert not health.is_healthy
    health.record_success()
    assert health.is_healthy


def test_stopped_loops_are_healthy_so_draining_pods_are_not_killed():
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    health.mark_stopped()
    clock.advance(600)
    assert health.status == LoopStatus.STOPPED
    assert health.is_healthy


def test_mark_running_rearms_a_stalled_loop():
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    clock.advance(61)
    health.mark_running()
    assert health.is_healthy


def test_seconds_since_success_reports_the_gap():
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    clock.advance(12)
    assert health.seconds_since_success == 12


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

def test_empty_registry_is_healthy():
    # A process with no background loops can't have a wedged one.
    registry = LoopHealthRegistry()
    assert registry.is_healthy
    assert registry.snapshot() == {}


def test_registry_reports_unhealthy_when_any_loop_stalls():
    clock = FakeClock()
    registry = LoopHealthRegistry()
    registry.register('fast', time_func=clock)
    stalled = registry.register('slow', stale_after_seconds=10, time_func=clock)
    clock.advance(11)
    assert not registry.is_healthy
    assert registry.stalled_names() == ['slow']
    assert registry.snapshot() == {'fast': LoopStatus.OK, 'slow': LoopStatus.STALLED}
    stalled.record_success()
    assert registry.is_healthy


def test_reregistering_rearms_instead_of_duplicating():
    # cog_load after cog_unload must not orphan the old entry at 'stopped'.
    registry = LoopHealthRegistry()
    first = registry.register('loop')
    first.mark_stopped()
    second = registry.register('loop')
    assert first is second
    assert second.status == LoopStatus.OK
    assert list(registry.snapshot()) == ['loop']


def test_configure_updates_unpinned_windows_only():
    # A loop whose window was sized for its own cadence keeps it; generic loops
    # follow the process-wide setting.
    registry = LoopHealthRegistry()
    generic = registry.register('generic')
    pinned = registry.register('pinned', stale_after_seconds=42)
    registry.configure(900)
    assert generic.stale_after_seconds == 900
    assert pinned.stale_after_seconds == 42
    assert registry.register('later').stale_after_seconds == 900


def test_register_for_interval_scales_with_the_loops_cadence():
    registry = LoopHealthRegistry()
    # Fast loop: the process default already dwarfs the cadence
    assert registry.register_for_interval('fast', 1.0).stale_after_seconds == DEFAULT_STALE_AFTER_SECONDS
    # Slow producer: must be allowed to miss a few scheduled runs, not be judged
    # by a window built for a sub-second poller
    assert registry.register_for_interval('slow', 600.0).stale_after_seconds == 1800.0


def test_unregister_drops_the_series():
    registry = LoopHealthRegistry()
    registry.register('loop')
    registry.unregister('loop')
    assert registry.snapshot() == {}
    registry.unregister('never-existed')  # no KeyError


def test_reset_restores_the_default_window():
    registry = LoopHealthRegistry()
    registry.configure(5)
    registry.register('loop')
    registry.reset()
    assert registry.snapshot() == {}
    assert registry.register('loop').stale_after_seconds == DEFAULT_STALE_AFTER_SECONDS


# ---------------------------------------------------------------------------
# Heartbeat value
# ---------------------------------------------------------------------------

def test_heartbeat_value_is_none_for_unregistered_loops():
    # Emit nothing rather than a permanent 0 for a loop that doesn't run here.
    assert heartbeat_observation_value('never-registered') is None


def test_heartbeat_value_follows_health():
    clock = FakeClock()
    health = LOOP_HEALTH.register('loop', stale_after_seconds=60, time_func=clock)
    assert heartbeat_observation_value('loop') == 1
    clock.advance(61)
    assert heartbeat_observation_value('loop') == 0
    health.record_success()
    assert heartbeat_observation_value('loop') == 1


# ---------------------------------------------------------------------------
# Idle-safe queue get
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_aware_queue_get_returns_the_item():
    queue: asyncio.Queue = asyncio.Queue()
    health = LoopHealth('loop', stale_after_seconds=60)
    await queue.put('item')
    assert await health_aware_queue_get(queue, health, idle_timeout=0.01) == 'item'


@pytest.mark.asyncio
async def test_health_aware_queue_get_rearms_while_idle():
    # A consumer parked on an empty queue is doing its job. Without the re-arm
    # it would drift into 'stalled' during any quiet period and — with the
    # liveness probe reading this — get its own pod restarted.
    queue: asyncio.Queue = asyncio.Queue()
    clock = FakeClock()
    health = LoopHealth('loop', stale_after_seconds=60, time_func=clock)
    getter = asyncio.get_event_loop().create_task(
        health_aware_queue_get(queue, health, idle_timeout=0.01)
    )
    await asyncio.sleep(0.05)  # several idle timeouts elapse
    clock.advance(59)
    assert health.is_healthy
    await queue.put('item')
    assert await getter == 'item'


@pytest.mark.asyncio
async def test_health_aware_queue_get_does_not_drop_items_on_idle_timeout():
    # The re-arm cancels a pending Queue.get every idle_timeout; an item put
    # concurrently must still be delivered, not swallowed.
    queue: asyncio.Queue = asyncio.Queue()
    health = LoopHealth('loop', stale_after_seconds=60)
    received = []

    async def consume():
        for _ in range(5):
            received.append(await health_aware_queue_get(queue, health, idle_timeout=0.001))

    consumer = asyncio.get_event_loop().create_task(consume())
    for index in range(5):
        await asyncio.sleep(0.002)
        await queue.put(index)
    await asyncio.wait_for(consumer, timeout=2)
    assert received == [0, 1, 2, 3, 4]
