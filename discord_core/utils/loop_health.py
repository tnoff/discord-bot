"""
Per-loop health tracking — the single source of truth for "is this process
effectively down".

Every background loop in every process (bot cogs, dispatcher workers, the
downloader's worker driver) registers a ``LoopHealth`` here.  Both consumers of
health read from this one registry:

- the ``heartbeat`` observable gauge (OTLP -> alerting), and
- the health servers' ``/health`` + ``/ready`` endpoints (k8s probes).

Health is **time-based, not error-count-based**: a loop is healthy while it has
completed an iteration without raising within ``stale_after_seconds``.  This is
deliberate.  The previous design gave up after 5 consecutive errors at a 1 s
backoff — a ~5 s budget, shorter than any rolling update — which turned a ~20 s
broker deploy skew into a permanently dead consumer
(docs findings/2026-07-31 search-seam deploy skew).  A staleness window rides
out a deploy skew while still catching a genuine wedge.

"Success" means *the iteration returned*, not that it did work, so an idle loop
polling an empty queue stays healthy.

Registration is what creates a series: a loop that never starts in this process
(e.g. the bot-side download loop under HA) simply reports nothing, rather than
sitting at a permanent 0 and tripping the stalled-loop alert.

Kept free of heavy imports (stdlib only) so the dispatcher's slim image and the
sqlalchemy-free health server base can both import it.
"""
import asyncio
import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)

# A loop is considered wedged after this long with no successful iteration.
# Sized for the k8s livenessProbe: a restart is the consequence, so this must
# comfortably clear a rolling deploy skew (~20 s), a broker roll (~30 s) and a
# Redis sentinel failover (~60 s).  Override per-deployment via
# MonitoringLoopHealthConfig.stale_after_seconds.
DEFAULT_STALE_AFTER_SECONDS = 300.0

# How many scheduled iterations a slow-cadence loop may miss before it is called
# stalled (see LoopHealthRegistry.register_for_interval).
_INTERVAL_STALE_MULTIPLIER = 3


class LoopStatus:
    """Reported status strings for a loop. Values appear in the health payload."""
    OK = 'ok'
    STALLED = 'stalled'
    STOPPED = 'stopped'


class LoopHealth:
    """
    Health state for a single background loop.

    Healthy while an iteration has completed without raising within
    ``stale_after_seconds``.  ``last_success`` starts at construction time, so a
    freshly registered loop gets a full window before it can be called stalled —
    the startup grace that keeps a slow boot (or ``bot.wait_until_ready()``)
    from failing the liveness probe.

    ``mark_stopped`` takes a loop out of the health calculation entirely: a loop
    that exited on purpose (shutdown / ExitEarlyException) is not a wedge, and
    must not 503 the health endpoint while the pod drains.

    ``time_func`` defaults to ``time.monotonic`` and can be replaced for tests.
    """

    def __init__(self, name: str, stale_after_seconds: float = DEFAULT_STALE_AFTER_SECONDS,
                 time_func: Callable[[], float] = time.monotonic, pinned: bool = False):
        self.name = name
        self.stale_after_seconds = stale_after_seconds
        # A pinned window was sized for this loop's own cadence (a producer loop
        # that sleeps 5 minutes per iteration cannot be judged by a window built
        # for a sub-second poller) and is not overwritten by registry.configure().
        self.pinned = pinned
        self._time = time_func
        self._last_success: float = time_func()
        self._consecutive_errors = 0
        self._stopped = False

    @property
    def consecutive_errors(self) -> int:
        """Errors since the last successful iteration. Diagnostic only — not the health test."""
        return self._consecutive_errors

    @property
    def seconds_since_success(self) -> float:
        """Age of the last successful iteration, in seconds."""
        return self._time() - self._last_success

    def record_success(self) -> None:
        """An iteration completed without raising. Re-arms the staleness window."""
        self._last_success = self._time()
        self._consecutive_errors = 0
        self._stopped = False

    def record_error(self) -> None:
        """An iteration raised. Does not itself make the loop unhealthy — time does."""
        self._consecutive_errors += 1

    def mark_stopped(self) -> None:
        """The loop exited deliberately (shutdown). Excluded from the health calculation."""
        self._stopped = True

    def mark_running(self) -> None:
        """(Re)start the staleness window — call when the loop's task is created."""
        self._last_success = self._time()
        self._consecutive_errors = 0
        self._stopped = False

    @property
    def status(self) -> str:
        """``ok`` / ``stalled`` / ``stopped`` — the value shown in the health payload."""
        if self._stopped:
            return LoopStatus.STOPPED
        if self.seconds_since_success > self.stale_after_seconds:
            return LoopStatus.STALLED
        return LoopStatus.OK

    @property
    def is_healthy(self) -> bool:
        """True unless the loop has gone ``stale_after_seconds`` with no successful iteration."""
        return self.status != LoopStatus.STALLED


class LoopHealthRegistry:
    """
    Process-wide registry of ``LoopHealth`` objects.

    ``is_healthy`` is the one bit that answers "is this process effectively
    down": every registered loop is healthy (or deliberately stopped).  An empty
    registry is healthy — a process with no background loops can't have a
    wedged one.
    """

    def __init__(self, stale_after_seconds: float = DEFAULT_STALE_AFTER_SECONDS):
        self._stale_after_seconds = stale_after_seconds
        self._loops: dict[str, LoopHealth] = {}

    def configure(self, stale_after_seconds: float) -> None:
        """
        Set the default staleness window for this process.

        Applies to future loops and to existing unpinned ones; loops registered
        with an explicit window (sized for their own cadence) keep it.
        """
        self._stale_after_seconds = stale_after_seconds
        for loop in self._loops.values():
            if not loop.pinned:
                loop.stale_after_seconds = stale_after_seconds

    def register(self, name: str, stale_after_seconds: float | None = None,
                 time_func: Callable[[], float] = time.monotonic) -> LoopHealth:
        """
        Return the ``LoopHealth`` for *name*, creating it on first call.

        Pass ``stale_after_seconds`` for a loop whose natural cadence is slower
        than the process default — a producer loop that sleeps 5 minutes between
        iterations would otherwise sit permanently on the edge of stalled, and
        (because the livenessProbe reads this) restart its own pod at random.

        Re-registering an existing name re-arms it rather than replacing it, so a
        cog that reloads (``cog_load`` after ``cog_unload``) keeps one series
        instead of orphaning the old entry at ``stopped``.
        """
        existing = self._loops.get(name)
        if existing is not None:
            existing.mark_running()
            return existing
        loop = LoopHealth(
            name,
            stale_after_seconds=stale_after_seconds if stale_after_seconds is not None else self._stale_after_seconds,
            time_func=time_func,
            pinned=stale_after_seconds is not None,
        )
        self._loops[name] = loop
        return loop

    def register_for_interval(self, name: str, interval_seconds: float,
                              time_func: Callable[[], float] = time.monotonic) -> LoopHealth:
        """
        Register a loop that sleeps ``interval_seconds`` per iteration.

        The window is the larger of the process default and
        ``_INTERVAL_STALE_MULTIPLIER`` iterations, so a slow-cadence loop must
        miss several scheduled runs — not merely take longer than a fast loop
        would — before it counts as wedged.
        """
        return self.register(
            name,
            stale_after_seconds=max(self._stale_after_seconds, interval_seconds * _INTERVAL_STALE_MULTIPLIER),
            time_func=time_func,
        )

    def get(self, name: str) -> LoopHealth | None:
        """Return the registered ``LoopHealth``, or None if the loop never started here."""
        return self._loops.get(name)

    def mark_stopped(self, *names: str) -> None:
        """
        Mark the named loops stopped, ignoring any that never registered here.

        Cancelling a task never runs the loop runner's exit path, so shutdown
        paths (``cog_unload``, ``stop``) call this to say "deliberate stop, not a
        wedge" — otherwise a draining pod would fail its own liveness probe once
        the cancelled loops went stale.
        """
        for name in names:
            loop = self._loops.get(name)
            if loop is not None:
                loop.mark_stopped()

    def unregister(self, name: str) -> None:
        """Drop a loop from the registry (stops emitting its heartbeat series)."""
        self._loops.pop(name, None)

    def reset(self) -> None:
        """Drop every registered loop and restore the default window. For tests."""
        self._loops.clear()
        self._stale_after_seconds = DEFAULT_STALE_AFTER_SECONDS

    def snapshot(self) -> dict[str, str]:
        """``{loop_name: status}`` for the health payload."""
        return {name: loop.status for name, loop in self._loops.items()}

    @property
    def is_healthy(self) -> bool:
        """True when no registered loop is stalled."""
        return all(loop.is_healthy for loop in self._loops.values())

    def stalled_names(self) -> list[str]:
        """Names of currently stalled loops, for log lines."""
        return [name for name, loop in self._loops.items() if not loop.is_healthy]


# Process-wide default registry. Entrypoints call configure(); loops call register().
LOOP_HEALTH = LoopHealthRegistry()


async def health_aware_queue_get(queue: asyncio.Queue, health: LoopHealth,
                                 idle_timeout: float | None = None):
    """
    Await ``queue.get()``, re-arming *health* while the queue stays empty.

    A consumer blocked on an empty queue is healthy — waiting is exactly its job
    — but it completes no iterations, so a plain ``await queue.get()`` would let
    a quiet period read as a wedge and (because the livenessProbe reads this)
    restart the pod. Waking every ``idle_timeout`` to re-arm keeps idle honest,
    while a consumer stuck *processing* an item still goes stale as it should.

    Defaults to a third of the loop's staleness window, so an idle consumer
    re-arms comfortably inside it. Cancelling ``Queue.get`` this way does not
    drop items: CPython's implementation removes the pending getter and
    re-dispatches any value handed to it to the next waiter.
    """
    timeout = idle_timeout if idle_timeout is not None else max(1.0, health.stale_after_seconds / 3)
    while True:
        try:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            health.record_success()


def heartbeat_observation_value(name: str) -> int | None:
    """
    Heartbeat gauge value for *name*: 1 healthy, 0 stalled, None when the loop
    isn't registered in this process (emit no observation at all).
    """
    loop = LOOP_HEALTH.get(name)
    if loop is None:
        return None
    return 1 if loop.is_healthy else 0
