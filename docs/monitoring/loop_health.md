# Background Loop Health

Every background loop in every process — the music cog's consumers, the markov
and delete-messages loops, the dispatcher worker pool, the downloader's worker
driver — registers a `LoopHealth` in a process-wide registry
(`discord_bot/utils/loop_health.py`).

That registry is the **single source of truth for "is this process effectively
down"**, and both consumers of health read from it:

- the [`heartbeat` gauge](metrics_reference.md#heartbeat) (OTLP → alerting), and
- the [health server's](health_server.md) `/health` and `/ready` endpoints (k8s probes).

Because they read the same bit, "the alert fired" and "the pod is unhealthy" can
never disagree.

## Health is time-based, not error-count-based

A loop is healthy while it has **completed an iteration without raising** inside
its staleness window.

Errors are counted for diagnostics (they appear in the loop-runner log line) but
never decide health by themselves. This is deliberate. The previous design gave
up permanently after 5 consecutive errors, which at a 1 s backoff is a ~5 s
budget — shorter than any rolling update. A ~20 s broker deploy skew therefore
killed the search-result consumer for the life of the pod. A staleness window
rides out a skew while still catching a genuine wedge.

Two consequences worth internalising:

- **"Success" means the iteration returned**, not that it did work. A loop
  polling an empty queue is healthy — that is exactly its job.
- **Loops retry forever.** `return_loop_runner` logs, backs off (1 s doubling to
  30 s, reset on the next success), and continues. A loop can therefore recover
  on its own the moment its dependency comes back; health is what raises the
  alarm in the meantime, instead of the task having to die to say so.

## Statuses

| Status | Meaning | Heartbeat | Probe |
|---|---|---|---|
| `ok` | An iteration succeeded inside the window | `1` | passes |
| `stalled` | No successful iteration for `stale_after_seconds` | `0` | **fails (503)** |
| `stopped` | Deliberately stopped (shutdown, `cog_unload`, drain) | `1` | passes |
| *unregistered* | The loop does not run in this process | *no series emitted* | ignored |

`stopped` exists so a draining pod does not fail its own liveness probe on the
way out — cancelling a task never runs the loop runner's exit path, so shutdown
paths call `LOOP_HEALTH.mark_stopped(...)` explicitly.

*Unregistered* matters just as much: a loop that legitimately doesn't run in a
given deployment mode (the bot-side download loop under HA, where the loop lives
in the downloader pod) emits **no series at all**, rather than a permanent `0`
that would peg the stalled-loop alert forever.

## Configuration

```yaml
general:
  monitoring:
    loop_health:
      stale_after_seconds: 300  # default
```

| Field | Type | Default | Description |
|---|---|---|---|
| `stale_after_seconds` | float | `300` | How long a loop may go without a successful iteration before it is reported unhealthy |

Applied at startup by `setup_observability()`, so every entrypoint (bot,
dispatcher, broker, downloader, full) picks it up.

**This is also the "how long before we restart the pod" knob.** The Kubernetes
`livenessProbe` consumes the same bit, so a stalled loop gets the pod killed.
Size it to comfortably clear the transients a restart cannot fix:

| Event | Rough duration |
|---|---|
| Rolling deploy skew | ~20 s |
| Broker pod roll | ~30 s |
| Redis sentinel failover | ~60 s |

The 300 s default leaves generous headroom over all three. Widening it makes the
bot more tolerant of long peer outages at the cost of noticing a real wedge
later; narrowing it does the reverse. Note the probe's own
`failureThreshold × periodSeconds` stacks on top of this window.

### Per-loop windows

Loops whose natural cadence is slower than the process default get their own
window via `register_for_interval(name, interval_seconds)`, which allows three
missed runs. The markov and delete-messages producers sleep
`loop_sleep_interval` (default 300 s) per iteration, so under a single global
300 s window they would sit permanently on the edge of `stalled` — and, with
liveness gating, restart their own pods at random.

A window set this way is *pinned*: `loop_health.stale_after_seconds` does not
override it.

### Idle consumers

Loops that block on `queue.get()` indefinitely (the markov and delete-messages
result consumers) use `health_aware_queue_get`, which wakes every window/3 to
re-arm health while the queue stays empty. Without it, a quiet evening would read
as a wedge. A consumer stuck *processing* an item still goes stale as it should.

## Adding a loop

```python
from discord_bot.utils.loop_health import LOOP_HEALTH

LOOP_MY_THING = 'my_thing'  # registry key AND heartbeat background_job label

# Register the gauge once, at construction:
create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                        partial(loop_heartbeat_observations, LOOP_MY_THING),
                        'My thing loop heartbeat')

# Register the health when the loop actually starts:
self._task = self.bot.loop.create_task(
    return_loop_runner(self.my_thing, self.bot, self.logger,
                       health=LOOP_HEALTH.register(LOOP_MY_THING))()
)

# And mark it stopped on shutdown, since cancellation bypasses the exit path:
LOOP_HEALTH.mark_stopped(LOOP_MY_THING)
```

Loops not driven by `return_loop_runner` call `record_success()` /
`record_error()` themselves — see `MessageDispatcher._worker_loop` and
`cli/downloader.py:_drive_worker`.

Register the health where the loop *starts*, not where the gauge is created: the
"no series when unregistered" behaviour depends on it.

## Testing note

`LOOP_HEALTH` is process-global — correct in production (one registry per pod)
but leaky across tests in a single pytest process. `tests/conftest.py` has an
autouse `reset_loop_health` fixture that clears it between tests.

## History

The design comes from the 2026-07-31 search-seam deploy-skew incident, where a
transient 404 window during a rolling update permanently killed the bot's
search-result consumer. See
`findings/2026-07-31-discord-search-seam-deploy-skew.md` in the docs repo.
