# Metrics Reference

Complete reference for all OpenTelemetry metrics exported by the Discord bot.

## Overview

The bot exports metrics in OpenTelemetry format when OTLP is enabled. All metrics are observable gauges unless otherwise noted.

## Music Player Metrics

These metrics are exported by the Music cog when enabled.

### `music.active_players`

**Type**: Observable Gauge
**Unit**: players
**Description**: Number of active music players (one per guild)
**Labels**:
- `guild_id` (string) - Discord guild ID

**Usage**: Monitor active voice connections and player usage across guilds.

### `music.multirequest_bundles`

**Type**: Observable Gauge
**Unit**: bundles
**Description**: Number of active multi-request bundles (batch playlist/album downloads)
**Labels**:
- `guild_id` (string) - Discord guild ID

**Usage**: Track ongoing batch operations for playlists/albums.

### `music.cache_filesystem_max`

**Type**: Observable Gauge
**Unit**: bytes
**Description**: Total size of the cache filesystem
**Labels**: None

**Usage**: Monitor available storage for cache.

### `music.cache_filesystem_used`

**Type**: Observable Gauge
**Unit**: bytes
**Description**: Used size of the cache filesystem
**Labels**: None

**Usage**: Monitor cache disk usage.

### `music.download_result_queue_depth`

**Type**: Observable Gauge
**Unit**: dimensionless (1)
**Description**: Completed download results sitting on the broker's bot-ready
queue, waiting for a bot pod to route them to players
**Emitted by**: the broker pod (`cli/broker.py` → `BrokerMetrics`), from a
background poller — the queue lives in Redis and an observable-gauge callback is
synchronous, so it reads a cached value refreshed every 15s
**Labels**:
- `background_job` = `broker`

**Usage**: A sustained non-zero value means the bot's result consumer
(`_result_task`) is falling behind or has stalled. Under normal load this drains
to zero quickly after each download completes.

The bot pod used to publish this same metric with
`background_job="process_download_results"`, reading a queue that only ever
existed in single-process mode — under HA that series was a permanent, confident
`0` sitting next to the broker's real one. It was removed with the broker
dual-path collapse (projects/discord-bot-ha-only); **scope any query for this
metric to `job="discord-broker"` or to `background_job="broker"`**, not to the
bare metric name.

### `music.search_result_queue_depth`

**Type**: Observable Gauge
**Unit**: dimensionless (1)
**Description**: Resolved searches sitting on the broker's bot-ready queue,
waiting for a bot pod to submit them to the download pipeline
**Emitted by**: the broker pod, same poller and same 15s refresh as above
**Labels**:
- `background_job` = `broker`

**Usage**: The search-side twin of the download queue. A sustained non-zero value
means `process_search_results` on the bot is behind. Same scoping caveat: the bot
pod's permanent-zero copy of this series is gone.

## MessageDispatcher Metrics

### `dispatcher_ready_check`

**Type**: Counter (exported to Prometheus/Mimir without a `_total` suffix, as
`dispatcher_ready_check`)
**Unit**: dimensionless (1)
**Description**: Count of dispatcher readiness probes performed by the bot pod,
by outcome. Incremented once per `/ready` probe whenever `dispatch_http_url` is
configured (HA bot mode).
**Labels**:
- `outcome` = `ok` | `unavailable`

**Usage**: `sum by (outcome) (rate(dispatcher_ready_check[5m]))`. A rising
`unavailable` rate means the bot cannot reach the dispatcher's TCP port — an
early warning for the readiness-split regression class. Only the bot pod
(`cli.bot`) emits this; the dispatcher pod has no peer to probe.

### `message_dispatcher_queue_depth`

**Status**: Planned — **not currently emitted**. The work queue is a single
priority queue (asyncio in single-process, a Redis sorted set in HA), not a set
of per-guild queues, so there is no per-guild breakdown to label, and an
observable-gauge callback (synchronous) cannot read the Redis depth without an
async poller. Tracked for a follow-up that adds a `depth()` to the `WorkQueue`
interface plus a background poller; until then, dispatcher backlog is inferred
from the worker `heartbeat` and `dispatcher_ready_check` signals.

## Heartbeat Metrics

These metrics indicate that background loops are active and running.

### `heartbeat`

**Type**: Observable Gauge
**Unit**: dimensionless (1)
**Description**: Health of a background loop
**Labels**: `background_job`

`1` while the loop is completing iterations, `0` once it has gone its staleness
window (default 300 s) without a successful one. This is **loop health, not task
liveness** — a loop that keeps erroring stays alive so it can recover, and says
so through this gauge instead of by dying. See
[Background Loop Health](loop_health.md) for the model and its configuration.

The same bit backs the [health server's](health_server.md) `/health` and `/ready`
probes, so this metric and the pod's readiness can never disagree.

| `background_job` label | Emitted by | Description |
|------------------------|------------|-------------|
| `message_dispatcher` | dispatcher pod (HA) or bot (single-process) | Dispatcher worker pool |
| `markov_check` | bot | Markov producer loop (submits Discord fetch requests) |
| `markov_result` | bot | Markov dispatch-result consumer |
| `delete_message_check` | bot | Message-deletion producer loop |
| `delete_message_result` | bot | Message-deletion result consumer |
| `cleanup_players` | bot | Inactive music player cleanup (Music) |
| `download_files` | downloader pod | Audio download loop — the bot no longer runs one, so the series only ever carries the pod's label set |
| `process_download_results` | bot | Download result routing (Music) |
| `process_search_results` | bot | Resolved-search consumer, submits downloads (Music) |
| `youtube_music_search` | search pod | YouTube Music search loop — the bot no longer runs one, so the series only ever carries the pod's label set |
| `post_play_processing` | bot | Post-play history/playlist tracking (Music) — only with a configured database |
| `downloader_worker` | downloader pod | Download consumer driver |
| `broker` | broker pod, or bot with an embedded broker | Broker HTTP server accepting requests* |
| `downloader` | downloader pod | Downloader HTTP server accepting requests* |

\* The two HTTP-server heartbeats report "the socket is serving", not loop
health — they are not backed by `LoopHealth` and do not gate the probes.

**Usage**: alert on `heartbeat{background_job="..."} == 0`, with a `for:` window.
No rule changes were needed when the underlying signal moved from liveness to
health — the metric name, label, and `1`/`0` meaning are unchanged.

**A loop that does not run in a given process emits no series at all**, rather
than a permanent `0`. Absence of a series means "not running here" (e.g.
`download_files` on an HA bot pod); a `0` always means "running here, and
wedged". Write alerts against the series that exist rather than asserting a
label set.

## Configuration-Dependent Metrics

Some metrics are only exported when certain features are enabled:

| Metric | Required Config | Required Feature |
|--------|----------------|------------------|
| `music.*` | `include.music: true` | Music cog enabled |
| `heartbeat` | N/A | Cog-specific (varies) |
| `music.cache_filesystem_*` | Music cog + filesystem cache | Download directory configured |

## Metric Cardinality

Be aware of metrics with potentially high cardinality:

| Metric | Label | Cardinality | Notes |
|--------|-------|-------------|-------|
| `music.active_players` | `guild_id` | # of guilds bot is in | Can be 100s-1000s |
| `music.multirequest_bundles` | `guild_id` | # of guilds bot is in | Can be 100s-1000s |

High cardinality can impact metrics backend performance. Consider:

- Aggregating `guild_id` metrics in queries rather than storing all values
- Using recording rules in Prometheus to pre-aggregate high-cardinality metrics
- Filtering to specific guilds of interest

## Traces

The bot also exports distributed traces for:

- **Discord Commands** - Command execution with duration and status
- **Database Queries** - SQLAlchemy queries with SQL comments
- **HTTP Requests** - Outbound requests (YouTube, Spotify APIs, etc.)
- **Background Jobs** - Async task execution

Traces use the service name configured in `OTEL_SERVICE_NAME` (default: `discord-bot`).

### Trace Attributes

Common trace attributes:

- `retry_count` - Number of retries for the operation
- `background_job` - Boolean indicating if operation is a background task
- `guild_id` - Discord guild context
- `user_id` - Discord user context
- `channel_id` - Discord channel context
- `egress.hostname` / `egress.ip` - Live VPN/proxy exit a download left from, on `music.download_client.create_source` spans (or `unknown`); requires `music.download.egress_probe` (see [music docs](../music.md#egress-exit-attribution))

## Logs

When OTLP is enabled, logs are forwarded to the configured OTLP log exporter with structured attributes.

### Log Attributes

Logs include OpenTelemetry resource attributes:

- `service.name` - Service name (discord-bot)
- `service.version` - Bot version (if configured)
- `deployment.environment` - Environment (production, staging, etc.)
- `host.name` - Container/pod hostname
- `process.pid` - Process ID
