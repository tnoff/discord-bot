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
**Description**: Total number of completed download results waiting to be routed to players, summed across all guilds
**Labels**:
- `background_job` = `process_download_results`

**Usage**: A sustained non-zero value means the result consumer (`_result_task`) is falling behind or has stalled. Under normal load this should drain to zero quickly after each download completes.

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
**Description**: Heartbeat for various background loops
**Labels**: Varies by loop (typically `job_name` or similar)

The bot exports heartbeat metrics for these loops:

| `background_job` label | Description |
|------------------------|-------------|
| `message_dispatcher` | `1` while the dispatcher worker pool is running (emitted by the discord-dispatcher pod in HA mode, or the bot itself in single-process mode) |
| `markov_check` | Markov chain message processing loop |
| `delete_message_check` | Automated message deletion loop |
| `cleanup_players` | Inactive music player cleanup loop (Music) |
| `download_files` | Audio file downloading loop (Music) |
| `process_download_results` | Download result routing loop (Music) |
| `post_play_processing` | Post-play history/playlist tracking loop (Music) |
| `search_youtube_music` | YouTube Music search processing loop (Music) |

**Usage**: A value of `0` means the loop task has exited unexpectedly. Alert on
`heartbeat{background_job="..."} == 0`.

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
