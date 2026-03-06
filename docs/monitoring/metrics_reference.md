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

### `music.cache_file_count`

**Type**: Observable Gauge
**Unit**: files
**Description**: Number of audio files in the download cache
**Labels**: None

**Usage**: Monitor cache size and cleanup effectiveness.

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

## MessageDispatcher Metrics

### `message_dispatcher_queue_depth`

**Type**: Observable Gauge
**Unit**: dimensionless (1)
**Description**: Total number of work items pending across all per-guild queues
**Labels**:
- `background_job` = `message_dispatcher_queue`

**Usage**: A sustained non-zero value indicates the dispatcher is processing a
backlog. Spike during heavy music downloads or bulk operations is expected; a
value that never drains suggests a stuck worker.

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
| `message_dispatcher_workers` | Count of active per-guild dispatcher worker tasks |
| `markov_check` | Markov chain message processing loop |
| `delete_message_check` | Automated message deletion loop |
| `cleanup_players` | Inactive music player cleanup loop (Music) |
| `download_files` | Audio file downloading loop (Music) |
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

## Logs

When OTLP is enabled, logs are forwarded to the configured OTLP log exporter with structured attributes.

### Log Attributes

Logs include OpenTelemetry resource attributes:

- `service.name` - Service name (discord-bot)
- `service.version` - Bot version (if configured)
- `deployment.environment` - Environment (production, staging, etc.)
- `host.name` - Container/pod hostname
- `process.pid` - Process ID
