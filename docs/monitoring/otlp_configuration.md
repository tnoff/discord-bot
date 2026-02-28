# OpenTelemetry (OTLP) Configuration

This guide covers how to configure OpenTelemetry for the Discord bot to export metrics, traces, and logs.

## Overview

The bot uses OpenTelemetry to instrument and export observability data. When enabled, it exports:

- **Traces** - Distributed traces for command execution, database queries, API calls
- **Metrics** - System and application metrics (player stats, memory usage, etc.)
- **Logs** - Structured logs forwarded to OTLP collectors

## Configuration File

### Basic Configuration

Add the monitoring section to your config file under `general`:

```yaml
general:
  monitoring:
    otlp:
      enabled: true
```


### Configuration Options

#### OTLP Section (`monitoring.otlp`)

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `enabled` | boolean | Yes | N/A | Enable/disable OTLP instrumentation for metrics, traces, and logs |
| `filter_high_volume_spans` | boolean | No | `true` | Enable filtering of high volume spans that are in OK state |
| `high_volume_span_patterns` | list[string] | No | See below | List of regex patterns to match span names for filtering |

#### Logging Section (`logging`)

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `log_level` | int | Yes | N/A | Log level: 0=NOTSET, 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL |
| `otlp_only` | boolean | No | `false` | When `true`, skip local file logging and send logs via OTLP only |
| `log_dir` | string | Yes (unless `otlp_only`) | N/A | Directory to write log files |
| `log_file_count` | int | Yes (unless `otlp_only`) | N/A | Number of backup log files to keep |
| `log_file_max_bytes` | int | Yes (unless `otlp_only`) | N/A | Max log file size in bytes before rotation |
| `logging_format` | string | No | `%(asctime)s - %(levelname)s - %(message)s` | Python logging format string |
| `logging_date_format` | string | No | `%Y-%m-%dT%H-%M-%S` | Date format for log timestamps |
| `third_party_log_level` | int | No | `30` (WARNING) | Log level applied to third-party loggers (discord.py, etc.) |

##### OTLP-Only Logging

To send logs exclusively via OTLP and skip writing local log files, set `otlp_only: true` in the logging section. The `log_dir`, `log_file_count`, and `log_file_max_bytes` fields are not required in this mode:

```yaml
general:
  logging:
    log_level: 20
    otlp_only: true
  monitoring:
    otlp:
      enabled: true
```

### High Volume Span Filtering

When `filter_high_volume_spans` is enabled, spans matching the configured regex patterns are filtered out if they complete successfully (OK status). Failed spans are always exported for debugging purposes.

#### Default Patterns

By default, the following patterns are filtered:

```yaml
high_volume_span_patterns:
  - '^sql_retry\.retry_db_command$'
  - '^utils\.retry_command_async$'
  - '^utils\.message_send_async$'
```

#### Custom Patterns

You can override or extend the default patterns with your own regex patterns:

```yaml
general:
  monitoring:
    otlp:
      enabled: true
      filter_high_volume_spans: true
      high_volume_span_patterns:
        # Filter all spans starting with "internal."
        - '^internal\.'
        # Filter specific heartbeat spans
        - '^heartbeat\.check$'
        # Filter spans containing "cache" anywhere in the name
        - '.*cache.*'
        # Keep default retry patterns
        - '^sql_retry\.retry_db_command$'
        - '^utils\.retry_command_async$'
        - '^utils\.message_send_async$'
```

#### Pattern Examples

| Pattern | Matches | Does Not Match |
|---------|---------|----------------|
| `^utils\.` | `utils.retry`, `utils.message_send` | `my_utils.foo` |
| `.*heartbeat.*` | `system.heartbeat.check`, `heartbeat` | `heart_beat` |
| `^db\.(query\|insert)$` | `db.query`, `db.insert` | `db.delete`, `db.query.slow` |
| `^api\.v[0-9]+\.` | `api.v1.users`, `api.v2.posts` | `api.legacy.users` |

#### Disabling Filtering

To export all spans including high volume ones:

```yaml
general:
  monitoring:
    otlp:
      enabled: true
      filter_high_volume_spans: false
```

## Environment Variables

The bot uses standard OpenTelemetry environment variables for endpoint configuration.

### Required Variables

```bash
# OTLP Endpoint (gRPC)
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

# Service Name
export OTEL_SERVICE_NAME=discord-bot
```

### Optional Variables

```bash
# OTLP Protocol (default: grpc)
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc

# Resource Attributes
export OTEL_RESOURCE_ATTRIBUTES="service.version=1.0.0,deployment.environment=production"

# Specific Endpoint Overrides
export OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://tempo:4317
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://mimir:4317
export OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://loki:4317
```

## Instrumentation Details

### Automatic Instrumentation

The bot automatically instruments:

- **SQLAlchemy** - Database query traces with SQL comments
- **Requests** - HTTP client request traces
- **Discord.py** - Custom instrumentation for Discord API calls