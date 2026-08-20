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

Span filtering is **not done by this application**. It lives in the
otel-collector, as the `filter/drop-ok-high-volume-spans` processor in
`monitoring/collector/config.yaml` in the `docker-apps` repo.

It used to be here: a `FilterOKRetrySpans` span processor driven by
`filter_high_volume_spans` / `high_volume_span_patterns`, configured per
service. Both keys are retired. A config that still sets them loads fine and
they are ignored.

Two reasons it moved:

**One rule set instead of five.** Each service carried its own pattern list in
its own ConfigMap block, the lists drifted, and a change only took effect when
the pod rolled. The collector applies one rule set to every service, including
the ones that were never in this codebase, and a change takes effect on the
collector roll.

**It could not filter what actually needed filtering.** The redis
auto-instrumentation emits a CLIENT span per command, and the services issue
most of theirs from background poll loops outside any request context, so each
becomes its own single-span trace in Tempo — measured at 99.5% of
`discord-search`'s span volume. Filtering that here was not an option, because
this filter matched on span *name* and redis names its spans after the bare
command (`GET`, `SET`, `DEL`), which collides with the HTTP client spans
`RequestsInstrumentor` produces. The collector matches on the `db.system`
attribute instead, which tells the two apart.

The collector filter keeps every ERROR span, and never drops SERVER or
CONSUMER spans regardless of name — the trace error-rate alerts divide errors
by all spans of that kind, so dropping the successful ones would leave a
denominator made only of errors.

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