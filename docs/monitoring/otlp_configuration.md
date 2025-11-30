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