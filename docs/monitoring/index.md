# Monitoring and Observability

This directory contains documentation for monitoring the Discord bot using OpenTelemetry (OTLP).

## Overview

The Discord bot includes comprehensive monitoring capabilities using OpenTelemetry to export **metrics**, **logs**, and **traces** to any OTLP-compatible collector. This provides full observability into the bot's runtime behavior, performance, and issues.

## Documentation

### Core Monitoring Guides

- **[OTLP Configuration](otlp_configuration.md)** - How to configure OpenTelemetry for the bot
- **[Metrics Reference](metrics_reference.md)** - Complete list of available metrics and queries
- **[Memory Profiling](memory_profiling.md)** - Guide to using the built-in memory profiler
- **[Process Metrics](process_metrics.md)** - Guide to using the built-in process profiler
- **[Health Server](health_server.md)** - HTTP liveness endpoint for Docker/Kubernetes probes

## Observability Components

### Metrics

The bot exports OpenTelemetry metrics to track runtime state and performance:

- **Music Cog Metrics** - Active players per guild, multirequest bundles, cache usage
- **Memory Metrics** - Object counts and memory usage by Python class type
- **Heartbeat Metrics** - Background loop health indicators
- **System Metrics** - Filesystem usage, database connections

**Implementation**: Metrics are implemented using OpenTelemetry's `create_observable_gauge()` with callback functions. The MeterProvider is initialized when OTLP is enabled and metrics are exported to the configured OTLP endpoint.

See [Metrics Reference](metrics_reference.md) for the complete list of available metrics.

### Logs

Structured logs are exported to the OTLP collector when enabled:

- **Standard Python Logging** - All bot logs use Python's standard logging module
- **File Rotation** - Logs written to local files with automatic rotation (optional, see `otlp_only`)
- **OTLP Export** - When OTLP is enabled, logs are forwarded to the collector with OpenTelemetry resource attributes
- **OTLP-Only Mode** - Set `logging.otlp_only: true` to skip local file logging entirely and rely solely on OTLP log export
- **Log Levels** - Configurable per-logger levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)

**Implementation**: Logs use Python's `logging` module. By default, `RotatingFileHandler` is used for local file storage. When `otlp_only` is enabled, the file handler is skipped and logs flow exclusively through OpenTelemetry's `LoggingHandler` to the OTLP endpoint.

### Traces

Distributed traces track request flow and timing:

- **Command Execution** - Traces for Discord commands with duration and status
- **Database Queries** - SQLAlchemy operations with SQL comments
- **HTTP Requests** - Outbound API calls (YouTube, Spotify, etc.)
- **Background Jobs** - Async task execution
- **Retry Logic** - Automatic instrumentation of retry attempts

**Implementation**: Traces use OpenTelemetry's `TracerProvider` with automatic instrumentation for SQLAlchemy and Requests libraries. Custom spans are created using the `@otel_span_wrapper` decorator. All traces include contextual attributes like `guild_id`, `user_id`, and `retry_count`.

## Architecture

### OTLP Export Flow

```
Bot Runtime
    │
    ├─> Metrics (Observable Gauges)
    │       └─> PeriodicExportingMetricReader
    │               └─> OTLPMetricExporter (gRPC)
    │
    ├─> Logs (Python logging)
    │       └─> LoggingHandler
    │               └─> OTLPLogExporter (gRPC)
    │
    └─> Traces (Spans)
            └─> BatchSpanProcessor
                    └─> OTLPSpanExporter (HTTP)
                            │
                            ▼
                    OTLP Collector (localhost:4317)
```

### Instrumentation

The bot uses both **automatic** and **manual** instrumentation:

**Automatic Instrumentation:**
- SQLAlchemy - All database queries
- Requests - All HTTP client requests

**Manual Instrumentation:**
- Discord.py commands - Custom spans via decorators
- Background jobs - Heartbeat metrics and trace attributes
- Music player operations - Domain-specific metrics
