# Process Metrics

Guide for using the process metrics profiler to monitor process-level resource usage.

## Overview

The process metrics profiler uses `psutil` to track process-level metrics including:

- **RSS (Resident Set Size)**: Physical memory usage
- **VMS (Virtual Memory Size)**: Virtual memory including swapped pages
- **USS (Unique Set Size)**: Memory unique to this process (most accurate)
- **CPU Usage**: CPU percentage
- **Thread Count**: Number of threads
- **File Descriptors**: Number of open file handles (Linux/Unix)

## Configuration

Add to your `discord.cnf`:

```yaml
general:
  monitoring:
    process_metrics:
      enabled: true
      interval_seconds: 15  # How often to log metrics (default: 15)
```

## Output Format

The profiler logs metrics every `interval_seconds` to `logs/process_metrics.log`:

```
**Process Metrics**

Memory Usage:
  RSS (Physical):      390.45 MB
  VMS (Virtual):      1065.23 MB
  USS (Unique):        385.12 MB

Memory Changes (since last snapshot):
  RSS Delta:            +0.15 MB
  VMS Delta:            +0.00 MB
  USS Delta:            +0.12 MB

Resources:
  CPU Usage:             12.3%
  Threads:                 24
  File Descriptors:       156
```