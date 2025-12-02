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

## Use Cases

### 1. **Memory Leak Detection**

Watch for continuously increasing RSS/USS over time:

```bash
# Monitor memory growth
tail -f logs/process_metrics.log | grep "RSS Delta"
```

If you see consistent positive deltas like `+5.00 MB`, `+5.00 MB`, `+5.00 MB`, you have a leak.

### 2. **Resource Usage Monitoring**

Track overall resource consumption:

```bash
# Check current memory usage
tail -20 logs/process_metrics.log
```

### 3. **Comparing with Memory Profiler**

Use both profilers together to diagnose issues:

- **Process Metrics** shows **total memory** (including native code)
- **Memory Profiler** shows **Python heap allocations** only

If you see:
- Process RSS: 400 MB
- Memory Profiler total: 28 MB
- **Gap**: 372 MB in native code (FFmpeg, SSL, etc.)

## Configuration Recommendations

### Development
```yaml
monitoring:
  process_metrics:
    enabled: true
    interval_seconds: 5   # Frequent updates for debugging
```

### Production
```yaml
monitoring:
  process_metrics:
    enabled: true
    interval_seconds: 60  # Less frequent to reduce overhead
```

### With Memory Profiler
```yaml
monitoring:
  memory_profiling:      # Detailed Python allocation tracking
    enabled: true
    interval_seconds: 60
    top_n_lines: 25

  process_metrics:       # Total process resource usage
    enabled: true
    interval_seconds: 15  # More frequent than memory_profiling
```

## Understanding the Metrics

### Memory Metrics

**RSS (Resident Set Size)**
- Physical RAM used by the process
- What shows up in `top` and Kubernetes metrics
- Includes shared libraries

**VMS (Virtual Memory Size)**
- Total virtual address space
- Usually much larger than RSS
- Includes memory-mapped files

**USS (Unique Set Size)**
- Memory unique to this process
- Most accurate for "true" memory usage
- Doesn't include shared libraries

**Memory Deltas**
- Positive delta = memory growing
- Negative delta = memory freed
- Consistent growth indicates leak

### Resource Metrics

**CPU Usage**
- Percentage of CPU used since last check
- 100% = 1 full CPU core
- Can exceed 100% on multi-core systems

**Threads**
- Number of Python + native threads
- Discord.py creates threads for voice
- High count (>100) may indicate issues

**File Descriptors**
- Open files, sockets, pipes
- Linux/Unix only
- High count may indicate fd leak

## Integration with OpenTelemetry

When OTLP is enabled, metrics are also exported:

- `process.memory.rss` - RSS in bytes
- `process.memory.vms` - VMS in bytes
- `process.memory.uss` - USS in bytes

These can be visualized in Grafana alongside other metrics.

## Troubleshooting

### High Memory Usage

If RSS is consistently high:

1. Check memory profiler for Python leaks
2. Calculate native memory: `RSS - Python heap`
3. Look for FFmpeg/audio processes
4. Check for unclosed voice connections

### Memory Not Decreasing

After stopping music player, if RSS doesn't decrease:

1. Check if FFmpegPCMAudio cleanup is called
2. Look for leaked voice client references
3. Check async tasks not properly cancelled

### CPU Spikes

If CPU usage is high:

1. Check number of active players
2. Look for busy loops in logs
3. Check FFmpeg encoding processes
