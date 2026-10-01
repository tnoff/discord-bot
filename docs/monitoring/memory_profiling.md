# Memory Profiling

The Discord bot includes a built-in memory profiler (`discord_core/utils/memory_profiler.py`, `MemoryProfiler`) that periodically logs snapshots of memory allocations by source location. This is useful for identifying memory leaks and understanding memory consumption patterns.

## Overview

The memory profiler works by:
1. Using Python's `tracemalloc` to trace every allocation's file and line number
2. Taking periodic snapshots and ranking allocation sites by total size
3. Comparing each snapshot to the previous one to report growth/shrinkage per site
4. Logging the top allocation sites by current size and by change since the last snapshot

## What Gets Tracked

The profiler shows **allocations traced by `tracemalloc`**, attributed to the
file and line number that made them — not grouped by object class. This
points directly at the code responsible for a leak (`music_player.py:142`,
for example), rather than only naming the type of object involved.

`tracemalloc` has to be enabled (`tracemalloc.start()`, done automatically
when profiling is turned on) before it can trace anything; allocations made
before tracing started are invisible to it.

## Configuration

Enable memory profiling in your config file:

```yaml
general:
  monitoring:
    memory_profiling:
      enabled: true           # Enable memory profiling
      interval_seconds: 60    # How often to log snapshots (default: 60)
      top_n_lines: 25         # Number of lines to include (default: 25)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable/disable memory profiling |
| `interval_seconds` | integer | `60` | Interval between snapshots in seconds (minimum: 10) |
| `top_n_lines` | integer | `25` | Number of top allocation lines to include in each snapshot (minimum: 1) |

## Log Output

Memory snapshots are logged to the `memory_profiler` logger with INFO level:

```
**Memory Snapshot (tracemalloc)**

Top 25 allocation sites by current size:
#1: discord_gateway/cogs/music_helpers/music_player.py:142: 45.20 MB
    self._buffer.append(chunk)
#2: discord_core/utils/dispatch_queue.py:88: 12.10 MB
    pending[request_id] = payload
...

Top 25 allocation changes since last snapshot:
#1: discord_gateway/cogs/music_helpers/music_player.py:142: +3.40 MB
    self._buffer.append(chunk)
...
```