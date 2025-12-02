# Memory Profiling

The Discord bot includes a built-in memory profiler that periodically logs snapshots of Python object counts and memory usage. This is useful for identifying memory leaks and understanding memory consumption patterns.

## Overview

The memory profiler works by:
1. Using Python's garbage collector (`gc.get_objects()`) to enumerate tracked objects
2. Grouping objects by their class type
3. Calculating object counts and approximate memory usage
4. Logging periodic snapshots with the top classes by count and memory

## What Gets Tracked

The profiler shows **objects tracked by Python's garbage collector**, which includes:

- **User-defined class instances** (Discord messages, bot objects, caches, etc.)
- **Container types** (lists, dicts, sets)
- **Functions and classes**
- **Most objects that can participate in reference cycles**

The profiler does NOT track:
- Small integers (-5 to 256) - Python caches these
- Interned strings - Common strings Python reuses
- Simple immutable objects that can't form cycles
- C extension objects not exposed to Python's GC

This limitation is actually ideal for finding application-level memory leaks, as these are the "interesting" objects most likely to cause issues.

## Configuration

Enable memory profiling in your config file:

```yaml
general:
  monitoring:
    memory_profiling:
      enabled: true           # Enable memory profiling
      interval_seconds: 60    # How often to log snapshots (default: 60)
      top_n_classes: 50       # Number of classes to include (default: 50)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable/disable memory profiling |
| `interval_seconds` | integer | `60` | Interval between snapshots in seconds (minimum: 10) |
| `top_n_classes` | integer | `50` | Number of top classes to include in each snapshot (minimum: 1) |

## Log Output

Memory snapshots are logged to the `memory_profiler` logger with INFO level:

```
Memory Snapshot

Top 10 by object count:
  discord.message.Message: 1,234 objects
  dict: 5,678 objects
  list: 3,456 objects
  ...

Top 10 by memory usage:
  discord.message.Message: 45.2 MB
  list: 23.1 MB
  dict: 18.7 MB
  ...
```