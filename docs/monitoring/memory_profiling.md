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

## Usage Examples

### Finding Memory Leaks

1. **Enable profiling** with a reasonable interval (60-300 seconds)
2. **Run your bot** through normal operations
3. **Watch the snapshots** over time
4. **Look for classes that grow** without bound

If you see a class steadily increasing in count/memory over time without corresponding decreases, you may have a leak.

### Performance Tuning

The memory profiler has minimal overhead but can impact performance if configured too aggressively:

- **Reduce overhead**: Increase `interval_seconds` (e.g., 300 for 5 minutes)
- **Reduce metric cardinality**: Decrease `top_n_classes` (e.g., 20-25)
- **Disable when not needed**: Set `enabled: false` when not investigating issues

### Querying Logs

If you're using an OTLP log backend like Loki, you can query memory snapshots:

**Find all snapshots:**
```logql
{job="discord-bot", logger="memory_profiler"} |= "Memory Snapshot"
```

**Extract object counts:**
```logql
{job="discord-bot", logger="memory_profiler"}
  |= "Top 10 by object count"
```

**Extract memory usage:**
```logql
{job="discord-bot", logger="memory_profiler"}
  |= "Top 10 by memory usage"
```

## How It Works

The memory profiler runs in a background daemon thread that:

1. Calls `gc.collect()` to ensure accurate counts
2. Iterates through `gc.get_objects()` to enumerate all tracked objects
3. Groups objects by their fully-qualified class name (e.g., `discord.message.Message`)
4. Calculates counts and uses `sys.getsizeof()` for memory estimates
5. Formats and logs the top N classes by count and memory
6. Sleeps until the next interval

**Note**: Memory sizes are approximate as `sys.getsizeof()` only measures the object itself, not referenced objects.

## Common Patterns

### Discord Message Accumulation

If you see `discord.message.Message` counts growing:
- Check if you're caching messages unnecessarily
- Ensure message caches have size limits
- Verify old messages are being garbage collected

### List/Dict Growth

Large numbers of lists or dicts often indicate:
- Data structures without cleanup
- Caches that aren't being pruned
- Event handlers accumulating state

### Custom Class Leaks

Your own classes appearing in large numbers suggest:
- Missing cleanup in event handlers
- Circular references preventing GC
- Objects stored in global caches without expiration

## Troubleshooting

### High Memory Usage from Profiler

The profiler itself uses memory and CPU when running. If it's causing issues:

```yaml
general:
  monitoring:
    memory_profiling:
      enabled: true
      interval_seconds: 300  # Run less frequently
      top_n_classes: 20      # Track fewer classes
```

### Missing Snapshots

If snapshots aren't appearing in your logs:

1. Verify `memory_profiling.enabled: true` in config
2. Check the `memory_profiler` logger is configured
3. Ensure log level is INFO or lower
4. Check bot logs for startup message: "Main :: Starting memory profiler"

### Understanding Memory Growth

Memory growth isn't always a leak:
- **Normal**: Gradual growth that plateaus (caches filling up)
- **Leak**: Continuous unbounded growth over time
- **Spike**: Sudden jumps (batch operations, large downloads)

Use the profiler over extended periods (hours/days) to identify true leaks.

## See Also

- [OTLP Configuration](otlp_configuration.md) - Configure OpenTelemetry and logging
- [Metrics Reference](metrics_reference.md) - Available OTLP metrics
- [Monitoring Overview](README.md) - Main monitoring documentation
