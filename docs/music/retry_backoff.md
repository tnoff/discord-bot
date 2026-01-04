# Download Retry Backoff System

## Overview

The download retry backoff system implements a simple adaptive rate-limiting strategy that automatically adjusts wait times between media downloads based on recent failure counts. When downloads fail, the system linearly increases backoff times based on the number of recent failures. As downloads succeed or failures age out, backoff times return to normal.

This system helps the bot recover gracefully from rate limiting, network issues, or other transient download problems without manual intervention.

## The Problem

When downloading videos from media via yt-dlp, several types of transient failures can occur:

- Network timeouts (`Read timed out.`)
- TLS handshake failures (`tlsv1 alert protocol version`)
- Other temporary service issues

Without adaptive backoff, the bot would continue downloading at its configured base rate even during periods of frequent failures. This can:
1. Waste bandwidth and CPU retrying downloads that will likely fail
2. Trigger more aggressive rate limiting from media
3. Delay successful downloads behind a queue of failing requests

The solution is to dynamically increase wait times when failures occur, then decrease them as successes occur or failures age out.

## The Algorithm

The retry backoff system uses a **simple counting approach** where the backoff multiplier is directly equal to the number of recent failures in the queue.

### Step 1: Track Failures and Successes

Each download result is tracked:
- **Failures**: Added to the queue with a timestamp
- **Successes**: Remove one item from the failure queue (if any exist)

Old failures (older than `max_age_seconds`) are automatically removed from the queue.

### Step 2: Calculate Backoff Factor

The backoff multiplier is simply the size of the failure queue:

```
backoff_multiplier = queue.size
```

Where:
- `queue.size` = number of failures currently in the queue

**Key properties**:
- When queue is empty (no recent failures): multiplier = 0 (no additional backoff)
- Each failure adds 1 to the multiplier
- Each success removes 1 from the multiplier
- Queue size is bounded by `max_size` parameter

### Step 3: Apply Backoff

The multiplier is multiplied by the base wait time to get additional backoff:

```
additional_backoff = base_wait × backoff_multiplier
total_wait = base_wait + additional_backoff
```

For example, with base_wait=30 seconds and 3 failures in queue:
```
additional_backoff = 30 × 3 = 90 seconds
total_wait = 30 + 90 = 120 seconds (2 minutes)
```

## Configuration Parameters

All parameters can be tuned via the bot configuration file.

### Core Parameters

**`youtube_wait_period_minimum`** (default: 30 seconds)
- Base wait time between media downloads
- The minimum delay regardless of failure count
- Located in: `music.download.youtube_wait_period_minimum`

### Failure Tracking Parameters

**`failure_tracking_max_size`** (default: 100)
- Maximum number of failures to track
- When this limit is reached, oldest failures are dropped to make room
- Also acts as the maximum backoff multiplier
- Larger values = higher potential backoff but more memory
- Located in: `music.download.failure_tracking_max_size`

**`failure_tracking_max_age_seconds`** (default: 300 seconds)
- Maximum age of failures to keep
- Failures older than this are automatically discarded
- Should typically be several times the base wait period
- Located in: `music.download.failure_tracking_max_age_seconds`

## Behavior Examples

Using default configuration (base_wait=30, max_size=100, max_age=300):

### Scenario 1: Single Recent Failure

```
Failures in queue: 1
Backoff multiplier: 1
Additional backoff: 30 × 1 = 30 seconds
Total wait: 30 + 30 = 60 seconds
```

A single recent failure doubles the wait time.

### Scenario 2: Multiple Recent Failures

```
Failures in queue: 5
Backoff multiplier: 5
Additional backoff: 30 × 5 = 150 seconds
Total wait: 30 + 150 = 180 seconds (3 minutes)
```

Each additional failure adds one base_wait period to the delay.

### Scenario 3: Successes Reduce Backoff

```
Initial state: 5 failures in queue
After 1 success: 4 failures in queue
After 2 successes: 3 failures in queue
```

Each successful download removes one failure from the queue, gradually reducing backoff.

### Scenario 4: Old Failures Age Out

```
Failures older than 300 seconds are automatically removed
Queue size decreases as failures age out
Backoff gradually returns to normal without manual intervention
```

### Scenario 5: Empty Queue

```
Failures in queue: 0
Backoff multiplier: 0
Additional backoff: 30 × 0 = 0 seconds
Total wait: 30 + 0 = 30 seconds (base wait)
```

With no recent failures, the system operates at normal speed.

### Scenario 6: Queue Full (Maximum Backoff)

```
Failures in queue: 100 (max_size reached)
Backoff multiplier: 100
Additional backoff: 30 × 100 = 3000 seconds (50 minutes)
Total wait: 30 + 3000 = 3030 seconds
```

Note: With default settings, maximum backoff is very high. Consider lowering `max_size` if this is too aggressive.

## Integration with Retry Logic

The backoff system integrates with the existing retry mechanism:

1. **Download Attempt**: When `DownloadClient.create_source()` is called
2. **Success Case**: If download succeeds, add success to queue (removes one failure)
3. **Failure Detection**: If a `RetryableException` is raised
4. **Track Failure**: Exception is added to `DownloadFailureQueue` as a failed `DownloadStatus`
5. **Calculate Backoff**: Queue size is used as the backoff multiplier
6. **Apply Delay**: Additional backoff is added: `base_wait × queue.size`
7. **Retry**: After delay, request is re-queued (if retries remain)

### Exception Types

The system only tracks **retryable** exceptions in the failure queue:

**Tracked (RetryableException)**:
- Network timeouts
- TLS handshake failures
- Bot detection warnings
- Unknown/transient errors

**Not Tracked (DownloadTerminalException)**:
- Age-restricted videos
- Private videos
- Videos removed for ToS violations
- Invalid format errors
- Permanently unavailable videos

Terminal exceptions don't contribute to backoff because retrying won't help.

## Automatic Cleanup

The `DownloadFailureQueue` automatically maintains itself:

**Time-based cleanup**:
- Every time a new item is added, old failures are purged
- Items older than `max_age_seconds` are removed
- Prevents unbounded growth and ensures recent data

**Size-based cleanup**:
- If queue exceeds `max_size`, oldest items are dropped
- Implemented as a circular buffer
- Ensures bounded memory usage

**Success-based cleanup**:
- Each successful download removes one failure from the queue
- Helps the system recover quickly from transient issues

## Monitoring and Tuning

### Observing Behavior

The backoff system can be monitored via:
- Queue size: `download_failure_queue.size` (this is also the current backoff multiplier)
- Recent failures: Inspect `download_failure_queue.queue.items()`

### Tuning Guidelines

**If backoff is too aggressive** (excessive wait times):
- Decrease `failure_tracking_max_size` (lower maximum multiplier)
- Increase `failure_tracking_max_age_seconds` (failures age out slower)
- Decrease `youtube_wait_period_minimum` (lower base wait)

**If backoff is too lenient** (not helping during failures):
- Increase `failure_tracking_max_size` (higher maximum multiplier)
- Decrease `failure_tracking_max_age_seconds` (failures age out faster)
- Increase `youtube_wait_period_minimum` (higher base wait)

**If system responds too slowly to changes**:
- Decrease `failure_tracking_max_age_seconds`

**Recommended settings for typical use**:
- Consider setting `failure_tracking_max_size` to 10-20 instead of default 100
- This caps maximum backoff at more reasonable levels (5-10 minutes instead of 50 minutes)

## Implementation Details

Located in `discord_bot/cogs/music_helpers/download_client.py`:

- **`DownloadStatus`**: Dataclass storing success/failure state, exception info, and timestamp
- **`DownloadFailureQueue`**: Manages the failure queue and backoff calculation
  - `add_item()`: Add new status (failure or success) with automatic cleanup
  - `size`: Property returning current queue size (= backoff multiplier)

Used in `discord_bot/cogs/music.py`:
- Created during Music cog initialization with config parameters
- Populated when download completes (success) or fails (`RetryableException`)
- Queried via `size` property to calculate additional backoff before retrying

## Mathematical Summary

The complete formula is straightforward:

```
Given:
  - F = set of recent failures (after age-based cleanup)
  - N = number of failures in F (queue size)
  - W = base_wait_seconds

Calculate:
  backoff_multiplier = N                    [queue size]
  additional_backoff = W × backoff_multiplier  [multiply by base wait]
  total_wait = W + additional_backoff       [final wait time]
```

This creates a system that:
- Responds immediately to new failures (queue size increases by 1)
- Recovers gradually as failures age out (automatic cleanup)
- Recovers quickly with successes (each success reduces queue by 1)
- Has bounded maximum backoff (capped at max_size)
- Self-maintains through automatic cleanup
- Is simple to understand and tune
