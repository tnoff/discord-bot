# Download Retry Backoff System

## Overview

The download retry backoff system implements an adaptive rate-limiting strategy that automatically adjusts wait times between media downloads based on recent failure patterns. When downloads fail frequently, the system exponentially increases backoff times to reduce load and avoid triggering additional rate limits. As failures become less frequent or age out, backoff times gradually return to normal.

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

The solution is to dynamically increase wait times when failures occur, then gradually decrease them as the failure rate subsides.

## The Algorithm

The retry backoff system uses **exponential decay scoring** to weight recent failures more heavily than old ones, combined with an **S-curve transformation** to smoothly scale from normal operation (1x) to maximum backoff.

### Step 1: Calculate Failure Score

Each download failure is recorded with a timestamp. When calculating backoff, the system computes a weighted score where recent failures contribute more than old ones:

```
score = Σ exp(-age_i / τ)
```

Where:
- `age_i` = time since failure i occurred (in seconds)
- `τ` (tau) = decay time constant (configurable via `decay_tau_seconds`)

**Key insight**: The exponential decay `exp(-age/τ)` means:
- A brand new failure (age=0) contributes 1.0 to the score
- After τ seconds, contribution drops to ~37% (exp(-1) ≈ 0.368)
- After 3τ seconds, contribution drops to ~5% (exp(-3) ≈ 0.05)

This creates a sliding window where failures naturally "age out" over time.

### Step 2: Transform Score to Backoff Factor

The failure score is transformed into a backoff multiplier using an S-curve:

```
factor = 1.0 + (max_factor - 1.0) × (1 - exp(-k × score))
```

Where:
- `max_factor` = maximum backoff multiplier (configured via `failure_rate_threshold`)
- `k` = aggressiveness parameter (configured via `failure_tracking_backoff_aggressiveness`)
- `score` = weighted failure score from step 1

**Key properties of this function**:
- When score=0 (no failures): factor = 1.0 (no additional backoff)
- As score increases: factor smoothly approaches max_factor
- Never exceeds max_factor (capped)
- The `k` parameter controls how quickly the curve rises:
  - Higher k = more aggressive, reaches max faster
  - Lower k = more gradual, tolerates more failures before hitting max

### Step 3: Apply Backoff

The calculated factor is multiplied by the base wait time:

```
total_wait = base_wait + (base_wait × factor)
```

For example, with base_wait=300 seconds and factor=2.26:
```
total_wait = 300 + (300 × 2.26) = 978 seconds (~16 minutes)
```

## Configuration Parameters

All parameters can be tuned via the bot configuration file.

### Core Parameters

**`youtube_wait_period_minimum`** (default: 300 seconds)
- Base wait time between media downloads
- The minimum delay regardless of failure rate
- Located in: `music.download.youtube_wait_period_minimum`

**`failure_rate_threshold`** (default: 3.0)
- Maximum backoff multiplier
- Caps the backoff factor to prevent excessive delays
- With default value, maximum wait = base_wait × 3.0
- Located in: `music.download.failure_rate_threshold`

### Failure Tracking Parameters

**`failure_tracking_max_size`** (default: 100)
- Maximum number of failures to track
- Older failures are dropped when limit reached
- Larger values = more memory but better long-term trends
- Located in: `music.download.failure_tracking_max_size`

**`failure_tracking_max_age_seconds`** (default: 300 seconds)
- Maximum age of failures to keep
- Failures older than this are automatically discarded
- Should typically match or exceed the base wait period
- Located in: `music.download.failure_tracking_max_age_seconds`

**`failure_tracking_decay_tau_seconds`** (default: 75 seconds)
- Time constant for exponential decay (τ in the formula)
- Controls how quickly old failures lose influence
- Smaller values = failures age out faster
- Rule of thumb: set to 1/3 to 1/5 of max_age_seconds
- Located in: `music.download.failure_tracking_decay_tau_seconds`

**`failure_tracking_backoff_aggressiveness`** (default: 1.0)
- Controls steepness of the S-curve (k in the formula)
- Higher values = reach max backoff with fewer failures
- Lower values = more gradual response to failures
- Typical range: 0.5 (gentle) to 2.0 (aggressive)
- Located in: `music.download.failure_tracking_backoff_aggressiveness`

## Behavior Examples

Using default configuration (base_wait=300, τ=75, max_factor=3.0, k=1.0):

### Scenario 1: Single Recent Failure

```
Failures: 1 failure just now (age=0)
Score: exp(-0/75) = 1.0
Factor: 1 + (3-1) × (1 - exp(-1.0 × 1.0)) = 1 + 2 × 0.632 ≈ 2.26
Wait: 300 × 2.26 ≈ 678 seconds (~11 minutes)
```

A single recent failure causes moderate backoff.

### Scenario 2: Multiple Rapid Failures

```
Failures: 5 failures in last 30 seconds
Score: exp(-0/75) + exp(-10/75) + exp(-20/75) + exp(-30/75) + exp(-40/75)
     = 1.0 + 0.875 + 0.766 + 0.670 + 0.586 ≈ 3.90
Factor: 1 + 2 × (1 - exp(-1.0 × 3.90)) = 1 + 2 × 0.980 ≈ 2.96
Wait: 300 × 2.96 ≈ 888 seconds (~15 minutes)
```

Multiple rapid failures quickly approach maximum backoff.

### Scenario 3: Old Failures (Decayed)

```
Failures: 5 failures, but all 225 seconds ago (3 × τ)
Score: 5 × exp(-225/75) = 5 × exp(-3) = 5 × 0.05 ≈ 0.25
Factor: 1 + 2 × (1 - exp(-1.0 × 0.25)) = 1 + 2 × 0.221 ≈ 1.44
Wait: 300 × 1.44 ≈ 432 seconds (~7 minutes)
```

Old failures have minimal impact, allowing the system to recover.

### Scenario 4: Empty Queue

```
Failures: None
Score: 0.0
Factor: 1.0
Wait: 300 × 1.0 = 300 seconds (base wait)
```

With no recent failures, the system operates at normal speed.

## Integration with Retry Logic

The backoff system integrates with the existing retry mechanism:

1. **Download Attempt**: When `DownloadClient.create_source()` is called
2. **Failure Detection**: If a `RetryableException` is raised
3. **Track Failure**: Exception is added to `DownloadFailureQueue`
4. **Calculate Backoff**: `get_backoff_multiplier()` computes current factor
5. **Apply Delay**: Additional backoff is added: `base_wait × (factor - 1.0)`
6. **Retry**: After delay, request is re-queued (if retries remain)

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
- Every time a new failure is added, old items are purged
- Items older than `max_age_seconds` are removed
- Prevents unbounded growth and ensures recent data

**Size-based cleanup**:
- If queue exceeds `max_size`, oldest items are dropped
- Implemented as a circular buffer
- Ensures bounded memory usage

## Monitoring and Tuning

### Observing Behavior

The backoff system can be monitored via:
- Queue size: `download_failure_queue.size`
- Current multiplier: `download_failure_queue.get_backoff_multiplier()`
- Recent failures: Inspect `download_failure_queue.queue.items()`

### Tuning Guidelines

**If backoff is too aggressive** (excessive wait times):
- Decrease `failure_tracking_backoff_aggressiveness` (k)
- Increase `failure_tracking_decay_tau_seconds` (τ)
- Decrease `failure_rate_threshold` (max_factor)

**If backoff is too lenient** (not helping during failures):
- Increase `failure_tracking_backoff_aggressiveness` (k)
- Decrease `failure_tracking_decay_tau_seconds` (τ)
- Increase `failure_rate_threshold` (max_factor)

**If system responds too slowly to changes**:
- Decrease `failure_tracking_max_age_seconds`
- Decrease `failure_tracking_decay_tau_seconds` (τ)

**If system is too jittery** (wait times fluctuate rapidly):
- Increase `failure_tracking_decay_tau_seconds` (τ)
- Decrease `failure_tracking_backoff_aggressiveness` (k)

## Implementation Details

Located in `discord_bot/cogs/music_helpers/download_client.py`:

- **`DownloadFailureMode`**: Dataclass storing exception type, message, and timestamp
- **`DownloadFailureQueue`**: Manages the failure queue and computes backoff
  - `add_item()`: Add new failure with automatic cleanup
  - `get_backoff_multiplier()`: Calculate current backoff factor
  - `size`: Property returning current queue size

Used in `discord_bot/cogs/music.py`:
- Created during Music cog initialization with config parameters
- Populated when `RetryableException` is caught in download loop
- Queried to calculate additional backoff before retrying downloads

## Mathematical Summary

The complete formula chain:

```
Given:
  - F = set of failures with ages {age_1, age_2, ..., age_n}
  - τ = decay_tau_seconds
  - k = aggressiveness
  - M = max_backoff_factor
  - W = base_wait_seconds

Calculate:
  score = Σ exp(-age_i / τ)                    [exponential decay sum]
  factor = 1 + (M - 1) × (1 - exp(-k × score)) [S-curve transformation]
  additional_backoff = W × (factor - 1)        [convert to time]
  total_wait = W + additional_backoff          [final wait time]
```

This creates a system that:
- Responds quickly to new failures (immediate score increase)
- Recovers gradually as failures age (exponential decay)
- Never exceeds safe limits (capped at max_factor)
- Self-maintains through automatic cleanup
