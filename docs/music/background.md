# Discord Bot Music Background Loops - Architecture Explainer

## Overview

The music system operates through independent background loops that run continuously throughout the bot's lifecycle. Each loop handles a specific responsibility, working together to provide seamless music playback, caching, and user interaction.

All loops run asynchronously and are managed by the Discord bot's event loop. They are started during `cog_load()` and gracefully shut down during `cog_unload()`.

---

## Background Loop Architecture

### Loop Lifecycle

**Startup** (`cog_load()`):
- All loops are created as asyncio tasks using `bot.loop.create_task()`
- Each loop is wrapped with `return_loop_runner()` which provides:
  - Retry-forever on unexpected exceptions, with backoff doubling 1 s → 30 s and
    resetting on the next success. The loop never exits on error, so it recovers
    on its own once a failing dependency comes back
  - Health reporting via `LoopHealth`, which drives both the loop's heartbeat
    gauge and the process's health-server probe — see
    [Background Loop Health](../monitoring/loop_health.md)
  - Graceful shutdown handling
  - OpenTelemetry integration

**Shutdown** (`cog_unload()`):
1. `bot_shutdown` flag is set to `True`
2. All guild players are stopped
3. All background tasks are cancelled via `task.cancel()`
4. Loops detect shutdown and exit via `ExitEarlyException`

---

## Messaging

All Discord API calls are handled by `MessageDispatcher`, the sole worker of the separate `discord-dispatcher` pod (`discord_dispatcher/workers/message_dispatcher.py`) — not a cog loaded alongside Music. It is Redis-backed (one sorted-set queue per guild, drained by `BZPOPMIN` workers) rather than an in-process `asyncio.PriorityQueue`, and the bot reaches it over HTTP via `HttpDispatchClient` (`discord_core/clients/http_dispatch_client.py`); `dispatch_http_url` is a required config value, with no in-process fallback. No dedicated send-messages background loop runs inside the Music cog.

See [messaging.md](../messaging.md) and [AGENTS.md](../../AGENTS.md#messagedispatcher-is-a-separate-pod-not-a-cog) for details.

---

## The Six Background Loops, Across Four Pods

Two of these (Download Files, YouTube Music Search) run in their own
standalone pods, not the bot, and the History Worker runs in the broker pod;
the other three run in the gateway pod (`discord_gateway/cogs/music.py`) and
are what the old "Four Background Loops" title here used to mean, before the
rest moved out.

### 1. **Download Files Loop** (`download_files()`)

> **Where it runs**: the standalone downloader pod, not the bot. The cog used to
> drive this loop itself when no `music.download_client` was configured; that
> in-process path is gone (`projects/discord-bot-ha-only`), so the bot submits
> over HTTP and consumes results, and everything below describes the pod.

**Purpose**: Download media files from YouTube using yt-dlp

**Key Responsibilities**:
- Pull `MediaRequest` objects from `download_queue`
- Wait for YouTube rate limiting (backoff period)
- Execute yt-dlp downloads
- Process downloaded files (audio normalization if enabled)
- Copy files to guild-specific directories
- Add to cache database (if caching enabled)
- Enqueue to player queue or save to playlist

**Processing Flow**:
1. Get next `MediaRequest` from `download_queue.get_nowait()`
2. Check if player still exists (might have disconnected)
3. Check cache first via `__check_video_cache()`
4. If not cached:
   - Update bundle status to `BACKOFF`
   - Wait for YouTube backoff period (default: 30s + 0-10s variance)
   - Update bundle status to `IN_PROGRESS`
   - Download via `download_client.download()`
   - Process file via `ready_file()` (copy to guild directory)
5. Add to cache database (if enabled)
6. Enqueue to player or add to playlist
7. Update bundle status to `COMPLETED` or `FAILED`

**Queue Type**: per-guild Redis fair-distribution queue (`discord_core/workers/redis_guild_queue.py`) — not `DistributedQueue`, which no longer exists in production (see [Queue Systems](#queue-systems) below)

**Shutdown Behavior**: Exits immediately when shutdown flag is set

**Error Handling**:
- `ExistingFileException`: Video already cached, skip download
- `BotDownloadFlagged`: Video flagged/unavailable, mark as failed
- `DownloadClientException`: General download errors, retry or fail

---

### 2. **YouTube Music Search Loop**

> **Where it runs**: the standalone search pod (`discord-search`), not the
> bot. Like the Download Files Loop above, the cog used to drive this
> in-process; that path is gone (`projects/discord-bot-ha-only`). The
> current implementation is `YoutubeMusicSearchDriver`
> (`discord_search/workers/youtube_music_search_driver.py`) driving
> `RedisYoutubeMusicSearchWorker`
> (`discord_search/workers/redis_youtube_music_search_worker.py`), not a
> bare `search_youtube_music()` function — the Processing Flow below is the
> conceptual shape; check those two files for the exact current mechanics
> rather than the numbered steps here.

**Purpose**: Convert text searches to YouTube video URLs using YouTube Music API

**Key Responsibilities**:
- Pull queued search requests (per-guild Redis work, not an in-process queue)
- Query YouTube Music API for best match
- Convert search results to YouTube video URLs
- Hand off resolved requests to the broker, for the bot to pick up via `process_search_results`

**Rate-limit backoff**: A 429 arms a backoff window of `youtube_wait_period_minimum
* 2**failures`. The loop waits that window out **before** popping, and only for
`_SEARCH_BACKOFF_SLICE_SECONDS` per iteration:

- Waiting before the pop means no request is held in pod memory across the
  window — under the Redis-backed worker the pop DELetes the request, so a
  restart mid-wait would lose it outright.
- Slicing keeps each iteration short enough to re-arm
  [loop health](../monitoring/loop_health.md); the full window outgrows the
  staleness default after four failures, and a loop that never returns inside
  its window fails the livenessProbe — restarting the pod over a rate limit that
  a restart cannot fix (and which, under Redis, is shared with every other
  search pod anyway).

**Shutdown Behavior**: Exits immediately when shutdown flag is set

**Why Separate Loop?**:
- API searches are fast (~100-500ms)
- Downloads are slow (~30s+ backoff)
- Allows batching searches while downloads happen in parallel
- Prevents search delays from blocking downloads

---

### 3. **Process Search Results Loop** (`process_search_results()`)

**Where it runs**: the bot (gateway) pod — this is the bot-side tail of
search resolution: cache-check then download submit, which can only run
where the download client and cache live. Mirrors Process Download Results
Loop below.

**Purpose**: Pick up resolved YouTube Music searches from the broker and
either serve them from cache or hand them to the download pipeline.

**Key Responsibilities**:
- Poll the broker for the next resolved search result (`broker_client.next_search_result()`)
- Push a `QUEUED` lifecycle event for the media request
- Check the video cache; on a hit, the request is already marked `COMPLETED`
- On a cache miss, submit the request to the downloader
  (`download_client.submit()`), respecting per-guild queue priority
- On `PutsBlocked`/`QueueFull` (the downloader answering, not failing to
  answer), mark the request `DISCARDED`
- On any other error (most often the downloader pod being unreachable
  mid-rollout), requeue the search result to the broker and re-raise, so the
  loop runner's backoff applies rather than busy-spinning against a pod
  that is still down

**Shutdown Behavior**: Raises `ExitEarlyException` when the shutdown flag is set

### 4. **Process Download Results Loop** (`process_download_results()`)

**Where it runs**: the bot (gateway) pod.

**Purpose**: Pick up completed (or permanently failed) downloads from the
broker and route them to a player or a playlist handler.

**Key Responsibilities**:
- Poll the broker for the next finished result (`broker_client.next_result()`)
- On a terminal failure, distinguish a rejection (video declined — too
  long, banned, private, age-restricted) from a genuine fault; only genuine
  faults mark the span ERROR, so ordinary user input doesn't page on the
  Consumer Span Error Rate alert
- On success, hand the media off to the player queue or, for a playlist
  add, to the playlist handler
- Retryable errors are handled inside the downloader's own worker; only
  successes and terminal failures ever reach this loop

**Shutdown Behavior**: Raises `ExitEarlyException` when the shutdown flag is set

---

### 5. **Cleanup Players Loop** (`cleanup_players()`)

**Purpose**: Disconnect bot from voice channels with no members

**Key Responsibilities**:
- Check all active guild players
- Detect empty voice channels (no human members)
- Send disconnect notification
- Trigger player cleanup
- Remove player from active players dictionary

**Processing Flow**:
1. Iterate through all `self.players`
2. For each player, call `player.voice_channel_inactive()`
3. If channel is empty:
   - Queue notification message: "No members in guild, removing myself"
   - Set `player.shutdown_called = True`
   - Add guild to cleanup list
4. After iteration, cleanup all flagged guilds via `cleanup(guild)`

**Shutdown Behavior**: Exits immediately when shutdown flag is set

**Two-Phase Processing**:
- Phase 1: Identify inactive players (while iterating)
- Phase 2: Cleanup players (separate loop)
- Prevents dictionary size change during iteration

---

### 6. **History Worker** (`HistoryWorker`, broker pod)

**Purpose**: Record each track that played out to the database. It replaces the
gateway's old post-play processing loop: the queue (and with it the knowledge
that a track finished) moved into the broker, so recording moved with it.

**Key Responsibilities**:
- Count the play in the guild's analytics (total plays, total duration, cache hits)
- Add the track to the guild's history playlist (created on first use)
- Trim the history playlist when it exceeds its limit

**Processing Flow**:
1. `finish_track` (not skipped) pushes a play record onto the `history_events`
   list in Redis as the track ends
2. The worker takes the next record and validates it (a record with no URL can
   never be stored, so it is dropped rather than retried)
3. `record_play`, `ensure_history_playlist` and `record_history_item` go to the
   db pod over HTTP

**Failure Handling**: Playback never waits on this. A db that is slow or down delays
the history, not the music: records wait in Redis and are retried (up to 5 attempts
for errors that say the db could not answer; anything else is logged and dropped).
Delivery is at-most-once across a crash between taking a record and finishing it.

**Conditional**: Only runs in the broker pod, with a db pod configured

**Heartbeat**: `history_worker`; queue depth is reported as `history_event_queue_depth`

---

---

## Queue Systems

### Per-Guild Player Queue (broker)

The queue the player plays from is not in the gateway at all. The broker keeps it
in Redis, per guild, next to the guild's play history, its now-playing record and
its play-order message (`discord_broker/workers/guild_queue.py`, with the Redis
side in `guild_queue_registry.py`). The gateway reaches it through
`broker_client` (`enqueue_track`, `claim_next_track`, `finish_track`,
`get_guild_queue`, `remove_queued_track`, `bump_queued_track`, `shuffle_queue`,
`clear_queue`, `open_guild`, `close_guild`).

**Behavior**:
- FIFO, one queue per guild, with a size limit (`queue_max_size`)
- A claim takes a track off the queue and marks it playing; it is parked until
  confirmed, so a gateway that dies mid-claim does not lose the track
- The playing record is kept alive by the player's heartbeat and lapses after 15s
  without one
- `open_guild` is the handoff between gateways: it reopens the guild, records the
  text channel, and puts a track the previous gateway started and never finished
  back at the head of the queue (once its heartbeat has lapsed)
- Every change re-renders the play-order message, so the gateway no longer does
- A graceful restart leaves the queue as it is; any other cleanup closes the guild

### Per-Guild Fair-Distribution Queue

There is no `DistributedQueue` class any more — it survives only as a test
double (`tests/fakes/distributed_queue.py`). The real implementation is
`discord_core/workers/redis_guild_queue.py` plus pod-specific
Asyncio/Redis worker variants in `discord_downloader`/`discord_search` (see
[terminology.md](terminology.md#distributedqueue--retired)). The
fair-distribution and priority-scheduling model below describes the design
intent this queue class shared with its predecessor; verify against
`redis_guild_queue.py` directly for exact current behavior rather than
trusting this section's bullets line-for-line.

**Used By**:
- Media downloads, in the `discord-downloader` pod
- YouTube Music searches, in the `discord-search` pod

**Behavior**:
- One queue per guild
- Fair distribution across guilds
- Priority-based scheduling
- Automatic queue cleanup when empty

**Key Features**:

1. **Fair Guild Distribution**:
   - Each guild gets its own queue
   - Oldest unprocessed guild is served first
   - Prevents one guild from monopolizing resources

2. **Priority System**:
   - Guilds can have different priorities (configured in settings)
   - Higher priority guilds are served first
   - Falls back to oldest timestamp for same priority

3. **Automatic Cleanup**:
   - Removes empty guild queues
   - Reduces memory usage
   - Prevents queue dictionary bloat

**Example**:
```
Guild A: [req1, req2, req3] (priority: 100, last served: 10:00:00)
Guild B: [req4, req5]       (priority: 100, last served: 10:00:05)
Guild C: [req6]             (priority: 200, last served: 10:00:10)

Next item served: req6 (Guild C - highest priority)
Then:            req1 (Guild A - oldest timestamp, same priority as B)
Then:            req4 (Guild B - now oldest timestamp)
```

---

## Loop Coordination

### Message Flow

```
User Command → MediaRequest Created
    ↓
YouTube Music Search Loop (discord-search pod) → Convert search to URL
    ↓
Process Search Results Loop (bot pod) → cache check, submit to downloader
    ↓
Download Files Loop (discord-downloader pod) → Download file
    ↓
Process Download Results Loop (bot pod) → hand off to player
    ↓
Player Queue → Play audio
    ↓
Post-Play Processing Loop (bot pod) → Record to database + cache cleanup
```

### Message Updates

```
Process Download/Search Results Loop updates bundle → dispatcher pod notified over HTTP
    ↓
MessageDispatcher worker (per-guild, discord-dispatcher pod) → Edit Discord message
```

### Shutdown Coordination

```
cog_unload() called
    ↓
bot_shutdown = True
    ↓
All players shutdown
    ↓
Queues stop accepting new items
    ↓
Loops process remaining items
    ↓
Loops exit via ExitEarlyException
    ↓
Tasks cancelled
```

---

## Heartbeat Monitoring

Each loop reports its health through an OpenTelemetry observable gauge
(`MetricNaming.HEARTBEAT`, tagged by `AttributeNaming.BACKGROUND_JOB`):

| `background_job` | Loop |
|---|---|
| `cleanup_players` | Inactive player cleanup |
| `download_files` | Audio downloads — **downloader pod only**; the cog registers no such loop and emits no series |
| `process_download_results` | Download result routing |
| `process_search_results` | Resolved-search consumer |
| `youtube_music_search` | YouTube Music search — **search pod only**; the cog registers no such loop |
| `history_worker` | Play history/analytics recording — **broker pod only**; the cog registers no such loop |

**Value**: `1` while the loop is completing iterations, `0` once it has gone its
staleness window without a successful one. This is loop *health*, not task
liveness — the task stays alive through failures so it can recover, and reports
`0` in the meantime. An idle loop polling an empty queue is healthy.

The same value backs the health server's probe, so a wedged loop both fires the
alert and fails the pod's liveness check. See
[Background Loop Health](../monitoring/loop_health.md).

---

## Error Handling Strategies

### Continue on Error
**`MessageDispatcher`'s worker** (`discord-dispatcher` pod, not a bot-side loop):
- Continues on `DiscordServerError` (temporary API issues)
- Allows Discord to recover without restarting the worker

### Exit on Error
**Download Files Loop** (`discord-downloader` pod):
- Exits on `BotDownloadFlagged` (permanent failures)
- Specific errors logged and bundle updated

### Graceful Skip
**Post-Play Processing Loop** (bot pod — cache cleanup is folded into this loop, not a separate one):
- Skips files in use
- Continues to next file
- Prevents partial cleanup failures

### Exit Early Pattern
All loops check `bot_shutdown` flag and raise `ExitEarlyException` to exit cleanly.
