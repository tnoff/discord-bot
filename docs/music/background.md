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
  - Automatic restart on exceptions (unless shutdown)
  - Heartbeat monitoring via checkpoint files
  - Graceful shutdown handling
  - OpenTelemetry integration

**Shutdown** (`cog_unload()`):
1. `bot_shutdown` flag is set to `True`
2. All guild players are stopped
3. All background tasks are cancelled via `task.cancel()`
4. Loops detect shutdown and exit via `ExitEarlyException`

---

## Messaging

All Discord API calls are handled by the `MessageDispatcher` cog (`discord_bot/cogs/message_dispatcher.py`), which is loaded before the Music cog and shared across all cogs. It maintains one `asyncio.PriorityQueue` per guild and one lazy worker task per active guild — no dedicated send-messages background loop runs inside the Music cog.

See [messaging.md](./messaging.md) and [AGENTS.md](../../AGENTS.md#messagedispatcher) for details.

---

## The Four Background Loops

### 1. **Download Files Loop** (`download_files()`) — local mode

> This loop only runs in **local mode** (`remote_download_worker: false`). In remote worker mode the bot starts a **Redis Result Reader** loop instead (see below).

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

**Queue Type**: `DistributedQueue` (fair distribution across multiple guilds)

**Shutdown Behavior**: Exits immediately when shutdown flag is set

**Error Handling**:
- `ExistingFileException`: Video already cached, skip download
- `BotDownloadFlagged`: Video flagged/unavailable, mark as failed
- `DownloadClientException`: General download errors, retry or fail

---

### 2. **Redis Result Reader Loop** (`run_redis_result_reader()`) — remote mode only

**Purpose**: Receive completed `DownloadResult` objects from the remote download worker via Redis Streams

**Active when**: `remote_download_worker: true` in bot config

**Key Responsibilities**:
- Read from `discord_bot:download:result:<process_id>` via `xread_latest`
- Decode each message into a `DownloadResult`
- Push results to the local `_result_queue` for `process_download_results` to handle
- Track `_result_stream_last_id` to avoid reprocessing old messages

**Processing Flow**:
1. Raise `ExitEarlyException` if shutdown event is set
2. Call `xread_latest(stream_key, last_id)` (blocking read)
3. For each message: decode → put on `_result_queue`
4. Advance `_result_stream_last_id` to the last message ID seen
5. Loop

**Shutdown Behavior**: Exits on `ExitEarlyException` when shutdown event is set before or after read

---

### 3. **YouTube Music Search Loop** (`search_youtube_music()`)

**Purpose**: Convert text searches to YouTube video URLs using YouTube Music API

**Key Responsibilities**:
- Pull `MediaRequest` objects from `youtube_music_search_queue`
- Query YouTube Music API for best match
- Convert search results to YouTube video URLs
- Check cache for converted URLs
- Hand off to download queue

**Processing Flow**:
1. Get next `(MediaRequest, channel)` from `youtube_music_search_queue.get_nowait()`
2. Call `search_client.search_youtube_music()` with search string
3. If result found:
   - Convert to YouTube URL format (`https://youtube.com/watch?v=...`)
   - Update `media_request.search_string` with URL
4. Check cache via `_enqueue_media_download_from_cache()`
5. If not cached, add to `download_queue`
6. Update bundle status to `QUEUED`


**Queue Type**: `DistributedQueue` (10x larger than download queue due to lightweight operations)

**Shutdown Behavior**: Exits immediately when shutdown flag is set

**Why Separate Loop?**:
- API searches are fast (~100-500ms)
- Downloads are slow (~30s+ backoff)
- Allows batching searches while downloads happen in parallel
- Prevents search delays from blocking downloads

---

### 4. **Cleanup Players Loop** (`cleanup_players()`)

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

### 4. **Post-Play Processing Loop** (`post_play_processing()`)

**Purpose**: Record playback history/analytics to database and run cache cleanup after each track finishes

**Key Responsibilities**:
- Record each played video to the guild's history playlist
- Update guild analytics (total plays, duration, cache hit rate)
- Evict stale cached files via `MediaBroker.cache_cleanup()`
- Delete old history items when the playlist limit is exceeded

**Processing Flow**:
1. Get next `history_item` from `history_playlist_queue.get_nowait()`
2. Update guild analytics:
   - Increment `total_plays`
   - Add to `total_duration_seconds`
   - Increment `cached_plays` if cache hit
   - Update `updated_at` timestamp
3. Skip if video was originally played from history (prevent duplicates)
4. Get or create history playlist for guild
5. Add video to playlist via `__playlist_insert_item()`
6. If history playlist is full, delete oldest item
7. Call `media_broker.cache_cleanup()` to evict any files now safe to remove

**Queue Type**: Standard `Queue` (FIFO)

**Conditional**: Only runs if `db_engine` is configured (database required)

**Shutdown Behavior**: Exits when shutdown flag is set AND queue is empty

**Analytics Tracked**:
- Total plays per guild
- Total duration played
- Cache hit rate
- Last update timestamp

---

---

## Standalone Worker Loops

These loops run inside `discord-bot-download-worker`, not inside the Music cog.

### **Redis Feeder Loop** (`run_redis_feeder()`)

**Purpose**: Feed the local `DownloadClient` input queue from the Redis Stream

**Key Responsibilities**:
- Read `MediaRequest` items from `discord_bot:download:input` via `XREADGROUP` (consumer group `discord_bot:download:workers`)
- Skip items whose guild is currently blocked (rate-limited)
- Submit items to the local `_input_queue` via `DownloadClient.submit()`
- Acknowledge processed messages

**Shutdown Behavior**: Raises `ExitEarlyException` when shutdown event is set

### **Worker Run Loop** (`run()`)

**Purpose**: Process items from the local input queue, download them, and publish results

**Key Responsibilities**:
- Pull `MediaRequest` items from `_input_queue`
- Apply backoff / wait period
- Execute yt-dlp download via `create_source()`
- Publish `DownloadResult` to `discord_bot:download:result:<process_id>` via `xadd`
- Handle retries and permanent failures

**Shutdown Behavior**: Raises `ExitEarlyException` when shutdown event is set mid-wait or between iterations

---

## Queue Systems

### Standard Queue (`Queue`)

**Used By**:
- `history_playlist_queue` (post-play processing)

**Behavior**:
- FIFO (First In, First Out)
- Single queue for all items
- Simple get/put operations
- Supports blocking/unblocking

### Distributed Queue (`DistributedQueue`)

**Used By**:
- `download_queue` (media downloads)
- `youtube_music_search_queue` (searches)

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

### Message Flow — local mode

```
User Command → MediaRequest Created
    ↓
YouTube Search Loop → Convert search to URL
    ↓
Download Loop → Download file
    ↓
Player Queue → Play audio
    ↓
Post-Play Processing Loop → Record to database + cache cleanup
```

### Message Flow — remote worker mode

```
User Command → MediaRequest Created
    ↓
YouTube Search Loop → submit_to_redis() → discord_bot:download:input (Redis Stream)
    ↓
                    [remote discord-bot-download-worker process]
                    Redis Feeder Loop → DownloadClient.run()
                    → DownloadResult written to discord_bot:download:result:<id>
    ↓
Redis Result Reader Loop → _result_queue
    ↓
process_download_results() → Player Queue → Play audio
    ↓
Post-Play Processing Loop → Record to database + cache cleanup
```

### Message Updates

```
Download Loop updates bundle → MessageDispatcher notified
    ↓
MessageDispatcher worker (per-guild) → Edit Discord message
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

Each loop reports a heartbeat via an OpenTelemetry observable gauge every iteration:

**Heartbeat gauges** (all use `MetricNaming.HEARTBEAT`, tagged by `AttributeNaming.BACKGROUND_JOB`):
- `cleanup_player` - Cleanup players loop
- `download_file` - Download files loop
- `youtube_search` - YouTube Music search loop
- `post_play_processing` - Post-play processing loop (database + cache cleanup)

**Purpose**:
- Monitor loop health via OpenTelemetry metrics
- Detect stuck loops (no heartbeat updates)
- Alert on loop failures

**Format**: Timestamp integer written to file

---

## Error Handling Strategies

### Continue on Error
**Send Messages Loop**:
- Continues on `DiscordServerError` (temporary API issues)
- Allows Discord to recover without restarting loop

### Exit on Error
**Download Loop**:
- Exits on `BotDownloadFlagged` (permanent failures)
- Specific errors logged and bundle updated

### Graceful Skip
**Cache Cleanup Loop**:
- Skips files in use
- Continues to next file
- Prevents partial cleanup failures

### Exit Early Pattern
All loops check `bot_shutdown` flag and raise `ExitEarlyException` to exit cleanly.
