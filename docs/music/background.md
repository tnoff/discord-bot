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

## The Six Background Loops

### 1. **Send Messages Loop** (`send_messages()`)

**Purpose**: Dispatch all Discord messages (progress updates, errors, queue displays)

**Key Responsibilities**:
- Process messages from `MessageQueue`
- Handle multi-mutable bundles (editable progress messages)
- Handle single immutable messages (one-off notifications)
- Execute message edit/delete/send operations
- Update message references for tracking

**Processing Flow**:
1. Call `message_queue.get_next_message()`
2. For **multi-mutable** messages:
   - Get bundle content via `bundle.print()`
   - Generate dispatch functions (edit/delete/send)
   - Execute all operations
   - Update message references
3. For **single immutable** messages:
   - Execute send function directly

**Shutdown Behavior**: Exits when shutdown flag is set AND no messages remain in queue

**Special Handling**:
- Continues on `DiscordServerError` (temporary Discord API issues)
- Allows 404 errors for message operations (message already deleted)

---

### 2. **Download Files Loop** (`download_files()`)

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

**Conditional**: Only runs if `enable_youtube_music_search` is `True`

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

### 5. **Cache Cleanup Loop** (`cache_cleanup()`)

**Purpose**: Remove old cached files when cache limit is exceeded

**Key Responsibilities**:
- Mark cache files for deletion (LRU eviction)
- Check if files are currently in use
- Delete files not in use
- Backup files to S3 (if configured)
- Update cache database

**Processing Flow**:
1. Call `video_cache.ready_remove()` to mark files for deletion (LRU)
2. Query database for files marked `delete_ready`
3. For each file:
   - Check if file is in `sources_in_transit` (currently being copied)
   - If not in use, add to delete list
4. Delete files via `video_cache.remove_video_cache()`
5. Query database for files without S3 backup
6. Backup files to S3 via `video_cache.object_storage_backup()`

**Conditional**: Only runs if `enable_cache` is `True`

**Shutdown Behavior**: Exits immediately when shutdown flag is set

**Safety**: Never deletes files currently in use (prevents race conditions)

### 6. **Playlist History Update Loop** (`playlist_history_update()`)

**Purpose**: Track playback history and analytics to database

**Key Responsibilities**:
- Record each played video to history playlist
- Update guild analytics (total plays, duration, cache hit rate)
- Create new playlist items
- Delete old items when history limit exceeded

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

**Queue Type**: Standard `Queue` (FIFO)

**Conditional**: Only runs if `db_engine` is configured (database required)

**Shutdown Behavior**: Exits when shutdown flag is set AND queue is empty

**Analytics Tracked**:
- Total plays per guild
- Total duration played
- Cache hit rate
- Last update timestamp

---

## Queue Systems

### Standard Queue (`Queue`)

**Used By**:
- `history_playlist_queue` (history updates)
- `single_immutable_queue` (one-off messages)

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

### Message Flow

```
User Command → MediaRequest Created
    ↓
YouTube Search Loop → Convert search to URL
    ↓
Download Loop → Download file
    ↓
Player Queue → Play audio
    ↓
History Loop → Record to database
```

### Message Updates

```
Download Loop updates bundle → Message Queue notified
    ↓
Send Messages Loop → Edit Discord message
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

Each loop updates a checkpoint file every iteration:

**Checkpoint Files**:
- `send_message_checkfile` - Send messages loop
- `cleanup_player_checkfile` - Cleanup players loop
- `cache_cleanup_checkfile` - Cache cleanup loop
- `download_file_checkfile` - Download files loop
- `playlist_history_checkfile` - Playlist history loop
- `youtube_search_checkfile` - YouTube search loop

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
