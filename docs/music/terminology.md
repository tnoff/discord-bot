# Music System Terminology

## Overview

This document defines all key components, types, and concepts used throughout the music system. Refer to this as a reference when reading other documentation.

## Core Components

### **`MusicPlayer`**
- Per-guild music player instance that manages playback
- Maintains its own play queue (`_play_queue`) and history queue
- Runs a background loop (`player_loop()`) that continuously plays queued tracks
- Handles voice connection, and track transitions
- One instance per Discord server (guild) that has active music playback
- Located in `discord_bot/cogs/music_helpers/music_player.py`

### **`MediaRequest`**
- Represents a user's request to play a song
- Contains: search string, requester info, guild/channel IDs, search type
- Immutable - created once and passed through the system
- Has a unique UUID for tracking through the pipeline
- Can be linked to a `MultiMediaRequestBundle` via `bundle_uuid`
- Located in `discord_bot/cogs/music_helpers/media_request.py`

### **`MediaDownload`**
- Represents a successfully downloaded audio file
- Contains: file path, metadata (title, uploader, duration), yt-dlp data
- Created after yt-dlp downloads complete
- Added to `MusicPlayer._play_queue` for playback
- Deleted from disk after track finishes playing
- Located in `discord_bot/cogs/music_helpers/media_download.py`

### **`MultiMediaRequestBundle`**
- Tracks progress for multi-track operations (playlists, albums)
- Manages message updates showing "3/10 tracks processed"
- Uses `DapperTable` for paginated progress display
- Maintains counters: total, completed, failed, discarded
- Has frozen pagination after `all_requests_added()` is called
- Located in `discord_bot/cogs/music_helpers/media_request.py`

### **`MessageQueue`**
- Central dispatcher for all Discord messages
- Routes messages to appropriate handlers (mutable vs immutable)
- Manages `MessageMutableBundle` instances for editable messages
- Handles single immutable messages (one-off notifications)
- Located in `discord_bot/cogs/music_helpers/message_queue.py`

### **`MessageMutableBundle`**
- Collection of related Discord messages that can be edited in-place
- Manages multiple `MessageContext` objects
- Implements smart diffing to minimize API calls (edits instead of delete+send)
- Supports "sticky" mode to keep messages at bottom of channel
- Located in `discord_bot/cogs/music_helpers/message_context.py`

### **`SearchClient`**
- Parses and resolves search inputs
- Handles Spotify URLs, YouTube URLs, and text searches
- Converts inputs to `SearchResult` objects
- Integrates with Spotify API, YouTube API, and YouTube Music API
- Returns list of `SearchResult` for each track found
- Located in `discord_bot/cogs/music_helpers/search_client.py`

### **`DownloadClient`**
- Wrapper around yt-dlp for downloading media
- Handles download retries and error cases
- Processes audio files (normalization, silence removal if enabled)
- Creates `MediaDownload` objects from successful downloads
- Located in `discord_bot/cogs/music_helpers/download_client.py`

### **`VideoCacheClient`**
- Manages local cache of downloaded files
- Implements LRU (Least Recently Used) eviction
- Integrates with database to track cached files
- Optionally backs up files to S3 storage
- Checks cache before downloads to avoid re-downloading
- Located in `discord_bot/cogs/music_helpers/video_cache_client.py`

---

## Queue Types

### **`DistributedQueue`**
- Fair distribution queue across multiple guilds
- Each guild has its own sub-queue
- Serves oldest unprocessed guild (with priority weighting)
- Prevents one guild from monopolizing resources
- Used for: `download_queue`, `youtube_music_search_queue`
- Located in `discord_bot/utils/distributed_queue.py`

### **Standard `Queue`**
- Simple FIFO (First In, First Out) queue
- Single queue for all items
- Used for: player play queue (`_play_queue`), history queue, single immutable messages
- Located in `discord_bot/utils/queue.py`

---

## Search Types

### **`SearchType.SPOTIFY`**
- Text search from Spotify that should be converted via YouTube Music API
- Example: "Artist - Song Name" extracted from Spotify album
- Requires YouTube Music search to find actual video URL

### **`SearchType.YOUTUBE`**
- Direct YouTube video URL
- Example: `https://youtube.com/watch?v=abc123`
- Can be downloaded immediately (after cache check)

### **`SearchType.DIRECT`**
- Direct media URL (non-YouTube)
- Example: Twitter video, SoundCloud, direct MP3 link
- Passed directly to yt-dlp for downloading

### **`SearchType.SEARCH`**
- Plain text search query
- Example: "never gonna give you up"
- Requires YouTube Music search to find video URL

### **`SearchType.YOUTUBE_PLAYLIST`**
- YouTube playlist URL
- Example: `https://youtube.com/playlist?list=xyz`
- Expanded into multiple individual video URLs

### **`SearchType.OTHER`**
- Catch-all for unrecognized search types
- Passed to yt-dlp to attempt download

---

## Lifecycle Stages

### **`MediaRequestLifecycleStage`**

Tracks progress of each `MediaRequest` through the system:

**`SEARCHING`**
- Initial search phase
- Request is being processed by search client
- YouTube Music API lookup may be in progress

**`QUEUED`**
- Added to download queue
- Waiting for download loop to process
- May wait for other guilds' downloads to complete

**`BACKOFF`**
- Waiting for YouTube rate limit cooldown
- Typically 30-40 seconds between downloads
- Prevents YouTube from blocking the bot

**`IN_PROGRESS`**
- Currently downloading via yt-dlp
- File is being downloaded and processed
- Audio normalization may be running (if enabled)

**`COMPLETED`**
- Successfully downloaded and added to player queue
- File ready for playback
- Row cleared from progress display

**`FAILED`**
- Download or processing failed
- Includes error reason in message
- Does not block other tracks in bundle

**`DISCARDED`**
- Skipped for operational reasons
- Examples: queue full, player disconnected, shutdown called
- Not counted as failure

---

## Message Types

### **`MessageType.SINGLE_IMMUTABLE`**
- One-off messages sent once and optionally deleted after timeout
- Examples: error messages, command confirmations
- Not edited after sending
- Queued in `single_immutable_queue`

### **`MessageType.MULTIPLE_MUTABLE`**
- Bundles of messages that update in-place via edits
- Examples: download progress, play queue display
- Efficiently edited instead of deleted and resent
- Managed by `MessageMutableBundle`

---

## Background Loops

All loops run continuously in the background and are managed by the Discord bot's event loop.

### **Send Messages Loop**
- **Purpose**: Dispatch all Discord messages (progress updates, errors, queue displays)
- **Processes**: `MessageQueue` items

### **Download Files Loop**
- **Purpose**: Download media files via yt-dlp
- **Processes**: `download_queue` items (DistributedQueue)

### **YouTube Music Search Loop**
- **Purpose**: Convert text searches to YouTube URLs
- **Processes**: `youtube_music_search_queue` items (DistributedQueue)

### **Cleanup Players Loop**
- **Purpose**: Disconnect from empty voice channels
- **Processes**: Checks all active players

### **Cache Cleanup Loop**
- **Purpose**: Remove old cached files when limit exceeded, backup to S3
- **Processes**: Database queries for cache files

### **Playlist History Update Loop**
- **Purpose**: Record playback history to database, update analytics
- **Processes**: `history_playlist_queue` items

See the [background documentation](./background.md) for detailed explanation of background loops.


## Common Abbreviations

- **ctx**: Discord Context (contains guild, channel, author info)
- **yt-dlp**: YouTube Download Python library (successor to youtube-dl)
- **UUID**: Universally Unique Identifier (for tracking requests)
- **LRU**: Least Recently Used (cache eviction strategy)
- **FIFO**: First In, First Out (queue ordering)
- **API**: Application Programming Interface (Spotify, YouTube, etc.)
- **S3**: Amazon Simple Storage Service (object storage)
- **FFmpeg**: Audio/video processing library
