# Music System HA Plan

## Goal

Split the music system into independently deployable, horizontally scalable components so that no single pod failure causes a full outage, and pods can be drained and replaced without interrupting active playback.

---

## Target Architecture

```
[Download Workers (N, stateless)]
    pull from external queue (Redis Streams)
    run yt-dlp
    POST file to Global Broker
    delete local copy — no persistent state

          ↓ file ready notification

[Global Broker (HA, Redis-backed)]
    single global instance (HA via Redis state + multiple pods)
    owns all base files + video cache
    tracks IN_FLIGHT / AVAILABLE / CHECKED_OUT per guild
    serves files to players on prefetch request
    atomic cache deduplication (no race on simultaneous requests)

          ↓ prefetch on demand

[Music Player Pods (M, each owns a configured set of guild IDs)]
    prefetch next N files from broker into local buffer
    play from local file — FFmpegPCMAudio unchanged
    checkout / release with broker
    guild assignment via static config, changed by drain + config update

[Message Dispatch Workers (separate deployment)]
    no Discord gateway — REST only
    reads from external queue
    resolves channel IDs at call time
    independently scalable from audio pipeline
```

---

## Pre-Steps Sequence

All steps below are prerequisites for the HA split. They are ordered by dependency — each step makes the next one possible or significantly easier. Steps 1 and 2 are the immediate focus.

| # | Step | Status | Why it unblocks |
|---|---|---|---|
| 1 | Enable YouTube Music search by default | ✅ Done | Every item entering any queue has a canonical video ID — prerequisite for a clean cache check at the broker |
| 2 | Broker owns files; download worker is stateless | Not started | Establishes the clean handoff boundary between workers and broker; workers become disposable |
| 3 | Broker gets Redis backing store | Not started | State survives pod restarts; enables multiple broker instances |
| 4 | External download queue (Redis Streams) | ✅ Partially done | Download workers can run as separate pods with no bot process dependency |
| 5 | Player prefetch buffer | Not started | Players stop needing a shared filesystem; broker serves files directly |
| 6 | Player guild partitioning + graceful drain | Not started | Operators can split players across pods via config; pods can be replaced without dropping tracks |

---

## Step 1 — Enable YouTube Music Search by Default

### Problem (resolved)

The YouTube Music search step was gated behind a `config.download.enable_youtube_music_search` flag. When disabled, text queries and Spotify-sourced titles entered the download queue as raw strings. The canonical video ID was not known until yt-dlp resolved it internally — after the download had already started.

This created two problems for broker-level caching:
- The cache check could not use a stable key for text queries (`"taylor swift shake it off"` is not a reliable cache key)
- Two workers processing the same query simultaneously could both slip past the cache check and download the same video twice

The flag has been removed. `YoutubeMusicClient` is always initialised and the search task always runs.

### How routing works

`SearchClient.check_source()` classifies every input into a `SearchType`:

| SearchType | Example input | Has canonical video ID? |
|---|---|---|
| `YOUTUBE` | `https://youtube.com/watch?v=...` | Yes — extracted by regex |
| `YOUTUBE_PLAYLIST` | `https://youtube.com/playlist?list=...` | Expands to `YOUTUBE` items, each with an ID |
| `DIRECT` | Any other `https://` URL | No video ID (arbitrary URL) |
| `SEARCH` | `"taylor swift shake it off"` | No — resolved inside yt-dlp at download time |
| `SPOTIFY` | Spotify track/album/playlist URL | Expands to artist+title strings (no video ID) |

`SEARCH` and `SPOTIFY` items are routed through `youtube_music_search_queue`. The search loop resolves each item to a `youtube.com/watch?v=<id>` URL before it enters the download queue.

`YOUTUBE`, `YOUTUBE_PLAYLIST`, and `DIRECT` correctly bypass the search queue — they either already have a canonical ID or never will.

### Invariant this establishes

> Every `MediaRequest` in the download queue has a `resolved_search_string` of the form `https://www.youtube.com/watch?v=<11-char-id>`, except `DIRECT` type requests which carry an arbitrary URL.

The broker can rely on this invariant to do an atomic `video_id → cached?` check as the very first operation before any download work begins.

---

## Step 2 — Broker Owns Files; Download Worker is Stateless

### Problem

Currently the download worker writes a file to its local disk and retains ownership of it indefinitely. The broker tracks metadata (state, references) but the file lives on the worker's filesystem. This couples the worker pod to the file's lifetime — the pod cannot be killed while any guild is using a file it downloaded, and the player needs access to the same filesystem as the worker (shared volume or same host).

### Target ownership model

| Component | Responsibility | Has persistent state? |
|---|---|---|
| Download worker | Pull from queue → run yt-dlp → POST file to broker → delete local copy | No |
| Global broker | Owns base files on its own volume; tracks lifecycle state; serves files to players | Yes (volume + Redis) |
| Music player | Prefetches files from broker into a small local buffer; plays from local file | Small local buffer only |

The download worker becomes a pure processor: it needs only enough disk for the yt-dlp working directory (one file at a time), and that file is deleted immediately after a successful upload to the broker.

### File handoff: worker → broker

After `create_source()` completes, the worker streams the file to the broker via HTTP:

```
POST /files/{content_hash}
Content-Type: application/octet-stream
[file body]

→ broker writes to its volume
→ broker transitions entry IN_FLIGHT → AVAILABLE
→ worker deletes local copy
→ broker publishes "file ready" event (or worker notifies via separate call)
```

The `content_hash` here is the video ID (or a hash derived from it) — this is what makes the atomic deduplication possible. If two workers race on the same video ID, the broker can reject the second POST with a `409 Conflict` once the first has been registered as IN_FLIGHT, without either worker needing to coordinate with the other.

### Eliminating the per-guild file copy

Currently `ready_file()` in `MediaDownload` copies the base file to a guild-specific subdirectory so that each guild holds an independent file reference it can delete without affecting other guilds playing the same video. This copy exists solely to solve a filesystem reference counting problem.

With the broker owning the base file, this copy is no longer needed. Each player prefetches its own copy from the broker into its local buffer — that prefetched copy IS the per-guild copy. The broker's base file is the single shared reference, guarded by the existing `can_evict_base()` logic. The `ready_file()` copy step is removed entirely.

This also eliminates the synchronous `shutil.copyfile()` call that currently runs on the event loop for every track.

### Player prefetch buffer

Rather than streaming the base file from the broker directly into FFmpegPCMAudio (which had reliability issues previously), each player maintains a small local buffer of N pre-downloaded files.

The prefetch loop:
1. Watches the broker for `AVAILABLE` entries belonging to the player's guild queue
2. When a slot is open in the buffer (size N, configurable), issues a `GET /files/{content_hash}` to the broker and writes to a local temp file
3. Marks the entry `CHECKED_OUT` once the local copy is confirmed written
4. The player loop dequeues from the local buffer and plays via `FFmpegPCMAudio(local_path)` — unchanged from today

HTTP is used only for the transfer (seconds), not for playback (minutes). FFmpegPCMAudio never touches the network.

N should be tuned to keep the buffer full relative to average download time vs. average track duration. A reasonable default is 3–5 tracks. Expose as a config value per deployment.

### Updated broker zones

The three-zone model from `media_broker.md` extends slightly to accommodate the prefetch step:

```
IN_FLIGHT       → download worker is processing this request
AVAILABLE       → file is on broker disk, no player has it yet
PREFETCHING     → a player has claimed the file and is downloading it to local buffer
                  (optional intermediate state — can be collapsed into CHECKED_OUT
                   if the distinction is not needed for eviction logic)
CHECKED_OUT     → player has a local copy and has it queued or is actively playing
evictable       → all CHECKED_OUT references released, zone returns to AVAILABLE,
                  broker deletes base file when no other guilds hold references
```

### Eviction

`can_evict_base(video_id)` on the broker returns true when:
- No entries for this video are in `IN_FLIGHT` or `PREFETCHING`
- All `CHECKED_OUT` references have been released

At that point the broker deletes the base file from its volume. No player or worker needs to coordinate this — the broker is the only thing with file ownership.

### Broker as a critical service

By taking on file ownership the broker becomes a more critical piece of infrastructure than it is today. Operational implications:

- Broker pod requires a persistent volume with enough headroom for the working set (all AVAILABLE + CHECKED_OUT base files simultaneously)
- Broker needs a readiness probe that validates the volume is mounted before accepting uploads from workers
- If the broker pod is replaced, Redis holds all lifecycle state so recovery is automatic — files on the volume are still valid, the registry entries in Redis describe their state
- If the broker volume is lost, IN_FLIGHT downloads are re-queued automatically (workers retry), AVAILABLE/CHECKED_OUT files are gone and their players will detect the missing local prefetch copy and re-request

---

## Steps 3–6 (Later)

### Step 3 — Broker Redis backing store

Replace the in-memory dict in `MediaBroker._registry` with Redis hash operations. The interface (`register_request`, `register_download`, `checkout`, `release`, `evict`, `can_evict_base`) does not change for callers. Multiple broker pod instances can share state. The design doc (`docs/music/media_broker.md`) already identifies this as the intended seam.

The `youtube_download_wait_timestamp` and `youtube_music_wait_timestamp` backoff floats on the cog also move to Redis keys at this point — shared backoff state prevents multiple download workers from independently 429-storming YouTube.

### Step 4 — External queues ✅ (download side implemented)

The download queue is now backed by Redis Streams. The `discord-bot-download-worker` process reads `MediaRequest` objects from `discord_bot:download:input` via `XREADGROUP`, downloads them via yt-dlp, and writes `DownloadResult` objects to `discord_bot:download:result:<process_id>`. The bot reads the result stream and routes results to the player queue.

Enable with `download_worker_redis: true` and `remote_download_worker: true` in the bot config. See [Remote Download Worker](../music.md#remote-download-worker) for full details.

**Remaining**: The `youtube_music_search_queue` is not yet externalised — search still runs in-process. A future step would replace `DistributedQueue` for the search queue with a Redis Stream, allowing search workers to scale independently from the bot process.

After the search queue is also externalised, download and search workers need only:
- Redis connection (queues + backoff state)
- Broker HTTP endpoint (file upload — Step 2)
- No Discord token or gateway connection

### Step 5 — Player prefetch (completes Step 2)

Step 2 designs the prefetch buffer; Step 5 is when it is actually deployed with the broker running as a separate service. At this point the shared volume between download workers and the original bot process is eliminated.

### Step 6 — Player guild partitioning and graceful drain

Each player pod is configured with an explicit set of guild IDs:

```yaml
music_player:
  guild_ids: [123456789, 987654321]
```

Reassigning a guild from one pod to another:
1. Remove the guild ID from the old pod's config and trigger a config reload
2. Old pod detects it no longer owns the guild, runs graceful drain (finish current track, disconnect)
3. Add the guild ID to the new pod's config
4. New pod detects the new guild, queries broker for any CHECKED_OUT entries, rebuilds prefetch buffer from AVAILABLE entries, rejoins voice

The broker's `get_checked_out_by(guild_id)` method (already documented) supports the queue reconstruction step.

---

## Key Constraints and Non-Negotiables

**Voice connections are single-node per guild.** Discord enforces that only one voice connection per guild exists at a time regardless of bot token. If two pods attempt to connect to the same guild's voice channel, Discord silently drops the first. Guild partitioning (Step 6) must ensure non-overlapping ownership.

**The broker must be globally single (logically).** Multiple broker pod instances sharing Redis state are fine — that is horizontal scaling of a single logical broker. Running two independent broker instances with separate Redis databases would break cache deduplication.

**Canonical video ID is a hard prerequisite.** Steps 2 onward assume every download queue item carries a canonical YouTube video ID. Step 1 must be complete before any broker-level cache deduplication logic is implemented.

**`DIRECT` type requests are always exceptional.** They carry arbitrary HTTPS URLs with no extractable video ID. They can still use the URL string as a cache key but cannot benefit from the YouTube Music search deduplication path. This is acceptable — direct URLs are typically unique content (clips, streams) not worth deduplicating.
