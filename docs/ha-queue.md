# HA Download Queue

## Context

The download worker is co-located with the bot by default. To enable HA (high
availability), the download step can be extracted into a separate process that
reads work from Redis Streams, downloads the file, and streams it back to the
broker (co-located with the bot) via HTTP. The bot's block volume stays in one
place; only the worker moves.

This is gated behind a config flag (`music.ha.enabled`). When disabled,
behaviour is identical to the original single-process mode.

---

## Architecture

### Non-HA mode (default, unchanged behaviour)

```
User → put_nowait → InProcessDownloadQueue (wraps DistributedQueue)
     → download_files loop → DownloadClient → MediaBroker → Player
```

### HA mode

```
User → put_nowait → RedisDownloadQueue (XADD + stores MediaRequest in _pending)
     → external worker (XREADGROUP) → DownloadClient
     → HTTP POST (streaming + X-Content-MD5)
     → BrokerHTTPServer (bot process) → verifies MD5 → writes file
     → puts BrokerResult in asyncio.Queue
     → ha_download_result_loop → looks up MediaRequest from _pending
     → MediaBroker.register_download → Player
```

**PlaylistAddRequest** always uses the in-process queue (never Redis). Its flow
is unchanged in both modes.

---

## Configuration

Add to your YAML config file under the `music` key:

```yaml
music:
  ha:
    enabled: true
    redis_url: redis://localhost:6379
    broker_host: 0.0.0.0
    broker_port: 8765
    worker_consumer_group: music_workers
```

All fields have defaults so the section can be omitted entirely for non-HA mode.

### `MusicHAConfig` fields

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Enable HA mode |
| `redis_url` | `redis://localhost:6379` | Redis connection URL |
| `broker_host` | `0.0.0.0` | Bind address for the broker HTTP server |
| `broker_port` | `8765` | Port for the broker HTTP server |
| `worker_consumer_group` | `music_workers` | Redis consumer group name |

---

## New Files

### `discord_bot/utils/download_queue.py`

- **`InProcessDownloadQueue`** — async wrapper around `DistributedQueue` for
  non-HA mode. All methods complete without I/O yield points, preserving the
  `clear_queue → block` atomicity guarantee in asyncio.
- **`RedisDownloadQueue`** — Redis Streams-backed queue for HA mode. Stores
  original `MediaRequest` objects in `_pending` (keyed by UUID) so the bot's
  live state machines can be retrieved when the worker completes a download.

### `discord_bot/utils/broker_http_server.py`

Minimal asyncio HTTP server (no external framework).

**Endpoints:**
- `POST /upload` — streaming file upload with MD5 verification
- `POST /upload/error` — worker-reported download failure

**Headers for `/upload`:**
- `X-Request-Id` — `MediaRequest.uuid`
- `X-Guild-Id` — guild_id
- `X-Content-MD5` — hex MD5 of the full file body
- `X-Ytdlp-Data` — JSON-encoded yt-dlp metadata dict
- `Content-Length` — byte count of the body

Files are written to a temp path first, then atomically renamed on MD5 match.

### `discord_bot/workers/download_worker.py`

Entry point: `discord-download-worker --config /path/to/config.yaml`

**Worker loop:**
1. Scan Redis for `music:download_queue:*` streams (refreshed every 10 s)
2. `XREADGROUP` to claim one message at a time (5 s block timeout)
3. Check if guild is in the blocked set — if so, ACK and skip
4. Deserialize `MediaRequest` from the stream payload
5. Call `DownloadClient.create_source` to download the file
6. Compute MD5 in one pass, then stream the file to the broker in a second pass
7. `POST /upload` (success) or `POST /upload/error` (failure) to the broker
8. `XACK` the message regardless of outcome

Environment variable: `BROKER_URL` overrides the default
`http://{broker_host}:{broker_port}` if the worker runs on a different host.

---

## Changes to Existing Files

### `discord_bot/cogs/music.py`

- Added `MusicHAConfig` Pydantic model and `ha` field to `MusicConfig`
- `__init__`: conditionally creates `InProcessDownloadQueue` or
  `RedisDownloadQueue` + `BrokerHTTPServer`
- `cog_load`: in HA mode starts the broker server and `ha_download_result_loop`
  task instead of `download_files`
- `cog_unload`: stops broker server and Redis connection in HA mode
- `download_files`, `cleanup`, `search_youtube_music`, playlist add: all
  `download_queue.put_nowait / get_nowait / block / clear_queue` calls are now
  `await`ed (async interface)
- Added `ha_download_result_loop` — drains `BrokerResult` items and registers
  completed downloads, mirroring `download_files`'s post-download path

### `requirements.txt`

Added:
```
aiohttp>=3.9.0
redis>=5.0.0
```

### `setup.py`

Added console script:
```
discord-download-worker = discord_bot.workers.download_worker:main
```

---

## Running the Worker

```bash
# Install the package
pip install -e .

# Run the worker (reads config from YAML)
discord-download-worker --config /path/to/config.yaml

# Override broker URL if worker is on a different host
BROKER_URL=http://10.0.0.1:8765 discord-download-worker --config config.yaml

# Specify a worker ID (auto-generated if omitted)
discord-download-worker --config config.yaml --worker-id worker-prod-0
```

Multiple workers can run in parallel — they share the same Redis consumer group
and each message is processed by exactly one worker.

---

## Verification

1. **Baseline tests**: `venv/bin/pytest tests/ -x -q` — all 775 tests pass
2. **Non-HA mode**: run bot with no `music.ha` config — behaviour unchanged
3. **HA mode (integration test)**:
   - Start Redis: `docker run -p 6379:6379 redis`
   - Config: `music.ha.enabled: true`
   - Start bot → broker HTTP server starts on configured port
   - Start worker: `discord-download-worker --config config.yaml`
   - Request a song → verify it appears in Redis stream, worker picks it up,
     file arrives at broker, player plays it
4. **MD5 verification**: modify worker to corrupt 1 byte → broker returns 400
5. **Guild cleanup in HA mode**: while download is in-flight, `/stop` →
   `clear_queue` discards pending requests and blocks further puts
