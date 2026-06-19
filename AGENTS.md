# AGENTS.md

Guidance for AI coding agents working in this repository. For user-facing
configuration and CLI usage see [README.md](README.md). For setup, tests,
linting, and how to add cogs / commands see [DEVELOPMENT.md](DEVELOPMENT.md).
Per-cog and subsystem docs live under [`docs/`](docs/) — that's the
authoritative reference for the message dispatcher, monitoring, music
internals, etc.

## Where things live

| Topic | Location |
|-------|----------|
| Full bot entry-point (gateway + cogs) | `discord_bot/cli/full.py` (registered as `discord-bot`) |
| Min bot entry-point (cogs only, no in-process dispatcher) | `discord_bot/cli/bot.py` (registered as `discord-bot-min`) |
| Dispatcher-only entry-point | `discord_bot/cli/dispatcher.py` (registered as `discord-dispatcher`) |
| `POSSIBLE_COGS` registry | `discord_bot/cli/_lib/cog_registry.py` |
| `CogHelperBase` + dispatch helpers | `discord_bot/cogs/common.py` (see `docs/common.md`) |
| `CogHelper` (adds async DB session helpers) | `discord_bot/cogs/cog_helper.py` |
| Per-guild priority dispatcher worker | `discord_bot/workers/message_dispatcher.py` (see `docs/message_dispatcher.md`) |
| Dispatch protocols / interfaces | `discord_bot/interfaces/dispatch_protocols.py` |
| Music subsystem | `discord_bot/cogs/music.py` + `cogs/music_helpers/` (see `docs/music/`) |
| Config models (`GeneralConfig`, `IncludeConfig`, …) | `discord_bot/utils/common.py` |
| OTel naming enums, span/metric wrappers | `discord_bot/utils/otel.py` (see `docs/monitoring/`) |
| DB models (`BASE`-inheriting) | `discord_bot/database.py` |
| Async retry helpers | `discord_bot/utils/common.py`, `utils/sql_retry.py` |
| Test fixtures and fakes | `tests/helpers.py` |

## Non-obvious internals

### Use `venv/bin/pytest`, not system `pytest`

The system environment has an older `dappertable` (0.2.4) missing kwargs used
throughout the tests, producing ~27 false failures. Always invoke pytest
through the project venv. Same goes for pylint.

### `MessageDispatcher` worker runs outside the cog system

The per-guild priority dispatcher used to live in `cogs/message_dispatcher.py`
but is now `discord_bot/workers/message_dispatcher.py`. The `discord-bot`
(full) entry-point spins it up alongside the gateway; the
`discord-dispatcher` entry-point runs only the dispatcher + its HTTP server;
the `discord-bot-min` entry-point runs the gateway and cogs but leaves
dispatch to an out-of-process dispatcher. Other cogs retrieve the dispatcher
lazily via `self._dispatcher` (in-process worker or `HTTPDispatchClient`
depending on `general.dispatch_cross_process`). If neither is wired up,
`_dispatcher` raises `RuntimeError` on first use — there is no silent
fallback. `CommandErrorHandler` is registered unconditionally before any
cog list is loaded.

### `bot.loop` is `None` in tests

`FakeBot.loop = None` (`tests/helpers.py`). Code that runs during a test body
(not inside `cog_load`, which only runs under discord.py's real runtime) must
use `asyncio.get_running_loop()`:

```python
# Wrong — AttributeError in tests
self.bot.loop.create_task(...)

# Right — works under both discord.py and pytest-asyncio
asyncio.get_running_loop().create_task(...)
```

### Database URL rewriting is automatic

The CLI rewrites `postgresql://` → `postgresql+asyncpg://` at startup.
PostgreSQL is the only supported backend; non-postgres drivernames raise
at boot. Config files use the standard `postgresql://` URL; don't write
`+asyncpg` into config or the rewrite double-applies.

### Broad-except is rare and pylint-annotated

Production code does **not** use bare `except Exception:`. The exceptions
that exist all live in the dispatcher worker loop and request handlers
(lines tagged `# pylint: disable=broad-except`) — they catch and log so an
unbounded handler exception cannot kill the dispatcher worker or leave an
`asyncio.Future` caller hanging. Don't add new broad-excepts elsewhere; if
you copy this pattern into a new dispatcher-like loop, mirror the pylint
annotation and the log+propagate behaviour.

### `cleanup_source` is module-level, not a method

`cleanup_source` is a top-level function in
`discord_bot/cogs/music_helpers/music_player.py`, not a method on
`MusicPlayer`. It is the **only** safe way to release the FFmpeg
subprocess held by a `PCMAudio` source — calling `voice_client.stop()`
alone leaks fds. Anywhere the player drops a track, route through this
function.

### Two dispatch modes: in-process vs cross-process Redis/HTTP

`CogHelperBase._dispatcher` returns either the in-process
`MessageDispatcher` worker or an `HTTPDispatchClient` depending on
`general.dispatch_cross_process` in config. Either way, helper methods
look identical to the caller. If `dispatch_cross_process` is false and
the in-process dispatcher is missing, accessing `self._dispatcher` raises
`RuntimeError` — there is **no** silent fallback. Tests rely on either
having the dispatcher wired up or not calling dispatch helpers.

### `MediaBroker` zone transitions are one-way

`IN_FLIGHT → AVAILABLE → CHECKED_OUT`. Eviction guards (`can_evict_base`,
`can_evict_request`) must succeed before you delete the underlying file —
otherwise an in-flight or checked-out copy gets pulled out from under a
consumer. Full design in `docs/music/media_broker.md`.

### Heartbeat gauge pattern

Every cog with a background loop registers an observable gauge keyed on
`AttributeNaming.BACKGROUND_JOB.value`. The callback returns `1` when the
task is live and `0` when it's done. New metric names go into
`MetricNaming` in `utils/otel.py` before first use. See
`docs/monitoring/metrics_reference.md` for the full list.

## Project layout

```
discord_bot/
  cli/
    full.py                     # `discord-bot` entry-point (gateway + cogs + in-process dispatcher)
    bot.py                      # `discord-bot-min` entry-point (gateway + cogs, no dispatcher)
    dispatcher.py               # `discord-dispatcher` entry-point (dispatcher worker + HTTP server)
    health.py                   # HealthServer factory (kept out of _lib so importing it doesn't pull sqlalchemy)
    _lib/                       # shared CLI helpers (imported by the entry-points)
      common.py                 # shared bot utilities (bot_lifecycle, load_cogs, setup_logging, etc.)
      cog_registry.py           # POSSIBLE_COGS list (import here to add a new cog)
      db.py                     # shared DB engine bootstrap
  common.py                     # DISCORD_MAX_MESSAGE_LENGTH = 2000
  database.py                   # SQLAlchemy models (BASE declarative)
  exceptions.py                 # DiscordBotException, CogMissingRequiredArg, ExitEarlyException
  interfaces/
    dispatch_protocols.py       # Protocol classes for dispatch clients / workers
  workers/
    message_dispatcher.py       # per-guild priority dispatcher (was cogs/message_dispatcher.py)
    asyncio_queues.py           # in-process asyncio queue backend
    redis_queues.py             # cross-process Redis queue backend
  cogs/
    common.py                   # CogHelper base class + dispatch helpers
    error.py                    # CommandErrorHandler — catches CommandNotFound / MissingRequiredArgument
    general.py                  # General cog — !hello, !roll, !meta commands
    schema.py                   # Pydantic StorageConfig (s3 backend literal)
    markov.py                   # Markov chain message generation
    music.py                    # Music playback cog
    delete_messages.py          # Automated channel cleanup
    role.py                     # RoleAssignment — self-serve role commands
    urban.py                    # UrbanDictionary — !word command
    music_helpers/
      common.py                 # SearchType enum, StorageOptions enum, YouTube URL prefix constants
      database_functions.py     # Shared DB helpers (ensure_guild_video_analytics, etc.)
      download_client.py        # yt-dlp wrapper; DownloadClientException hierarchy
      media_broker.py           # MediaBroker — IN_FLIGHT/AVAILABLE/CHECKED_OUT lifecycle
      music_player.py           # MusicPlayer — playback queue, FFmpegPCMAudio, cleanup_source()
      search_client.py          # SearchClient — URL parsing, Spotify/YouTube patterns
      video_cache_client.py     # VideoCacheClient — local file cache with S3 backup
  clients/
    broker_client.py            # in-process broker shim
    dispatch_client_base.py     # shared base for HTTP/Redis dispatch clients
    http_dispatch_client.py     # HTTPDispatchClient — cross-process dispatch over HTTP
    redis_client.py             # Redis-backed dispatch client / queue helpers
  servers/
    base.py                     # shared aiohttp server base
    broker_server.py            # in-process broker HTTP shim
    dispatch_server.py          # dispatcher HTTP API (used by HTTPDispatchClient)
    health_server.py            # main bot readiness/liveness server
    health_server_base.py       # shared health-server scaffolding
    dispatch_health_server.py   # dispatcher process readiness/liveness server
  types/
    __init__.py
    catalog.py                  # CatalogItem, CatalogResponse
    download.py                 # DownloadResult, DownloadStatus
    history_playlist_item.py    # HistoryPlaylistItem
    media_download.py           # MediaDownload, media_download_attributes
    media_request.py            # RetryInformation, MediaRequest, BundledMediaRequest,
                                #   MediaRequestStateMachine, MultiMediaRequestBundle,
                                #   media_request_attributes, chunk_list
    search.py                   # SearchResult, SearchCollection
  utils/
    audio.py                    # edit_audio_file() — audio normalisation via ffmpeg subprocess (loudnorm)
    common.py                   # async_retry_discord_message_command, get_logger,
                                # return_loop_runner, create_observable_gauge,
                                # GeneralConfig and all other config models
    distributed_queue.py        # DistributedQueue — per-guild priority + FIFO round-robin
    failure_queue.py            # FailureQueue — operation failure-rate tracking
    memory_profiler.py          # MemoryProfiler — tracemalloc-based memory tracking
    otel.py                     # MetricNaming, AttributeNaming, otel_span_wrapper,
                                # command_wrapper, METER_PROVIDER
    process_metrics.py          # ProcessMetricsProfiler — psutil RSS/CPU/FD metrics
    queue.py                    # AsyncQueue[T] — shuffleable asyncio queue with PutsBlocked
    sql_retry.py                # async_retry_database_commands
    clients/                    # s3.py, spotify.py, youtube.py, youtube_music.py
tests/                          # mirrors discord_bot/ structure
  helpers.py                    # shared fixtures and fake classes
docs/                           # markdown docs per cog + monitoring/
alembic/                        # migration scripts
```

## Things to keep doing

- Use `self.dispatch_message(guild_id, channel_id, content)` /
  `self.dispatch_fetch(guild_id, func)` / `self.dispatch_delete(...)`
  instead of calling `async_retry_discord_message_command` directly. See
  `discord_bot/cogs/common.py` for the full helper set and
  `docs/message_dispatcher.md` for the underlying priority model.
- Use `select()` / `delete()` — `AsyncSession` does not support the legacy
  `session.query()` API. Wrap commits via `self.retry_commit(db_session)`
  (defined in `discord_bot/cogs/cog_helper.py`).
- Use `async_otel_span_wrapper` with `async with` for spans, or
  `@command_wrapper` on command handlers.
- Pydantic-validate every new config section by passing a `config_model` to
  `CogHelperBase.__init__`. Validation errors should raise
  `CogMissingRequiredArg`.
