# AGENTS.md

Agent and developer guide for the discord-bot project.

## Setup

```bash
pip install -e ".[all]"               # full install (editable, picks up local changes)
discord-bot /path/to/config.yml       # run the full bot (gateway + all cogs)
discord-dispatcher /path/to/config.yml  # run the dispatcher process only
```

## Testing

**Always use the project venv**, not the system Python:

```bash
venv/bin/pytest -q                              # full suite
venv/bin/pytest tests/path/to/test_file.py -q  # single file
```

The system environment has an older `dappertable` (0.2.4) that is missing kwargs
used throughout the tests, causing ~27 false failures. The venv has the correct version.

Test configuration lives in `pyproject.toml` (`[tool.pytest.ini_options]`). All async tests must be decorated with
`@pytest.mark.asyncio` (mode is `strict`).

Coverage threshold is 90%. Tox runs py311–py314:

```bash
tox   # runs pylint (both configs) + pytest across all Python versions
```

## Linting

```bash
venv/bin/pylint --rcfile .pylintrc discord_bot/         # production code
venv/bin/pylint --rcfile .pylintrc.test tests/          # test code
```

Target score is 10.00/10. Both configs are permissive about line length and argument
counts; see the rc files for the full disabled-checks list.

## Project layout

```
discord_bot/
  cli.py                        # entry point, config loading, bot init, POSSIBLE_COGS list
  common.py                     # DISCORD_MAX_MESSAGE_LENGTH = 2000
  database.py                   # SQLAlchemy models (BASE declarative)
  exceptions.py                 # DiscordBotException, CogMissingRequiredArg, ExitEarlyException
  cogs/
    common.py                   # CogHelper base class + dispatch helpers
    message_dispatcher.py       # per-guild priority queue dispatcher
                                # also defines MessageContext, MessageMutableBundle
    error.py                    # CommandErrorHandler — catches CommandNotFound / MissingRequiredArgument
    general.py                  # General cog — !hello, !roll, !meta commands
    schema.py                   # Pydantic StorageConfig (s3 backend literal)
    markov.py                   # Markov chain message generation
    music.py                    # Music playback cog
    delete_messages.py          # Automated channel cleanup
    role.py                     # RoleAssignment — self-serve role commands
    urban.py                    # UrbanDictionary — !word command
    database_backup.py          # Scheduled S3 database backup
    music_helpers/
      common.py                 # SearchType enum, StorageOptions enum, YouTube URL prefix constants
      database_functions.py     # Shared DB helpers (ensure_guild_video_analytics, etc.)
      download_client.py        # yt-dlp wrapper; DownloadClientException hierarchy
      media_broker.py           # MediaBroker — IN_FLIGHT/AVAILABLE/CHECKED_OUT lifecycle
      music_player.py           # MusicPlayer — playback queue, FFmpegPCMAudio, cleanup_source()
      search_client.py          # SearchClient — URL parsing, Spotify/YouTube patterns
      video_cache_client.py     # VideoCacheClient — local file cache with S3 backup
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
    database_backup_client.py   # DatabaseBackupClient — JSON backup/restore to/from S3
    distributed_queue.py        # DistributedQueue — per-guild priority + FIFO round-robin
    failure_queue.py            # FailureQueue — operation failure-rate tracking
    health_server.py            # HealthServer — lightweight HTTP /health endpoint
    memory_profiler.py          # MemoryProfiler — tracemalloc-based memory tracking
    otel.py                     # MetricNaming, AttributeNaming, otel_span_wrapper,
                                # command_wrapper, METER_PROVIDER
    process_metrics.py          # ProcessMetricsProfiler — psutil RSS/CPU/FD metrics
    queue.py                    # AsyncQueue[T] — shuffleable asyncio queue with PutsBlocked
    sql_retry.py                # retry_database_commands (sync, backup only), async_retry_database_commands
    clients/                    # s3.py, spotify.py, youtube.py, youtube_music.py
scripts/
  restore_database.py           # CLI: restore DB from local file or S3 (--config, --file/--s3-*)
tests/                          # mirrors discord_bot/ structure
  helpers.py                    # shared fixtures and fake classes
docs/                           # markdown docs per cog + monitoring/
alembic/                        # migration scripts
```

## Adding a new cog

Three places require changes:

1. **`discord_bot/utils/common.py`** — add a field to `IncludeConfig`:
   ```python
   class IncludeConfig(BaseModel):
       my_cog: bool = False
   ```

2. **`discord_bot/cli.py`** — add to `POSSIBLE_COGS` (order matters;
   `MessageDispatcher` must stay first):
   ```python
   POSSIBLE_COGS = [
       MessageDispatcher,
       ...
       MyCog,
   ]
   ```

3. **`discord_bot/cogs/my_cog.py`** — implement the cog (see pattern below).

## Cog conventions

All cogs inherit `CogHelper` (`discord_bot/cogs/common.py`):

```python
from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from pydantic import BaseModel

class MyCogConfig(BaseModel):
    loop_sleep_interval: float = 300.0

class MyCog(CogHelper):
    def __init__(self, bot: Bot, settings: dict, db_engine: AsyncEngine):
        # Guard: raise CogMissingRequiredArg if not enabled or misconfigured
        if not settings.get('general', {}).get('include', {}).get('my_cog', False):
            raise CogMissingRequiredArg('MyCog not enabled')
        super().__init__(bot, settings, db_engine,
                         settings_prefix='my_cog',
                         config_model=MyCogConfig)
        # self.config.loop_sleep_interval now available
```

`CogHelper.__init__` sets up:
- `self.bot`, `self.settings`, `self.db_engine` (`AsyncEngine | None`)
- `self.logger` — configured via `general.logging` settings; logger name is the
  lowercase class name
- `self.logging_config` — `LoggingConfig` instance
- `self.config` — Pydantic-validated cog config (if `config_model` supplied)

**`MessageDispatcher` must be loaded first** — other cogs retrieve it at runtime
via `self.bot.get_cog('MessageDispatcher')`.

Command prefix is `!` (or `@mention`), e.g. `!play`, `!help`.

### CogHelper dispatch helpers

Use these instead of calling `async_retry_discord_message_command` directly:

```python
# Send a plain text message (fire-and-forget via dispatcher; direct retry as fallback)
await self.dispatch_message(ctx, 'text')

# Fetch any Discord object with retry
result = await self.dispatch_fetch(guild_id, partial(channel.history, limit=100))

# Enqueue a batch of callables (e.g. message deletes)
await self.send_funcs(guild_id, [partial(msg.delete) for msg in messages])
```

### Background loop pattern

```python
from discord_bot.utils.common import return_loop_runner

async def cog_load(self):
    self._task = self.bot.loop.create_task(
        return_loop_runner(self.my_loop, self.bot, self.logger)()
    )

async def cog_unload(self):
    if self._task:
        self._task.cancel()

async def my_loop(self):
    # one iteration — return_loop_runner re-calls indefinitely
    ...
```

### Heartbeat gauge pattern

```python
from discord_bot.utils.otel import METER_PROVIDER, MetricNaming, AttributeNaming
from discord_bot.utils.common import create_observable_gauge
from opentelemetry.metrics import Observation

create_observable_gauge(
    METER_PROVIDER,
    MetricNaming.HEARTBEAT.value,
    self.__loop_active_callback,
    'My cog loop heartbeat',
)

def __loop_active_callback(self, _options):
    value = 1 if (self._task and not self._task.done()) else 0
    return [Observation(value, attributes={
        AttributeNaming.BACKGROUND_JOB.value: 'my_cog_check'
    })]
```

Add new `MetricNaming` entries to `discord_bot/utils/otel.py` before using them.

## Cog reference

| Cog class | Config key | Bot commands |
|-----------|-----------|--------------|
| `MessageDispatcher` | (always loaded) | — |
| `CommandErrorHandler` | (always loaded) | — |
| `General` | (always loaded) | `!hello`, `!roll <dice>`, `!meta` |
| `DeleteMessages` | `delete_messages` | `!delete <n>`, `!autodelete` |
| `DatabaseBackup` | `database_backup` | — (scheduled loop) |
| `Markov` | `markov` | `!markov speak`, `!markov on/off` |
| `Music` | `music` | `!play`, `!pause`, `!resume`, `!skip`, `!stop`, `!queue`, `!history`, etc. |
| `RoleAssignment` | `role` | `!role add/remove/list` |
| `UrbanDictionary` | `urban` | `!word <term>` |

`POSSIBLE_COGS` order in `cli.py`:
```python
[MessageDispatcher, DeleteMessages, DatabaseBackup, Markov, Music, RoleAssignment, UrbanDictionary, General]
```
`CommandErrorHandler` is registered unconditionally before the loop.

## MessageDispatcher

`discord_bot/cogs/message_dispatcher.py`

One `asyncio.PriorityQueue` per guild; one lazy worker task per active guild.

| Priority | Value | Item type | Used for |
|----------|-------|-----------|----------|
| HIGH | 0 | `_MutableSentinel` | Flush mutable bundle (music queue display) |
| NORMAL | 1 | `_ImmutableItem`, `_SendItem` | One-off sends, message deletes |
| LOW | 2 | `_ReadItem` | Background reads (channel history, fetch_message) |

Public API:

| Method | Description |
|--------|-------------|
| `update_mutable(key, guild_id, content, channel_id, …)` | Queue mutable update; rapid-fire calls collapse |
| `remove_mutable(key)` | Delete messages and remove bundle |
| `update_mutable_channel(key, guild_id, new_channel_id)` | Move bundle to new channel |
| `send_message(guild_id, channel_id, content, …)` | Enqueue plain text send at NORMAL priority |
| `send_single(guild_id, funcs)` | Enqueue list of callables at NORMAL priority |
| `fetch_object(guild_id, func, max_retries=3, allow_404=False)` | Enqueue func at LOW priority; blocks caller until worker executes it with retry |

`MessageContext` and `MessageMutableBundle` are defined in `message_dispatcher.py`.

## OpenTelemetry

All spans and metrics use the enums in `discord_bot/utils/otel.py`.

### Spans

```python
from discord_bot.utils.otel import async_otel_span_wrapper
from opentelemetry.trace import SpanKind

async with async_otel_span_wrapper('my_cog.operation', kind=SpanKind.INTERNAL,
                                   attributes={'discord.guild': guild_id}):
    ...

# For command handlers, use the decorator instead:
from discord_bot.utils.otel import command_wrapper

@command()
@command_wrapper
async def my_command(self, ctx):
    ...
```

`async_otel_span_wrapper` is an `asynccontextmanager` and must be used with
`async with`. The sync `otel_span_wrapper` is still available for sync-only
contexts (e.g. the backup client).

### MetricNaming enum values

Defined in `discord_bot/utils/otel.py`. Add new entries here before using them.

| Name | Value | Used for |
|------|-------|----------|
| `HEARTBEAT` | `'heartbeat'` | Background loop liveness (all cogs) |
| `ACTIVE_PLAYERS` | `'active_players'` | Music — active guild players |
| (others) | — | See `otel.py` for full list |

### AttributeNaming

`AttributeNaming.BACKGROUND_JOB.value` — `'background_job'`, used as the
label key on heartbeat/queue-depth observations.

## Retry logic

`async_retry_discord_message_command` (`discord_bot/utils/common.py`) retries on
`RateLimited`, `DiscordServerError`, `TimeoutError`, `ServerDisconnectedError`.
It does **not** catch general exceptions — let those propagate.

```python
from discord_bot.utils.common import async_retry_discord_message_command
from functools import partial

result = await async_retry_discord_message_command(
    partial(channel.send, content='hello'),
    max_retries=3,
    allow_404=False,
)
```

Prefer `self.dispatch_fetch` / `self.send_funcs` over calling this directly in cogs.

## Database

SQLAlchemy 2.x with `declarative_base()`. All models inherit from `BASE`
(`discord_bot/database.py`). All session work is **async** — the engine is an
`AsyncEngine` backed by `asyncpg` (PostgreSQL) or `aiosqlite` (SQLite). The CLI
automatically rewrites `postgresql://` → `postgresql+asyncpg` and `sqlite://` →
`sqlite+aiosqlite` so the config URL does not need to change.

### Models

| Model | Table | Used by |
|-------|-------|---------|
| `MarkovChannel` | `markov_channel` | Markov |
| `MarkovRelation` | `markov_relation` | Markov |
| `Playlist` | `playlist` | Music |
| `PlaylistItem` | `playlist_item` | Music |
| `VideoCache` | `video_cache` | Music |
| `VideoCacheBackup` | `video_cache_backup` | Music / DatabaseBackup |

```python
# Async session context manager from CogHelper
async with self.with_db_session() as db:
    result = (await db.execute(select(MarkovChannel).where(...))).scalars().all()
    await self.retry_commit(db)  # commit with retry on PendingRollbackError
```

All queries use the SQLAlchemy 2.x `select()` API. The legacy `session.query()`
is **not** supported by `AsyncSession`.

```python
from sqlalchemy import select, delete

# Fetch one
row = (await db.execute(select(Model).where(Model.id == x))).scalars().first()

# Fetch all
rows = (await db.execute(select(Model).where(...))).scalars().all()

# Count
n = (await db.execute(select(func.count()).select_from(Model).where(...))).scalar()

# Delete
await db.execute(delete(Model).where(Model.id == x))
await db.commit()
```

`DatabaseBackupClient` is the only component that retains a **sync** engine —
it runs in a background thread and does its own session management.

### DB retry

`async_retry_database_commands` (`discord_bot/utils/sql_retry.py`) retries on
`OperationalError` (with rollback + sleep) and `PendingRollbackError` (with
rollback), up to 3 attempts:

```python
from discord_bot.utils.sql_retry import async_retry_database_commands

result = await async_retry_database_commands(db_session, lambda: database_functions.get_x(db_session, ...))

# Or for commit:
await async_retry_database_commands(db_session, db_session.commit)
```

The sync `retry_database_commands` still exists for `DatabaseBackupClient` only.

Migrations via Alembic (`DATABASE_URL` env var required):

```bash
alembic upgrade head
alembic revision --autogenerate -m "description"
```

## Configuration

Settings come from a YAML file passed to `discord-bot <config>`. Cog-specific
sections are read via `settings_prefix` in `CogHelper.__init__`. Config validation
uses Pydantic; validation errors raise `CogMissingRequiredArg`.

Core config structure:

```yaml
general:
  discord_token: ...
  sql_connection_statement: sqlite:///path/to/db.sql
  logging:
    log_level: 20
  monitoring:
    otlp:
      enabled: true
    health_server:
      enabled: true
      port: 8080
    memory_profiling:
      enabled: false
    process_metrics:
      enabled: false
  include:
    music: true
    markov: true
    delete_messages: true
    role: true
    urban: true
    database_backup: false
intents:
  - members
```

Config models live in `discord_bot/utils/common.py`:
`GeneralConfig`, `LoggingConfig`, `IncludeConfig`, `MonitoringConfig`,
`MonitoringOtlpConfig`, `MonitoringHealthServerConfig`,
`MonitoringMemoryProfilingConfig`, `MonitoringProcessMetricsConfig`.

## Monitoring utilities

### HealthServer (`discord_bot/utils/health_server.py`)

Asyncio-based socket server. Responds `200 {"status":"ok"}` when the bot is
running, `503` otherwise. Enabled via `general.monitoring.health_server`.

### MemoryProfiler (`discord_bot/utils/memory_profiler.py`)

tracemalloc wrapper. Exports `get_top_allocations()`, `get_allocation_diff()`,
`get_snapshot_summary()`, `start()`, `stop()`. Enabled via
`general.monitoring.memory_profiling`.

### ProcessMetricsProfiler (`discord_bot/utils/process_metrics.py`)

psutil wrapper. Tracks RSS/VMS/USS memory, CPU, threads, open file descriptors,
child processes. Runs in a background thread. Enabled via
`general.monitoring.process_metrics`.

## Utility modules

### `discord_bot/utils/queue.py` — `AsyncQueue[T]`

`asyncio.Queue` subclass with extra methods:
`block()`, `shuffle()`, `size()`, `clear()`, `remove_item()`, `bump_item()`,
`items()`. Raises `PutsBlocked` when shutdown.

### `discord_bot/utils/distributed_queue.py` — `DistributedQueue`

Manages per-guild queues with priority and FIFO round-robin selection.
Exports `DistributedQueueItem` dataclass.

### `discord_bot/utils/failure_queue.py` — `FailureQueue`

Tracks operation failure rates using `FailureStatus`. Features age-based
cleanup and status summaries. Used by the music download pipeline.

### `discord_bot/utils/audio.py`

`edit_audio_file(path)` — normalises audio via ffmpeg subprocess (`loudnorm` filter), outputs s16le PCM at 48 kHz stereo. Called via `loop.run_in_executor` in `download_client.py` so it doesn't block the event loop.
Helpers: `get_finished_path()`, `get_editing_path()`.

### `discord_bot/utils/database_backup_client.py` — `DatabaseBackupClient`

Streaming JSON backup/restore to minimise memory. Exports `create_backup()`,
`restore_backup()` with metadata and batch restoration. Used by `DatabaseBackup`
cog and `scripts/restore_database.py`.

## Music system

The `Music` cog delegates to several `music_helpers/` sub-modules.

### Key classes

| Class / file | Role |
|---|---|
| `MusicPlayer` (`music_player.py`) | Per-guild playback queue; `cleanup_source()` |
| `MediaBroker` (`media_broker.py`) | Aggregate lifecycle tracker (IN_FLIGHT → AVAILABLE → CHECKED_OUT) |
| `MediaRequest` (`types/media_request.py`) | Per-request user-facing state machine |
| `MediaRequestStateMachine` (`types/media_request.py`) | Drives bundle UI state transitions |
| `MultiMediaRequestBundle` (`types/media_request.py`) | Tracks progress for multi-track operations |
| `MediaDownload` (`types/media_download.py`) | Immutable yt-dlp metadata (id, title, webpage_url, duration, …) |
| `DownloadResult` (`types/download.py`) | Raw result from yt-dlp before broker handoff |
| `SearchResult` / `SearchCollection` (`types/search.py`) | Parsed search input; single result or multi-track collection |
| `DownloadClient` (`download_client.py`) | yt-dlp wrapper; exception hierarchy: `DownloadClientException → DownloadTerminalException / RetryableException` |
| `SearchClient` (`search_client.py`) | URL/string parsing; Spotify / YouTube / direct URL patterns; `SearchException` |
| `VideoCacheClient` (`video_cache_client.py`) | Local file cache with optional S3 backing |

### SearchType enum (`music_helpers/common.py`)

`SPOTIFY`, `YOUTUBE_PLAYLIST`, `YOUTUBE`, `DIRECT`, `SEARCH`, `OTHER`

### MediaBroker zones

`Zone.IN_FLIGHT` → `Zone.AVAILABLE` → `Zone.CHECKED_OUT`

`can_evict_base(webpage_url)` guards the shared base file;
`can_evict_request(uuid)` guards the per-guild copy.

Design doc: `docs/music/media_broker.md`.

## Scripts

### `scripts/restore_database.py`

Standalone CLI for database restoration:

```bash
python scripts/restore_database.py --config /path/to/config.yml \
    --file /path/to/backup.json          # from local file
    # or
    --s3-bucket my-bucket --s3-object backup.json  # from S3
    --clear   # drop existing rows before restore
    --verbose
```

## Error handling

- **No broad `except Exception`** in production code. Let exceptions propagate
  so tracebacks are visible.
- Catch only specific, known exceptions (e.g. `RateLimited`, `NotFound`,
  `PydanticValidationError`).
- `async_retry_discord_message_command` handles Discord-specific transient errors;
  everything else is a real bug.
- **Exception**: `_ReadItem` dispatch in `MessageDispatcher` uses a documented broad
  except to prevent `asyncio.Future` callers from hanging indefinitely. The exception
  is logged at ERROR with `exc_info=True` and re-raised via `future.set_exception`.

## Writing tests

### Fixtures

All shared test infrastructure is in `tests/helpers.py`:

```python
from tests.helpers import (
    fake_context,        # pytest fixture → dict with bot/guild/author/channel/context
    fake_engine,         # pytest fixture → async in-memory aiosqlite engine (AsyncEngine)
    fake_sync_engine,    # pytest fixture → sync in-memory SQLite engine (backup tests only)
    FakeChannel,         # channel.send() records messages in channel.messages list
    FakeGuild,
    FakeAuthor,
    FakeMessage,
    FakeVoiceClient,
    FakeContext,
    fake_bot_yielder,    # factory: fake_bot_yielder(channels=[...])() → FakeBot instance
    generate_fake_context,  # non-fixture version of fake_context
    fake_source_dict,    # create a MediaRequest for music tests
    fake_media_download, # context manager: yields MediaDownload with a temp audio file
    random_id,           # generate a random 12-digit integer
    random_string,       # generate a random lowercase string
    async_mock_session,  # async context manager: yields AsyncSession bound to fake_engine
    mock_session,        # sync context manager (backup tests only)
)
```

`fake_engine` is an `AsyncEngine` (`sqlite+aiosqlite:///:memory:`). Use
`async_mock_session` to open a session directly in tests:

```python
async with async_mock_session(fake_engine) as session:
    rows = (await session.execute(select(MyModel))).scalars().all()
```

`fake_context` returns a dict:
```python
{
    'bot':     FakeBot,      # bot.loop is None; bot.get_cog() always returns None
    'guild':   FakeGuild,
    'author':  FakeAuthor,
    'channel': FakeChannel,
    'context': FakeContext,
}
```

### Important: `bot.loop` is `None` in tests

`FakeBot.loop = None`. Code that runs during the test body (not inside `cog_load`)
must use `asyncio.get_running_loop()` rather than `self.bot.loop`:

```python
# Wrong — raises AttributeError in tests
self.bot.loop.create_task(...)

# Right — works everywhere
asyncio.get_running_loop().create_task(...)
```

`cog_load` is called by discord.py's runtime, not during unit tests, so
`self.bot.loop.create_task(...)` there is fine.

### Typical async test structure

```python
import pytest

@pytest.mark.asyncio
async def test_something(fake_context):  # pylint: disable=redefined-outer-name
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    ...
```

### Synchronising with the MessageDispatcher in tests

`fetch_object` goes through the LOW-priority queue — it is not a reliable barrier
for NORMAL-priority work. Use `drain_dispatcher` to wait for all NORMAL-priority
work to complete (copy into your test file as needed):

```python
async def drain_dispatcher(dispatcher, guild_id, timeout=5.0):
    """Wait until all NORMAL-priority work for guild_id has been processed."""
    import asyncio
    done = asyncio.Event()
    async def _sentinel():
        done.set()
    dispatcher.send_single(guild_id, [_sentinel])
    await asyncio.wait_for(done.wait(), timeout=timeout)
```

### Mocking dispatcher absence

`FakeBot.get_cog()` returns `None`, so `CogHelper.dispatch_fetch` and friends
fall back to direct `async_retry_discord_message_command` calls automatically.
No special setup is needed in most cog tests.

## Docs

Markdown docs live in `docs/`. Update them when changing public APIs:

- `docs/message_dispatcher.md` — MessageDispatcher API reference
- `docs/common.md` — CogHelper helpers
- `docs/monitoring/metrics_reference.md` — all exported metrics and spans
- `docs/monitoring/health_server.md`, `memory_profiling.md`, `process_metrics.md`
- Per-cog docs: `docs/markov.md`, `docs/music.md`, `docs/delete_messages.md`, etc.
- Music deep-dives: `docs/music/media_broker.md`, `flow.md`, `messaging.md`, etc.
