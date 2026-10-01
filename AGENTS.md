# AGENTS.md

Guidance for AI coding agents working in this repository. For user-facing
configuration and CLI usage see [README.md](README.md) and
[docs/configuration.md](docs/configuration.md). For setup, tests,
linting, and how to add cogs / commands see [DEVELOPMENT.md](DEVELOPMENT.md).
Per-cog and subsystem docs live under [`docs/`](docs/) — that's the
authoritative reference for the message dispatcher, monitoring, music
internals, etc.

## Where things live

This repo is seven Python packages, not one. `discord_core` is the shared
package every pod installs; the other six are the deployable pods
(`discord_gateway`, `discord_dispatcher`, `discord_broker`, `discord_db`,
`discord_downloader`, `discord_search`), each with its own `pyproject.toml`
and its own `dependencies`. There is no `discord_bot` import root any more —
if you see one in an older doc, comment, or your own training data's
assumptions about this repo, it's stale; check the current tree instead.

| Topic | Location |
|-------|----------|
| Bot (gateway) entry-point — the `discord-bot` process: gateway connection + cogs, HTTP client of every other pod | `discord_gateway/cli/bot.py` (registered as `discord-bot`) |
| Dispatcher entry-point — the `discord-dispatcher` process | `discord_dispatcher/cli/dispatcher.py` (registered as `discord-dispatcher`, a separate package, not a sibling module) |
| `POSSIBLE_COGS` registry | `discord_gateway/cli/_lib/cog_registry.py` |
| `CogHelperBase` + dispatch helpers | `discord_gateway/cogs/common.py` (see `DEVELOPMENT.md`'s Cog skeleton / Dispatch helpers sections). There is no separate `CogHelper` class with DB-session helpers — that was retired along with the bot's direct database connection; `CogHelperBase` takes an injected `dispatcher` and an optional remote `stores` bundle, not a `db_engine`. |
| `MessageDispatcher` worker | `discord_dispatcher/workers/message_dispatcher.py` (see `docs/message_dispatcher.md`) — its own package, not a module under the bot's |
| Dispatch protocols / interfaces | `discord_core/interfaces/dispatch_protocols.py` |
| Music subsystem (cog + gateway-side helpers) | `discord_gateway/cogs/music.py` + `discord_gateway/cogs/music_helpers/` (see `docs/music/`) — download/search/broker/db-side pieces live in their own pods, see the project layout below |
| Config models (`GeneralConfig`, `IncludeConfig`, …) | `discord_core/utils/common.py` |
| OTel naming enums, span/metric wrappers | `discord_core/utils/otel.py` (see `docs/monitoring/`) |
| DB models (`BASE`-inheriting) | `discord_db/database.py` |
| Async retry helpers | `discord_core/utils/common.py`; `discord_db/utils/sql_retry.py` (db pod only — the bot holds no DB connection) |
| Test fixtures and fakes | `tests/helpers.py` (central — see "Tests are per-package now" below) |

## Non-obvious internals

### Use `venv/bin/pytest`, not system `pytest`

The system environment has an older `dappertable` (0.2.4) missing kwargs used
throughout the tests, producing ~27 false failures. Always invoke pytest
through the project venv. Same goes for pylint.

### Tests are per-package now — do not pass `tests/` to pytest

Each of the seven packages has its own `tests/` tree
(`discord_core/tests/`, `discord_gateway/tests/`, …), mirroring that
package's own layout. Only genuinely cross-package tests (a real HTTP
client in one package tested against a real server in another, the
import-boundary/CI-build-filter checks) stay in the central `tests/` at
repo root. Run pytest with **no path argument** — bare `pytest --cov ...`
from repo root, exactly as `tox.ini` does. Passing `tests/` explicitly
collects only the central tree and silently skips the other six; this
already broke CI once (PR #1005) before the invocation was fixed.
Similarly, `pip install -e .` alone only installs the root project's `test`
extra — you also need each package you're touching installed, e.g.
`pip install -e ./discord_core -e ./discord_gateway ...` (see
`tox.ini`'s `deps =` for the full list, or install all seven if unsure).

### `MessageDispatcher` is a separate pod, not a cog

`MessageDispatcher` (`discord_dispatcher/workers/message_dispatcher.py`) is
the sole worker of the standalone `discord-dispatcher` pod, its own package
(`discord_dispatcher`), reached over HTTP. It is not one of `discord_gateway`'s
cogs and is not in `POSSIBLE_COGS`. `CommandErrorHandler` is registered
unconditionally before any cog list is loaded.

See [HA architecture](docs/architecture.md#ha-is-the-only-mode) for why
`dispatch_http_url`/`database_http_url` are hard-required with no
single-process fallback. The one code-shape fact worth repeating here:
every cog receives its dispatcher as an already-injected `HttpDispatchClient`
via `CogHelperBase.__init__` — there is no lazy `self._dispatcher` property
to reach for.

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

### Database URL rewriting is automatic — and only in `discord_db`

The `discord-db` CLI (`discord_db/cli/_lib/db.py`) rewrites `postgresql://` →
`postgresql+asyncpg://` at startup. PostgreSQL is the only supported backend;
non-postgres drivernames raise at boot. Config files use the standard
`postgresql://` URL; don't write `+asyncpg` into config or the rewrite
double-applies. No other pod does this rewrite, because no other pod opens a
database connection — `sql_connection_statement` is `discord-db`'s own config
key, read only there.

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
`discord_gateway/cogs/music_helpers/music_player.py`, not a method on
`MusicPlayer`. It calls the audio source's own `.cleanup()`; anywhere the
player drops a track, route through this function rather than relying on
`voice_client.stop()` alone. Playback uses plain `discord.PCMAudio`
(pre-decoded PCM in memory), not `FFmpegPCMAudio` — there is no ffmpeg
subprocess to leak here any more, but the cleanup call is still the only
place that releases the source's underlying buffer/file handle.

### Broker zone transitions are one-way

`Zone.IN_FLIGHT → Zone.AVAILABLE → Zone.CHECKED_OUT` (`discord_broker/interfaces/broker_protocols.py`).
There is no `MediaBroker` class any more — the Redis-backed implementation
is `RedisBroker` (`discord_broker/workers/redis_broker.py`), reached by
every other pod over HTTP via `HttpBrokerClient`
(`discord_core/clients/http_broker_client.py`). Eviction guards
(`can_evict_base`, `can_evict_request`, on `RedisBroker`) must succeed
before you delete the underlying file — otherwise an in-flight or
checked-out copy gets pulled out from under a consumer. Full design in
`docs/music/media_broker.md`.

### Heartbeat gauge pattern

Every cog with a background loop registers an observable gauge keyed on
`AttributeNaming.BACKGROUND_JOB.value`. The callback returns `1` when the
task is live and `0` when it's done. New metric names go into
`MetricNaming` in `discord_core/utils/otel.py` before first use. See
`docs/monitoring/metrics_reference.md` for the full list.

## Project layout

Seven top-level packages, each with its own `pyproject.toml` and its own
`tests/` (mirroring that package's own layout — see "Tests are per-package
now" below). No single tree to walk any more; a per-package summary, not a
full file listing (that goes stale the moment a file moves — check the real
tree with `find <package> -maxdepth 2 -type d` rather than trusting a
hand-maintained list here):

- **`discord_core/`** — shared by every pod. `cli/_lib/` (bot-lifecycle
  helpers imported by every entrypoint), `clients/` (dispatch/broker/seam
  HTTP client base classes), `cogs/` (shared cog-support code, e.g.
  `cogs/music_helpers/common.py`), `interfaces/`, `routes/` (the HTTP
  contract registries every pod's routes are declared against — "seams" are
  a route shape here, not a package), `servers/` (shared aiohttp server
  base + health-server base), `types/`, `utils/` (config models, otel
  naming, loop health, retry helpers), `workers/`.
- **`discord_gateway/`** — the `discord-bot` pod: gateway connection + all
  cogs. `cli/bot.py` is the entrypoint; `cli/_lib/cog_registry.py` holds
  `POSSIBLE_COGS`. `cogs/` holds every cog (`common.py` is `CogHelperBase`);
  `cogs/music_helpers/` holds the gateway-side pieces of the music
  subsystem (`music_player.py`, `search_client.py` — download, search,
  broker and db-side pieces live in their own pods below). `clients/` holds
  this pod's own HTTP client wrappers for talking to the other pods
  (`http_download_client.py`, `http_markov_store.py`, etc.).
- **`discord_dispatcher/`** — the `discord-dispatcher` pod. `workers/message_dispatcher.py`
  is the `MessageDispatcher` worker (Redis-backed, per-guild); `servers/`
  serves its HTTP API.
- **`discord_broker/`** — the `discord-broker` pod: S3 media checkout/release
  coordination. `workers/media_bundle.py` holds `BundleState`/`BundleRenderer`
  (the split of what used to be one `MultiMediaRequestBundle` class);
  `workers/redis_broker.py` / `broker_registry.py` are the Redis-backed
  broker itself.
- **`discord_db/`** — the `discord-db` pod: the only one with a real
  PostgreSQL connection. `database.py` holds the SQLAlchemy models;
  `cli/_lib/migrations.py` runs alembic; `clients/` and `cogs/music_helpers/`
  hold the store implementations the other pods reach over HTTP.
- **`discord_downloader/`** — the `discord-downloader` pod: yt-dlp downloads.
  `workers/redis_download_worker.py` is the HA worker;
  `interfaces/download_protocols.py` defines the shared download protocol.
- **`discord_search/`** — the `discord-search` pod: media-search + YouTube
  Music search. `clients/media_search_client.py`,
  `workers/youtube_music_search_driver.py`.

Central `tests/`, `docs/`, `alembic/`, `docker/` sit alongside the seven
packages at the repo root, shared across all of them.

## Things to keep doing

- Use `self.dispatch_message(guild_id, channel_id, content)` /
  `self.dispatch_fetch(guild_id, func)` / `self.dispatch_delete(...)`
  instead of calling `async_retry_discord_message_command` directly. See
  `discord_gateway/cogs/common.py` for the full helper set and
  `docs/message_dispatcher.md` for the underlying mechanism. `dispatch_fetch`
  routes through the dispatcher's `fetch_object`, which only
  `MessageDispatcher` itself implements — the `HttpDispatchClient` every
  gateway cog actually receives does not have this method, so calling
  `dispatch_fetch` from a real cog raises `AttributeError` today. Treat it as
  dead code until that's reconciled, not as a pattern to copy.
- No cog in `discord_gateway` holds a database session — that pod has no DB
  engine at all. Reach data through the HTTP store wrappers in
  `discord_gateway/clients/database_stores.py` (`HttpMarkovStore`,
  `HttpPlaylistStore`, `HttpGuildAnalyticsStore`, …), which call the
  `discord-db` pod. `select()` / `delete()` / `AsyncSession` and an actual
  commit-retry helper only exist inside `discord_db` itself
  (`discord_db/cli/database.py`), for code that runs in that pod.
- Use `async_otel_span_wrapper` with `async with` for spans, or
  `@command_wrapper` on command handlers.
- Pydantic-validate every new config section by passing a `config_model` to
  `CogHelperBase.__init__`. Validation errors should raise
  `CogMissingRequiredArg`.
