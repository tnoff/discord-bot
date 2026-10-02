# Development

Setup, test, lint, and conventions for working in this repo. User-facing
configuration is in [README.md](README.md) and
[docs/configuration.md](docs/configuration.md). Per-cog and subsystem docs
are under [`docs/`](docs/).

## System dependencies

`ffmpeg` must be on `PATH` for music-related tests and the music cog:

```bash
# Debian/Ubuntu
apt install ffmpeg

# macOS
brew install ffmpeg
```

## Installation

This repo is seven Python packages — `discord_core` (shared by every pod)
plus one per pod (`discord_gateway`, `discord_dispatcher`, `discord_broker`,
`discord_db`, `discord_downloader`, `discord_search`), each with its own
`pyproject.toml` and its own real `dependencies`. There is no single-package
`.[bot,search,test]`-style extra any more; the root `pyproject.toml` only
holds the `test` extra and installs no production code of its own
(`packages = []`).

Use a virtualenv. Editable install every package you need, plus the root's
`test` extra — if in doubt, install all seven (this is what `tox.ini`'s
`deps =` does, and what the full test suite needs regardless of which pod
you're actually changing):

```bash
virtualenv venv
source venv/bin/activate
pip install -e ./discord_core -e ./discord_gateway -e ./discord_broker \
            -e ./discord_db -e ./discord_dispatcher -e ./discord_downloader \
            -e ./discord_search -e ".[test]"
```

Each package's own `pyproject.toml` documents what it depends on and why
(e.g. `discord_search` pulls in spotipy/ytmusicapi, `discord_db` pulls in
SQLAlchemy/asyncpg/alembic). `discord_core` is the one every pod installs
alongside its own — it carries the shared set (aiohttp, redis, pydantic, the
OTel stack, boto3, etc.).

### Per-package versions

Each of the seven packages also has its own `VERSION` file now
(`discord_core/VERSION`, `discord_gateway/VERSION`, …), tracked
independently of the root `VERSION`. The root file is unrelated and untouched
by this — it still drives the one repo-wide release/changelog/tag process
(`assemble-changelog`, `CHANGELOG.md`, the single `vX.Y.Z` tag), unchanged
since before this split. A PR that touches a package bumps that package's own
`VERSION` by hand, the same way the root `VERSION` has always been bumped by
hand as part of the PR that needed it — touching a package's nested `tests/`
counts as touching the package, no exemptions. `ci.yml`'s "Detect package
version bumps" job (`tests/cli/_package_versions.py`) enforces this: it fails
a PR that changes a package without also changing that package's `VERSION`.
There is no dependency-propagation rule yet — a `discord_core`-only change
does not require bumping every pod's `VERSION` too, since every pod still
installs `discord_core` from the live checkout path rather than a pinned
release, so there is nothing downstream to re-pin.

The test suite uses `pytest-postgresql`, which expects `pg_ctl` and friends
on `PATH`. Install the system postgres binaries (`apt install postgresql`
or `brew install postgresql`) or run `docker compose -f docker/docker-compose.multiprocess.yml up -d postgres`
before running the suite.

## Running the bot

```bash
discord-bot /path/to/config.yml
```

`discord-bot` (`discord_gateway.cli.bot`) is one of six pods and will not
start on its own — it hard-requires `general.dispatch_http_url` and
`general.database_http_url` pointing at running `discord-dispatcher` and
`discord-db` instances (see [docs/configuration.md](docs/configuration.md#database)
and [docs/architecture.md](docs/architecture.md)). `docker/docker-compose.multiprocess.yml` is the
easiest way to bring up the other pods for local development.

Config schema is in README.md; full per-cog config keys are in each
`docs/<cog>.md`.

## Tests

**Always invoke through the project venv** — system Python has an older
`dappertable` that causes ~27 false failures.

Each package has its own `tests/` tree, mirroring its own layout
(`discord_core/tests/`, `discord_gateway/tests/`, …); only genuinely
cross-package tests (a real HTTP client in one package tested against a real
server in another, the CI import-boundary/build-filter checks) live in the
central `tests/` at repo root. **Run pytest with no path argument** — passing
`tests/` explicitly collects only the central tree and silently skips the
other six (this broke CI once, PR #1005, before the invocation was fixed):

```bash
venv/bin/pytest -q                                   # full suite, every package
venv/bin/pytest discord_gateway/tests/cogs/test_music.py -q   # single file
venv/bin/pytest --cov --cov-report=html               # coverage, no path arg either
```

Coverage threshold is 99% (`--cov-fail-under=99` in `tox.ini`), measured
against `source = ["."]` in `pyproject.toml`'s `[tool.coverage.run]` — repo-rooted,
not a single package. All async tests must be marked `@pytest.mark.asyncio`
(mode is `strict`); see `[tool.pytest.ini_options]` in `pyproject.toml`.

## Linting

```bash
venv/bin/pylint --ignore=tests discord_core/ discord_gateway/ discord_broker/ \
    discord_db/ discord_dispatcher/ discord_downloader/ discord_search/   # production code
venv/bin/pylint --rcfile .pylintrc.test tests/ discord_core/tests/ discord_gateway/tests/ \
    discord_broker/tests/ discord_db/tests/ discord_dispatcher/tests/ \
    discord_downloader/tests/ discord_search/tests/                       # test code, all eight locations
```

`--ignore=tests` on the production line matters: each package's own `tests/`
now lives *inside* it, so a plain scan would otherwise lint test code under
the production rcfile too.

Target score is 10.00/10. Tox runs both pylint invocations, bandit, and
pytest across py311–py314:

```bash
tox
```

## Alembic migrations

Alembic reads `DATABASE_URL` from the environment.

```bash
alembic upgrade head                                       # apply migrations
alembic revision --autogenerate -m "description of change" # generate one
```

After editing `discord_db/database.py`, regenerate the revision and review
the generated `op.*` calls — autogenerate doesn't catch every change. Alembic
itself, and the schema it manages, belong only to the `discord_db` pod — no
other package has a database connection.

## Adding a new cog

Cogs live only in `discord_gateway` now — there is no dispatcher-only cog
mode; `discord_dispatcher` runs a single Redis-backed worker
(`MessageDispatcher`), not a cog list. Two files change:

1. **`discord_core/utils/common.py`** — if you want the cog to be enabled
   via the typed Pydantic config, add a field to `IncludeConfig`. (Some
   cogs read `general.include.<name>` straight from the raw dict — see
   `discord_gateway/cogs/role.py`. The Pydantic field is optional but
   recommended.)

   ```python
   class IncludeConfig(BaseModel):
       my_cog: bool = False
   ```

2. **`discord_gateway/cli/_lib/cog_registry.py`** — append to
   `POSSIBLE_COGS`. There is no ordering requirement any more —
   `MessageDispatcher` isn't in this list at all, since it's a separate
   pod's own worker, not a cog.

   ```python
   POSSIBLE_COGS = [
       DeleteMessages,
       ...
       MyCog,
   ]
   ```

3. **`discord_gateway/cogs/my_cog.py`** — implement the cog (see below).

### Cog skeleton

Every cog inherits `CogHelperBase` (`discord_gateway/cogs/common.py`) — there
is no separate database-aware subclass. Cogs reach the database, if they
need it, through an HTTP store wrapper (`discord_gateway/clients/database_stores.py`)
passed in via `stores`, not a local `AsyncEngine`. See [Dispatch
helpers](#dispatch-helpers) below for the dispatcher methods every cog gets.

```python
from discord_gateway.cogs.common import CogHelperBase
from discord_core.exceptions import CogMissingRequiredArg
from pydantic import BaseModel

class MyCogConfig(BaseModel):
    loop_sleep_interval: float = 300.0

class MyCog(CogHelperBase):
    def __init__(self, bot, settings, dispatcher, stores=None):
        if not settings.get('general', {}).get('include', {}).get('my_cog', False):
            raise CogMissingRequiredArg('MyCog not enabled')
        super().__init__(bot, settings, dispatcher, stores=stores,
                         settings_prefix='my_cog',
                         config_model=MyCogConfig)
        # self.config.loop_sleep_interval now available
```

`CogHelperBase.__init__` provides:

- `self.bot`, `self.settings`, `self.dispatcher` (an `HttpDispatchClient`
  in the one entrypoint that actually builds cogs, `discord_gateway/cli/bot.py`)
- `self.logger` — name is the lowercase class name; config from
  `general.logging`
- `self.config` — Pydantic-validated cog config (if `config_model` supplied)

There is no `self.db_engine` — no cog holds a database connection.

### Dispatch helpers

Prefer the helpers over calling `async_retry_discord_message_command`
directly. They live on `CogHelperBase`:

```python
# Send (NORMAL priority, returns the content for early-exit patterns)
content = await self.dispatch_message(guild_id, channel_id, 'hello')

# Delete a message by ID (NORMAL priority)
await self.dispatch_delete(guild_id, channel_id, message_id)

# Fetch any Discord object with retry (LOW priority)
result = await self.dispatch_fetch(guild_id, partial(channel.history, limit=100))

# Fire-and-forget request/response (results land in self._result_queue —
# call self.register_result_queue() once in cog_load first)
await self.dispatch_channel_history(guild_id, channel_id, limit=100)
await self.dispatch_guild_emojis(guild_id)
```

The helpers route through `self.dispatcher`, injected at construction time —
in the one entrypoint that builds cogs (`discord_gateway/cli/bot.py`) that is
always an `HttpDispatchClient` talking to the `discord-dispatcher` pod. There
is no in-process fallback and no config toggle any more; `dispatch_http_url`
is a hard requirement of the bot process itself, checked before any cog is
even loaded. `dispatch_fetch` is the one exception worth knowing about: it
calls `self.dispatcher.fetch_object(...)`, which only the dispatcher-pod-only
`MessageDispatcher` class implements — `HttpDispatchClient` has no
`fetch_object` method, so calling `dispatch_fetch` from a real cog raises
`AttributeError` today. It is exercised only in unit tests with a fake
dispatcher; don't copy it into new code until that's reconciled.

There is no `send_funcs` method — it does not exist anywhere in
`CogHelperBase` or any cog.

### Background loop

```python
from discord_core.utils.common import return_loop_runner

async def cog_load(self):
    self._task = self.bot.loop.create_task(
        return_loop_runner(self.my_loop, self.bot, self.logger)()
    )

async def cog_unload(self):
    if self._task:
        self._task.cancel()

async def my_loop(self):
    # one iteration — return_loop_runner re-invokes indefinitely
    ...
```

### Heartbeat gauge

Every cog with a background loop should register a heartbeat gauge so
ops can alert on stuck loops. See
[docs/monitoring/metrics_reference.md](docs/monitoring/metrics_reference.md)
for the canonical pattern; new `MetricNaming` entries go in
`discord_core/utils/otel.py`.

## Database

No cog in `discord_gateway` holds a database session — that pod has no DB
engine at all. Cogs reach data through the HTTP store wrappers in
`discord_gateway/clients/database_stores.py` (`HttpMarkovStore`,
`HttpPlaylistStore`, `HttpGuildAnalyticsStore`, `HttpVideoCacheStore`, …),
which call the `discord-db` pod over HTTP.

The only package with a real SQLAlchemy engine is `discord_db` itself.
SQLAlchemy 2.x, fully async (`AsyncEngine`, asyncpg). PostgreSQL is the only
supported backend. Code that runs inside `discord_db` opens a session via
`with_db_session()` (`discord_db/cli/database.py`):

```python
from sqlalchemy import select, delete

async with with_db_session() as db:
    row  = (await db.execute(select(Model).where(Model.id == x))).scalars().first()
    rows = (await db.execute(select(Model).where(...))).scalars().all()
    n    = (await db.execute(select(func.count()).select_from(Model).where(...))).scalar()
    await db.execute(delete(Model).where(Model.id == x))
    await db.commit()
```

`session.query()` is **not** supported on `AsyncSession`.

### DB retry

`async_retry_database_commands` (`discord_db/utils/sql_retry.py`) retries on
`OperationalError` (rollback + sleep) and `PendingRollbackError` (rollback),
up to 3 attempts:

```python
from discord_db.utils.sql_retry import async_retry_database_commands

result = await async_retry_database_commands(
    db_session,
    lambda: database_functions.get_x(db_session, ...),
)
await async_retry_database_commands(db_session, db_session.commit)
```

This helper, like the session it wraps, is `discord_db`-internal — nothing
outside that pod opens a database connection to retry against.

## Error handling

- **No broad `except Exception`** in production code — let it propagate so
  tracebacks are visible. Catch only specific exceptions (`RateLimited`,
  `NotFound`, `PydanticValidationError`, etc.).
- `async_retry_discord_message_command` handles Discord transient errors
  (`RateLimited`, `DiscordServerError`, `TimeoutError`,
  `ServerDisconnectedError`); everything else is a real bug.
- The sanctioned broad-excepts live in the dispatcher worker loop and its
  request handlers (`discord_dispatcher/workers/message_dispatcher.py`,
  each tagged `# pylint: disable=broad-except`) — see
  [AGENTS.md](AGENTS.md#broad-except-is-rare-and-pylint-annotated).

## Test infrastructure

Shared fixtures and fakes are in `tests/helpers.py`:

| Name | Kind | What it provides |
|------|------|------------------|
| `fake_context` | fixture | dict with `bot/guild/author/channel/context` |
| `fake_engine` | fixture | `AsyncEngine` against a session-scoped postgres (via `pytest-postgresql`); tables truncated per test |
| `async_mock_session` | async ctx mgr | `AsyncSession` bound to `fake_engine` |
| `fake_bot_yielder` | factory | `fake_bot_yielder(channels=[...])() → FakeBot` |
| `generate_fake_context` | non-fixture | inline equivalent of `fake_context` |
| `fake_source_dict` | helper | constructs a `MediaRequest` for music tests |
| `fake_media_download` | ctx mgr | yields a `MediaDownload` with a temp audio file |
| `random_id`, `random_string` | helpers | test data generators |
| `FakeBot`, `FakeGuild`, `FakeAuthor`, `FakeChannel`, `FakeMessage`, `FakeVoiceClient`, `FakeContext` | fakes | drop-in substitutes for discord.py objects |

`FakeChannel.send()` records every message in `channel.messages`, so tests
assert against the list directly.

Cog tests construct `CogHelperBase` subclasses with a fake or mocked
dispatcher passed in directly (there is no `self.bot.get_cog()`-based lookup
or fallback any more — the dispatcher is a required constructor argument).

### Typical async test

```python
import pytest

@pytest.mark.asyncio
async def test_something(fake_context):  # pylint: disable=redefined-outer-name
    guild_id = fake_context['guild'].id
    channel = fake_context['channel']
    ...
```

### Synchronising with `MessageDispatcher`

This only applies to `discord_dispatcher`'s own worker tests
(`discord_dispatcher/tests/cogs/test_message_dispatcher.py`) — cog tests in
`discord_gateway` interact with a fake/mocked `HttpDispatchClient`, not a
real `MessageDispatcher`, and have nothing to drain. `MessageDispatcher` is
Redis-backed now (a `WorkQueue`/`BundleStore` pair, not an in-process
`asyncio.PriorityQueue`), so synchronising means polling its work queue
empty, not waiting on a sentinel through a priority ordering:

```python
async def drain_dispatcher(dispatcher, timeout=5.0):
    """Wait until the work queue is empty and all in-flight work has completed."""
    await asyncio.sleep(0)  # let pending enqueue create_tasks run first
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if dispatcher._work_queue._queue.empty():  # pylint: disable=protected-access
            return
        await asyncio.sleep(0.01)
```

### `bot.loop` in tests

`FakeBot.loop = None`. Any code that runs during the test body (not inside
`cog_load`) must use `asyncio.get_running_loop()` rather than
`self.bot.loop`. See [AGENTS.md](AGENTS.md#botloop-is-none-in-tests) for
why.
