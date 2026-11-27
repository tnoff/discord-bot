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

This repo is eight Python packages — `discord_core` (shared by every pod)
plus one per pod (`discord_gateway`, `discord_dispatcher`, `discord_broker`,
`discord_db`, `discord_downloader`, `discord_search`, `discord_commands`), each with its own
`pyproject.toml` and its own real `dependencies`. There is no single-package
`.[bot,search,test]`-style extra any more; the root `pyproject.toml` only
holds the `test` extra and installs no production code of its own
(`packages = []`).

Use a virtualenv. Editable install every package you need, plus the root's
`test` extra — if in doubt, install all eight (this is what `tox.ini`'s
`deps =` does, and what the full test suite needs regardless of which pod
you're actually changing):

```bash
virtualenv venv
source venv/bin/activate
pip install -e ./discord_core -e ./discord_gateway -e ./discord_broker \
            -e ./discord_db -e ./discord_dispatcher -e ./discord_downloader \
            -e ./discord_search -e ./discord_commands -e ".[test]"
```

Each package's own `pyproject.toml` documents what it depends on and why
(e.g. `discord_search` pulls in spotipy/ytmusicapi, `discord_db` pulls in
SQLAlchemy/asyncpg/alembic). `discord_core` is the one every pod installs
alongside its own — it carries the shared set (aiohttp, redis, pydantic, the
OTel stack, etc.).

### Per-package versions

Each of the eight packages has its own `VERSION` file
(`discord_core/VERSION`, `discord_gateway/VERSION`, …) and its own
`CHANGELOG.md`. There is no repo-wide `VERSION`, `vX.Y.Z` tag or release: a
package is released by its own `<short>-vX.Y.Z` tag (`core-v…`, `gateway-v…`),
cut by `release.yml`'s `package-tags` job. The root `CHANGELOG.md` is the
history through 3.0.0 and is no longer written to.

A PR that touches a package bumps that package's own `VERSION`. A change confined to
the package's own `tests/` directory does not count — test code ships in no image or
distribution, so it would only cut an empty release — but source next to it does.
`ci.yml`'s "Detect package version bumps" job
(`ci/_package_versions.py`) fails a PR that changes a package without
also changing that package's `VERSION`. Renovate's `dev-` PRs are bumped for
you: the `bump-version` job runs the same module with `--list` to find the
touched packages, bumps each one's `VERSION` (patch) and adds a fragment under
that package's `changelog.d/`. Anything else bumps by hand, and may add a
fragment (`<package>/changelog.d/<name>.md`) to describe the change.

On merge, `release.yml` folds every package's fragments into its
`CHANGELOG.md` in a single commit (`docs: assemble changelog …`), then tags
each package whose `VERSION` is new. Tagging waits for that fold, so a tag
points at the commit that carries its own changelog section.

There is no dependency-propagation rule — a `discord_core`-only change does not
require bumping every pod's `VERSION` too. Every pod installs `discord_core`
from its own pinned `core-v…` tag, so a core change reaches a pod only when that
pod's pin is bumped, which is a change to the pod's `pyproject.toml` and bumps
its `VERSION` like any other.

Database tests run on SQLite (see `fake_engine` below), so the suite needs no
database server and nothing to start before running it.

## Running the bot

```bash
discord-gateway /path/to/config.yml
```

`discord-gateway` (`discord_gateway.cli.bot`) is one of seven pods and will not
start on its own — it hard-requires `general.dispatch_http_url`,
`general.database_http_url` and `general.commands_http_url` pointing at running
`discord-dispatcher`, `discord-db` and `discord-commands` instances (see [docs/configuration.md](docs/configuration.md#database)
and [docs/architecture.md](docs/architecture.md)). `docker/docker-compose.multiprocess.yml` is the
easiest way to bring up the other pods for local development.

Config schema is in README.md; full per-cog config keys are in each
`docs/<cog>.md`.

## Tests

**Always invoke through the project venv** — system Python has an older
`dappertable` that causes ~27 false failures.

Each package has its own `tests/` tree, mirroring its own layout
(`discord_core/tests/`, `discord_gateway/tests/`, …). A package's tests may use that package, the
pinned `discord_core` and the shared test infrastructure in `testing/` (fakes, helpers, suites), but not another pod's production
code, and core's tests import no pod at all. Anything that needs two pods (a real HTTP client in one
pod tested against a real server in another, the CI import-boundary/build-filter checks) is an integration
test and lives in the central `tests/` at repo root; `tests/checks/test_package_test_imports.py` enforces it, with no exceptions; the integration tests are in
`tests/integration/`, one directory per seam, named `<client>_to_<server>` (`commands_to_search`: the bot's commands pod
calling the search pod).

**Testing a seam: contract first, one round trip last.** A pod-to-pod seam is tested from each end against one
description of it, so neither end needs the other pod. The description is a `Contract` per route
(`testing/contracts/<seam>.py`: request model, declared replies, built from `discord_core` alone). The *server*
pod proves its handlers against it (`tests/routes/` runs every declared reply on a live handler, and the pod's own
`tests/servers/` cover the behaviour around that over raw HTTP). The *client* pod tests its client against a
`ContractStub` (`testing/contracts/stub.py`) that serves the contract: the stub fails the test if the client sends a
body the request model refuses, and refuses to play a reply the contract does not declare. What neither end can
show alone is that a value *means* the same thing on both sides, so `tests/integration/<client>_to_<server>/` keeps one round
trip per route against the real server. Media search is the first seam done this way; the rest still have the
older wire tests in `tests/integration/` and are being converted.

The repo-root folders besides the packages: `testing/` (the shared test infrastructure the pods' tests import: `helpers`,
`db_helpers`, the `*_suite` bases, and `fakes/`, the test doubles; it holds no tests), `tests/` (the tests that belong
to no single pod: `routes/` the pods' OpenAPI contracts, `integration/` the two-pod tests, `checks/` repo-wide
conventions, the generated-doc checks and the few tests that read two pods to pin a shared value or shape, `fakes/` the tests of the doubles, `ci/` the tests of the CI tooling) and `ci/`, the CI and doc tooling itself (change selection, the image build filter,
version and pin checks, the generators behind the generated doc blocks). `ci/` is not a package and ships in no
image; it is run as `python -m ci._<name>`. **Run pytest with no path argument** — passing
`tests/` explicitly collects only the central tree and silently skips the
other seven (this broke CI once, PR #1005, before the invocation was fixed):

```bash
venv/bin/pytest -q                                   # full suite, every package
venv/bin/pytest discord_gateway/tests/cogs/test_music.py -q   # single file
venv/bin/pytest --cov --cov-report=html               # coverage, no path arg either
```

Coverage threshold is 100% (`--cov-fail-under=100` in `tox.ini`), measured
against `source = ["."]` in `pyproject.toml`'s `[tool.coverage.run]` — repo-rooted,
not a single package. All async tests must be marked `@pytest.mark.asyncio`
(mode is `strict`); see `[tool.pytest.ini_options]` in `pyproject.toml`.

## Linting

```bash
venv/bin/pylint --ignore=tests discord_core/ discord_gateway/ discord_broker/ \
    discord_db/ discord_dispatcher/ discord_downloader/ discord_search/   # production code
venv/bin/pylint --rcfile .pylintrc.test tests/ testing/ ci/ discord_core/tests/ discord_gateway/tests/ \
    discord_broker/tests/ discord_db/tests/ discord_dispatcher/tests/ \
    discord_downloader/tests/ discord_search/tests/                       # test code, all eight locations
```

`--ignore=tests` on the production line matters: each package's own `tests/`
now lives *inside* it, so a plain scan would otherwise lint test code under
the production rcfile too.

Target score is 10.00/10. Tox runs both pylint invocations, bandit, and
pytest across py313–py315 (pytest narrowed to the packages a PR can affect — see
[Which tests CI runs](#which-tests-ci-runs)):

```bash
tox
```

## What CI skips

`ci.yml`'s `metadata` job runs `ci/_skippable_changes.py` over the PR's
diff and skips work a change cannot affect:

| The PR changes only | Skipped |
|---|---|
| `renovate.json` | everything |
| the above, root prose (`README.md`, `AGENTS.md`, `CONTRIBUTING.md`, `DEVELOPMENT.md`, `CHANGELOG.md`, `LICENSE`) or `docs/` | the tox matrix |

A docs-only PR still runs the secret scan, the version and pin checks and the
workflow-contract check, which take seconds. The image build is separately
conditional on the images a change reaches.

It is an allow-list: a path not named in the module runs everything. Four docs
and `docs/openapi/` are deliberately **not** on it, because tests compare them
against the code (`docs/architecture.md`, `docs/configuration.md`,
`docs/monitoring/metrics_reference.md`, `docs/monitoring/trace_linking.md`); editing
one by hand can fail a test. The exception is an edit to one of those four that
leaves every `<!-- BEGIN GENERATED(...) -->` block byte-identical: the test splices a
regenerated body into those blocks and compares, so prose around them cannot change
the result, and that edit skips tox. A new or deleted doc, or any change inside a
block, runs it. `test_no_test_reads_a_file_that_skips_tox` fails if a test starts
reading a doc that is on the list. Prose inside a package, including its
`changelog.d/`, is that package changing and is never skipped.

## Which tests CI runs

`ci.yml`'s `tox` job runs `ci/_run_tests.py` (called from `tox.ini`), which narrows
pytest to the packages and central test files a pull request can affect. The same module is the only place pytest
is invoked now, so its docstring is where the two-pass structure and the coverage rules live.

How it decides, from `ci/_affected_tests.py`: it parses every first-party file and builds
the import graph (imports, relative imports, and dotted strings such as `monkeypatch` targets,
followed through the shared `testing.*` fakes). A package's `tests/` tree runs when something it can
reach changed, and so does each file of the central `tests/` tree, one at a time. Rules worth knowing:

- **A `discord_core` change selects only `discord_core`'s tests.** Core is a released, versioned
  artifact: every pod pins a tag, and the pods' tests (and the central ones) run against that pin, not this
  checkout. The pods meet a new core in the PR that bumps their pin, which is where a break shows up.
  That only holds because pytest is told not to put the repo root first on `sys.path`
  (`--import-mode=append`, and `python -P` in CI); `ci/_run_tests.py` checks the installed core
  against the pin before a run and refuses to narrow if it is not the pin. `test_every_pod_pins_core`
  guards the pins.
- **A central test file that reads the repo always runs.** The import graph cannot see a dependency
  that is not an import, so any file that (or whose shared helpers) mentions `REPO_ROOT`/`__file__`,
  runs `subprocess`, reads TOML, or holds a string naming a repo path (`docs/…`, `.github/…`,
  `discord_x/…`) is on the always-run side. That is **derived on every run**, not a list to maintain;
  `repo_reads` in `_affected_tests.py` is the detector, and it is deliberately generous.
- **Central inputs don't force a full run.** `docs/openapi/`, `docs/monitoring/`, `docs/architecture.md`,
  `docs/configuration.md`, `.github/`, `docker/`, `.dockerignore`, `.pylintrc*`, `catalog-info.yaml` and the
  measured image closure are read only by those always-running files (or by nothing), so changing one
  selects no package.
  `test_no_package_test_reads_a_central_input` fails if a package's own tests start naming one;
  if you need that, drop the path from `CENTRAL_INPUTS`.
- **A package's own inputs select only that package.** `alembic/` and `alembic.ini` are read only by
  `discord_db`'s tests (and the always-running central ones), so changing one runs the db test tree and
  not the packages that import `discord_db`. `test_only_the_owning_package_tests_read_a_package_input`
  fails if another package's tests start reading one.
- **Anything else it cannot classify runs everything**: the root `pyproject.toml` (a test-dependency bump can change test behavior), `tox.ini`,
  a data file under `tests/`, a deleted or moved file. So does every run that is not a
  pull request: local `tox`, the weekly job.

Coverage is gated at 100% on the whole tree for a full run. A narrowed run gates each package
whose *source* changed, which is exact because every test that can execute it is in the run. A
test-only edit is not gated; the weekly full run in `scheduled.yml` is what would catch a
coverage drop from one.

**Locally** a plain `venv/bin/pytest` uses whichever core your venv has, normally an editable install of this
checkout, which is convenient for development. CI uses the pin. A change to a pod that needs core code
that has not been released yet will therefore pass locally and fail there until core is tagged and the pins
are bumped, which is the point.

Core is covered only by its own tests (`discord_core/tests`), because the pods and the central tests run
against the pinned release, not this checkout, so the 100% gate on the whole tree also holds core to 100%
on its own terms. A core change needs core tests; a pod test no longer carries it.

Try it before pushing, from a venv with the packages installed:

```bash
venv/bin/python -m ci._affected_tests --base origin/main      # what would run
venv/bin/python -m ci._run_tests --base origin/main --dry-run  # the commands
venv/bin/python -m ci._run_tests --base origin/main            # run them
```

**What keeps it narrow.** The savings exist because a test does not import pods it does not
use, and does not read the repo. Importing a pod from the root `conftest.py` or from
`testing/helpers.py` puts that pod on *every* test's path and silently turns the narrowing back into a
full run; that coupling once made most PRs select all eight packages.
`test_root_conftest_reaches_no_pod` fails if it comes back, and
`test_the_root_conftest_and_shared_helpers_read_nothing_from_the_repo` is the same guard for a
repo read, which would mark every central file as always-run. Put fixtures that need a pod in a
module only the tests that need them import, as `testing/db_helpers.py` does.

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

The db pod rolls with the old pod still serving while the new one migrates, so
the old code briefly runs against the new schema. Keep a revision additive (new
table, nullable column, index). Anything that drops, renames or `alter_column`s
what the previous release still uses must be split: release N adds the new
schema and uses it, release N+1 removes the old one. That second revision sets
`CONTRACT_STEP = '<why it is safe>'`, and for that one release discord-db's
strategy goes back to `Recreate`. `discord_db/tests/test_migration_guard.py`
fails a new revision that does this without the marker.

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
is no separate database-aware subclass. The gateway's cogs reach no database: persistence belongs to the commands pod and the broker,
which call the db pod through HTTP store wrappers, not a local `AsyncEngine`. See [Dispatch
helpers](#dispatch-helpers) below for the dispatcher methods every cog gets.

```python
from discord_gateway.cogs.common import CogHelperBase
from discord_core.exceptions import CogMissingRequiredArg
from pydantic import BaseModel

class MyCogConfig(BaseModel):
    loop_sleep_interval: float = 300.0

class MyCog(CogHelperBase):
    def __init__(self, bot, settings, dispatcher, redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('my_cog', False):
            raise CogMissingRequiredArg('MyCog not enabled')
        super().__init__(bot, settings, dispatcher, redis_manager=redis_manager,
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

No cog in `discord_gateway` holds a database session or a database client: that pod has no DB
engine and does not call the db pod at all. The commands pod reaches data through the HTTP store
wrappers in `discord_commands/clients/` (`HttpMarkovStore`, `HttpPlaylistStore`,
`HttpGuildAnalyticsStore`), and the broker through its own narrower ones, all of which call the
`discord-db` pod over HTTP.

The only package with a real SQLAlchemy engine is `discord_db` itself.
SQLAlchemy 2.x, fully async (`AsyncEngine`; asyncpg or aiosqlite). PostgreSQL
and SQLite are both supported backends, so anything written inside `discord_db`
has to behave the same on both: no `FOR UPDATE` for correctness (SQLite drops
it; use an atomic `UPDATE`), no dialect-specific SQL, and use `UTCDateTime`
(not `DateTime`) for timestamps so SQLite reads back aware UTC like postgres. Code that runs inside `discord_db` opens a session via
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

Shared fixtures and fakes live in three modules, split by what they need to import
(see [Which tests CI runs](#which-tests-ci-runs) for why the split matters):

- `testing/helpers.py` — pure helpers and fakes. Imports `discord_core` and discord.py
  only, because the root `conftest.py` imports it and so does every package's test run.
- `testing/db_helpers.py` — `fake_engine`, `fake_stores`, `fake_markov_store` and
  `async_mock_session`. Needs `discord_db`; import it only from tests that use a real engine.
- `testing/fakes/in_process_stack.py` — `attach_in_process_broker`, `_download` and `_search`,
  `queue_in_broker`, `play_through_in_broker`. Needs the broker, downloader, search and db fakes.

| Name | Module | Kind | What it provides |
|------|--------|------|------------------|
| `fake_context` | `testing.helpers` | fixture | dict with `bot/guild/author/channel/context` |
| `fake_engine` | `testing.db_helpers` | fixture | SQLite `AsyncEngine` built by `setup_db` over a fresh temp file, schema from `create_all` |
| `async_mock_session` | `testing.db_helpers` | async ctx mgr | `AsyncSession` bound to `fake_engine` |
| `fake_bot_yielder` | `testing.helpers` | factory | `fake_bot_yielder(channels=[...])() → FakeBot` |
| `generate_fake_context` | `testing.helpers` | non-fixture | inline equivalent of `fake_context` |
| `fake_source_dict` | `testing.helpers` | helper | constructs a `MediaRequest` for music tests |
| `fake_media_download` | `testing.helpers` | ctx mgr | yields a `MediaDownload` with a temp audio file |
| `random_id`, `random_string` | `testing.helpers` | helpers | test data generators |
| `FakeBot`, `FakeGuild`, `FakeAuthor`, `FakeChannel`, `FakeMessage`, `FakeVoiceClient`, `FakeContext` | `testing.helpers` | fakes | drop-in substitutes for discord.py objects |

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
