# Development

Setup, test, lint, and conventions for working in this repo. User-facing
configuration is in [README.md](README.md). Per-cog and subsystem docs are
under [`docs/`](docs/).

## System dependencies

`ffmpeg` must be on `PATH` for music-related tests and the music cog:

```bash
# Debian/Ubuntu
apt install ffmpeg

# macOS
brew install ffmpeg
```

## Installation

Use a virtualenv. Editable install picks up local changes:

```bash
virtualenv venv
source venv/bin/activate
pip install -e ".[bot,test,sqlite]"
```

Available extras:

| Extra | Use case |
|-------|----------|
| `bot` | Bot-specific dependencies (media, database, etc.) |
| `sqlite` | SQLite async driver |
| `postgres` | PostgreSQL async driver |
| `test` | Test tooling (pytest, pylint, etc.) |

## Running the bot

```bash
discord-bot /path/to/config.yml
```

Config schema is in README.md; full per-cog config keys are in each
`docs/<cog>.md`.

## Tests

**Always invoke through the project venv** — system Python has an older
`dappertable` that causes ~27 false failures.

```bash
venv/bin/pytest -q                              # full suite
venv/bin/pytest tests/path/to/test_file.py -q   # single file
venv/bin/pytest --cov=discord_bot --cov-report=html tests/
```

Coverage threshold is 90%. All async tests must be marked
`@pytest.mark.asyncio` (mode is `strict`); see `[tool.pytest.ini_options]`
in `pyproject.toml`.

## Linting

```bash
venv/bin/pylint --rcfile .pylintrc discord_bot/   # production code
venv/bin/pylint --rcfile .pylintrc.test tests/    # test code
```

Target score is 10.00/10. Tox runs both pylint configs + pytest across
py311–py314:

```bash
tox
```

## Alembic migrations

Alembic reads `DATABASE_URL` from the environment.

```bash
alembic upgrade head                                       # apply migrations
alembic revision --autogenerate -m "description of change" # generate one
```

After editing `discord_bot/database.py`, regenerate the revision and review
the generated `op.*` calls — autogenerate doesn't catch every change.

## Adding a new cog

Three files change:

1. **`discord_bot/utils/common.py`** — if you want the cog to be enabled
   via the typed Pydantic config, add a field to `IncludeConfig`. (Some
   cogs read `general.include.<name>` straight from the raw dict — see
   `cogs/role.py`. The Pydantic field is optional but recommended.)

   ```python
   class IncludeConfig(BaseModel):
       my_cog: bool = False
   ```

2. **`discord_bot/cli/bot.py`** — append to `POSSIBLE_COGS` (order matters;
   `MessageDispatcher` must stay first). If the cog should also run in
   dispatcher-only mode, add it to `discord_bot/cli/dispatcher.py` too.

   ```python
   POSSIBLE_COGS = [
       MessageDispatcher,
       ...
       MyCog,
   ]
   ```

3. **`discord_bot/cogs/my_cog.py`** — implement the cog (see below).

### Cog skeleton

Cogs that need a database inherit `CogHelper`
(`discord_bot/cogs/cog_helper.py`); cogs that only need Discord/Redis
inherit `CogHelperBase` (`discord_bot/cogs/common.py`). The dispatch
helpers live on the base, so both subclasses get them. See
[docs/common.md](docs/common.md) for the full API.

```python
from discord_bot.cogs.common import CogHelper
from discord_bot.exceptions import CogMissingRequiredArg
from pydantic import BaseModel

class MyCogConfig(BaseModel):
    loop_sleep_interval: float = 300.0

class MyCog(CogHelper):
    def __init__(self, bot, settings, db_engine):
        if not settings.get('general', {}).get('include', {}).get('my_cog', False):
            raise CogMissingRequiredArg('MyCog not enabled')
        super().__init__(bot, settings, db_engine,
                         settings_prefix='my_cog',
                         config_model=MyCogConfig)
        # self.config.loop_sleep_interval now available
```

`CogHelper.__init__` provides:

- `self.bot`, `self.settings`, `self.db_engine` (`AsyncEngine | None`)
- `self.logger` — name is the lowercase class name; config from
  `general.logging`
- `self.config` — Pydantic-validated cog config (if `config_model` supplied)

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

The helpers route through either the in-process `MessageDispatcher` cog or
a `RedisDispatchClient` depending on `general.dispatch_cross_process`
config. Either way, accessing the dispatcher when none is configured
raises `RuntimeError`.

### Background loop

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
    # one iteration — return_loop_runner re-invokes indefinitely
    ...
```

### Heartbeat gauge

Every cog with a background loop should register a heartbeat gauge so
ops can alert on stuck loops. See
[docs/monitoring/metrics_reference.md](docs/monitoring/metrics_reference.md)
for the canonical pattern; new `MetricNaming` entries go in
`discord_bot/utils/otel.py`.

## Database

SQLAlchemy 2.x, fully async. The engine is `AsyncEngine` (asyncpg /
aiosqlite). Open a session via the `CogHelper` context manager:

```python
from sqlalchemy import select, delete

async with self.with_db_session() as db:
    row  = (await db.execute(select(Model).where(Model.id == x))).scalars().first()
    rows = (await db.execute(select(Model).where(...))).scalars().all()
    n    = (await db.execute(select(func.count()).select_from(Model).where(...))).scalar()
    await db.execute(delete(Model).where(Model.id == x))
    await self.retry_commit(db)
```

`session.query()` is **not** supported on `AsyncSession`.

### DB retry

`async_retry_database_commands` (`discord_bot/utils/sql_retry.py`) retries on
`OperationalError` (rollback + sleep) and `PendingRollbackError` (rollback),
up to 3 attempts:

```python
from discord_bot.utils.sql_retry import async_retry_database_commands

result = await async_retry_database_commands(
    db_session,
    lambda: database_functions.get_x(db_session, ...),
)
await async_retry_database_commands(db_session, db_session.commit)
```

`self.retry_commit(db)` in `CogHelper` is the cog-level shorthand.

## Error handling

- **No broad `except Exception`** in production code — let it propagate so
  tracebacks are visible. Catch only specific exceptions (`RateLimited`,
  `NotFound`, `PydanticValidationError`, etc.).
- `async_retry_discord_message_command` handles Discord transient errors
  (`RateLimited`, `DiscordServerError`, `TimeoutError`,
  `ServerDisconnectedError`); everything else is a real bug.
- The only sanctioned broad-except lives in `MessageDispatcher._ReadItem`
  dispatch — see [AGENTS.md](AGENTS.md#the-one-allowed-broad-except).

## Test infrastructure

Shared fixtures and fakes are in `tests/helpers.py`:

| Name | Kind | What it provides |
|------|------|------------------|
| `fake_context` | fixture | dict with `bot/guild/author/channel/context` |
| `fake_engine` | fixture | in-memory `AsyncEngine` (`sqlite+aiosqlite:///:memory:`) |
| `async_mock_session` | async ctx mgr | `AsyncSession` bound to `fake_engine` |
| `fake_bot_yielder` | factory | `fake_bot_yielder(channels=[...])() → FakeBot` |
| `generate_fake_context` | non-fixture | inline equivalent of `fake_context` |
| `fake_source_dict` | helper | constructs a `MediaRequest` for music tests |
| `fake_media_download` | ctx mgr | yields a `MediaDownload` with a temp audio file |
| `random_id`, `random_string` | helpers | test data generators |
| `FakeBot`, `FakeGuild`, `FakeAuthor`, `FakeChannel`, `FakeMessage`, `FakeVoiceClient`, `FakeContext` | fakes | drop-in substitutes for discord.py objects |

`FakeChannel.send()` records every message in `channel.messages`, so tests
assert against the list directly.

`FakeBot.get_cog()` always returns `None`, so `CogHelper.dispatch_*` falls
back to direct retry calls automatically — no special setup needed in
most cog tests.

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

`fetch_object` goes through the LOW-priority queue — it is **not** a barrier
for NORMAL-priority work. Use a sentinel:

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

### `bot.loop` in tests

`FakeBot.loop = None`. Any code that runs during the test body (not inside
`cog_load`) must use `asyncio.get_running_loop()` rather than
`self.bot.loop`. See [AGENTS.md](AGENTS.md#botloop-is-none-in-tests) for
why.
