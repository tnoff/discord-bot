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
| Bot entry, config loading, `POSSIBLE_COGS` | `discord_bot/cli/bot.py` (dispatcher-only mode: `cli/dispatcher.py`) |
| `CogHelperBase` + dispatch helpers | `discord_bot/cogs/common.py` (see `docs/common.md`) |
| `CogHelper` (adds async DB session helpers) | `discord_bot/cogs/cog_helper.py` |
| Per-guild priority dispatcher | `discord_bot/cogs/message_dispatcher.py` (see `docs/message_dispatcher.md`) |
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

### `MessageDispatcher` must be first in `POSSIBLE_COGS`

Other cogs retrieve it lazily via `self._dispatcher` (which calls
`self.bot.get_cog('MessageDispatcher')` under the hood). If
`MessageDispatcher` isn't loaded **and** `general.dispatch_cross_process`
is false, `_dispatcher` raises `RuntimeError` on first use — there is no
silent fallback. Two `POSSIBLE_COGS` lists exist:
`discord_bot/cli/bot.py` (full bot) and `discord_bot/cli/dispatcher.py`
(dispatcher-only process for cross-process mode). `CommandErrorHandler`
is registered unconditionally before either list is loaded.

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

The CLI rewrites `postgresql://` → `postgresql+asyncpg://` and `sqlite://` →
`sqlite+aiosqlite://` at startup. Config files use the standard URLs;
don't write `+asyncpg` into config or the rewrite double-applies.

### Broad-except is rare and pylint-annotated

Production code does **not** use bare `except Exception:`. The exceptions
that exist all live in `MessageDispatcher`'s worker loop and request
handlers (lines tagged `# pylint: disable=broad-except`) — they catch and
log so an unbounded handler exception cannot kill the dispatcher worker or
leave an `asyncio.Future` caller hanging. Don't add new broad-excepts
elsewhere; if you copy this pattern into a new dispatcher-like loop, mirror
the pylint annotation and the log+propagate behaviour.

### `cleanup_source` is module-level, not a method

`cleanup_source` is a top-level function in
`discord_bot/cogs/music_helpers/music_player.py`, not a method on
`MusicPlayer`. It is the **only** safe way to release the FFmpeg
subprocess held by a `PCMAudio` source — calling `voice_client.stop()`
alone leaks fds. Anywhere the player drops a track, route through this
function.

### Two dispatch modes: in-process vs cross-process Redis

`CogHelperBase._dispatcher` returns either the in-process
`MessageDispatcher` cog or a `RedisDispatchClient` depending on
`general.dispatch_cross_process` in config. Either way, helper methods
look identical to the cog. If `dispatch_cross_process` is false and the
`MessageDispatcher` cog is missing, accessing `self._dispatcher` raises
`RuntimeError` — there is **no** silent fallback. Tests rely on either
having `MessageDispatcher` loaded or not calling dispatch helpers.

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
