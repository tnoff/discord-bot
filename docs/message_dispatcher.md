# MessageDispatcher

An app-wide Discord API dispatcher that serialises calls per guild, applies retry
logic, and deduplicates rapid-fire mutable message updates.

## Why it exists

Discord rate-limits API calls per guild. Without coordination, cogs compete for
the same rate-limit buckets:

- Music sends/edits progress messages at high frequency during downloads.
- Markov and DeleteMessages issue channel history reads and message deletions.

`MessageDispatcher` solves this by owning **one `asyncio.PriorityQueue` per guild**
and **one worker task per active guild**. All Discord API calls route through the
appropriate guild queue and drain in priority order.

## Priority levels

| Priority | Value | Used for |
|----------|-------|----------|
| HIGH | 0 | Mutable bundle updates (music queue display) |
| NORMAL | 1 | One-off sends, message deletes |
| LOW | 2 | Background reads (channel history, fetch_message) |

Python's `asyncio.PriorityQueue` is a min-heap, so `0` is served first.

## Work item types

| Type | Priority | Description |
|------|----------|-------------|
| `_MutableSentinel` | HIGH | Triggers flush of a mutable bundle update |
| `_ImmutableItem` | NORMAL | List of arbitrary callables (e.g. message deletes) |
| `_SendItem` | NORMAL | Plain text channel send with optional `delete_after` |
| `_ReadItem` | LOW | Callable that resolves a future for the caller |

## Public API

### `update_mutable(key, guild_id, content, channel_id, sticky=True, delete_after=None)`

Queue a mutable bundle update. Rapid-fire calls for the same `key` collapse: only
the latest `content` is kept and only one sentinel is ever in the queue per key at
a time.

- `key` — unique string identifying the bundle (e.g. `play_order-{guild_id}`)
- `guild_id` — guild to route through
- `content` — list of strings, one per Discord message
- `channel_id` — channel to send to (required on first call for a new key)
- `sticky` — if `True`, messages are re-sent at the bottom of the channel when
  other messages appear below them
- `delete_after` — seconds after which messages are deleted; bundle is also
  removed from the dispatcher after a single dispatch (ephemeral bundle)

### `remove_mutable(key)`

Delete all Discord messages managed by `key` and remove the bundle. Deletions
are fire-and-forget.

### `update_mutable_channel(key, guild_id, new_channel_id)`

Move a mutable bundle to a different channel. Immediately deletes messages from
the old channel (fire-and-forget) then re-queues with the new channel.

### `send_message(guild_id, channel_id, content, delete_after=None, allow_404=False)`

Enqueue a plain text send at NORMAL priority. The dispatcher resolves the channel
at call-time via `bot.get_channel()`.

### `send_single(guild_id, funcs)`

Enqueue a list of callables at NORMAL priority. Use this for atomic batches (e.g.
delete several messages together).

### `fetch_object(guild_id, func, max_retries=3, allow_404=False)`

Enqueue `func` at LOW priority and block until the worker executes it. Runs after
all HIGH and NORMAL items for the guild, ensuring background reads (channel history,
fetch_message) do not compete with message sends and edits.

The worker applies retry logic internally. If the call raises, the exception is
logged at ERROR level and re-raised at the caller's `await` site.

```python
messages = await dispatcher.fetch_object(
    guild_id,
    partial(channel.history, limit=100),
)
```

## Mutable bundles

A `MessageMutableBundle` tracks the live Discord message objects for a given key.
On each flush the dispatcher:

1. Checks whether existing messages are still at the bottom of the channel
   (sticky check).
2. Computes a minimal diff: edit messages whose content changed, delete surplus
   messages, send new messages for additions.
3. Updates internal `MessageContext` references with the newly sent `Message`
   objects so future flushes can edit rather than re-send.

Bundles are created lazily on the first `update_mutable` call for a key and live
until `remove_mutable` is called (or the bundle has `delete_after` set).

## Using the dispatcher from a cog

`CogHelper` (the base class for all cogs) provides three thin wrappers that
route through the dispatcher when it is loaded and fall back to direct calls
otherwise:

```python
# Send a plain message (fire-and-forget via dispatcher, or direct send as fallback)
await self.dispatch_message(ctx, 'Something happened')

# Fetch a Discord object with retry
channel = await self.dispatch_fetch(guild_id, partial(bot.fetch_channel, channel_id))

# Enqueue callables (e.g. message deletes)
await self.send_funcs(guild_id, [partial(message.delete)])
```

These helpers keep individual cogs free of direct `async_retry_discord_message_command`
imports and dispatcher availability checks.

## Observability

### Traces

Each dispatch type emits an OpenTelemetry span:

| Span name | Attributes |
|-----------|------------|
| `message_dispatcher.process_mutable` | `key`, `discord.guild` |
| `message_dispatcher.immutable` | `discord.guild` |
| `message_dispatcher.send` | `discord.channel`, `discord.guild` |
| `message_dispatcher.fetch` | `discord.guild` |

### Metrics

| Metric | Description |
|--------|-------------|
| `heartbeat{background_job="message_dispatcher_workers"}` | Count of active per-guild worker tasks |
| `message_dispatcher_queue_depth{background_job="message_dispatcher_queue"}` | Total pending work items across all guild queues |

### Logging

All messages use the `message_dispatcher` logger at `DEBUG` level except warnings
(channel not found, message truncation) which use `WARNING`.

## Architecture notes

- Workers are started lazily on the first work item for a guild and exit
  automatically when the shutdown event is set and the queue drains.
- `bot.get_channel()` and `bot.get_guild()` are O(1) lookups into discord.py's
  gateway-fed internal cache. The dispatcher closes over `channel_id` integers
  and resolves them at call-time, so there is no stale object concern.
- Content longer than 2000 characters is truncated to 1900 before sending and a
  `WARNING` is logged.
