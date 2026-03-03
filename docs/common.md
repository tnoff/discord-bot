# Common Functions

Functions included in the "common" cog that is automatically included.

## CogHelper base class

All cogs extend `CogHelper`, which provides shared utilities in addition to the
Discord commands below.

### Dispatcher helpers

Three methods route Discord API calls through `MessageDispatcher` when it is
loaded, with direct fallbacks otherwise. Individual cogs should use these instead
of calling `async_retry_discord_message_command` or the dispatcher directly.

#### `dispatch_message(ctx, content) -> str`

Send `content` to `ctx`'s channel. Returns `content` so it can be used as an
early-exit value.

```python
return await self.dispatch_message(ctx, 'Done!')
```

#### `dispatch_fetch(guild_id, func, **retry_kwargs)`

Fetch a Discord object at LOW priority through the dispatcher, or directly if
the dispatcher is not loaded. Pass retry options as keyword arguments.

```python
channel = await self.dispatch_fetch(guild_id, partial(bot.fetch_channel, channel_id))
messages = await self.dispatch_fetch(guild_id, partial(channel.history, limit=100), max_retries=5)
```

#### `send_funcs(guild_id, funcs)`

Enqueue a list of callables at NORMAL priority, or await them directly as a
fallback.

```python
await self.send_funcs(guild_id, [partial(message.delete)])
```

## Hello

Say hello to the bot and it will say hello back. Mostly used to see if the bot is running

```
!hello
> Waddup tnoff
```

### Roll

Do a random dice roll

```
!roll 2d6
> tnoff rolled: 6 + 4 = 10
```

### Meta

Probably the most useful basic function, show user id, channel id, and guild (server) id.

```
!meta
> Server id: <redacted>
> Channel id: <redacted>
> User id: <redacted>
```