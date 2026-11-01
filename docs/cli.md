# CLI and Application Lifecycle

Documentation for the Discord bot command-line interface, application lifecycle, and graceful shutdown handling.

## Running the Bot

The gateway is started using the `discord-gateway` command with a configuration file:

```bash
discord-gateway /path/to/config.yml
```

### Configuration File

The configuration file should be in YAML format. `discord_token` alone is not
enough to start: `discord_gateway/cli/bot.py::run()` also hard-requires
`dispatch_http_url`, `database_http_url` and `commands_http_url` (pointing at
the `discord-dispatcher`, `discord-db` and `discord-commands` pods) and raises
`DiscordBotException` at startup if any is missing — there is no standalone mode.

```yaml
general:
  discord_token: YOUR_TOKEN_HERE
  dispatch_http_url: "http://dispatcher:8082"
  database_http_url: "http://db:8085"
  commands_http_url: "http://commands:8086"
```

See individual cog documentation for additional configuration options, and
[docs/configuration.md](configuration.md#database) / [docs/architecture.md](architecture.md) for
what each pod does.

## Graceful Shutdown

The bot implements graceful shutdown handling to ensure all background tasks are properly stopped and resources are cleaned up when the application exits.

### Supported Signals

The bot handles two types of shutdown signals:

| Signal | Source | Description |
|--------|--------|-------------|
| `SIGINT` | Ctrl+C in terminal | Interactive shutdown |
| `SIGTERM` | `docker stop`, `systemctl stop`, `kill` | Programmatic shutdown |

### Shutdown Process

When a shutdown signal is received, the following sequence occurs:

1. **Signal Detection**: The signal handler catches `SIGINT` or `SIGTERM`
2. **Shutdown Flag**: Sets `shutdown_triggered = True` to prevent duplicate shutdowns
3. **Bot Closure**: Schedules `bot.close()` to disconnect from Discord
4. **Cog Cleanup**: Calls `cog_unload()` on each loaded cog in sequence
5. **Final Cleanup**: Ensures bot connection is fully closed
6. **Exit**: Process terminates cleanly

### Cog Cleanup Actions

Each cog performs specific cleanup during `cog_unload()`:

#### Music Cog
- Cleans up all active guilds first — terminates state machines, sends a
  shutdown message, cancels player tasks (each guild's play queue stays in
  the broker for the next gateway to pick up)
- Cancels its own loop-health-tracked loops: `cleanup_players` and
  `process_gateway_intents`, plus internal init/cleanup tasks (including the
  one-shot session resume). The result loops (`process_download_results`,
  `process_search_results`) run in the commands pod, not here
- There is no message-sending loop or download-file loop to cancel any
  more — dispatch is HTTP-based (see
  [Message dispatcher](message_dispatcher.md)) and downloads run in the
  standalone `discord-downloader` pod. The gateway holds no client for the
  downloader or the search pod at all; it talks to the broker, the
  dispatcher, the db pod and the commands pod
- Removes temporary download/player directories

#### Commands pod (`discord-commands`)
- Has no cogs. It runs the music result loops, and the markov and
  delete-messages loops when `general.include` turns them on; on SIGTERM it
  cancels those first (a result lost there is fetched again on the next sweep). On SIGTERM it drains its HTTP server first (so nothing is
  accepted after the executor stops, which would be a 202 for a command that
  never runs), then stops the command executor, then closes the dispatcher and
  db clients — in that order, because a command still running needs the
  dispatcher to send its reply

### Logging During Shutdown

The bot logs the shutdown process for monitoring and debugging:

```
Main :: Received SIGTERM, triggering graceful shutdown...
Main :: Calling cog_unload on Music
Main :: Graceful shutdown complete
```

If any cog encounters an error during shutdown, it will be logged but won't prevent other cogs from cleaning up:

```
Main :: Error during cog_unload for Music: <error details>
```

## Docker Integration

When running in Docker, the graceful shutdown process is triggered by `docker stop`:

### Default Behavior

Docker sends `SIGTERM` and waits 10 seconds before sending `SIGKILL`. The bot typically shuts down in 1-2 seconds, well within this grace period.

```bash
# Stop container gracefully
docker stop my-discord-bot
```

### Extended Grace Period

For slower systems or heavily loaded bots, you can extend the grace period:

```bash
# Wait up to 30 seconds for graceful shutdown
docker stop --time 30 my-discord-bot
```

### Dockerfile Considerations

The real `docker/Dockerfile.gateway` uses a shared `entrypoint.sh`, not a direct
`CMD`, so the console script can be selected by `DISCORD_BOT_CMD` (the same
entrypoint script is reused across all seven pods' Dockerfiles):

```dockerfile
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/opt/discord/cnf/discord.cnf"]
```

`entrypoint.sh` `exec`s the selected command (`discord-gateway` by default) as
the final step, so it still becomes PID 1 and receives signals directly from
Docker — no init system (tini, dumb-init) is required.