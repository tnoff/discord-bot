# CLI and Application Lifecycle

Documentation for the Discord bot command-line interface, application lifecycle, and graceful shutdown handling.

## Running the Bot

The bot is started using the `discord-bot` command with a configuration file:

```bash
discord-bot /path/to/config.yml
```

### Configuration File

The configuration file should be in YAML format and contain at minimum:

```yaml
general:
  discord_token: YOUR_TOKEN_HERE
```

See individual cog documentation for additional configuration options.

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
- Cancels 6 background tasks:
  - Message sending loop
  - Player cleanup loop
  - Download file loop
  - Cache cleanup loop (if enabled)
  - Playlist history update loop (if database enabled)
  - YouTube Music search loop (if enabled)
- Disconnects all active voice clients
- Cleans up player resources for all guilds
- Removes temporary download directories
- Clears in-progress request bundles

#### Markov Cog
- Cancels message checking background task
- Ensures no database transactions are pending

#### Delete Messages Cog
- Cancels message deletion background task
- Completes any in-progress deletions

### Logging During Shutdown

The bot logs the shutdown process for monitoring and debugging:

```
Main :: Received SIGTERM, triggering graceful shutdown...
Main :: Calling cog_unload on Music
Main :: Calling cog_unload on Markov
Main :: Calling cog_unload on DeleteMessages
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

The bot runs as PID 1 in the container, so it directly receives signals from Docker. No init system (tini, dumb-init) is required.

```dockerfile
CMD ["discord-bot", "/opt/discord/cnf/discord.cnf"]
```