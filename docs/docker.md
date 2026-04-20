# Discord Bot Docker Build

Build docker file for discord-bot to run however you want.

## Docker Images

Three images are provided:

| Dockerfile | Use case | pip extra | Entrypoint |
|------------|----------|-----------|------------|
| `docker/Dockerfile` | Standalone — bot + dispatcher in one container | `[all]` | `discord-bot` |
| `docker/Dockerfile.bot` | HA mode — bot only, connects to a separate dispatcher | `[bot]` | `discord-bot` |
| `docker/Dockerfile.dispatcher` | HA mode — dispatcher only | `[dispatcher]` | `discord-dispatcher` |

## Build Docker Image Locally

Build the standalone image:

```bash
docker build -f docker/Dockerfile .
```

Build the HA bot image:

```bash
docker build -f docker/Dockerfile.bot .
```

## Security

The Docker container runs as a **non-root user** (`discord` with UID/GID 1000) for security best practices.

### Automatic Directory Creation

The container includes an entrypoint script (`docker-entrypoint.sh`) that:
- Automatically creates required subdirectories (`/opt/discord/cnf`, `/opt/discord/downloads`) if they don't exist
- Checks write permissions on mounted volumes
- Displays helpful warnings if permission issues are detected

This means you can mount an empty directory, and the necessary subdirectories will be created automatically.

## Volume Permissions

### Volumes that need write access

The following directories need to be writable by the `discord` user:

1. **`/opt/discord`** - Working directory (downloads, cache, database, etc.)
   - Subdirectories: `/opt/discord/downloads`, `/opt/discord/cnf` (auto-created)
2. **`/var/log/discord`** - Log files

### Setting up volume permissions

#### Option 1: Set ownership on host (Recommended)

Before starting the container, ensure the host directories have proper ownership:

```bash
# Create directories on host
mkdir -p /path/to/discord/data
mkdir -p /path/to/discord/logs
mkdir -p /path/to/discord/config

# Set ownership to UID/GID 1000
sudo chown -R 1000:1000 /path/to/discord/data
sudo chown -R 1000:1000 /path/to/discord/logs
sudo chown -R 1000:1000 /path/to/discord/config
```

#### Option 2: Use Docker user flag

You can override the user at runtime if needed:

```bash
docker run --user $(id -u):$(id -g) ...
```

But note this may cause issues if the UIDs don't match.

### Checking permissions

If you encounter permission errors:

1. Check ownership on the host:
   ```bash
   ls -la /path/to/discord/data
   ```

2. Check if the container user can write:
   ```bash
   docker run -it --rm -v /path/to/discord/data:/opt/discord your-image-name \
     sh -c "touch /opt/discord/test && rm /opt/discord/test && echo 'Write test passed'"
   ```

3. If using SELinux, you may need to add the `:z` or `:Z` flag:
   ```bash
   -v /path/to/discord/data:/opt/discord:z
   ```

## Config

The `discord.cnf` file should be mounted into `/opt/discord/cnf/discord.cnf` file for the bot to use.

It is also recommended that the download files for the music bot are set to a volume. This path can be updated via the config.

### Common mount points

Based on the bot's configuration, you'll likely want to mount:

- **Data/Downloads**: `/opt/discord` (or a subdirectory like `/opt/discord/downloads`)
- **Database**: `/opt/discord` (if using SQLite)
- **Config**: `/opt/discord/cnf`
- **Logs**: `/var/log/discord`

## Health Check

The image ships with a built-in `HEALTHCHECK` directive that calls the bot's HTTP health endpoint (port 8080). To use it:

1. Enable the health server in your config:
   ```yaml
   general:
     monitoring:
       health_server:
         enabled: true
         port: 8080
   ```

2. Publish the port when running the container:
   ```bash
   docker run -d -p 8080:8080 ... discord-bot
   ```

Docker will automatically probe `http://localhost:8080/health` every 60 seconds and mark the container unhealthy if the bot is not ready. See the [Health Server documentation](./monitoring/health_server.md) for full details.

## Usage Example

```bash
# Ensure proper permissions on host directories
sudo chown -R 1000:1000 /path/to/discord/data
sudo chown -R 1000:1000 /path/to/discord/logs

# Run the container
docker run -d \
  -p 8080:8080 \
  -v /path/to/discord/data:/opt/discord:rw \
  -v /path/to/discord/logs:/var/log/discord:rw \
  -v /path/to/discord.cnf:/opt/discord/cnf/discord.cnf:ro \
  discord-bot
```

## Docker Compose

Two Compose files are provided for different deployment scenarios.

### Setup A — single container (`docker-compose.yml`)

Standard single-process deployment with no Redis:

```bash
# Place your config at cnf/discord.cnf, then:
docker compose -f docker/docker-compose.yml up -d
```

The `bot` service mounts `./cnf/discord.cnf` and exposes port 8080 for the health check.

### Setup B — multi-process with Redis (`docker-compose.multiprocess.yml`)

Runs the dispatcher and bot as separate containers connected via Redis Streams.
See [Cross-process dispatch](./message_dispatcher.md#cross-process-dispatch-via-redis-streams) for background.

```bash
docker compose -f docker/docker-compose.multiprocess.yml up -d
```

Three services are started:

| Service | Command | pip extra |
|---------|---------|-----------|
| `redis` | — | — |
| `dispatcher` | `discord-dispatcher` | `[dispatcher]` |
| `bot` | `discord-bot` | `[bot]` |

Each image installs only the dependencies it needs. The dispatcher image uses `pip install ".[dispatcher]"` (includes Redis client, excludes heavy media deps). The bot image uses `pip install ".[bot]"` (includes media/database deps, excludes Redis). For a standalone single-container deployment, `pip install ".[all]"` installs everything.

**Required config files:**

`cnf/discord.dispatcher.cnf`:
```yaml
general:
  discord_token: "YOUR_TOKEN"
  redis_url: "redis://redis:6379/0"
  dispatch_server:
    host: 0.0.0.0
    port: 8082
  dispatch_process_id: "dispatcher"
  dispatch_shard_id: 0
  dispatch_worker_count: 4
  monitoring:
    health_server:
      enabled: true
      port: 8080
  include:
    default: false
    message_dispatcher: true
```

`cnf/discord.bot.cnf`:
```yaml
general:
  discord_token: "YOUR_TOKEN"
  dispatch_http_url: "http://dispatcher:8082"
  include:
    default: true
    message_dispatcher: false
    markov: true
    delete_messages: true
```

> **Note:** Only the dispatcher container connects to the Discord gateway. The bot container forwards all Discord API calls to the dispatcher over HTTP — it does not need `redis_url` or a gateway connection of its own.

See [HA architecture](./ha.md) for a full explanation of how the pods communicate.

## Debug Builds

### heaptrack

`docker/Dockerfile` accepts an `INSTALL_HEAPTRACK` build arg (default `false`) for memory profiling sessions. The production image intentionally excludes it to keep the image size down.

```bash
docker build --build-arg INSTALL_HEAPTRACK=true -f docker/Dockerfile -t discord-bot:debug .
```

Keep debug images local — there's no need to push them to the registry.

## Database Setup

A driver for postgres is setup automatically in the docker image to use with sqlalchemy. Any other drivers will need to be added.