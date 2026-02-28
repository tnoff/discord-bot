# Discord Bot Docker Build

Build docker file for discord-bot to run however you want.

## Build Docker Image Locally

Build just the discord-bot docker image:

```bash
docker build .
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

## Database Setup

A driver for postgres is setup automatically in the docker image to use with sqlalchemy. Any other drivers will need to be added.