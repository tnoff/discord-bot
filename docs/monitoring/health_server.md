# Health Server

The bot ships two health server implementations selected automatically based on the process role.

## Bot health server

Used when `dispatch_gateway: true` (the default). Verifies the bot is connected to Discord.

### Configuration

```yaml
general:
  monitoring:
    health_server:
      enabled: true
      port: 8080  # default
```

| Field     | Type    | Default | Description                          |
|-----------|---------|---------|--------------------------------------|
| `enabled` | boolean | `false` | Start the health server on boot      |
| `port`    | integer | `8080`  | TCP port to listen on (1–65535)      |

### Endpoint

`GET /health` (any path works — only the port matters)

| Bot state                              | HTTP status | Body                         |
|----------------------------------------|-------------|------------------------------|
| Ready and connected (`is_ready=True`)  | `200 OK`    | `{"status": "ok"}`           |
| Not yet ready or closed                | `503 Service Unavailable` | `{"status": "unavailable"}` |

## Dispatcher health server

Used automatically when `dispatch_gateway: false` **and** `redis_url` is set. Checks Redis connectivity instead of Discord gateway state — the dispatcher process never opens a gateway connection so `is_ready()` is always `False`.

### Configuration

```yaml
general:
  redis_url: "redis://redis:6379/0"
  dispatch_gateway: false
  monitoring:
    health_server:
      enabled: true
      port: 8080
```

### Endpoint

Same path/port as the bot server.

| Redis state        | HTTP status | Body                         |
|--------------------|-------------|------------------------------|
| Ping succeeds      | `200 OK`    | `{"status": "ok"}`           |
| Ping raises        | `503 Service Unavailable` | `{"status": "unavailable"}` |

## Docker integration

`docker/Dockerfile` includes a `HEALTHCHECK` for the bot image:

```dockerfile
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=5)"
```

`docker/Dockerfile.dispatcher` includes the same directive for the dispatcher image.

Port `8080` is declared with `EXPOSE 8080` in both Dockerfiles. To use the healthcheck you must enable the health server in your config **and** publish the port:

```bash
docker run -d \
  -p 8080:8080 \
  -v /path/to/discord.cnf:/opt/discord/cnf/discord.cnf:ro \
  discord-bot
```

## Implementation notes

- Both servers run as an `asyncio` task inside the process's main event loop — no extra threads or dependencies.
- Use only Python stdlib (`asyncio.start_server`, `json`) plus `redis.asyncio` for the dispatcher variant.
- Listen on `0.0.0.0` so they are reachable from the Docker host or a Kubernetes probe.
- The dispatcher server holds a single persistent Redis connection; it is closed cleanly when the server task is cancelled.
