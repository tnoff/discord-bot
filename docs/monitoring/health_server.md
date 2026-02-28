# Health Server

The health server provides a lightweight HTTP liveness endpoint so Docker and Kubernetes probes can verify that the bot process is alive and connected to Discord.

## Configuration

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

## Endpoint

`GET /health` (any path works — only the port matters)

| Bot state                              | HTTP status | Body                         |
|----------------------------------------|-------------|------------------------------|
| Ready and connected (`is_ready=True`)  | `200 OK`    | `{"status": "ok"}`           |
| Not yet ready or closed                | `503 Service Unavailable` | `{"status": "unavailable"}` |

## Docker integration

The `Dockerfile` already includes a built-in `HEALTHCHECK` that hits this endpoint:

```dockerfile
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=5)"
```

Port `8080` is also declared with `EXPOSE 8080`. To use the healthcheck you must enable the health server in your config **and** publish the port when running the container:

```bash
docker run -d \
  -p 8080:8080 \
  -v /path/to/discord.cnf:/opt/discord/cnf/discord.cnf:ro \
  discord-bot
```

## Implementation notes

- Runs as an `asyncio` task inside the bot's main event loop — no extra threads or dependencies.
- Uses only Python stdlib (`asyncio.start_server`, `json`, `urllib`).
- Listens on `0.0.0.0` so it is reachable from the Docker host or a Kubernetes probe.
