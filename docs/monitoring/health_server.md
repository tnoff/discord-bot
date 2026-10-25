# Health Server

Each pod builds its own health server explicitly in its own `cli/*.py` — there
is no runtime selection based on a config flag any more (`dispatch_gateway`
is inert; nothing reads it to choose behavior). There are four
implementations:

| Pod(s) | Class | What it checks |
|---|---|---|
| `discord-gateway` | `HealthServer` (`discord_gateway/servers/health_server.py`) | Discord connection (`is_ready` and not closed) + background-loop health. **No peer probes** |
| `discord-commands` | `CommandsHealthServer` (`discord_commands/servers/commands_health_server.py`) | The process is serving + background-loop health |
| `discord-dispatcher`, `discord-broker`, `discord-downloader`, `discord-search` | `RedisPingHealthServer` (`discord_core/servers/redis_health_server.py`, built via `discord_core/cli/_lib/worker_pod.py` for the two worker pods) | Redis ping |
| `discord-db` | `DatabasePingHealthServer` (`discord_db/servers/database_health_server.py`) | Database `SELECT 1` (postgres or SQLite) |

## Bot (gateway) health server

Verifies the bot is connected to Discord.

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

### Endpoints

`GET /health` — liveness. `GET /ready` (also `/readyz`, `/readiness`) — readiness.
Both run the **same** check; any other path is treated as liveness.

The gateway deliberately does not probe its peers (dispatcher, db, broker,
downloader, search, commands). A readiness probe that fails when a peer is down
takes the gateway out of service for a problem a restart or a wait cannot fix
from here, and a gateway that is out of service cannot tell users anything. A
peer being down shows up where it matters instead: the music loops that poll
the broker re-arm rather than stall on an unreachable peer (see [Background
Loop Health](loop_health.md#peer-outages)), and a command forwarded to an
unreachable commands pod is answered with "Commands are unavailable right now".

| Bot state                              | HTTP status | Body                         |
|----------------------------------------|-------------|------------------------------|
| Ready and connected (`is_ready=True`)  | `200 OK`    | `{"status": "ok"}`           |
| Not yet ready or closed                | `503 Service Unavailable` | `{"status": "unavailable"}` |
| A background loop is stalled           | `503 Service Unavailable` | `{"status": "unavailable", "loops": {...}}` |

The bot holds no database engine, so there is no `SELECT 1` ping and no `"db"`
key, and the payload carries no per-peer keys.

## Commands pod (`discord-commands`)

`CommandsHealthServer` reports `200 {"status": "ok"}` while the process is
serving and no registered loop is stalled. The pod holds no connection that can
be down: its work is an in-memory queue per guild and HTTP calls made per
command. Its `/health` starts answering before the commands HTTP server is
listening, so the image's Docker `HEALTHCHECK` probes the commands port (8086)
instead; Kubernetes liveness uses `/health` on 8080.

## Redis-backed pods (`discord-dispatcher`, `discord-broker`, `discord-downloader`, `discord-search`)

These four pods share `RedisPingHealthServer` (`discord_core/servers/redis_health_server.py`).
It checks Redis connectivity — a `PING` — instead of any Discord or HTTP-peer
state, and is identical across the four; the two worker pods
(`discord-downloader`, `discord-search`) build it via
`discord_core/cli/_lib/worker_pod.py`'s `build_redis_health_server`, the
dispatcher and broker build it directly in their own `cli/*.py`.

### Configuration

```yaml
general:
  redis_url: "redis://redis:6379/0"
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

## `discord-db` health server

`DatabasePingHealthServer` (`discord_db/servers/database_health_server.py`)
runs `SELECT 1` against its database (postgres or SQLite, per the DSN) — this is the one pod in the fleet whose
liveness rests on the database alone, since it is the only pod that holds an
engine at all.

| Database state | HTTP status | Body |
|---|---|---|
| `SELECT 1` succeeds | `200 OK` | `{"status": "ok", "db": "ok"}` |
| `SELECT 1` fails | `503 Service Unavailable` | `{"status": "unavailable", "db": "unavailable"}` |

## Background loop health

Every health server — bot, commands, dispatcher, broker, db, downloader, search — also
fails its probe when a registered background loop has stopped making
progress, and names each loop in the response:

```json
{
  "status": "unavailable",
  "loops": {"process_search_results": "stalled", "cleanup_players": "ok"}
}
```

Statuses are `ok`, `stalled`, and `stopped` (a deliberate shutdown, which does
**not** fail the probe so a draining pod isn't killed mid-drain). Processes with
no registered loops are unaffected and omit the `loops` key entirely.

This is the same signal as the [`heartbeat` gauge](metrics_reference.md#heartbeat),
by design — see [Background Loop Health](loop_health.md) for the model, the
`stale_after_seconds` knob, and the sizing guidance.

**Operational note:** because this gates *liveness*, a wedged loop gets the pod
restarted by the kubelet. That is what recovers a genuinely stuck consumer
without a human. A loop that opts in to peer-outage tolerance (the gateway's
music, delete-messages and markov loops) is not called stalled merely because a
peer pod is unreachable, so a broker, downloader, search or dispatcher outage no
longer restarts the gateway; see [Peer outages](loop_health.md#peer-outages).
Loops that have not opted in, and any error that is not a peer being unreachable
(a 500, say), still stall the loop after `stale_after_seconds`, so a sustained
Redis outage can still restart a Redis-backed pod. Widen the window if a
dependency is expected to be unavailable for long stretches.

## Docker integration

All seven Dockerfiles carry a `HEALTHCHECK`, but they don't all check the same
thing. Only the bot image's actually hits the health server's HTTP endpoint:

```dockerfile
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
  CMD python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=5)"
```

The other six (`Dockerfile.dispatcher`, `.broker`, `.db`, `.downloader`,
`.search`, `.commands`) probe a raw TCP connection to that pod's own application
port instead (8082, 8081, 8085, 8083, 8084, 8086 respectively) — e.g.
`socket.create_connection(('localhost', 8082), timeout=5)` for the
dispatcher. That confirms something is listening on the port; it does not
exercise the health server's `/health`/`/ready` logic (Redis ping, database
`SELECT 1`, loop health) the way the bot image's check does.

Port `8080` is declared with `EXPOSE 8080` on the bot image; each of the
other six `EXPOSE`s its own application port plus `8080` for the health
server itself, which Kubernetes-style liveness/readiness probes (unlike
these Docker-level `HEALTHCHECK`s) do hit directly. To use the bot's Docker
healthcheck you must enable the health server in your config **and** publish
the port:

```bash
docker run -d \
  -p 8080:8080 \
  -v /path/to/discord.cnf:/opt/discord/cnf/discord.cnf:ro \
  discord-gateway
```

## Implementation notes

- All four implementations share `HealthServerBase`
  (`discord_core/servers/health_server_base.py`), which is where loop health
  is folded into the probe result — so every process gets it identically.
- Each runs as an `asyncio` task inside the process's main event loop — no extra threads or dependencies.
- Use only Python stdlib (`asyncio.start_server`, `json`) plus `redis.asyncio`
  for `RedisPingHealthServer` and SQLAlchemy for `DatabasePingHealthServer`.
- Listen on `0.0.0.0` so they are reachable from the Docker host or a Kubernetes probe.
- The Redis-backed servers hold a single persistent Redis connection; it is closed cleanly when the server task is cancelled.
