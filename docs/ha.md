# High-Availability Architecture

Describes how the bot runs in multi-pod mode, how the components communicate,
and what each pod is responsible for.

---

## Single-process vs HA mode

**Single-process (default):** All cogs run in one process. `CogHelper._dispatcher`
returns the in-process `MessageDispatcher` cog directly. No Redis, no HTTP.

**HA mode:** Cog logic (music, markov, etc.) runs in one or more bot pods.
Discord API calls are handled exclusively by a dedicated dispatcher pod.
Bot pods forward calls over HTTP; the dispatcher pod queues and executes them.

The switch is driven purely by config:
- Dispatcher pod sets `general.dispatch_server` to start the HTTP server.
- Bot pod sets `general.dispatch_http_url` to route calls over HTTP instead of in-process.

---

## Components

```
┌─────────────────────────────────────────────────────────────────┐
│  Bot pod(s)                                                     │
│  discord.bot.cnf                                                │
│                                                                 │
│  ┌──────────────┐  ┌──────────┐  ┌───────────────┐             │
│  │ Music cog    │  │  Markov  │  │ DeleteMessages │  ...        │
│  └──────┬───────┘  └────┬─────┘  └───────┬───────┘             │
│         │               │                │                      │
│         └───────────────┴────────────────┘                      │
│                         │ CogHelper._dispatcher                 │
│                         │ (HttpDispatchClient)                  │
└─────────────────────────┼───────────────────────────────────────┘
                          │ HTTP  (port 8082)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  Dispatcher pod                                                 │
│  discord.dispatcher.cnf                                         │
│                                                                 │
│  DispatchHttpServer (aiohttp, port 8082)                        │
│    POST /dispatch/send                                          │
│    POST /dispatch/delete                                        │
│    POST /dispatch/update_mutable                                │
│    POST /dispatch/remove_mutable                                │
│    POST /dispatch/update_mutable_channel                        │
│    POST /dispatch/fetch_history   ──► returns {request_id}      │
│    POST /dispatch/fetch_emojis    ──► returns {request_id}      │
│    GET  /dispatch/results/{id}    ──► 200 result | 202 pending  │
│         │                                                       │
│         ▼ enqueue                                               │
│  RedisDispatchQueue (Redis sorted set)                          │
│         │                                                       │
│         ▼ BZPOPMIN (×N workers)                                 │
│  MessageDispatcher._process_*_redis()                           │
│    → Discord REST API calls                                     │
└─────────────────────────┬───────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│  Redis                                                          │
│  Work queue, payloads, fetch results, bundle state, locks       │
└─────────────────────────────────────────────────────────────────┘
```

---

## Request flow

### Fire-and-forget (send, delete, mutable updates)

1. Cog calls e.g. `self.dispatch_message(ctx, 'text')`.
2. `CogHelper._dispatcher` is an `HttpDispatchClient` — it creates an async task
   that POSTs `{"guild_id": ..., "channel_id": ..., "content": ...}` to
   `POST /dispatch/send` on the dispatcher pod.
3. `DispatchHttpServer` extracts W3C traceparent from headers, enqueues the
   payload to the Redis sorted set, and returns 202.
4. A Redis worker (`_redis_worker`) on the dispatcher pod pops the item via
   `BZPOPMIN`, routes by member prefix, and calls the appropriate
   `MessageDispatcher` method, which issues the Discord API call with retry.

The cog does not wait for the Discord call to complete.

### Awaitable fetches (channel history, guild emojis)

1. A cog submits a `FetchChannelHistoryRequest` via `submit_request`.
2. `HttpDispatchClient` POSTs to `POST /dispatch/fetch_history`.
3. The server computes a stable `request_id` (SHA-256 of params), enqueues the
   fetch, and returns `{"request_id": "<hex>"}` with 202.
4. `HttpDispatchClient._poll_result()` polls
   `GET /dispatch/results/{request_id}` with exponential backoff
   (0.5 s base, 10 s max, 300 s timeout).
5. A Redis worker executes the history fetch, stores the result in Redis
   under `discord_bot:dispatch:result:{request_id}` (TTL 1 day), and the next
   poll returns 200 with the result body.
6. `HttpDispatchClient` decodes the result and delivers it to the cog's result
   queue (registered via `register_cog_queue`).

### Mutable bundle deduplication across pods

Mutable bundles (e.g. the music queue display) must not be updated concurrently
by two workers on different pods. The dispatcher uses a per-bundle Redis lock:

```
acquire SET NX discord_bot:dispatch:executing:{bundle_key}  (TTL 30 s)
    ↓ acquired
execute bundle flush
    ↓
release DEL discord_bot:dispatch:executing:{bundle_key}
```

If the lock is already held, the worker re-enqueues the sentinel at HIGH
priority and moves on. The lock TTL of 30 s is a safety net — if a pod dies
mid-execution, the lock expires and another pod can proceed within 30 s.

Bundle state is persisted to Redis (`discord_bot:bundle:{bundle_key}`, TTL 1 day)
after each flush, and loaded back on dispatcher startup so mutable messages
survive pod restarts.

---

## Redis key reference

| Key pattern | Structure | TTL | Description |
|-------------|-----------|-----|-------------|
| `discord_bot:dispatch:queue:{shard_id}` | Sorted Set | None | Work queue; score = `priority×10¹²+timestamp_ms` |
| `discord_bot:dispatch:payload:{member}` | String (JSON) | 1 day | Work item payload |
| `discord_bot:dispatch:result:{request_id}` | String (JSON) | 1 day | Completed fetch result |
| `discord_bot:dispatch:executing:{bundle_key}` | String (pod_id) | 30 s | Per-bundle execution lock |
| `discord_bot:bundle:{bundle_key}` | String (JSON) | 1 day | Persisted mutable bundle state |

**Priority scoring** (lower score = processed first):

| Priority | Value | Score range | Used for |
|----------|-------|-------------|----------|
| HIGH | 0 | `0 + ms` | Mutable bundle sentinels |
| NORMAL | 1 | `10¹² + ms` | Sends, deletes |
| LOW | 2 | `2×10¹² + ms` | Fetch history, fetch emojis |

---

## Configuration reference

### Dispatcher pod

```yaml
general:
  discord_token: "YOUR_TOKEN"      # authenticates outbound REST; dispatcher does NOT open the gateway
  redis_url: "redis://redis:6379/0"
  dispatch_server:
    host: 0.0.0.0                  # bind address
    port: 8082                     # HTTP server port
  dispatch_process_id: "dispatcher"  # pod identifier for Redis locks
  dispatch_shard_id: 0             # queue shard (0 unless running multiple shards)
  dispatch_worker_count: 4         # concurrent Redis worker coroutines
  monitoring:
    health_server:
      enabled: true
      port: 8080
```

### Redis connection: direct URL vs. Sentinel

`redis_url` connects to a single fixed endpoint — fine for local/dev, or a
single-primary deployment. In production Redis runs as Valkey + Sentinel (three
nodes, one primary), where the primary role floats across pods on failover. Set
`redis_sentinel` instead so the client discovers the current primary via
Sentinel on every connection — a promotion is then transparent to the app:

```yaml
general:
  # redis_sentinel takes precedence over redis_url when both are set.
  redis_sentinel:
    sentinels:
      - "redis-sentinel:26379"     # the redis-sentinel Service (one entry is enough; more sentinels are auto-discovered)
    service_name: "mymaster"       # the monitored primary's name (Sentinel's `mymaster`)
```

The same `redis_sentinel` block applies to the dispatcher, the broker, and any
process that opens Redis.

### Bot / cog pod

```yaml
general:
  discord_token: "YOUR_TOKEN"      # gateway connection
  dispatch_http_url: "http://dispatcher:8082"   # all dispatch calls go here
  include:
    default: true
    message_dispatcher: false      # must NOT be loaded on cog pods
    markov: true
    delete_messages: true
    music: true
```

`dispatch_http_url` activates `HttpDispatchClient`. If unset, `CogHelper._dispatcher`
falls back to the in-process `MessageDispatcher` cog (single-process mode).

---

## Observability

### Trace context propagation

W3C traceparent headers flow from cog pod → dispatcher pod → Redis payload:

- `HttpDispatchClient._trace_headers()` injects the active span's traceparent
  into outbound POST requests.
- `DispatchHttpServer` extracts it via `opentelemetry.propagate.extract(request.headers)`
  and creates a `SpanKind.SERVER` span for each handler.
- For async work (fire-and-forget), the span context is serialised into the
  Redis payload as a `{trace_id, span_id, trace_flags}` dict and reconstructed
  as a `trace.Link` on the worker span, so downstream spans link back to the
  originating command trace.

### Spans emitted

| Span name | Kind | Pod | Description |
|-----------|------|-----|-------------|
| `dispatch.send` | SERVER | dispatcher | Inbound send request |
| `dispatch.delete` | SERVER | dispatcher | Inbound delete request |
| `dispatch.update_mutable` | SERVER | dispatcher | Inbound mutable update |
| `dispatch.remove_mutable` | SERVER | dispatcher | Inbound mutable remove |
| `dispatch.update_mutable_channel` | SERVER | dispatcher | Inbound channel move |
| `dispatch.fetch_history` | SERVER | dispatcher | Inbound history fetch enqueue |
| `dispatch.fetch_emojis` | SERVER | dispatcher | Inbound emoji fetch enqueue |
| `dispatch_client.fetch_history` | CLIENT | bot | Awaitable history fetch (includes poll) |
| `dispatch_client.fetch_emojis` | CLIENT | bot | Awaitable emoji fetch (includes poll) |
| `message_dispatcher.process_mutable_redis` | INTERNAL | dispatcher | Bundle flush execution |
| `message_dispatcher.fetch_history_redis` | INTERNAL | dispatcher | History fetch execution |
| `message_dispatcher.fetch_emojis_redis` | INTERNAL | dispatcher | Emoji fetch execution |

Redis operations (BZPOPMIN, SET, GET, DEL, etc.) are traced automatically by
`RedisInstrumentor` on both pods.

### Metrics

| Metric | Description |
|--------|-------------|
| `message_dispatcher_queue_depth` | Pending items across all guild queues (dispatcher pod) |
| `heartbeat{background_job="message_dispatcher"}` | `1` while the worker pool is completing dequeue cycles ([loop health](monitoring/loop_health.md)) |

---

## Docker Compose

`docker/docker-compose.multiprocess.yml` starts the full split. The downloader
comes in two flavours behind two **mutually exclusive profiles** — pick one; the
core services have no profile and always start:

| Service | Profile | Image | Purpose |
|---------|---------|-------|---------|
| `redis` | — | `redis:7-alpine` | Shared state; persisted via named volume |
| `postgres` | — | `postgres:16-alpine` | Video-cache DB for the broker |
| `dispatcher` | — | `Dockerfile.dispatcher` | HTTP server + Redis workers; holds Discord gateway |
| `broker` | — | `Dockerfile.broker` | Persists download results, serves them to the bot |
| `search` | — | `Dockerfile.search` | Standalone search tier; resolutions return via the broker |
| `bot` | — | `Dockerfile` | Cog logic; enqueues to the downloader + reads results from the broker |
| `downloader-direct` | `local` | `Dockerfile.downloader` | Download worker with no tunnel — default egress, **no Mullvad key** |
| `gluetun` | `vpn` | `qmcgaw/gluetun` | One Mullvad WireGuard tunnel; `downloader-vpn` shares its netns |
| `downloader-vpn` | `vpn` | `Dockerfile.downloader` | Download worker, in-tunnel, per-download SOCKS5 exits (prod shape) |

**Contributor default — no Mullvad account needed:**

```bash
cp docker/.env.example docker/.env      # Discord + music creds; leave MULLVAD_* blank
docker compose -f docker/docker-compose.multiprocess.yml --profile local up -d --build
```

**Prod shape, with per-download Mullvad exits:**

```bash
cp docker/.env.example docker/.env      # ...plus a real MULLVAD_WG_PRIVATE_KEY
docker compose -f docker/docker-compose.multiprocess.yml --profile vpn up -d --build
```

gluetun never reports healthy without a real Mullvad key and the downloader is
gated on `service_healthy`, which is why the pair sits behind a profile: without
it, a contributor with no key cannot bring the stack up at all.

Both flavours answer to the network alias **`downloader-host`**, so
`discord.bot.cnf` points at `http://downloader-host:8083` either way (for the
tunnel flavour the alias lives on `gluetun`, since `downloader-vpn` shares its
netns and has no network identity of its own). Nothing in the bot config changes
when you switch profiles.

Config files go in `volumes/cnf/` (copy the `docker/*.cnf.example` files):
`discord.dispatcher.cnf`, `discord.broker.cnf`, `discord.search.cnf`,
`discord.bot.cnf`, and the downloader config for your profile —
`discord.downloader.direct.cnf` for `local`, `discord.downloader.cnf` (which
carries the `mullvad-socks5` egress block, see
[Egress Modes](./music.md#egress-modes)) for `vpn`. Each is mounted at
`/opt/discord/cnf/discord.cnf` in its container.

### Testing SOCKS5 egress

`downloader-vpn` runs `network_mode: service:gluetun`, so it egresses each download
through a different Mullvad exit's SOCKS5 over the one tunnel. gluetun keeps the
compose network off the tunnel (`FIREWALL_OUTBOUND_SUBNETS` + `DOT: off` /
`DNS_ADDRESS: 127.0.0.11`) so redis/broker stay reachable. Play a track, then watch
which exit each download left from:

```bash
docker compose -f docker/docker-compose.multiprocess.yml logs -f downloader-vpn | grep "egress via exit"
```

> **Note:** neither downloader config has an S3 bucket configured, so finished files stay local
> and won't play back (the bot can't read the downloader's disk). That's enough to
> validate egress; for real end-to-end playback add a MinIO service and a bucket to
> the downloader/broker/bot configs.

See [Docker documentation](./docker.md) for volume permissions and health check setup.
