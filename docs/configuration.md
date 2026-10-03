# Configuration

Detailed configuration for each pod's own YAML config file — there is no
single config that runs the whole project. Only the bot and dispatcher pods
need a real Discord token; see [README.md](../README.md#configuration) for
why, how to generate one, and `pyaml-env` environment-variable substitution.
The sections below cover what's needed beyond that, and which pod each
setting belongs to.

## Config File Location

Every pod's container expects its YAML config mounted at
`/opt/discord/cnf/discord.cnf`. The entrypoint script creates
`/opt/discord/cnf` and `/opt/discord/downloads` automatically if they don't
already exist, so an empty host directory is fine — see [Container runtime
details](./architecture.md#container-runtime-details) for the permission requirements
that come with that.

Common mount points:

| Purpose | Container path |
|---|---|
| Config | `/opt/discord/cnf` |
| Downloads/data | `/opt/discord` (or a subdirectory, e.g. `/opt/discord/downloads`) |
| Logs | `/var/log/discord` |

In the multi-pod Docker Compose stack each pod mounts a different config
file under this same path — `discord.dispatcher.cnf`, `discord.broker.cnf`,
etc. — see [HA architecture](./architecture.md#docker-compose) for the full list and
where they live on the host.

## Per-pod configuration reference

Each pod's CLI entrypoint module (`cli/<entrypoint>.py`) carries its own
"Configure with:" docstring block, naming every config key that pod's `run()`
actually reads, required vs. optional, with real defaults. The block below is
generated straight from those docstrings rather than retyped by hand, so it
cannot drift the way a hand-copied reference has before.

<!-- BEGIN GENERATED(config-reference) by tests/cli/test_config_reference.py. Do not edit this block by hand.
     Regenerate with: UPDATE_CONFIG_REFERENCE=1 pytest tests/cli/test_config_reference.py -->

Generated from each pod's own `cli/<entrypoint>.py` module docstring --
nothing here is retyped by hand, so it cannot drift from the code the way a
hand-copied reference has before. Per-cog config (`music.*`, `markov.*`,
`role.*`, `urban.*`, `delete_messages.*`) is named in the bot pod's block only
as a pointer; each has its own page (docs/music.md, docs/markov.md, etc.).

### discord-bot

```
Configure with:
    general.discord_token     — Discord bot token (required; see
                                require_discord_token)
    general.dispatch_http_url — Dispatcher pod URL; cogs route all outbound
                                messages through it (required — raises
                                DiscordBotException if missing)
    general.database_http_url — discord-db pod URL (required for HA bot mode —
                                raises DiscordBotException if missing)
    general.include           — which optional cogs to load (markov, urban, music,
                                delete_messages default off; default and
                                message_dispatcher default on) — see IncludeConfig
    general.intents           — extra discord.py Intents to enable beyond
                                Intents.default()
    general.rejectlist_guilds — guild IDs the bot leaves on sight (on_ready)
    general.monitoring        — optional OTLP / health-server config
    music.*                   — Music cog, see docs/music.md
    markov.*                  — Markov cog, see docs/markov.md
    role.*                    — RoleAssignment cog, see docs/role.md
    urban.*                   — UrbanDictionary cog, see docs/urban.md
    delete_messages.*         — DeleteMessages cog, see docs/delete_messages.md
```

### discord-dispatcher

```
Configure with:
    general.discord_token                      — Discord bot token (required for outbound REST
                                                 auth; see require_discord_token)
    general.redis_url / general.redis_sentinel — Redis connection (required — raises
                                                 DiscordBotException if neither is set)
    general.dispatch_shard_id                  — this process's shard index (default 0)
    general.dispatch_process_id                — this process's worker id for the sharded work
                                                 queue (default: a random uuid4)
    general.dispatch_server                    — {host, port} for the HTTP server
                                                 (default 0.0.0.0:8082)
    general.monitoring                         — optional OTLP / health-server config
```

### discord-broker

```
Configure with:
    general.redis_url             — Redis connection URL (required)
    general.database_http_url     — discord-db pod URL, e.g. http://discord-db:8085.
                                    Required when music.download.cache is enabled;
                                    without it the catalog is unreachable and the
                                    cache is disabled with a warning.
    general.broker_server         — {host, port} for the HTTP server (default 0.0.0.0:8081)
    general.dispatch_http_url     — Dispatcher URL; the broker pushes bundle-UI
                                    edits / failure summaries through it.  Without
                                    it the broker tracks bundle state but cannot
                                    update Discord messages.
    general.monitoring            — optional OTLP / health-server config
    music.storage.bucket_name     — S3 bucket (required for HA checkout)
    music.download.cache          — optional video-cache config. Only
                                    enable_cache_files is read here; the eviction
                                    policy belongs to the db pod that owns the rows.
    music.general.message_delete_after — seconds before Discord auto-expires the
                                    bundle summary / failure summary messages this
                                    process sends (default 300)
```

### discord-downloader

```
Configure with:
    general.redis_url / general.redis_sentinel — Redis connection (required)
    general.monitoring                 — optional OTLP / health-server config
    music.broker_client.url            — Broker pod URL (required; downloader posts
                                         IN_PROGRESS / RETRY / register_download_result)
    music.download.storage.bucket_name — S3 bucket (required for cache-mode checkout)
    music.download.download_dir_path   — scratch dir (default: a TemporaryDirectory)
    music.download.*                   — yt-dlp + backoff config (extra_ytdlp_options,
                                         max_video_length, banned_videos_list,
                                         youtube_wait_period_minimum, etc.)
    music.download.retry_backoff_seconds_minimum
                                       — hold-off before a failed YouTube download is
                                         retried, doubling per attempt (default 30;
                                         0 restores the immediate requeue)
    general.downloader_server          — {host, port} for the HTTP server
                                         (default 0.0.0.0:8083)
```

### discord-search

```
Configure with:
    general.redis_url / general.redis_sentinel — Redis connection (required)
    general.monitoring                 — optional OTLP / health-server config
    general.search_server              — {host, port} for the HTTP server
                                         (default 0.0.0.0:8084)
    music.broker_client.url            — Broker pod URL (required; the pod pushes
                                         RETRY_SEARCH / FAILED lifecycle updates
                                         and registers search results)
    music.download.spotify_credentials — {client_id, client_secret} for
                                         /search/spotify (optional; absent means
                                         that route answers MISSING_CREDENTIALS)
    music.download.youtube_api_key     — YouTube Data API key for /search/youtube
                                         (optional, same rule)
    music.download.youtube_wait_period_minimum / _max_variance — 429 backoff shape
    music.download.max_youtube_music_search_retries — per-request retry budget
    music.download.failure_tracking_max_size / _max_age_seconds — failure queue
    music.download.server_queue_priority — [{server_id, priority}] used when a
                                         retried request is re-enqueued
```

### discord-db

```
Configure with:
    general.sql_connection_statement — postgresql:// or sqlite:/// DSN (required; no
                                       engine means no pod, so this raises
                                       rather than warns)
    general.database_server          — {host, port} for the HTTP server
                                       (default 0.0.0.0:8085)
    general.monitoring               — optional OTLP / health-server config
    music.download.cache             — VideoCache catalog config. Disabled or
                                       absent means the video_cache routes are
                                       not registered at all and answer 404,
                                       which is the server's designed behaviour
                                       for a group with no store.
```

<!-- END GENERATED(config-reference) -->

## Health Check

Every pod ships an image-level Docker `HEALTHCHECK`, but only the bot pod's
config toggle below actually drives one that exercises real health-check
logic (Redis ping, database `SELECT 1`, loop health) rather than a bare TCP
probe — see [Health server documentation](./monitoring/health_server.md#docker-integration)
for that distinction and the full endpoint reference.

```yaml
general:
  monitoring:
    health_server:
      enabled: true
      port: 8080
```

## Database

Certain cogs, such as markov or music, have functions that require database
support — but the bot itself has no direct database connection. It is an
HTTP client of the `discord-db` pod, and needs that pod's URL:

```
---
general:
  discord_token: blah-blah-blah-discord-token
  dispatch_http_url: http://dispatcher:8082
  database_http_url: http://db:8085
```

`discord-db` is the pod that actually opens a database connection, and it
is configured separately, in its own config file. It doesn't require a real
Discord token — only the bot and dispatcher pods do:

```
---
general:
  sql_connection_statement: postgresql://user:pass@host:5432/discord_bot
```

PostgreSQL and SQLite are both supported. For SQLite, point the DSN at a file
and nothing else needs to run:

```
---
general:
  sql_connection_statement: sqlite:////var/lib/discord/discord.db
  run_migrations: true
```

`discord-db` rewrites the URL to the async driver at startup
(`postgresql+asyncpg://` or `sqlite+aiosqlite://`), so the config uses the
standard `postgresql://` / `sqlite:///` form. Any other backend is rejected at
boot.

SQLite notes:

- **Schema.** With `run_migrations: true`, a SQLite file that has never been
  migrated is built directly from the current models and stamped at the latest
  revision, rather than replaying the alembic chain (which was written for
  postgres and uses `ALTER COLUMN`, which SQLite cannot do). Later migrations
  run against that file as usual, so they must be written to work on SQLite
  (use `op.batch_alter_table`). `run_migrations` cannot be used with an
  in-memory database.
- **One pod, one file.** SQLite is a single-writer file database. Run a single
  `discord-db` replica against a persistent volume; the file is opened in WAL
  mode with foreign keys enforced and a 30s busy timeout.
- **Moving from postgres to SQLite.** `discord-db <config> --copy-to-sqlite
  <path>` copies a postgres database into a new SQLite file instead of serving.
  The source is the config's own `sql_connection_statement`, so the password
  stays in whatever that config already reads it from. Stop the `discord-db`
  pod first: the copy takes no lock, and it fails if any table's row count
  changed under it (it cannot see a row edited in place). It then:
  - refuses to run if `<path>` already exists, and builds the copy at
    `<path>.partial`, renaming it into place only after everything below passes;
  - refuses if any table or column differs from the models (rows are copied
    through the models, so a column they do not know would be lost), and if the
    source has tables the models do not know, unless `--allow-extra-tables`;
  - copies `alembic_version` verbatim, so the file is stamped at the revision
    the data was written at and the next `alembic upgrade head` carries on;
  - reads every table back from the file and compares row count and a SHA-256
    over the rows in primary-key order with what it read from the source.

  Point `sql_connection_statement` at the new file afterwards. Postgres is left
  untouched, so keep it until the SQLite pod has served for a while. Going the
  other way (SQLite to postgres) is not supported.

The database uses [alembic](https://alembic.sqlalchemy.org/en/latest/) to run
the migrations. To upgrade to the latest changes use:

```
$ alembic upgrade head
```

**Important**: If upgrading from version 2.4.x or earlier to 2.5.0+, a
database migration is required to convert Discord IDs from strings to
integers. Make sure to run the migration command above before starting the
bot with the new version.

Alembic assumes you have an environment variable with `DATABASE_URL` set
that is an sqlalchemy driver connection string.

For local dev, run the following to generate migrations after editing the
`discord_db/database.py` schema file:

```
$ alembic revision --autogenerate -m "we changed some things, it was neat"
```

## Log Setup

If no log section given, logs will go to stdout by default.

### File Logging

To write logs to rotating files:

```
---
general:
  discord_token: blah-blah-blah-discord-token
  logging:
    log_level: 20 # Log level (0=NOTSET, 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL)
    log_dir: /logs/discord # Log file path
    log_file_count: 2 # Max backup log files
    log_file_max_bytes: 1240000 # Size to rotate log files at
```

The `log_dir`/`log_file_count`/`log_file_max_bytes` keys are read by every
pod's config loader. Setting `log_dir` splits output into one file per
internal logger name rather than a single stream — every pod gets at least
`main.log` and `discord.log` from the shared startup code, and pods add more
of their own on top: the bot pod logs each loaded cog to its own file (so
look for `music.log` for music cog logs, for example), and the downloader
pod similarly splits `ytdlp.log`, `audio_editing.log`, and
`download_client.log`.

### OTLP-Only Logging

If you have OTLP enabled and want logs sent exclusively via OTLP (no local
log files), set `otlp_only: true`. The `log_dir`, `log_file_count`, and
`log_file_max_bytes` fields are not required in this mode:

```
---
general:
  discord_token: blah-blah-blah-discord-token
  logging:
    log_level: 20
    otlp_only: true
  monitoring:
    otlp:
      enabled: true
```

## Include Cogs

Bot pod only — the other five pods have no cogs. The "common" cog with some
basic functions will be included by default, the rest are opt-in
```
---
general:
  discord_token: blah-blah-blah-discord-token
  include:
    music: true
    markov: true
    urban: true
    delete_messages: true
    role: true
```

## Intents

Bot pod only — intents govern the Discord gateway connection, which only
the bot pod opens. Certain cogs and functions will require different
"intents" to be setup in the config, and enabled in your developer portal. You can read more about
that [here](https://discordpy.readthedocs.io/en/stable/intents.html).

You can find a list of intents
[here](https://discordpy.readthedocs.io/en/stable/api.html?highlight=intents#discord.Intents)
as well.

You can set intents in the config like so

```
intents:
  - members
```

## Remove Bot From Server

Bot pod only — `rejectlist_guilds` is a shared config field every pod's
schema accepts, but only the bot pod's gateway connection actually enforces
it, on `on_ready`. Setting it on another pod's config has no effect. Use it
to remove the bot from a server if you cannot remove it yourself; it takes
effect on the bot pod's next restart.

```yaml
general:
  rejectlist_guilds:
    - 123450501850  # Guild ID as integer (unquoted)
```
