# Discord Bot

A discord bot framework written in python. Supports starting a bot via a token, configuration via YAML files, database sessions, and includes plugin support.

Includes some pre-written cogs for:

- Playing audio from YouTube and other media sites in voice channels
- Looking up words in Urban Dictionary
- Auto generating messages from channel history via Markov Chains
- Auto deletion of messages in specific channels
- Advanced Role Based Access Control

## Setup

This project is made up of six pods that work together; assume all six are built and running — see [docs/setup.md](./docs/setup.md) for the full walkthrough. Docker Compose is the quickest way to stand up the whole stack:

```bash
$ git clone https://github.com/tnoff/discord-bot.git
$ cd discord-bot
$ cp docker/.env.example docker/.env      # Discord token + music creds; leave MULLVAD_* blank
$ docker compose -f docker/docker-compose.multiprocess.yml --profile local up -d --build
```

Installing just the bot's own package via `pip` is **not recommended** — with six inter-dependent pods, standing up the rest of the stack by hand is significantly more work than Compose does for you.

## Configuration

Each of the six pods takes its own YAML config file — there is no single config
that runs "the bot" by itself. Only the bot and dispatcher pods require a real
Discord bot token (generate one via the
[discord developer portal](https://discord.com/developers/docs/topics/oauth2)) —
the bot for its gateway connection, the dispatcher to authenticate outbound
REST calls; the other four pods don't need one. Every pod's config supports
[pyaml-env](https://pypi.org/project/pyaml-env/) so values can come from an
environment variable instead of being written in plain text. See
[docs/configuration.md](./docs/configuration.md) for the full per-pod
configuration reference — database, logging, cog include-list, intents,
guild-removal — and the [Monitoring Documentation](./docs/monitoring/index.md)
for OpenTelemetry setup.

## Help Page

To check the available functions, use `!help` command.

## Cogs

| Cog | Config key | Example commands | Docs |
|-----|------------|------------------|------|
| `General` | (always loaded) | `!hello`, `!roll <dice>`, `!meta` | [docs/general.md](./docs/general.md) |
| `DeleteMessages` | `delete_messages` | `!delete <n>`, `!autodelete` | [docs/delete_messages.md](./docs/delete_messages.md) |
| `Markov` | `markov` | `!markov speak`, `!markov on/off` | [docs/markov.md](./docs/markov.md) |
| `Music` | `music` | `!play`, `!pause`, `!skip`, `!queue`, `!history` | [docs/music.md](./docs/music.md) |
| `RoleAssignment` | `role` | `!role add/remove/list` | [docs/role.md](./docs/role.md) |
| `UrbanDictionary` | `urban` | `!word <term>` | [docs/urban.md](./docs/urban.md) |

`CommandErrorHandler` loads unconditionally and has no user-facing commands.
`MessageDispatcher` is not one of the bot's cogs at all — it is the sole
worker of the separate `discord-dispatcher` pod, which sends and edits
Discord messages on the bot's behalf over HTTP. See
[Message dispatcher](./docs/message_dispatcher.md).

## Additional docs

- [Setup](./docs/setup.md) — the Docker Compose walkthrough
- [Configuration](./docs/configuration.md) — database, logging, cog include-list, intents, guild removal, config file/volume locations
- [HA architecture](./docs/architecture.md) — what each of the six pods does, how they talk to each other, and container runtime details (non-root user, volume permissions, debug builds)
- [CLI and application lifecycle](./docs/cli.md)
- [`CogHelperBase` reference (for developers)](./DEVELOPMENT.md#cog-skeleton)
- [Message dispatcher](./docs/message_dispatcher.md)
- [Messaging system](./docs/messaging.md) — the dispatcher/bundle model used by every pod
- [Monitoring and observability](./docs/monitoring/)
- Music deep-dives: [overview](./docs/music.md), [media broker](./docs/music/media_broker.md), [flow](./docs/music/flow.md)
- For setup, tests, and contributing: [DEVELOPMENT.md](./DEVELOPMENT.md)
- For agent-specific guidance: [AGENTS.md](./AGENTS.md)