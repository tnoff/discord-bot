# Discord Bot

A discord bot framework written in python. Supports starting a bot via a token, configuration via YAML files, database sessions, and includes plugin support.

Includes some pre-written cogs for:

- Playing audio from youtube in voice channels
- Looking up words in Urban Dictionary
- Auto generating messages from channel history via Markov Chains
- Auto deletion of messages in specific channels
- Advanced Role Based Access Control

## Setup

To install the python package, install the pip file within the repo

```
$ git clone https://github.com/tnoff/discord-bot.git
$ pip install discord-bot/
```

## Docker

Docker support and builds are available, read me in the [docker docs](./docs/docker.md)

## Configuration

You'll need to set up a YAML config file for the bot to use. The only requirement is a discord bot token. You can generate one of these through the [discord developer portal](https://discord.com/developers/docs/topics/oauth2).


So at minimum a config file can look like:
```
---
general:
  discord_token: blah-blah-blah-discord-token
```

There is also support for [pyaml-env](https://pypi.org/project/pyaml-env/) so environment variables can be passed in:
```
---
general:
  discord_token: !ENV ${DISCORD_TOKEN}
```

### Database

Certain cogs, such as markov or music, have functions that require database support. You can pass in a database connection string that will then be passed into sqlalchemy.

```
---
general:
  discord_token: blah-blah-blah-discord-token
  sql_connection_statement: sqlite:///home/user/db.sql
```

The database uses [alembic](https://alembic.sqlalchemy.org/en/latest/) to run the migrations. To upgrade to the latest changes use:

```
$ alembic upgrade head
```

Alembic assumes you have an environment variable with `DATABASE_URL` set that is an sqlalchemy driver connection string.

For local dev, run the following to generate migrations after editing the `database.py` file:

```
$ alembic revision --autogenerate -m "we changed some things, it was neat"
```

### Monitoring and Observability

The bot includes comprehensive monitoring capabilities using OpenTelemetry (OTLP). Configure monitoring in your config file:

```yaml
general:
  monitoring:
    otlp:
      enabled: true
    memory_profiling:
      enabled: false  # Optional: enable memory profiling
```

See the [Monitoring Documentation](./docs/monitoring/) for complete setup instructions, available metrics, and configuration options.

### Log Setup

If no log section given, logs will go to stdout by default. If you wish to setup logs and have log rotation set:

```
---
general:
  discord_token: blah-blah-blah-discord-token
  logging:
    log_dir: /logs/discord # Log file path
    log_file_count: 2 # Max backup log files
    log_file_max_bytes: 1240000 # Size to rotate log files at
```

A `log_dir` can be passed and then each cog is setup to send to a log file within that dir. Each log file will be named after the cog, so look for `music.log` for music cog logs, for example.

### Include Cogs

The "common" cog with some basic functions will be included by default, the rest are opt-in
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

## Running bot

To run the bot via the command line

```
$ discord-bot /path/to/config/file
```

## Help Page

To check the available functions, use `!help` command.


## Intents

Certain cogs and function will require different "intents" to be setup in the config, and enabled in your developer portal. You can read more about that [here](https://discordpy.readthedocs.io/en/stable/intents.html).

You can find a list of intents [here](https://discordpy.readthedocs.io/en/stable/api.html?highlight=intents#discord.Intents) as well.

You can set intents in the config like so

```
intents:
  - members
```

## Remove Bot From Server

Use config values to remove bot from server if you cannot remove it yourself. This will take effect on the next restart of the bot.

```
general:
  rejectlist_guilds:
    - guild_id_1234505018501
```

## Additional Docs

- [CLI and Application Lifecycle](./docs/cli.md)
- [Common Cog](./docs/common.md)
- [Delete Messages Cog](./docs/delete_messages.md)
- [Markov Cog](./docs/markov.md)
- [Monitoring and Observability](./docs/monitoring/)
- [Music Cog](./docs/music.md)
- [Role Cog](./docs/role.md)
- [Urban Cog](./docs/urban.md)