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
$ pip install discord-bot/discord_bot/
```

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

### Log File Rotation

If no log section given, logs will go to stdout by default. If you wish to setup logs and have log rotation set:

```
---
general:
  discord_token: blah-blah-blah-discord-token
  logging:
    log_file: /logs/discord.log # Log file path
    log_file_count: 2 # Max backup log files
    log_file_max_bytes: 1240000 # Size to rotate log files at
```

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
$ discord-bot /path/to/config/file run
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

## Cog Docs

- [Common Cog](./docs/common.md)
- [Urban Cog](./docs/urban.md)
- [Delete Messages](./docs/delete_messages.md)
- [Markov](./docs/markov.md)
- [Role](./docs/role.md)
- [Music](./docs/music.md)


## Database dump and load

Dump database contents to a json file, and load database contents from that same json file


Prints json contents to screen
```
$ discord-bot /path/to/config/file db_dumps
# To save to a file
$ discord-bot /path/to/config/file db_dumps > db.json
```

Loads that same json to the db
```
$ discord-bot /path/to/config/file db_load db.json
```

## Plugins


You can add custom plugins in the `cogs/plugins` directly, that will be loaded automatically. The Cogs must inherit from the `CogHelper` class, and take the arguments `bot`,  `logger`, and `settings` as arguments. `db_engine` may also be passed as an optional arg.

You can also use the `BASE` declarative base from the database file in any plugin file, in order to create database tables.

If you place a `requirements.txt` file in the plugins directly, these should be installed during the `pip install` of the package.

Once you import CogHelper, you can add commands similar to how you would a Cog.

Example:

```
from asyncio import sleep
from discord_bot.cogs.common import CogHelper


class TestCog(CogHelper):
    def __init__(self, bot, logger, settings, db_engine=None):
        super().__init__(bot, logger, settings, db_engine=db_engine)
        BASE.metadata.create_all(self.db_engine)
        BASE.metadata.bind = self.db_engine
        self.loop_sleep_interval = settings['test'].get('loop_sleep_interval', 3600)

    async def __main_loop(self):
        '''
        Main loop runner
        '''
        # Do stuff
        await sleep(self.loop_sleep_interval) # Every 5 minutes

```

#### Aditional Settings


Additional settings can be added for plugins. The settings will be available in the `settings` variable passed into the plugin, and will be available under the key of the config section.

For example:

```
general:
  discord_token: foo
  log_file: discord.log
test:
  foo: bar
```

Will have the following settings value added to the config

```
{
  'general': {
    'discord_token': 'foo',
    'log_file': 'discord.log'
  },
  'test': {
    'foo': 'bar',
  }
}
```

### Allowed Roles

You can set access to discord commands in specific servers to allowed roles within the config. You'll need to specify the server id (sometimes called 'guild id'), any specific channel ids, and the role names.

You can specificy specific channel ids, or pass in 'all' for default options for any channel in the server.

Roles should be separated by the string ';;;'.

The format should look like:
```
general:
  allowed_roles:
    <server-id>:
      all: "@everyone;;;admin"
      <channel-id>: admin
```