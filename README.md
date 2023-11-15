# Discord Bot


Python bot for discord servers. Handles basic configurations, database sessions, and supports plugins.

For some example plugins including youtube music in voice chat, twitter, and markov chat, see [some example plugins here](https://github.com/tnoff/discord-bot-plugins)

You'll need to generate a discord bot token for the config, you can see more information on that [here](https://discord.com/developers/docs/getting-started)

## Setup

To install the python package, install the pip file within the repo

```
$ git clone https://github.com/tnoff/discord-bot.git
$ pip install discord-bot/discord_bot/
```

For example, to run the bot with the example plugins run:

```
$ git clone https://github.com/tnoff/discord-bot.git
$ git clone https://github.com/tnoff/discord-bot-plugins.git
$ cp discord-bot-plugins/* discord-bot/discord_bot/cogs/
$ pip install discord-bot/discord_bot/
```

## Running bot

To run the bot via the command line

```
$ discord-bot /path/to/config/file run
```

## Basic Functions

### Hello

Say hello to the bot and it will say hello back. Mostly used to see if the bot is running

```
!hello
> Waddup tnoff
```

### Roll

Do a random dice roll

```
!roll 2d6
> tnoff rolled: 6 + 4 = 10
```

### Meta

Probably the most useful basic function, show user id, channel id, and guild (server) id.

```
!meta
> Server id: <redacted>
> Channel id: <redacted>
> User id: <redacted>
```

### Disable Default Cog

You can pass the following config arg to disable to default cog

```
general:
  include_default_cog = false
```

### Help Page

To check the available functions, use `!help` command.

## Config

The config should be a file in the YAML format.

Two main arguments are required:
- A discord authentication token, you can read more about that [here](https://discord.com/developers/docs/topics/oauth2)
- A log file

You can also pass in a `sql_connection_statement` to have a persistent database. This is not required for any of the standard functions but you may want to include a plugin that requires a db. The statement should be a standard sqlalchemy connection string, for example: `sqlite://database.sql`

```
general:
  discord_token: blah-blah-blah-discord-token
  log_file: /logs/discord.log
  sql_connection_statement: sqlite:///home/user/db.sql
```

## Allowed Roles

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

### Plugins


You can add custom plugins in the `cogs/plugins` directly, that will be loaded automatically. The Cogs must use the `discord.ext.commands.cog.CogMeta` class, and take the arguments `bot`, `db_session`, `logger`, and `settings` as arguments. The easiest way to do this is to inherit the `CogHelper` object from the common cogs file.

You can also use the `BASE` declarative base from the database file in any plugin file, in order to create database tables.

You can find some example plugins here [some example plugins here](https://github.com/tnoff/discord-bot-plugins).

If you place a `requirements.txt` file in the plugins directly, these should be installed during the `pip install` of the package.

Once you import CogHelper, you can add commands similar to how you would a Cog.

Example:

```
from asyncio import sleep
from discord_bot.cogs.common import CogHelper


class TestCog(CogHelper):
    def __init__(self, bot, db_engine, logger, settings):
        super().__init__(bot, db_engine, logger, settings)
        BASE.metadata.create_all(self.db_engine)
        BASE.metadata.bind = self.db_engine
        self.loop_sleep_interval = settings['test'].get('loop_sleep_interval', 3600)


    async def cog_load(self):
        self._task = self.bot.loop.create_task(self.main_loop())

    async def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self.lock_file.exists():
            self.lock_file.unlink()

    async def main_loop(self):
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__main_loop()
            except Exception as e:
                self.logger.exception(e)
                print(f'Player loop exception {str(e)}')
                return

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