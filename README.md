Bot for discord servers. Includes functions for playing music in voice chats, music playlists, twitter, and a markov chat bot.

h1. Setup

To install the python package, install the pip file within the repo

```
$ git clone https://github.com/tnoff/discord-bot.git
$ pip install discord-bot/discord_bot/
```

To run the bot via the command line

```
discord-bot -c /path/to/config/file
```

h1. Config

Two main arguments are required
- A discord authentication token, you can read more about that [here|https://discord.com/developers/docs/topics/oauth2]
- A log file

```
[general]
discord_token=blah-blah-blah-discord-token
log_file=/logs/discord.log
```

h2. Database
The music playlist, twitter, and markov functions all use a database. Sqlite3 and mysql databases are current supported.


h3. Sqlite
The simpliest database type is a sqlite file
```
[general]
db_type=sqlite

[sqlite]
file=/path/to/sqlite/file
```


h3. Mysql
A mysql server can be used instead of sqlite

```
[general]
db_type=mysql

[mysql]
user=discord
password=example-password
database=discord
host=172.19.0.2
```

h2. Additional Settings

Additional settings can be added for plugins. The settings will be available in the `settings` variable passed into the plugin, and will be available under the key of the config section, then an underscore, then the option name.

For example:

```
[test]
foo=bar
```

Will have the following settings value

```
{
    "test_foo": "bar"
}
```

If the setting is "true" or "false" ( lower or upper case), it will be converted to a boolean value. Settings will also try to be converted to a number value if possible.

h1. Usage

To check the available functions, use `!help` command.

h1. Plugins

You can add custom plugins in the `cogs/plugins` directly, that will be loaded automatically. The Cogs must use the `discord.ext.commands.cog.CogMeta` class, and take the arguments `bot`, `db_session`, `logger`, and `settings` as arguments. The easiest way to do this is to inherit the `CogHelper` object from the common cogs file.

You can also use the `BASE` declarative base from the database file in any plugin file, in order to create database tables.