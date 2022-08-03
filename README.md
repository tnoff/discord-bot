# Discord Bot


Bot for discord servers. Includes functions for playing music in voice chats, music playlists, twitter, and a markov chat bot.

## Setup

To install the python package, install the pip file within the repo

```
$ git clone https://github.com/tnoff/discord-bot.git
$ pip install discord-bot/discord_bot/
```

To run the bot via the command line

```
discord-bot /path/to/config/file
```

### Plugins


You can add custom plugins in the `cogs/plugins` directly, that will be loaded automatically. The Cogs must use the `discord.ext.commands.cog.CogMeta` class, and take the arguments `bot`, `db_session`, `logger`, and `settings` as arguments. The easiest way to do this is to inherit the `CogHelper` object from the common cogs file.

You can also use the `BASE` declarative base from the database file in any plugin file, in order to create database tables.

For example plugins see: [https://github.com/tnoff/discord-bot-plugins]

## Config

The config should be a file in the YAML format.

Two main arguments are required
- A discord authentication token, you can read more about that [here|https://discord.com/developers/docs/topics/oauth2]
- A log file

```
general:
  discord_token: blah-blah-blah-discord-token
  log_file: /logs/discord.log
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

### Database

The commands in the default cog do not use the database, but plugins usually will. Sqlite3 and mysql databases are current supported.


#### Sqlite


The simpliest database type is a sqlite file
```
general:
  db_type: sqlite

sqlite:
  file: /path/to/sqlite/file
```


#### Mysql

A mysql server can be used instead of sqlite

```
general:
  db_type: mysql

mysql:
  user: discord
  password: example-password
  database: discord
  host: 172.19.0.2
```

### Aditional Settings


Additional settings can be added for plugins. The settings will be available in the `settings` variable passed into the plugin, and will be available under the key of the config section, then an underscore, then the option name.

For example:

```
test:
  foo: bar
```

Will have the following settings value

```
{
    "test_foo": "bar"
}
```

If the setting is "true" or "false" (lower or upper case), it will be converted to a boolean value. Settings will also try to be converted to a number value if possible.

## Usage

To check the available functions, use `!help` command.