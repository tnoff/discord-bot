# Discord Bot Docker Build

Build docker file for discord-bot to run however you want.

## Build Docker Image Locally

Build just the discord-bot docker image:

```
$ docker build .
```


## Setup Local Vars

You'll need a discord token to start the bot, you can generate one of these through the [discord developer portal](https://discord.com/developers/docs/topics/oauth2).
Then place this token in an `.env` file under `DISCORD_TOKEN`.

Note that the default `init.sql` assumes the password for all users is just `password`, you'll need to update the users via psql manually to update these.

### Volumes

The `discord.cnf` file should be mounted into `/opt/discord/cnf/discord.cnf` file for the bot to use.

It is also recommended that the download files for the music bot are set to a volume. This path can be updated via the config.
