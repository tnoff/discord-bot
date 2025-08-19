# Discord Bot Docker Build

Build docker file for discord-bot to run however you want.

## Build Docker Image Locally

Build just the discord-bot docker image:

```
$ docker build .
```

## Config

The `discord.cnf` file should be mounted into `/opt/discord/cnf/discord.cnf` file for the bot to use.

It is also recommended that the download files for the music bot are set to a volume. This path can be updated via the config.

## Database Setup

A driver for postgres is setup automatically in the docker image to use with sqlalchemy. Any other drivers will need to be added.