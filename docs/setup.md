# Setup

This project is made up of six pods that work together: `discord-bot` (gateway,
cogs, commands), `discord-dispatcher`, `discord-db`, `discord-broker`,
`discord-downloader`, and `discord-search`. It is not a standalone install —
assume all six need to be built and running. See [HA architecture](./architecture.md)
for what each pod does and how they talk to each other.

## Docker Compose

Docker Compose is the quickest way to get the whole stack up — it builds and
wires together all six pods (plus Redis and Postgres) for you, rather than
you standing up each HTTP dependency by hand:

```bash
$ git clone https://github.com/tnoff/discord-bot.git
$ cd discord-bot
$ cp docker/.env.example docker/.env      # Discord token + music creds; leave MULLVAD_* blank
$ docker compose -f docker/docker-compose.multiprocess.yml --profile local up -d --build
```

See [HA architecture](./architecture.md#docker-compose) for the full service table, the
`local` vs `vpn` downloader profiles, and the config files each pod mounts.
**Note:** the compose file doesn't currently wire up a `discord-db` service
on top of its bare `postgres` container, so database-backed cogs (markov,
playlists) have nowhere to connect until that's added — see the caveat in
[HA architecture](./architecture.md#docker-compose).

Building a single pod's Docker image directly (without Compose) is also
available:

```bash
$ docker build -f docker/Dockerfile.gateway .
```

See [Container runtime details](./architecture.md#container-runtime-details) for the
non-root user, volume permissions, and debug-build notes that apply either
way.
