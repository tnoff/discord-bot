# Changelog

## [3.5.11] - 2026-10-28

### Changed

- Bump the `discord_core` pin to core-v3.13.0 (#1030): `playlist.queue` carries its shuffle and count arguments (`PlaylistQueueArgs`).

## [3.5.10] - 2026-10-27

### Changed

- Bump the `discord_core` pin to core-v3.12.0 (#1030): the shared music clients, `TrackQueuer` and the `PlayerGateway` Protocol are available to the commands pod.

## [3.5.9] - 2026-10-27

### Changed

- Docstring only: `http_history_stores` points at the shared playlist and analytics stores in `discord_core`.

## [3.5.8] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.11.0 (#1030): the playlist store and `playlist_public_view` are available to the commands pod.

## [3.5.7] - 2026-10-25

### Changed

- Docstring only: point at where the playlist store client now lives.

## [3.5.6] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.10.0 (#1030): the markov store and `markov.on` arguments are available to the commands pod.

## [3.5.5] - 2026-10-25

### Changed

- Bumped tnoff/dappertable to v1.1.12

## [3.5.4] - 2026-10-23

### Changed

- Bump the `discord_core` pin to core-v3.9.0 (#1030): loops can treat a peer outage as non-fatal to liveness.

## [3.5.3] - 2026-10-22

### Changed

- Bump the `discord_core` pin to core-v3.8.0 (#1030, Phase 2): the commands route is part of the peer route check.

## [3.5.2] - 2026-10-21

### Changed

- Bump the discord_core pin to 3.7.0, which adds the commands contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.5.1] - 2026-10-21

### Changed

- Bumped tnoff/dappertable to v1.1.11

## [3.5.0] - 2026-10-20

### Changed

- Changed the supported Python floor to 3.13 (3.11 and 3.12 dropped): tox now runs py313-py315 with pylint and bandit on 3.15, and requires-python is >=3.13 (tox and Dockerfiles are repo-wide)

## [3.4.5] - 2026-10-20

### Changed

- Bump the discord_core pin to 3.5.1 (#1030)

## [3.4.4] - 2026-10-19

### Changed

- Serve the gateway-intent routes (#1030, Phase 2) and bump the discord_core pin from core-v3.4.3 to core-v3.5.0, which declares them. A gateway holds the voice connection, so another pod (the commands pod) cannot make it join, stop or wake a player; it writes an intent to the broker and the gateway claims it with a short poll. `workers/gateway_intents.py` is the Redis side: intents are ordered by a sequence number, a claim leases them to one gateway for 10s (so a slow join is not handed out twice and a gateway that dies mid-claim is covered), they expire unclaimed after 30s, and a second `wake` for a guild that has one pending is not stored. `servers/gateway_intent_handlers.py` is the mixin on `BrokerHttpServer`; the empty claim answers 204 and opens no span. The routes stay in core's `routes/gateway_intents.py` for now, so the broker seam tests expect the union of both registries; folding them into `routes/broker.py` is a later core release.

## [3.4.3] - 2026-10-13

### Changed

- Bumped the `discord-core` pin to 3.4.3.

## [3.4.2] - 2026-10-12

### Changed

- Bumped tnoff/dappertable to v1.1.10

## [3.4.1] - 2026-10-11

### Changed

- Tests only: stop building `PlayerSession` with the removed `queue` field (core 3.3.0, #1048).

## [3.4.0] - 2026-10-09

### Changed

- Add the broker history worker: when a track plays out (not skipped), finish queues a play record in Redis and a worker drains it into the db pod, counting the play in the guild's analytics and adding it to the guild's history playlist (skipping plays replayed from history). Records are retried when the db cannot answer, kept in play order, and dropped only if they can never succeed. Adds heartbeat and history_event_queue_depth metrics and music.playlist.server_playlist_max_size to the broker config. Needs general.database_http_url; without it nothing is recorded and a warning says so (#1048).

## [3.3.0] - 2026-10-09

### Changed

- Render the guild's play-order message from the broker (a port of MusicPlayer.get_queue_order_messages) on every queue change, in the text channel given to open; open now takes that channel, moves the message when it changes, and puts a track the previous gateway started and never finished back at the head of the queue. Keeps a durable current-track marker (gcurrent) for that recovery. Bump the discord-core pin to core-v3.2.0 (#1048).

## [3.2.0] - 2026-10-09

### Changed

- Serve the guild-queue routes (14 under /guilds/{guild_id}): queue, claim, heartbeat, skip, finish, history, close/open and a cheap /queue/state poll, over the GuildQueueBroker added in 3.1.0 (#1048). Bump the discord-core pin to core-v3.1.0.

## [3.1.0] - 2026-10-09

### Changed

- Add the per-guild player queue to the broker (GuildQueueRegistry and GuildQueueBroker): ordered queue, claim/confirm, now-playing record, skip marker, history and a change version, as Redis Lua operations. No routes yet; nothing calls it until the gateway cutover (#1048). Entries now store cache_hit.

## [3.0.7] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.5.

## [3.0.6] - 2026-10-08

### Changed

- Remove the unused broker prefetch (POST /prefetch, prefetch on the broker engine and client); prefetch is done by the player (#1036).

## [3.0.5] - 2026-10-08

### Changed

- Finish the checkout cleanup: no guild_path on checkout, a miss answers {}, BrokerEntry drops guild_file_path, and InMemoryMediaSearchClient is now LocalMediaSearchClient (#1036).

## [3.0.4] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.3.

## [3.0.3] - 2026-10-08

### Changed

- Drop the single-process leftovers from the broker seam: checkout always answers with an s3_key, and stale comments are cleaned up (#1036).

## [3.0.2] - 2026-10-06

### Changed

- Bumped tnoff/dappertable to v1.1.9

