# Changelog

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

