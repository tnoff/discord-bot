# Changelog
## [0.5.0] - 2026-10-28

### Changed

- **Breaking config:** `music.broker_client.url`, `music.download_client.url`, `music.youtube_music_search_client.url` and `music.media_search_client.url` are now required (the pod refuses to start without them), the same keys the gateway reads. The pod now serves every music command (#1030, Phase 2): `join`, `play`, `skip`, `stop`, `clear`, `shuffle`, `remove`, `bump`, `history`, `move-messages`, `random-play`, `music-stats`, and `playlist item-add`, `playlist queue`, `playlist save-queue` and `playlist save-history`. It has no player: `BrokerPlayerGateway` reads a guild's player from the broker's queue and asks the gateway to join, stop, move the status messages or wake the player with gateway intents, waiting up to 35 s for a join. Cache hits are queued on the broker by the pod (`TrackQueuer`) and the player is woken. The pod also reads `music.player.queue_max_size` (default 128) and `music.download.server_queue_priority`, starts the downloader client's status poller (what `pending_downloads` reads) and closes the new clients on shutdown. `play` and the other commands are queued per guild like the rest, so a `skip` waits behind a `play` that is still enqueuing a large playlist.

## [0.4.2] - 2026-10-28

### Changed

- Bump the `discord_core` pin to core-v3.13.0 (#1030): `playlist.queue` carries its shuffle and count arguments (`PlaylistQueueArgs`).

## [0.4.1] - 2026-10-27

### Changed

- Bump the `discord_core` pin to core-v3.12.0 (#1030): the shared music clients, `TrackQueuer` and the `PlayerGateway` Protocol are available to the commands pod.

## [0.4.0] - 2026-10-26

### Changed

- The pod now serves `playlist create`, `playlist list`, `playlist show`, `playlist delete`, `playlist rename`, `playlist item-remove` and `playlist merge` (#1030, Phase 2): the commands that only touch the playlist store moved here from the gateway and read and write through the db pod with `HttpPlaylistStore`, which the pod closes on shutdown. It reads `music.playlist.server_playlist_max_size` (default 64) and `music.general.message_delete_after` (default 300) from its own config. Replies can now carry a delete timer (`Reply` takes `delete_after`), which the playlist replies use as the gateway's did. A negative playlist index is now refused instead of indexing the list from its end. The "database is not enabled" reply is gone from these commands: the pod always has the db url.

## [0.3.1] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.11.0 (#1030): the playlist store and `playlist_public_view` are available to the commands pod.

## [0.3.0] - 2026-10-25

### Changed

- **Breaking config:** `general.database_http_url` is now required (the pod refuses to start without it). The pod now serves `markov on`, `markov off`, `markov list-channels` and `markov speak` (#1030, Phase 2): their logic moved here from the gateway and reads and writes through the db pod with `HttpMarkovStore`, which the pod closes on shutdown after the dispatcher client. `markov.server_reject_list` is read from this pod's config. `markov on` takes the channel-type check from the gateway (`channel_is_text_or_voice`) because the pod has no Discord connection.

## [0.2.3] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.10.0 (#1030): the markov store and `markov.on` arguments are available to the commands pod.

## [0.2.2] - 2026-10-25

### Changed

- Bumped tnoff/dappertable to v1.1.12

## [0.2.1] - 2026-10-23

### Changed

- Bump the `discord_core` pin to core-v3.9.0 (#1030): loops can treat a peer outage as non-fatal to liveness.

## [0.2.0] - 2026-10-22

### Changed

- The pod now serves `hello`, `roll`, `meta` and `urban` (#1030, Phase 2): their logic moved here from the gateway, and the pod replies through the dispatcher, so `general.dispatch_http_url` is now required. It starts the peer route check for its dispatcher client and closes the client after the executor on shutdown. Adds `beautifulsoup4` and `dappertable`.

## [0.1.1] - 2026-10-22

### Changed

- Bump the `discord_core` pin to core-v3.8.0 (#1030, Phase 2): the commands route is part of the peer route check.

## [0.1.0] - 2026-10-21

### Changed

- New pod: `discord-commands`, the process that will run command logic for the gateway (#1030, Phase 2). This is the skeleton and serves nothing yet: `POST /commands/{command}` validates the envelope and arguments, drops a replayed `request_id`, queues the command on its guild's FIFO and answers 202, but no command has a handler, so every known command answers 501 and the gateway forwards nothing to it. Handlers arrive with the logic, one command group at a time.

