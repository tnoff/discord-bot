# Changelog

## [3.2.25] - 2026-10-19

### Changed

- Bump the discord_core pin from core-v3.4.3 to core-v3.5.0, which adds the gateway-intent contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.2.24] - 2026-10-19

### Changed

- Tests only (#1030, Phase 1): the 39 playlist tests that exercise only store-backed playlist logic now call `PlaylistCommands` directly from `tests/command_logic/test_playlist.py`; the 58 that need the cog, the player or the download pipeline stay in `tests/cogs/music/test_playlist.py`, and a new parametrized test checks that the seven moved commands' stubs forward the envelope and arguments. No source change.

## [3.2.23] - 2026-10-19

### Changed

- The search-result and download-result handling now lives in `discord_gateway/loop_logic/music_results.py` (#1030, Phase 1), discord-free like the other loop logic; the cog keeps the loops, the broker pop and the idle sleep, and `add_source_to_player` keeps only waking the player. `PlayerGateway` gains `has_live_player` (a player exists and is not shutting down), which replaces the `get_player` / `shutdown_called` check on a finished download. The cog's duplicate `_push_state`, `create_bundle` and `delete_bundle` are gone (callers use the broker directly). Two small differences: the dead "download produced nothing" branch is removed (a `MediaDownload` is always built from a result), and a player that disappears between the liveness check and the enqueue now leaves the request as it was instead of DISCARDED.

## [3.2.22] - 2026-10-16

### Changed

- Queue edits (`!clear`, `!shuffle`, `!remove`, `!bump`) no longer call into the gateway to refresh the prefetch (#1030, Phase 1): `PlayerGateway.queue_changed` is removed. The player's per-track watcher (`MusicPlayer._watch_queue`, which also carries the skip poll) now re-aims the prefetch whenever `poll_guild_queue` shows the queue version moved, and the refresh drops the staged copies of tracks that left the queue. A download already in flight for a removed track is left to finish and is swept by the next refresh. The refresh happens within about a second of an edit, and works the same for an edit made by any pod. The watcher no longer takes a stop event: the player cancels it at the end of the track.

## [3.2.21] - 2026-10-15

### Changed

- `!skip` now goes through the broker's skip marker instead of stopping the player in-process (#1030, Phase 1). `MusicCommands.skip` reads the playing track from the broker and calls `skip_track`, a compare-and-set on its uuid, so a skip that arrives as a track ends cannot skip the next one. The player runs a per-track watcher (`MusicPlayer._watch_skip`) that polls `poll_guild_queue` once a second and, when a skip is pending for the playing track, marks it skipped and stops the voice client. `PlayerGateway.skip` is removed: skipping no longer needs a call into the gateway process. The skip takes effect on the next poll, up to about a second after the command, instead of immediately.

## [3.2.20] - 2026-10-15

### Changed

- The markov loops now run their per-iteration work through `discord_gateway.loop_logic` (#1030, Phase 1b): `MarkovLoop.request` (the producer), `process_emojis_result` and `process_history_result` (the consumer's handlers), with `clean_message` and `build_word_pairs` moving alongside. It reaches Discord only through the cog's `dispatch_guild_emojis` and `dispatch_channel_history`, and the store through `MarkovStore`; the emoji cache stays owned by the cog. The cog keeps the wiring (tasks, `LOOP_HEALTH`, gauges, the result-queue loop). No behaviour change.

## [3.2.19] - 2026-10-15

### Changed

- The delete-messages loops now run their per-iteration work through a new discord-free package, `discord_gateway.loop_logic` (#1030, Phase 1b): `DeleteMessagesLoop.request` (the producer) and `DeleteMessagesLoop.process_result` (the consumer). It reaches Discord only through the cog's `dispatch_channel_history` and `dispatch_delete`. The cog keeps the wiring (tasks, `LOOP_HEALTH`, heartbeat and queue-depth gauges). The import-boundary test now covers `loop_logic` as well as `command_logic`. No behaviour change.

## [3.2.18] - 2026-10-15

### Changed

- The `role` commands (`list`, `users`, `available`, `add`, `remove`) now run through `discord_gateway.command_logic` (#1030, Phase 1, last step): `RoleCommands`. Roles and members are reached only through the new `RoleGateway` Protocol (`CogRoleGateway` implements it over discord.py's guild cache) and plain `RoleInfo` / `MemberInfo` data, and `CommandEnvelope` gains `author_role_ids`. Roles and members are compared by id instead of by discord object. One extra `fetch_member` per user on `role add` / `role remove`, to get the member to edit. No other behaviour change.

## [3.2.17] - 2026-10-14

### Changed

- `play`, `playlist item-add`, `playlist queue` and `random-play` now run through `discord_gateway.command_logic` (#1030, Phase 1, step 4c, part 2): `MusicCommands.play/playlist_queue/random_play` and `PlaylistCommands.item_add/queue_items`. The search, cache-check and routing pipeline (`enqueue_media_requests`, the cache-hit path, `add_playlist_item`) moves into a discord-free `MediaEnqueuer`, which the search and download result loops on the cog also call, so there is one copy of it. The player is reached only through the new `PlayerGateway.add_cached`, which replaces passing a `MusicPlayer` into the pipeline. `SearchClient` moves from `cogs/music_helpers/` to `clients/` so command logic can import it. No behaviour change.

## [3.2.16] - 2026-10-14

### Changed

- `playlist save-queue` and `playlist save-history` now run through `discord_gateway.command_logic.playlist.PlaylistCommands.save` (#1030, Phase 1, step 4c, part 1). They read the broker's queue or history and ask the player only whether one exists (`PlayerGateway.status`), so `PlaylistCommands` now also takes the broker client and the player gateway. The cog keeps thin stubs. No behaviour change.

## [3.2.15] - 2026-10-14

### Changed

- `clear`, `shuffle`, `remove` and `bump` now run through `discord_gateway.command_logic.music.MusicCommands` (#1030, Phase 1, step 4b). The player-side follow-up (dropping staged copies of tracks that left the queue, refreshing the prefetch) is one new `PlayerGateway.queue_changed` call, implemented in-process by `CogPlayerGateway`. The cog keeps thin stubs. No behaviour change: `clear` still does not refresh the prefetch, the others still do.

## [3.2.14] - 2026-10-14

### Changed

- `music-stats`, `history`, `join`, `skip`, `stop` and `move-messages` now run through `discord_gateway.command_logic.music.MusicCommands` (#1030, Phase 1, step 4a). They reach the player only through a new `PlayerGateway` Protocol (`status`, `ensure`, `skip`, `stop`, `move_messages`) that the cog implements in-process as `CogPlayerGateway`, and the command envelope gains the author's voice-channel guild. The cog keeps thin stubs. No behaviour change.

## [3.2.13] - 2026-10-14

### Changed

- The `!markov` subcommands (`on`, `off`, `list-channels`, `speak`) now run through `discord_gateway.command_logic.markov.MarkovCommands` (#1030, Phase 1). The cog keeps thin stubs, the `markov` group's invalid-subcommand reply, and the Discord channel-type check that `markov on` asks for. No behaviour change.

## [3.2.12] - 2026-10-14

### Changed

- The store-only `!playlist` commands (`create`, `list`, `item-remove`, `show`, `delete`, `rename`, `merge`) now run through `discord_gateway.command_logic.playlist.PlaylistCommands`, and the lookups the player-bound playlist commands share with them (`get_playlist`, `create`, the public-index view, the database-enabled check) moved with them (#1030, Phase 1). The cog keeps thin stubs. No behaviour change.

## [3.2.11] - 2026-10-13

### Changed

- The `urban` and `general` commands now run through a discord-free `discord_gateway.command_logic` package (#1030, Phase 1). The cogs are thin stubs that build a `CommandEnvelope` from the discord.py `Context` and delegate; a test fails if anything under `command_logic` imports `discord` or `discord_gateway.cogs`. No behaviour change.

## [3.2.10] - 2026-10-13

### Changed

- Bumped the `discord-core` pin to 3.4.3. `HttpPlaylistStore` and `HttpGuildAnalyticsStore` drop `record_history_item`, `get_playlist_size` and `record_play`: nothing in the gateway calls them since the queue moved to the broker (#1048). The routes stay on the db pod; the broker's history worker is their only caller.

## [3.2.9] - 2026-10-12

### Changed

- A resumed session no longer loses its queue when the voice handshake is slow: the player waits for the voice client to be connected, not merely present, before playing. Previously `play()` hit "Not connected to voice", the player was torn down, and the broker queue was closed.

## [3.2.8] - 2026-10-12

### Changed

- The music loops wait up to 120s at startup for the broker, downloader and search pods to accept connections before they start, so a gateway that comes up first no longer opens with a burst of connection errors.

## [3.2.7] - 2026-10-12

### Changed

- Bumped tnoff/dappertable to v1.1.10

## [3.2.6] - 2026-10-11

### Changed

- `/ready` now also probes the broker, downloader and search pods the music cog calls on every loop tick (alongside the dispatcher and db), each reported under its own name and `peer_ready_check` target, so a gateway that starts before one of them reads as NotReady instead of logging connection errors from its loops.

## [3.2.5] - 2026-10-11

### Changed

- Tests only: a sweep that stops the player at every point on the broker path of a track boundary and checks the next gateway loses nothing, replays no finished track, and keeps the queue order (#1048).

## [3.2.4] - 2026-10-11

### Changed

- Tests only: stop building `PlayerSession` with the removed `queue` field (core 3.3.0, #1048).

## [3.2.3] - 2026-10-10

### Changed

- Save the player session before stopping the player loop on shutdown: stopping the loop clears the track it was playing, so the session was being written with was_playing=False mid-track. Resume decides by what is queued, so nothing broke, but the stored state was wrong. Also bring the music docs (flow, terminology, messaging, music) in line with the broker-owned queue: the player claims from the broker instead of keeping its own `_play_queue`, the broker renders the play-order message, and history is kept by the broker (#1048).

## [3.2.2] - 2026-10-10

### Changed

- Fix a graceful restart still losing the track it interrupted, and a duplicate cleanup. #1059 marked the player as shut down before disconnecting voice, but the loop runner re-enters the player loop as soon as it returns, so the player went on to claim another track; the broker keeps one now-playing marker per guild, so that hid the interrupted track and the resume recovered the wrong one. Marking the player shut down also made the cleanup_players loop start a second cleanup with the default queue_timeout reason, which closes the guild's queue. Cleanup now calls MusicPlayer.stop_loop() before the voice disconnect, which cancels the loop and waits for it, and a stopped player refuses to claim (#1048).

## [3.2.1] - 2026-10-10

### Changed

- Fix a graceful restart finishing the track it interrupted: shutting down disconnects voice, which fires the same callback a finished track does, so the player told the broker the track had played out. It was recorded in the guild's history and analytics and the next gateway had nothing to recover. Cleanup now marks the player as stopping before it disconnects voice, and a stopped player leaves its track with the broker, so the track is recovered at the head of the queue on resume (#1048).

## [3.2.0] - 2026-10-10

### Changed

- The gateway plays from the broker's guild queue instead of keeping its own: the player claims each track from the broker, heartbeats while it plays and reports how it ended, and the queue, history, now-playing record and play-order message are the broker's (#1048). A graceful restart now keeps the queue and picks it up again on the next start (waiting out the old gateway's heartbeat and recovering the track it was playing), instead of clearing it and replaying the saved session; a session is saved when the player joins voice so a crash can resume too, and a session that cannot be resumed closes the guild's queue. Plays are recorded by the broker's history worker, so the gateway's own recording loop and history-playlist types are removed; the history playlist is still created when a guild's player starts.

## [3.1.11] - 2026-10-09

### Changed

- Rename the InMemory*Client test fakes (broker, download, queue worker, YouTube Music search) to Asyncio*Client, matching the Asyncio* engines and workers; the app is HA-only and none of them is a runtime mode. Docs and docstrings follow.

## [3.1.10] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.2.0.

## [3.1.9] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.1.0.

## [3.1.8] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.5.

## [3.1.7] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.4.

## [3.1.6] - 2026-10-08

### Changed

- Prefetch is back: the player downloads the next queued tracks from S3 itself, reuses them at playback, and deletes staged files once played or removed from the queue (#1036).

## [3.1.5] - 2026-10-08

### Changed

- Finish the checkout cleanup: no guild_path on checkout, a miss answers {}, BrokerEntry drops guild_file_path, and InMemoryMediaSearchClient is now LocalMediaSearchClient (#1036).

## [3.1.4] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.3.

## [3.1.3] - 2026-10-08

### Changed

- Drop the TYPE_CHECKING import guards: Bot-facing helpers move to the gateway/dispatcher-only modules, and shared code types its injected objects with Protocols (#1036).

## [3.1.2] - 2026-10-08

### Changed

- Drop the single-process leftovers from the broker seam: checkout always answers with an s3_key, and stale comments are cleaned up (#1036).

## [3.1.1] - 2026-10-06

### Changed

- Bumped tnoff/dappertable to v1.1.9

