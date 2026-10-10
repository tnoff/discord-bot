# Changelog

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

