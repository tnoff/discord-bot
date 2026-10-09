# Changelog

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

