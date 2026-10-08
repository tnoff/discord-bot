# Changelog

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

