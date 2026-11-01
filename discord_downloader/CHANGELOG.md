# Changelog

## [3.3.12] - 2026-11-01

### Changed

- Reject downloads that decode to well under the duration yt-dlp reported (shorter than the larger of 3 s and 5 %), so a truncated file that ffmpeg opens cleanly is retried instead of cached; the source file is removed so the retry re-downloads rather than reusing it. Set `skip_unavailable_fragments: False` by default so a fragmented (HLS/DASH) download that cannot fetch a fragment fails and retries instead of finishing with the fragment missing; `extra_ytdlp_options` can override it. Log the format `bestaudio/best` actually chose (`format_id`, `acodec`, `abr`, `ext`, `protocol`) as a log line and `download.format.*` span attributes. Remove the MD5 comparison against `requested_downloads[0]['md5']`: yt-dlp never sets that key, so it never ran (#1170).

## [3.3.11] - 2026-11-01

### Changed

- Bump the `discord_core` pin to core-v3.18.0: the dispatch seam's route check now includes the guild-roles routes, so this pod expects the dispatcher to serve them (it has since #1166). The pin moves with the other six (tests/cli/test_package_pins.py requires them to agree).

## [3.3.10] - 2026-10-30

### Changed

- Bump the `discord_core` pin to core-v3.17.0, which adds the guild-roles contract (#1030). Nothing in this pod uses it yet; the pin moves with the other six (tests/cli/test_package_pins.py requires them to agree).

## [3.3.9] - 2026-10-30

### Changed

- Bump the `discord_core` pin to core-v3.15.0: no behavior change for this pod. The release only adds the guild-teardown route to the commands seam; the pin moves with the other six (tests/cli/test_package_pins.py requires them to agree).

## [3.3.8] - 2026-10-29

### Changed

- Bump the `discord_core` pin to core-v3.14.1: `/health` on this pod no longer pings Redis, so a Redis outage longer than roughly 60 to 90 seconds no longer fails three liveness probes in a row and restarts the pod for a fault a restart cannot fix. Liveness still fails for a stalled background loop (staleness window, 300 s by default), so a much longer outage can still restart it. The Redis `PING` moved to `/ready`, which nothing in Kubernetes probes today.

## [3.3.7] - 2026-10-29

### Changed

- Bump the `discord_core` pin to core-v3.14.0 (#1030): the guild-teardown contract (`HttpCommandsClient.teardown_guild`) is available to the commands pod and the gateway.

## [3.3.6] - 2026-10-28

### Changed

- Bump the `discord_core` pin to core-v3.13.0 (#1030): `playlist.queue` carries its shuffle and count arguments (`PlaylistQueueArgs`).

## [3.3.5] - 2026-10-27

### Changed

- Bump the `discord_core` pin to core-v3.12.0 (#1030): the shared music clients, `TrackQueuer` and the `PlayerGateway` Protocol are available to the commands pod.

## [3.3.4] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.11.0 (#1030): the playlist store and `playlist_public_view` are available to the commands pod.

## [3.3.3] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.10.0 (#1030): the markov store and `markov.on` arguments are available to the commands pod.

## [3.3.2] - 2026-10-25

### Changed

- Bumped yt-dlp to v2026.10.21

## [3.3.1] - 2026-10-23

### Changed

- Bump the `discord_core` pin to core-v3.9.0 (#1030): loops can treat a peer outage as non-fatal to liveness.

## [3.2.1] - 2026-10-22

### Changed

- Bump the `discord_core` pin to core-v3.8.0 (#1030, Phase 2): the commands route is part of the peer route check.

## [3.1.1] - 2026-10-21

### Changed

- Bump the discord_core pin to 3.7.0, which adds the commands contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.1.0] - 2026-10-20

### Changed

- Changed the supported Python floor to 3.13 (3.11 and 3.12 dropped): tox now runs py313-py315 with pylint and bandit on 3.15, and requires-python is >=3.13 (tox and Dockerfiles are repo-wide)

## [3.0.9] - 2026-10-20

### Changed

- Bump the discord_core pin to 3.5.1 (#1030)

## [3.0.8] - 2026-10-19

### Changed

- Bump the discord_core pin from core-v3.4.3 to core-v3.5.0, which adds the gateway-intent contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.0.7] - 2026-10-13

### Changed

- Bumped the `discord-core` pin to 3.4.3.

## [3.0.6] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.2.0.

## [3.0.5] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.1.0.

## [3.0.4] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.5.

## [3.0.3] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.4.

## [3.0.2] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.3.

## [3.0.1] - 2026-10-08

### Changed

- Drop stale single-process wording from comments (#1036).

