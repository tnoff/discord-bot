# Changelog

## [3.1.6] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.11.0 (#1030): the playlist store and `playlist_public_view` are available to the commands pod.

## [3.1.5] - 2026-10-25

### Changed

- Bump the `discord_core` pin to core-v3.10.0 (#1030): the markov store and `markov.on` arguments are available to the commands pod.

## [3.1.4] - 2026-10-23

### Changed

- Bump the `discord_core` pin to core-v3.9.0 (#1030): loops can treat a peer outage as non-fatal to liveness.

## [3.1.3] - 2026-10-22

### Changed

- Bump the `discord_core` pin to core-v3.8.0 (#1030, Phase 2): the commands route is part of the peer route check.

## [3.1.2] - 2026-10-21

### Changed

- Bump the discord_core pin to 3.7.0, which adds the commands contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.1.1] - 2026-10-21

### Changed

- Delayed message deletes (`delete_after`) now survive a dispatcher restart: the dispatcher records each one in Redis and every pod sweeps due deletes, instead of leaving them to discord.py's in-memory timer, which lost the delete when the pod was replaced during the delay (#1115)

## [3.1.0] - 2026-10-20

### Changed

- Changed the supported Python floor to 3.13 (3.11 and 3.12 dropped): tox now runs py313-py315 with pylint and bandit on 3.15, and requires-python is >=3.13 (tox and Dockerfiles are repo-wide)

## [3.0.10] - 2026-10-20

### Changed

- Bump the discord_core pin to 3.5.1 (#1030)

## [3.0.9] - 2026-10-19

### Changed

- Bump the discord_core pin from core-v3.4.3 to core-v3.5.0, which adds the gateway-intent contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.0.8] - 2026-10-13

### Changed

- Bumped the `discord-core` pin to 3.4.3.

## [3.0.7] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.2.0.

## [3.0.6] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.1.0.

## [3.0.5] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.5.

## [3.0.4] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.4.

## [3.0.3] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.3.

## [3.0.2] - 2026-10-08

### Changed

- Drop the TYPE_CHECKING import guards: Bot-facing helpers move to the gateway/dispatcher-only modules, and shared code types its injected objects with Protocols (#1036).

## [3.0.1] - 2026-10-08

### Changed

- Drop stale single-process wording from comments (#1036).

