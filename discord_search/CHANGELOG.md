# Changelog

## [3.1.0] - 2026-10-20

### Changed

- Changed the supported Python floor to 3.13 (3.11 and 3.12 dropped): tox now runs py313-py315 with pylint and bandit on 3.15, and requires-python is >=3.13 (tox and Dockerfiles are repo-wide)

## [3.0.14] - 2026-10-20

### Changed

- Bump the discord_core pin to 3.5.1 (#1030)

## [3.0.13] - 2026-10-19

### Changed

- Bump the discord_core pin from core-v3.4.3 to core-v3.5.0, which adds the gateway-intent contract (#1030, Phase 2). Nothing in this pod uses it yet.

## [3.0.12] - 2026-10-13

### Changed

- Bumped the `discord-core` pin to 3.4.3.

## [3.0.11] - 2026-10-12

### Changed

- fix(deps): update cloud sdks

## [3.0.10] - 2026-10-12

### Changed

- Bumped ytmusicapi to v1.12.3

## [3.0.9] - 2026-10-09

### Changed

- Rename the InMemory*Client test fakes (broker, download, queue worker, YouTube Music search) to Asyncio*Client, matching the Asyncio* engines and workers; the app is HA-only and none of them is a runtime mode. Docs and docstrings follow.

## [3.0.8] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.2.0.

## [3.0.7] - 2026-10-09

### Changed

- Bump the discord-core pin to core-v3.1.0.

## [3.0.6] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.5.

## [3.0.5] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.4.

## [3.0.4] - 2026-10-08

### Changed

- Finish the checkout cleanup: no guild_path on checkout, a miss answers {}, BrokerEntry drops guild_file_path, and InMemoryMediaSearchClient is now LocalMediaSearchClient (#1036).

## [3.0.3] - 2026-10-08

### Changed

- Bump the discord-core pin to core-v3.0.3.

## [3.0.2] - 2026-10-08

### Changed

- Drop the TYPE_CHECKING import guards: Bot-facing helpers move to the gateway/dispatcher-only modules, and shared code types its injected objects with Protocols (#1036).

## [3.0.1] - 2026-10-08

### Changed

- Drop stale single-process wording from comments (#1036).

