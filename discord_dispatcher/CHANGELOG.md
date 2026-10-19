# Changelog

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

