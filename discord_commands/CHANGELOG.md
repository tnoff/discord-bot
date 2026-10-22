# Changelog
## [0.2.0] - 2026-10-22

### Changed

- The pod now serves `hello`, `roll`, `meta` and `urban` (#1030, Phase 2): their logic moved here from the gateway, and the pod replies through the dispatcher, so `general.dispatch_http_url` is now required. It starts the peer route check for its dispatcher client and closes the client after the executor on shutdown. Adds `beautifulsoup4` and `dappertable`.

## [0.1.1] - 2026-10-22

### Changed

- Bump the `discord_core` pin to core-v3.8.0 (#1030, Phase 2): the commands route is part of the peer route check.

## [0.1.0] - 2026-10-21

### Changed

- New pod: `discord-commands`, the process that will run command logic for the gateway (#1030, Phase 2). This is the skeleton and serves nothing yet: `POST /commands/{command}` validates the envelope and arguments, drops a replayed `request_id`, queues the command on its guild's FIFO and answers 202, but no command has a handler, so every known command answers 501 and the gateway forwards nothing to it. Handlers arrive with the logic, one command group at a time.

