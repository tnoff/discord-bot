# Changelog
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

