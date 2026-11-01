# Changelog

## [3.19.1] - 2026-11-01

### Changed

- Docs only: `interfaces/player_gateway` says the commands pod implements the protocol over the broker, not that it would (#1030).

## [3.19.0] - 2026-11-01

### Changed

- `clients/http_markov_store` and `clients/http_guild_analytics_store` moved to `discord_commands/clients/` (#1030): since the markov loops left the gateway the commands pod is the only image that reaches them. Nothing outside the commands pod imports them, so no pod needs a pin bump; the db routes and the store Protocols stay here.

## [3.18.0] - 2026-10-31

### Changed

- The guild-roles routes are part of `dispatch.ALL` (#1030): the dispatcher has served them since #1166, so every dispatch client's peer route check now demands them of it. `routes/guild_roles` comes off `SHIPS_AHEAD_OF_CONSUMER`.

## [3.17.0] - 2026-10-30

### Changed

- Added the guild-roles contract (#1030): `routes/guild_roles.py` and `types/guild_roles.py`, the four dispatch fetches the commands pod will use for `role` (roles, one member, a role's members, add or remove a role). Declared ahead of the dispatcher that serves them, outside `dispatch.ALL`.

## [3.16.0] - 2026-10-30

### Changed

- `clients/http_download_client`, `http_media_search_client`, `http_queue_worker_client`, `http_youtube_music_search_client`, `search_client`, `types/playlist_add_result`, `utils/media_enqueuer` and `utils/playlist_view` moved to `discord_commands` (#1030): the commands pod is the only image that reaches them since the gateway dropped its downloader and search clients. Nothing outside the commands pod imported them; the routes, protocols and types the downloader and search pods serve stay here.

## [3.15.0] - 2026-10-29

### Changed

- `routes/commands.py` `ALL` now includes the guild-teardown route (`routes/guild_teardown.py`), so it is `HttpCommandsClient.ROUTES_CALLED` and the gateway's peer route check demands it of the commands pod, which has served it since #1160. Nothing is added to the wire. Any consumer must run against a commands pod from #1160 or later, or its route check reports the route missing.

## [3.14.1] - 2026-10-29

### Changed

- `RedisPingHealthServer` (the dispatcher, broker, downloader and search health endpoint) no longer pings Redis on `/health`. Liveness now fails only for a stalled background loop; the Redis `PING` moved to `/ready`, `/readyz` and `/readiness` and is bounded to 0.8 s. A Redis outage longer than roughly 60 to 90 seconds used to fail three liveness probes in a row and restart all four pods for a fault a restart cannot fix (on 2026-10-28 the search, downloader, dispatcher and broker pods were killed that way), and a ping against a Service with no ready endpoints hangs rather than refuses, so ignoring the result was not enough. A pod picks this up when its `discord_core` pin is bumped to this release.

## [3.14.0] - 2026-10-29

### Changed

- Add the guild-teardown contract (#1030, gateway cleanup): `routes/guild_teardown.py` (`POST /guilds/{guild_id}/teardown`), `GuildTeardownAccepted`, `CommandsClient.teardown_guild` and `HttpCommandsClient.teardown_guild`. The route is deliberately not in `routes/commands.py`'s `ALL` yet, so the gateway's peer route check does not demand it of a commands pod that predates it; it is folded in once the pod serves it. Nothing calls it yet.

## [3.13.0] - 2026-10-28

### Changed

- `playlist.queue` takes the rest of the line as `args` (#1030): `PlaylistQueueArgs` replaces `PlaylistIndexArgs` for it, so the commands pod can read `shuffle` and a count.

## [3.12.1] - 2026-10-28

### Changed

- `async_retry_broker_command` now retries a refused or reset connection (`ClientOSError`) with capped backoff for up to 60s instead of 3 times over about 7s, so a callee that is rolling (the db pod is `Recreate`) no longer drops the request. Untraced pollers and every other error keep the existing retry.

## [3.12.0] - 2026-10-27

### Changed

- The pieces of the music commands that the gateway and the commands pod both need now live in core (#1030, Phase 2), moved from `discord_gateway` rather than copied: the HTTP clients `HttpDownloadClient`, `HttpQueueWorkerClient`, `HttpMediaSearchClient`, `HttpYoutubeMusicSearchClient` (was `youtube_music_search_client.py`) and `HttpGuildAnalyticsStore`; `SearchClient` and `check_youtube_video`; the `PlayerGateway` Protocol with `PlayerStatus` and `EnsureResult` (`interfaces/player_gateway.py`); `PlaylistAddResult`; and `MediaEnqueuer` (`utils/media_enqueuer.py`). The broker half of queueing a downloaded track moves out of the gateway's `MusicResultsLoop` into `TrackQueuer` (same module; it needs only the broker, so a pod with no player of its own can use it), and `MediaEnqueuer` delegates to it. Its `on_queued` callback is now awaited, so it can be async.

## [3.11.0] - 2026-10-25

### Changed

- Add `HttpPlaylistStore` and `playlist_public_view` to core so the commands pod can serve the playlist commands that only touch the playlist store (the gateway keeps its own copies until it pins this release).

## [3.10.0] - 2026-10-25

### Changed

- Add `HttpMarkovStore` to core so the commands pod can build it (the gateway keeps its own copy until it pins this release), and give `markov.on` an argument, `channel_is_text_or_voice`, so the commands pod does not need a Discord fetch to validate the channel. Callers of `markov.on` must now send that argument.

## [3.9.1] - 2026-10-25

### Changed

- Bumped boto3 to v1.43.114

## [3.9.0] - 2026-10-23

### Changed

- `return_loop_runner` takes an optional `peer_unavailable` predicate, and `LoopHealth` gains `record_peer_unavailable()`. A matching failure (`is_peer_unavailable`: connection errors, timeouts, an open circuit breaker, 502/503/504) is logged as a warning and re-arms the staleness window, so a neighbour pod restarting for longer than `stale_after_seconds` no longer fails this process's `/health` and gets it restarted for an outage it cannot fix. It is not a success: `seconds_since_success` and the error count keep rising, and a loop failing for any other reason still goes stale. Opt-in; no existing caller changes.

## [3.8.0] - 2026-10-22

### Changed

- Fold the commands route into the peer route check (#1030, Phase 2): `HttpCommandsClient.ROUTES_CALLED` is now `routes/commands.py`'s `ALL`, so the gateway verifies the commands pod advertises `POST /commands/{command}`. The pod has served it since #1116; the gateway starts building the client in the change after this one.

## [3.7.0] - 2026-10-21

### Changed

- Add the commands contract to `discord_core` (#1030, Phase 2): how the gateway hands a parsed command to the commands pod. `routes/commands.py` declares `POST /commands/{command}`; `types/command_envelope.py` moves `CommandEnvelope` here from the gateway (with a wire body); `types/command_args.py` has an argument model per command and `COMMAND_ARGS`, the 36 commands the pod will serve; `types/command_wire.py` has the request and 202 response bodies and `parse_args`; `interfaces/commands_client.py` is the `CommandsClient` protocol and `CommandsUnavailable`; `clients/http_commands_client.py` forwards with one 3s attempt behind a circuit breaker and never retries, since commands are not idempotent. Like the intents contract it ships first: the route is in no registry or `ROUTES_CALLED` yet, because pods pin a released core.

## [3.6.1] - 2026-10-21

### Changed

- Bumped boto3 to v1.43.113

## [3.6.0] - 2026-10-20

### Changed

- Changed the supported Python floor to 3.13 (3.11 and 3.12 dropped): tox now runs py313-py315 with pylint and bandit on 3.15, and requires-python is >=3.13 (tox and Dockerfiles are repo-wide); renamed locals called `sentinel` in discord_core.clients.redis_client, which shadow the sentinel builtin new in Python 3.15

## [3.5.1] - 2026-10-19

### Changed

- Fold the gateway-intent routes into the broker seam (#1030, Phase 2): `routes/broker.py`'s `ALL` now includes `routes/gateway_intents.py`, so `HttpBrokerClient.ROUTES_CALLED` and the peer route check cover them. The broker has served them since #1098.

## [3.5.0] - 2026-10-19

### Changed

- Add the gateway-intent contract to `discord_core` (#1030, Phase 2): the broker-hosted way for another pod (the commands pod) to ask a gateway to do something only a gateway can, without a gateway API and without a pod-to-gateway call. `routes/gateway_intents.py` declares four routes (submit per guild, claim per gateway, complete and read the result per intent); `types/gateway_intents.py` has `IntentKind` (`join`, `stop`, `move_messages`, `wake`), `GatewayIntent` and `IntentResult`; `interfaces/gateway_intent_client.py` is the `GatewayIntentClient` protocol, which `BrokerClient` now includes; `clients/http_gateway_intents.py` is the HTTP mixin on `HttpBrokerClient`. The empty claim (204) mints no span and skips the retry wrapper, like `poll_guild_queue`. Like the guild-queue contract it ships first: the routes are not in the broker seam's registry or `ROUTES_CALLED` yet, because broker images pin a released core. The change that serves them, and bumps the pins, folds the registry in.

## [3.4.3] - 2026-10-12

### Changed

- Bumped opentelemetry-sdk to v1.45.1

## [3.4.2] - 2026-10-12

### Changed

- fix(deps): update cloud sdks

## [3.4.1] - 2026-10-12

### Changed

- Bumped pydantic to v2.14.1

## [3.4.0] - 2026-10-11

### Changed

- `PlaylistStore` and `GuildAnalyticsStore` no longer declare `record_history_item`, `get_playlist_size` and `record_play`: those moved to new `DbPlaylistStore` and `DbGuildAnalyticsStore` extensions, which are what the db pod serves. The gateway stopped recording plays when the queue moved to the broker (#1048), so its stores no longer need to carry them; the next gateway release can delete them. No route, wire or behaviour change.

## [3.3.0] - 2026-10-11

### Changed

- Remove `PlayerSession.queue` and its stale docstring. Nothing writes it since the queue moved to the broker (#1048); sessions already stored with a `queue` key still load, the key is ignored. Pods pinned to an earlier core keep working against this one in either direction.

## [3.2.1] - 2026-10-09

### Changed

- Rename the InMemory*Client test fakes (broker, download, queue worker, YouTube Music search) to Asyncio*Client, matching the Asyncio* engines and workers; the app is HA-only and none of them is a runtime mode. Docs and docstrings follow.

## [3.2.0] - 2026-10-09

### Changed

- Guild-queue contract: open_guild now takes the text channel for the play-order message (calling it again with another channel moves it) and returns the uuid of a track the previous owner started and never finished, requeued; GuildQueueSnapshot and GuildQueueResponse carry text_channel_id. The guild-queue routes join routes/broker.py's ALL and HttpBrokerClient.ROUTES_CALLED, so the peer route check covers them (#1048).

## [3.1.0] - 2026-10-09

### Changed

- Add the guild-queue contract to the broker seam: 14 routes under /guilds/{guild_id}, their response models, the GuildQueueClient protocol (part of BrokerClient) and HttpGuildQueueMixin on HttpBrokerClient. No broker serves the routes yet; the broker pod and the pin bump follow (#1048).

## [3.0.5] - 2026-10-08

### Changed

- Remove the unused broker prefetch (POST /prefetch, prefetch on the broker engine and client); prefetch is done by the player (#1036).

## [3.0.4] - 2026-10-08

### Changed

- Finish the checkout cleanup: no guild_path on checkout, a miss answers {}, BrokerEntry drops guild_file_path, and InMemoryMediaSearchClient is now LocalMediaSearchClient (#1036).

## [3.0.3] - 2026-10-08

### Changed

- Drop the TYPE_CHECKING import guards: Bot-facing helpers move to the gateway/dispatcher-only modules, and shared code types its injected objects with Protocols (#1036).

## [3.0.2] - 2026-10-08

### Changed

- Drop the single-process leftovers from the broker seam: checkout always answers with an s3_key, and stale comments are cleaned up (#1036).

