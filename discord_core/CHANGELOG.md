# Changelog

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

