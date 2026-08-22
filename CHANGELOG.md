# Changelog

All notable changes to the Discord bot will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.5.93] - 2026-08-22

### Changed

- Fixed: the S3 video cache now sizes the PCM it actually stores, so `max_cache_size_mb` means what it says. `_create_source` stamps `file_size_bytes` from the file yt-dlp produced, then converts that file to raw PCM and swaps `file_name` to the `.pcm` — but the swap never updated the size, and the PCM is what `__upload_s3` uploads and what `VideoCache` sums for eviction. Since `utils/audio` writes s16le/48k stereo (192 kB/s) from a ~128 kbps source, every cache row under-reported by roughly 12x, and `video_cache_mark_deletion_for_size` evicted against a total an order of magnitude too small. The failure was silent and looked like success from every angle: eviction ran on schedule, no entry was ever stuck flagged, `file_size_bytes` was non-NULL on every row, and the tracked total sat exactly on the configured cap — while the bucket behind it held ~11.5x that. Observed in prod on 2026-08-21: `discord_data_cache` reported 1137 rows totalling exactly 5000 MB against a 5000 MB cap, while the bucket held 1155 objects and 57.2 GiB. Existing rows are deliberately left carrying their old compressed sizes: because the tracked total already sits at the cap, each newly-correct entry displaces ~12 stale rows and the bucket drains toward the cap on its own. Backfilling those rows instead would spike the tracked total to the real ~57 GiB and evict ~90% of the cache in one sweep, so it is the wrong move unless that is what you want.

## [2.5.92] - 2026-08-22

### Changed

- The music cog is now a broker **client** only, completing the dual-path collapse (`projects/discord-bot-ha-only`). `music.broker_client.url` is required config: a missing url is a startup error instead of a quiet switch to an in-process `AsyncioBroker`. That fallback was the most dangerous of the three — under HA it would have given the bot a private registry the downloader and search pods cannot see, so downloads would complete into a broker nobody checks out from and the symptom would be "audio never plays", with nothing in the logs. Gone with it: the cog-embedded `music.broker_server` and its `BrokerHttpServer` (the broker pod has served that surface since the 2026-07-01 cutover, via `general.broker_server`), and the cog's `VideoCacheClient` — the broker pod builds the cache from the same `music.download.cache` keys, so those keys in a **bot** config are now inert. No prod config change is needed: a bot without `music.broker_client` could not have been running HA in the first place.
- `MusicPlayer` now annotates its broker handle as the `BrokerClient` Protocol rather than the `MediaBrokerBase` engine, which is what it has actually been handed all along. The wrong annotation was pulling `interfaces/broker_protocols` — and through it `VideoCacheClient` and `integrations.s3` — into every process that imported the player. Three heavy modules leave the bot's import chain: `workers/asyncio_broker`, `servers/broker_server` and `clients/broker_client`.
- Two bot-side gauges were removed: `music.download_result_queue_depth` and `music.search_result_queue_depth` read a queue that only ever existed in single-process mode, so under HA the bot published a permanent `0` alongside the broker pod's real value for the same metric name. The broker's series (`background_job="broker"`) is the one to query — scope dashboard queries to `job="discord-broker"` rather than the bare metric name.
- No package leaves the bot image, and that is the answer to the open question the project recorded: `boto3` reaches the bot through `music_player`'s `integrations.s3` `get_file` — under HA the broker's checkout returns an s3_key and the *bot* fetches the file before playback — and `sqlalchemy` through `delete_messages`, markov and the playlist tables. Both are the bot's own dependencies, not the broker's.
- `music.download_client` is now required config and the cog is a download *client* only. The in-process branch — an `AsyncioDownloadWorker` (or the Redis-backed one) behind an `InMemoryDownloadClient`, driven by the cog's own `download_files` loops — is gone, along with `max_concurrent_downloads`, which only ever sized those loops, and the `download_files` heartbeat the bot used to register. The downloader pod has owned that loop in production since the cutover.
- **`yt_dlp` leaves the bot's import chain**, which is what the collapse was for — measured, not assumed: importing `discord_bot.cli.bot` goes from `boto3 bs4 dappertable googleapiclient spotipy sqlalchemy yt_dlp` to the same set without `yt_dlp`. (`moviepy` was already absent at import time — `utils/audio` reaches it lazily — but it joins the forbidden list too, since the bot now has no path to it at all.) Two light modules were split out to get there, both following the pattern `BrokerClient` established in !213: `interfaces/download_client_protocol.py` (the `DownloadClient` Protocol plus `RETRY_BACKOFF_SECONDS_MINIMUM`, which the cog's config model defaults to) and `clients/http_download_client.py` (`HttpDownloadClient` alone). Both are re-exported from their old homes, so existing imports keep working. Without those splits the cog kept pulling the whole download engine in to annotate one attribute and read one integer.
- The bot image's import boundary in `tests/cli/test_import_boundaries.py` tightens accordingly: `yt_dlp` and `moviepy` join `ytmusicapi` on its forbidden list. `spotipy`, `googleapiclient` and `bs4` stay — the cog still builds the `SearchClient` source-expansion member, and that is media_search's to move.
- As with the search tier, a music section that fails to validate is fatal rather than a silent cog skip, and `tests.helpers.attach_in_process_download` rebuilds the in-process stack for the tests that exercise the real worker.

## [2.5.91] - 2026-08-21

### Changed

- The search pod's idle poll no longer takes the pop-lock. `RedisYoutubeMusicSearchWorker.get_input_nowait` now checks the round-robin ZSET with a lock-free `ZCARD` first, mirroring `RedisDownloadWorker._pool_is_empty`, which exists on the download side for the same reason. The loop polls every 250 ms whether or not work is queued, and an idle poll used to spend a `SET NX` + `GET` + `DEL` lock cycle plus a `ZRANGE` just to have `_round_robin_pop` discover an empty ZSET — four round-trips to learn nothing, four times a second.
- Measured against production before the change: 23 redis commands/s from a single pod against roughly 0.03 searches/s, or about 830 commands per search. Twenty of those 23 were the idle poll; the rest is the bot's 1 Hz status poller. The pre-check takes the idle path to one `ZCARD`, leaving the `GET` that refreshes the shared 429 backoff window as the only other per-iteration cost.
- The count is only trusted in one direction. An empty ZSET means `_round_robin_pop`'s `ZRANGE` would also have found nothing, so skipping is safe; a non-empty ZSET still takes the lock and re-checks under it, because a guild can sit in the tracker with an already-drained queue. A request arriving immediately after the check waits for the next poll, exactly as it did before.

## [2.5.90] - 2026-08-20

### Changed

- Span filtering leaves this codebase. `FilterOKRetrySpans` and the `monitoring.otlp.filter_high_volume_spans` / `high_volume_span_patterns` keys that drove it are gone; the OTLP setup now attaches the `BatchSpanProcessor` unconditionally. The filtering moved to the otel-collector, as the `filter/drop-ok-high-volume-spans` processor in `monitoring/collector/config.yaml` in `docker-apps`. Both keys are simply ignored if a deployed config still carries them, which is what lets the collector-side filter land first rather than forcing the two repos to deploy together.
- Filtering here had two problems. It was configured per service, so five ConfigMap blocks drifted apart and each only took effect on a pod roll. More importantly it could not reach the spans that actually needed filtering: the redis auto-instrumentation emits a CLIENT span per command, mostly from background poll loops running outside any request context, and those turned out to be 99.5% of `discord-search`'s span volume — each one landing in Tempo as its own single-span root trace, burying the instrumented spans at roughly one in eight hundred. A name-based filter could never take them safely, because redis names its spans after the bare command (`GET`, `SET`, `DEL`) and `RequestsInstrumentor` names HTTP client spans the same way. The collector matches on the `db.system` attribute instead, which tells them apart.

## [2.5.89] - 2026-08-20

### Changed

- `music.youtube_music_search_client` is now required config, and the music cog is a search *client* only. The in-process branch — an `AsyncioYoutubeMusicSearchWorker` (or Redis-backed worker) behind an `InMemoryYoutubeMusicSearchClient`, driven by the cog's own `search_youtube_music` loop — is gone, along with the loop, its heartbeat registration and the `YoutubeMusicSearchDriver` the cog used to build. The search pod has owned that loop in production since the cutover; the cog kept a second copy of the wiring that nothing could reach.
- **`ytmusicapi` leaves the `[bot]` extra.** It was pinned there because the cog built a real `YoutubeMusicClient` whenever the search url was absent, which is what `discord-bot!209` ran into when it tried to drop the package. With no in-process branch there is no client to build: `ytmusicapi` now lives only in `[search]`, and the test suite installs `bot,search,test`.
- **A bad music config is now fatal rather than a silent skip.** `load_cogs` treats `CogMissingRequiredArg` as "this cog opts out" and moves on with a debug line — correct for `include.music: false`, and dangerous for a music section that is present and fails to validate, since a fat-fingered client url would have started a bot with no music at all. The enabled-check runs before config validation, and a validation failure now raises `DiscordBotException`, which nothing catches.
- The `InMemory*` / `Asyncio*` search implementations still exist, as test doubles: `tests.helpers.attach_in_process_search` rebuilds exactly the stack the cog used to build, so the driver's behaviour stays under test without standing up an aiohttp server. A test asserts the cog module no longer references them at all, since a reintroduced fallback is invisible until a pod runs the wrong half.
- The single-process deployment is retired. `discord-bot` now names the HA bot process (`cli/bot.py`) instead of the full one-process entrypoint, `cli/full.py` is deleted along with `docker/docker-compose.yml` and `docker/discord.cnf.example`, and the multiprocess compose stack is the only supported way to run the bot. `discord-bot-min` survives as a deprecated alias for the same entrypoint, because the deployed manifests still set `DISCORD_BOT_CMD=discord-bot-min` and dropping the name before they flip would CrashLoop the bot pod on exec.
- Nothing builds an in-process `MessageDispatcher` any more: `cli/bot.py` requires `dispatch_http_url` and refuses to start without it, so an unset value is a startup error rather than a quiet fall back to single-process mode. The music cog's in-process broker, download and search fallbacks still exist in the code — those come out separately, once nothing can construct them from a deployed configuration.

## [2.5.88] - 2026-08-20

### Changed

- Fixed: a blocked or full guild queue no longer fails the whole command. `submit()` raising `PutsBlocked` or `QueueFull` is normal control flow that `cogs/music.py` handles at every call site — delete the bundle, push `DISCARDED`, return quietly — but that contract did not survive the pod split. The worker pod's `_handle_submit` caught only body-parse errors, so a refusal escaped as an unhandled exception, aiohttp turned it into a 500, `async_retry_broker_command` retried it three times with exponential backoff, and the bot received a `ClientResponseError` that `except PutsBlocked` could not match. A single `/play` against a blocked guild therefore spent ~7s retrying a deterministic refusal and then failed with an opaque Internal Server Error, leaving its bundle behind. The two queue-contract exceptions are now encoded as `409`/`429` by the worker pod and decoded back into the real exception type by the bot-side client, so the existing handlers work unchanged across the seam; being 4xx they also propagate on the first attempt instead of burning retries. Both submit spans stay `OK` for a refusal — it is a decision, not a fault, and marking it `ERROR` is what inflated the seam's error rate. This is one fix in the shared base class, so it covers the live download path and the YouTube-Music search path together; the search path is where it was observed (trace `fc7c6b0b`, 2026-08-17).

## [2.5.87] - 2026-08-19

### Changed

- The multiprocess compose stack no longer requires a Mullvad account to run. `docker compose --profile local up` starts `downloader-direct`, a downloader with no tunnel that egresses straight out of the compose network; `--profile vpn` keeps the prod shape (`gluetun` + `downloader-vpn`, a different Mullvad SOCKS5 exit per download). Previously gluetun was unconditional, and since it never reports healthy without a real WireGuard key — and the downloader is gated on `service_healthy` — a contributor without a Mullvad account could not bring the stack up at all.
- Both flavours answer to the `downloader-host` network alias, so `discord.bot.cnf` points at the same URL under either profile. The bot no longer declares `depends_on` on the downloader: a profile-less service cannot depend on a profiled one, and the bot already tolerates the downloader being absent or restarting — it polls the status endpoint and retries, which is the same path that logs "downloader status poller error" during a rollout.

## [2.5.86] - 2026-08-19

### Changed

- Changed: a video the bot declines is now reported as *rejected* rather than *failed*. Requests that end terminally because of the video itself — too long, banned, private, age restricted, unavailable, requested format missing — plus playlist adds refused as duplicates or over the size limit now render as `Media request rejected: "<name>"`, are counted in their own bundle counter (`47/48 media requests processed successfully, 1 rejected`, or `1 failed, 1 rejected` when a bundle has both), and are summarised under `Details for Rejected Requests` instead of `Error Details for Failed Downloads`. Downloads that genuinely broke (retry limit exhausted, no file produced, search rate limit) still read as failures. The classification rides along on the lifecycle status update, so it works the same in HA. `process_download_results` also leaves its consumer span `OK` for a rejection: those spans were the dominant benign source of `Consumer Span Error Rate` pages, and a declined video is a decision, not a fault.
- Each published image now has an enforced import boundary. `tests/cli/test_import_boundaries.py` asserts, per entrypoint and in a subprocess, the packages that image must never pull into `sys.modules` — because on a slim image an unexpected import is an ImportError at pod start, discovered on a rollout, not a red test. This replaces the two ad-hoc guards that covered only the search pod and the bot's ytmusicapi boundary, and adds a check that every published console script has a boundary, so a sixth image cannot silently ship without one.
- Writing the boundaries down found sqlalchemy reaching the downloader pod by two separate routes. `cli/downloader.py` was importing `HttpBrokerClient` from `clients/broker_client.py` — the heavy module the search pod had already stopped using — and `interfaces/download_protocols.py` was importing the `BrokerClient` Protocol from `interfaces/broker_protocols.py`, whose `MediaBrokerBase` annotations pull `VideoCacheClient` (sqlalchemy) and whose module pulls `integrations.s3` (boto3). The Protocol moves to `interfaces/broker_client_protocol.py`, re-exported from its old home — the same split, and the same reason, as `CheckoutResult` and `ClearGuildResult` before it. The downloader image no longer imports sqlalchemy or dappertable.

## [2.5.85] - 2026-08-19

### Changed

- Fixed: a successful download could still fail to register. `DownloadResult.ytdlp_data` carried yt-dlp's entire raw info dict, which is not JSON-safe — an HLS download attaches `FFmpegFixupM3u8PP` instances under `__postprocessors`, and `register_download_result` then died on `model_dump(mode='json')` with `PydanticSerializationError: Unable to serialize unknown type`. Because the media had already downloaded, this surfaced as a request stuck after a working download rather than as a download error. The field is now projected down to the six keys consumers actually read (`id`, `title`, `webpage_url`, `uploader`, `duration`, `extractor`) — the same shape the cache-hit path already builds by hand — which removes the whole class of failure rather than blacklisting the types yt-dlp happens to embed today, and drops the format list from every payload crossing HTTP and Redis.

## [2.5.84] - 2026-08-19

### Changed

- Reverted: the `socks5h://` -> `socks5://` change shipped in 2.5.83 was a no-op and never took effect in production. yt-dlp rewrites `socks5://` to `socks5h://` on every request (`clean_proxies`), so remote DNS was always on and the proxy behaved identically before and after. The real cause of the `HTTP Error 403: Forbidden` media fetches was unrelated to egress: YouTube began 403ing every format minted by the `android_vr` player client on 2026-08-17, and yt-dlp's removal of that client from its defaults is still unreleased. The resolver is back on `socks5h://` with the rewrite documented so this is not attempted again.
- Fixed: downloads now leave the Mullvad SOCKS5 exits over IPv4. The resolver used `socks5h://`, which let the relay resolve the destination and pick the address family; it answered over IPv6 and googlevideo returned `HTTP Error 403: Forbidden` on the media fetch from every exit in the pool, so exit rotation could not route around it. Switching to `socks5://` resolves locally on a pod that has no IPv6 address, so the relay is handed an IPv4 destination. Note that yt-dlp's `--force-ipv4` never applied here: `source_address` only constrains the hop to the relay.

## [2.5.83] - 2026-08-18

### Changed

- Added: the downloader pod now resolves and caches the public egress IP of each pool exit, so `egress.ip` is stamped on download spans in the socks5 pool modes instead of always reading `unknown`. Each exit is probed through its own yt-dlp client, i.e. over the exact transport its downloads use, so the recorded IP is the one the origin actually sees. A probe failure degrades that exit to `unknown` and never touches the download path.

## [2.5.82] - 2026-08-17

### Changed

- Bumped sqlalchemy to v2.0.52

## [2.5.81] - 2026-08-17

### Changed

- Fixed: the downloader and search pods now clear per-guild block keys left in Redis without an expiry when they start. Builds before the block carried a TTL wrote those keys and never read them back, so enforcing the block on submit turned every leftover into a permanent block — `!play` failed for the affected guild with `PutsBlocked` surfaced as an HTTP 500, and there was no unblock path to recover it. Keys that carry a TTL are left alone, so a teardown in progress keeps its block.
- `active_players` and `voice_clients_connected` now report an explicit `0` when there is nothing to count, instead of emitting no observations at all. Both callbacks yielded one observation per guild, so with no players or no voice connections the series vanished from Mimir entirely — which meant "bot up and idle" and "bot down" were indistinguishable, and the condition actually worth alerting on (a broker bundle still alive with no player behind it, exactly what the 2026-08-14 restart left behind) could not be expressed as a query at all. The zero only exists while the pod is running, which is precisely what separates the two cases. It carries no `guild` attribute because there is no guild to name, making it a distinct series from the per-guild ones, so consumers should aggregate with `sum()` rather than reading a single series; and because the per-guild series go stale rather than vanishing the instant a player is reaped, `sum()` can briefly count both a departing guild and the new zero, so any alert built on this wants a `for:` longer than the staleness window. This deliberately does not change the heartbeat gauges, which report nothing rather than a permanent `0` for the opposite and equally deliberate reason: a loop that does not run in a given deployment mode would otherwise emit a standing zero and trip the stalled-loop alert.

## [2.5.80] - 2026-08-16

### Changed

- `block_guild` now actually blocks in HA. Both Redis workers wrote `guild:{gid}:blocked` and nothing ever read it, so a guild whose queue had just been cleared during teardown kept accepting submissions — the block was write-only, and the key it wrote had no expiry and no deleter, so it also accumulated one orphan per blocked guild forever. `submit` now consults the flag and raises `PutsBlocked`, which is what the cog already catches in all five of its submit call sites. The key is written with a 60s TTL rather than an explicit unblock, because that is what the in-process behaviour it mirrors actually does: `DistributedQueue.block` blocks a per-guild *queue object*, and that object is dropped whenever the guild's queue drains (`get_nowait`) or is cleared (`clear_queue`), so the next `put_nowait` builds a fresh unblocked queue and no unblock method exists anywhere to copy. A Redis key with no expiry would not reproduce that, it would invert it — the guild would stay blocked for the life of the data and never accept another request — so the flag only needs to outlive the teardown it guards. Only `submit` is gated, deliberately: the workers' own retry, no-exit-available and deferred-promotion paths re-enqueue work that was already accepted, they run on the consumer loop with no `PutsBlocked` handler above them, and raising there would take the loop down rather than shed a single request. The shared logic lives in `redis_guild_queue` as `RedisGuildBlockMixin` alongside the other primitives the two workers share, rather than being copied into both — the pair had already drifted into two identical no-ops, which is how this survived. Nothing changes in single-process mode.
- The bot now rejoins its voice channel and resumes playback after a restart. On `BOT_SHUTDOWN` each guild's player writes a `PlayerSession` — voice channel, text channel, the current track plus everything queued behind it, and whether it was mid-track — and a one-shot startup task replays it once the gateway is ready. A session is consumed exactly once, dropped before the resume is attempted rather than after, so a resume that fails part-way through is not retried on the next restart against even staler state. Two guards decide whether a session is worth acting on: the bot must have actually been mid-track (a player merely parked in an empty voice channel is dropped), and the voice channel must still contain a non-bot member, since everyone leaving while the bot was down is the clearest available signal that nobody is waiting on the queue. There is deliberately no staleness cutoff: if listeners are still sitting in the channel, how long the bot was away does not make the queue less wanted. Replay goes through the cog's ordinary enqueue path, so a track whose media is still cached returns instantly and anything evicted re-downloads exactly as a fresh request would; each request is minted fresh from the stored one's already-resolved search result rather than replayed as-is, because the stored object carries a terminal lifecycle stage and a uuid whose broker entry may still exist, and re-registering it would look finished the moment it arrived. One guild's bad session cannot stop the others from resuming. `MusicPlayer` now takes `bot` / `guild` / `text_channel` instead of a `Context` — those three attributes were all a `Context` was ever read for, and a player rebuilt from a stored session has no command behind it — and `get_player` accepts the same two as explicit stand-ins. `get_file_paths` is replaced by `queued_media_downloads`, which performs the same traversal without projecting it down to file paths; it had no callers outside its own tests.

## [2.5.78] - 2026-08-15

### Changed

- The broker can now store per-guild player sessions, the persistence half of resume-after-restart. A `PlayerSession` records a guild's voice channel, text channel, ordered queue of media requests and whether it was mid-track, and the broker exposes it over `GET /sessions`, `PUT /sessions/{guild_id}` and `DELETE /sessions/{guild_id}`, backed by a Redis key with the same 24h TTL as the entries it references. The session carries the media requests themselves rather than broker entry uuids so that replaying one goes through the cog's ordinary enqueue path, reusing the cache-hit machinery every request already goes through instead of reconstructing `MediaDownload` objects out of registry rows; the queue field is the `AnyMediaRequest` discriminated union, so a `PlaylistAddRequest` survives the round-trip with its `playlist_id` rather than degrading to a bare `MediaRequest`. Nothing writes or reads sessions yet — this is the broker-side seam only, shipped ahead of the bot-side change because the two pods roll independently and the routes have to exist before anything calls them; for the same reason the client treats a 404 on any session route as "broker not upgraded yet" and degrades to "no sessions to resume" rather than failing a startup or aborting a shutdown. Session persistence lives in its own `PlayerSessionStore` / `PlayerSessionClient` interfaces and an `HttpPlayerSessionMixin` rather than being bolted onto the broker interfaces, because it is player state the broker merely hosts — in HA the bot holds no Redis connection of its own, so the broker pod is the only shared store it can reach. Single-process mode implements the same surface in memory, where it is inert by construction: the bot and its broker die together, so there is nothing to resume into and an empty session list is the honest answer.

## [2.5.77] - 2026-08-14

### Changed

- `cleanup(BOT_SHUTDOWN)` now parks the download and search queues instead of draining them, and leaves broker bundles standing. Draining was self-defeating in HA: `clear_guild_queue` drops a request from Redis, but the matching broker registry entry only leaves the `in_flight` zone when the follow-up `DISCARDED` push reaps it, and that loop is one round-trip per item — so when the pod's grace period expired part-way through, every remaining entry plus the bundle holding them was stranded until the 24h TTL, with no player and no queue left to reap them. A production restart on 2026-08-14 left `broker_entries{zone="in_flight"}=20`, `available=9` and `broker_bundles=1` pinned flat for over ten minutes against an empty download queue and no voice client; the cleanup that would have torn the bundle down was the last step in the function and never ran, because it sat behind the O(queue) discard loop it was competing with for the same 20 seconds. Parking removes the competition entirely: the downloader and search tiers outlive the bot pod, so they work the backlog on their own, every request reaches a terminal state that reaps its own entry, each bundle keeps rendering real progress and releases itself when its requests finish, and the media lands in the shared cache so a re-request after the restart is a cache hit. Shutdown is now O(1) in queue size rather than O(n). The play-order message is cleared on `BOT_SHUTDOWN` too — it was deliberately skipped, which stranded a queue listing in the channel describing a play queue no process owned any more — and the shutdown notice says the queue is cleared but queued downloads keep running. Non-shutdown cleanup reasons (`QUEUE_TIMEOUT`, `VOICE_INACTIVE`, `VOICE_DISCONNECT`) are unchanged: the guild is genuinely going away there, so its queues still drain, its bundles are still torn down, and its player dir is still removed.

## [2.5.76] - 2026-08-14

### Changed

- The standalone broker now passes `message_delete_after` to `RedisBroker`, so the messages it sends actually expire. `MediaBrokerBase` defaults the kwarg to `None`, and `discord_bot/cli/broker.py` never set it — only the in-process path (`cogs/music.py`) did — so in HA every broker-sent message went to Discord with no `delete_after` and stayed up permanently. The visible symptom was "Error Details for Failed Downloads": it goes out through a one-shot `send_message`, so once sent with no expiry nothing holds a reference to it and there is no key to `remove_mutable`, leaving failure summaries like `Media Request "X", Failure: Video is age restricted, cannot download` in the channel indefinitely. The same `None` also stopped the finished "Completed N/N" bundle summary from expiring, and inverted the dispatcher's `if delete_after is not None` branch so it kept re-saving those bundles instead of dropping them from its store. This was a silent regression: the fix that introduced `message_delete_after` wired the in-process broker and its unit tests, both of which pass, while the deployed HA path never received the value. Configurable as `music.general.message_delete_after` on the broker config (default 300, mirroring `MusicGeneralConfig`); a test pins the CLI's copy of that default to the pydantic model so the two cannot drift. Messages already sent without a `delete_after` are unreachable and need deleting by hand.

## [2.5.75] - 2026-08-14

### Changed

- Failed YouTube downloads now wait before becoming eligible again, doubling per attempt (`music.download.retry_backoff_seconds_minimum`, default 30s, capped at 300s; 0 restores the old immediate requeue). Pool mode replaced the pod-global backoff with a per-exit one, which rotates exits but paces a single request not at all — a retry re-queued instantly is popped within the poll interval and leases the next free exit. On 2026-08-13 that drained the whole 16-exit pool in ~45 seconds while seven distinct videos failed on every exit, which is a bot-check wave rather than anything per-request, and every request in flight exhausted its budget inside the wave instead of outliving it. The cost compounds: `_reserve_youtube_exit` locks each exit for `youtube_wait_period_minimum` (90s) whether the download succeeds or fails, so with 16 exits the pool tops out near 10 downloads/minute and instant retries spend that ceiling on attempts that cannot succeed yet, starving requests that would have. Deferred retries are held in a time-scored ZSET on the Redis worker (durable across pod restarts, claimed by ZREM so two pods can't promote the same request) and in memory on the asyncio worker, promoted once per consumer-loop iteration. They count toward a guild's queue size and are dropped by a queue clear, so a parked retry can neither look like a drained queue nor resurrect a cleared request. DIRECT (non-YouTube) requests are exempt and still retry immediately, matching how they bypass backoff everywhere else. The RETRY lifecycle update now reports this per-request hold-off as its `backoff_seconds` instead of the pod-global window.
- Worker pods now close their outbound broker session when they drain. Both the search and downloader pods hold an `HttpBrokerClient` for the life of the process and neither closed it, so aiohttp logged `Unclosed client session` at ERROR level on every single pod roll — observed in prod during the search cutover. Nothing leaked for long, since the process is exiting either way, but it put a recurring ERROR in the exact window an operator reads logs during a deploy, which is how a real shutdown failure gets missed. `worker_pod_main_loop` takes the client and closes it after draining the HTTP server and before closing Redis, so both pods get the fix from the one place they already share.

## [2.5.74] - 2026-08-13

### Changed

- The `attempt N/M` retry message now takes its M from the worker that owns the N. The count is reported by the downloader (or the search pod) and rendered by the broker, but the budget was read separately by the broker out of *its own* config file — a different file from the worker's, with `max_download_retries` defaulting to 3 in code if unset. Raising the downloader's budget to 5 without touching the broker's config therefore produced `Retrying "…" (attempt 4/3)` in prod on 2026-08-13: a live count against a stale budget. `LifecycleStatusUpdate` now carries `max_retries` alongside `retry_count`, the brokers persist it as `RetryInformation.retry_max`, and `get_retry_summary` prefers it over its own configured max. The broker-side config keys stay as the fallback for a request last touched by a worker that reports no budget, so nothing changes for a single-process deployment where both halves already came from one file.
- HA: the YouTube-Music search cutover (MR 6 of 6). Setting `music.youtube_music_search_client.url` now points the bot at the standalone search pod: the cog builds an `HttpYoutubeMusicSearchClient`, submits/clears/blocks over HTTP, and stops running the search loop in-process entirely — no worker, no queue, no driver, and no `youtube_music_search` LoopHealth registration on the bot. Resolutions come back through the broker's search-result queue, which `process_search_results` already consumes. Without the key nothing changes: the cog still builds the redis-backed or asyncio in-process worker exactly as before, so single-process and compose deployments are untouched. The key is `youtube_music_search_client`, not `search_client`, because the cog already has an unrelated `self.search_client` (the source-expansion member).
- Worker pods now publish their consumer loop's heartbeat. Both pods registered a `LoopHealth` — which is what makes a wedged loop 503 its own `/health` — but never exposed it as a metric, so the only `heartbeat` series a pod emitted came from its HTTP server and reported `is_serving`: the TCP site is up, not that the loop is turning. A wedged consumer read green in Mimir until the liveness probe restarted the pod, and the search loop would have lost its series entirely at this cutover rather than reappearing under the pod's job label. `cli/_lib/worker_pod.py` now registers the gauge where it already registers the health entry, so both pods get it and the next one gets it free. The download server's gauge is renamed `downloader` → `downloader_server` to match the search pod's existing convention, leaving `downloader_worker` / `downloader_server` and `youtube_music_search` / `youtube_music_search_server` as unambiguous pairs; any dashboard pointing at the old `downloader` series should move to `downloader_worker`, which is the signal it was always assumed to be showing.
- The HA bot no longer imports `ytmusicapi`. Every bot process loads the music cog through `cli/_lib/cog_registry.py`, and the cog imported `YoutubeMusicClient` at module scope, so the dependency the search tier exists to isolate was being pulled into the bot pod on every deployment. Three things carried it: a bare stdlib retry exception living in the ytmusicapi wrapper (now in `discord_bot.exceptions`, re-exported), an annotation-only import in the search worker base (now `TYPE_CHECKING`), and the cog's own client construction. `utils/integrations/youtube_music.py` is now a dependency-free boundary that resolves the real wrapper from `_youtube_music_impl` on first attribute access, so `cli/search.py` still loads it eagerly at pod start while the cog only pays for it in single-process mode. A subprocess test guards the chain.
- `ytmusicapi` still ships in the `[bot]` extra, though — that part of the MR 5 plan does not hold. Single-process mode (compose, local dev, and the test suite via tox's `extras = bot,test`) constructs a real client, so the package has to be installed even though the HA bot never touches it. Dropping it would save 1.1 MB of pure Python whose only dependency every image already has, and break single-process off a plain `[bot]` install.
- Fix: handled failures no longer report `OK` in tracing. `otel_span_wrapper` and `async_otel_span_wrapper` (`utils/otel.py`) stamped `span.set_status(StatusCode.OK)` unconditionally on the normal-exit path, immediately after the `yield` returns. Every caller that handles an error and *returns* rather than raises — the dominant pattern across the codebase, with 18 `set_status(StatusCode.ERROR)` sites in `download_protocols.py`, `cogs/music.py`, `workers/youtube_music_search_driver.py`, `utils/sql_retry.py`, `utils/discord_retry.py` and `utils/audio.py` — set `ERROR` and then exited the context manager cleanly, at which point the wrapper overwrote it. OTel treats `Ok` as final and lets it override `Error`, so the overwrite always won and the `ERROR` status never survived to the exporter: only a failure that propagated an exception all the way out of the `with` block was ever recorded as failed. The practical effect was that failed downloads were unfindable by span status in Tempo — a request that exhausted its retry budget and terminated showed `music.download_client.create_source` and `music.process_download_results` both green, which is how a prod terminal failure (`RETRY_LIMIT_EXCEEDED` after three exits returned 403 / a YouTube bot-check) came to leave no error-shaped trace at all. Both wrappers now route the normal-exit path through `_set_ok_unless_already_set`, which fills in `OK` only when the body left the status `UNSET` and otherwise leaves the caller's status alone; a non-recording span exposes no `status`, where `set_status` is a no-op anyway, so that path is unchanged. Nothing about the exception path moves, and spans that genuinely succeed still end `OK`.

## [2.5.73] - 2026-08-13

### Changed

- Downloader: stop paying full price to discover an empty queue. Since the pool-mode cutover the pod runs one driver per `worker_count` and each driver re-polls on the idle interval, so with four drivers at 250ms the pod issued ~128 redis commands a second while completely idle — and those polls were ~98% of its trace span volume, a flat span rate set by the poll interval rather than by anything happening (measured against prod: `ZRANGE`/`GET`/`SET`/`DEL` at ~30/sec each, versus ~0.003/sec for the spans that describe an actual download). Three changes. `_atomic_pop_direct` / `_atomic_pop_youtube` now check the round-robin ZSET lock-free (`_pool_is_empty`) before entering `_pop_lock`, so an idle poll costs one `ZCARD` per pool instead of a `SET NX` + `GET` + `DEL` lock cycle plus a `ZRANGE` — the pods stopped taking, and contending on, a distributed lock 32 times a second to learn there was nothing to do. The check only skips work `_round_robin_pop` would also have skipped (same ZSET, and a non-empty count still takes the lock and re-checks under it), and an empty pool now returns `None` rather than `('wait', ts)` in fixed http-proxy mode, which `_merged_get_nowait` already collapses to the same `QueueEmpty`. The idle peek itself moved behind `_peek_next_request`, which wraps `_merged_get_nowait` in OpenTelemetry's `suppress_instrumentation` so an empty poll emits no client spans at all; the suppression is scoped to the peek, so every span describing real work — `create_source`, `submit`, the audio/upload spans and the redis writes on the result path — is untouched, and the backoff branch's `_dequeue_direct` peeks stay instrumented since they only run while a backoff is active. Finally `_IDLE_POLL_BACKOFF_SECONDS` goes 0.25s to 1.0s, which also paces `create_source`'s `NO_EXIT_AVAILABLE` yield; together that takes idle redis traffic from ~128 to ~8 commands a second and idle spans to zero, at the cost of up to a second of pickup latency on a path where the download itself then takes seconds.

## [2.5.72] - 2026-08-12

### Changed

- HA: the YouTube-Music search subsystem gets its pod (MR 5 of 6). `discord-search` (`cli/search.py`, `docker/Dockerfile.search`) runs the search tier as its own process: it drains the shared Redis search queue, resolves each request through ytmusicapi, and hands the resolution back to the bot through the broker's search-result queue, while fronting the queue with the MR 4 HTTP server on `:8084` for submit/clear/block/status. The search loop body — pop, resolve, retry-or-fail, hand back — moved out of the music cog into `YoutubeMusicSearchDriver`, which the cog and the pod both drive, so the two halves of a seam that has already skewed twice in prod cannot drift apart. Nothing is wired up yet: the cog still runs the loop in-process against its own client until the MR 6 cutover. The image installs a new slim `[search]` extra (thin HTTP clients only — no yt-dlp/ffmpeg/deno, boto3 or sqlalchemy), which needed `HttpBrokerClient` split into `clients/http_broker_client.py` and `CheckoutResult` moved to `types/`, since importing the in-memory broker client drags the broker engine's dependencies along; the old import paths still work. The pod-side loop owns its `LoopHealth` and keeps the MR 3 backoff slicing, so a long 429 window still reads as progress rather than a wedge. The downloader and search entrypoints now share their pod scaffolding (`cli/_lib/worker_pod.py`): redis/broker validation, the health server, the guarded consumer loop and the drain.

## [2.5.71] - 2026-08-11

### Changed

- Bumped alembic to v1.19.1

## [2.5.70] - 2026-08-11

### Changed

- Bumped boto3 to v1.43.67

## [2.5.69] - 2026-08-11

### Changed

- Bumped ytmusicapi to v1.12.2

## [2.5.68] - 2026-08-11

### Changed

- Bumped setuptools to v84

## [2.5.67] - 2026-08-11

### Changed

- Fix: the dispatcher pod no longer drops accepted work when it shuts down. `cli/dispatcher.py` never called `drain_and_stop()` on its `DispatchHttpServer` — the broker (`cli/broker.py`) and downloader (`cli/downloader.py`) both do — so on SIGTERM the server kept listening on `:8082` right through `dispatcher.stop()` and `redis_manager.close()`, and its `serve()` task only ended when the event loop tore down under it. Two things fell out of that: in-flight requests died with the process instead of finishing, and a POST landing after the Redis handle closed got a `202` for work that was never queued, because the fire-and-forget entry points (`send_message`, `delete_message`, `update_mutable`, `remove_mutable`, `update_mutable_channel`) schedule their enqueue as a detached task whose failure never reaches the caller. `_on_shutdown` now drains the HTTP server first, so the pod stops accepting before the work queue goes away. Those detached enqueues are also tracked in a strong-referenced set and flushed at the top of `MessageDispatcher.stop()` while Redis is still open — previously the event loop held only a weak reference to each running task, so one could be garbage-collected mid-flight and `stop()` had nothing to wait on; work the caller already believed had succeeded was simply lost. A wedged enqueue is now reported as lost after `_ENQUEUE_DRAIN_TIMEOUT_SECONDS` rather than silently swallowed. Pairs with docker-apps!981, which raises the pods' termination grace periods above their own drain budgets — every one of the three was being SIGKILLed partway through the drain on each rolling update.

## [2.5.66] - 2026-08-08

### Changed

- HA: the YouTube-Music search subsystem gets its HTTP surface (MR 4 of 6). A search pod can now front its Redis queue with `YoutubeMusicSearchHttpServer` on `:8084` (`POST /search/ytmusic`, `/search/ytmusic/clear`, `/search/ytmusic/block`, `GET /search/ytmusic/status`), and bot pods talk to it through `HttpYoutubeMusicSearchClient` — submit/clear/block inline, with queue depth, failure summary and backoff seconds served from a cache a background poller refreshes. Nothing is wired up yet: the cog still runs its in-process search client until the MR 6 cutover. Because the downloader pod and the search pod are the same shape, the HTTP server handlers, the bot-side client, the in-memory client forwarding, and the gauge poller now live in shared bases (`QueueWorkerHttpServer`, `HttpQueueWorkerClient`, `InMemoryQueueWorkerClient`, `QueueMetricsBase`) that both tiers use. Two things fall out of that: clearing a guild's *search* queue now reports the bundle_uuids it preserved, so playlist-adds waiting on a search keep their bundles during cleanup the way download-queued ones already did; and each status poll carries its own timeout, so an unreachable worker pod can't park the refresh loop on aiohttp's five-minute default. New gauges `search_queue_depth`, `search_youtube_backoff_seconds` and `search_failure_count` report the search tier's own shared queue and 429 window. Routes sit under a per-provider segment on purpose: the search tier is meant to co-host the media_search providers (Spotify, the YouTube Data API) in the same slim image and pod — they are thin HTTP clients, not yt-dlp-sized dependencies — so `/search` stays the pod namespace and `/search/spotify` + `/search/youtube` stay free.

## [2.5.65] - 2026-08-07

### Changed

- Markov: recover from a deleted `last_message_id` again. The dispatcher flattens exceptions to a string when it ships a fetch result back to a cog, so the `isinstance(error, NotFound)` check guarding the clear-and-restart never matched and the channel re-requested a dead message every loop forever. Errors now cross the boundary with their status/code intact (`error_detail`), and the recovery matches on status — the channel clears its relations and rebuilds from the retention cutoff. `fetch_history`/`fetch_emojis` also carry `span_context` end to end, so result-consumer logs are traced and name the server as well as the channel.

## [2.5.64] - 2026-08-07

### Changed

- Bumped redis to v8.1.0

## [2.5.63] - 2026-08-06

### Changed

- Bumped alembic to v1.19.0

## [2.5.62] - 2026-08-06

### Changed

- Bumped boto3 to v1.43.65

## [2.5.61] - 2026-07-27

### Changed

- Bumped pytz to v2026.3.post1
- Music: add `music.download_client.url` to run downloads on a standalone downloader pod over HTTP (HA mode) — the cog builds an `HttpDownloadClient` and skips the in-process worker/loop; unset keeps the single-process in-memory downloader. `clear_guild_queue` now returns the preserved bundle_uuids so playlist-add bundles are not deleted during cleanup in HA.

## [2.5.60] - 2026-07-25

### Changed

- Bumped boto3 to v1.43.56
- CI: publish the `discord_downloader` image on the default branch via `push:image-downloader`, so the standalone downloader pod has an image to deploy (dormant until docker-apps consumes the pin).

## [2.5.59] - 2026-07-21

### Changed

- Bumped opentelemetry-sdk to v1.44.0

## [2.5.58] - 2026-07-21

### Changed

- Bumped boto3 to v1.43.51

## [2.5.57] - 2026-07-13

### Changed

- Bumped boto3 to v1.43.46

## [2.5.52] - 2026-07-06

### Changed

- Broker registry entry leak: `update_request_status` marked the state machine for a terminal `DISCARDED`/`FAILED` request but never deleted the entry — so every de-duplicated request (the download worker emits `DISCARDED` for each) sat in the `in_flight` zone until the 24h TTL, accumulating (observed: ~90 stale `in_flight` entries while idle). `update_request_status` now deletes the entry on `DISCARDED`/`FAILED` (Redis + in-memory brokers); the bundle keeps its own synced copy so the UI still shows the final state. `COMPLETED` is unchanged (the entry stays `available` for checkout).
- As a backstop, `in_flight` entries carry their own inactivity TTL (`IN_FLIGHT_TTL_SECONDS`), refreshed on every lifecycle update, so a request that stalls without ever reaching a terminal event (e.g. its download result never routes back) eventually expires. Held at the full 24h for now — a deep download queue can legitimately keep a request `in_flight` for a while before it's serviced, so we don't evict still-valid entries; this can be tuned down later once we have a feel for real queue depth. The terminal-event deletion above is what actually stops the leak.
- `broker.entries` now always reports the `in_flight` zone (added to the metric's known-zones) so it shows 0 rather than going absent when it drains.

## [2.5.51] - 2026-07-06

### Changed

- The broker now emits its Redis-backed state as metrics, so the broker is no longer a near-blind box. A background poller (`BrokerMetrics`, refreshed every 15s so a slow/absent Redis never blocks the metric export path) publishes:
- `broker.result_fetch` counter (`outcome="hit"|"empty"`) on `GET /results/next`, and `broker.ready_check` counter (`outcome="ok"|"unavailable"`) on each broker health probe — a flapping outcome is early warning of Redis trouble. `DownloadResultQueue` gained a `depth()` method (both the Redis and asyncio impls).

## [2.5.50] - 2026-07-06

### Changed

- The standalone broker process now emits a `heartbeat` gauge (`background_job="broker"`, `job="discord-broker"`) — `1` while its HTTP server is accepting requests, `0` while draining. Previously the broker emitted no liveness series at all, so a broker that was down or not yet accepting connections at startup was invisible on the Discord Health dashboard and surfaced only indirectly as the bot's `process_download_results` loop dying. `AiohttpServerBase` gained a `_serving` flag to back this.

## [2.5.49] - 2026-07-06

### Changed

- `RedisManager` can now connect through Redis Sentinel: set `general.redis_sentinel` (`sentinels: ["host:port"]` + `service_name`) and the client discovers the current primary via `master_for()`, so a Valkey failover is transparent to callers. `redis_url` still works for local/dev and takes second place when both are set. Pairs with the Valkey + Sentinel topology in docker-apps.

## [2.5.39] - 2026-06-16

### Changed

- Fixed duplicate "Processing" status messages on playlist requests: `MessageDispatcher._remove_mutable_redis` now acquires the same per-key execution lock as `_process_mutable_redis` (re-enqueueing on contention). Previously a `remove_mutable` issued during the search→enqueue status-message handoff could run concurrently with the in-flight create, load the bundle before it was persisted, delete nothing, and orphan the just-sent status message — leaving two "Processing" messages on screen.

## [2.5.21] - 2026-05-21

### Changed

- Suppressed `SelectableGroups dict interface is deprecated` `DeprecationWarning` from opentelemetry 1.42.0 on Python 3.11 so `tox -e py311` test collection no longer fails (Python 3.12+ unaffected)

## [2.5.3] - 2026-03-22

### Changed

- Moved all S3 file operations out of `VideoCacheClient` into `MediaBroker`; cache client now manages only DB records
- Added prefetch support to `MediaBroker` — pre-stages upcoming queue items from S3 to local disk ahead of playback
- Made S3 checkout and prefetch non-blocking: checkout runs via `asyncio.to_thread` and prefetch fires as a background task immediately after playback starts, eliminating the between-song gap caused by blocking S3 downloads
- Added `max_cache_size_mb` config option to enforce a disk size budget on the video cache; stored `file_size_bytes` per cache entry, with size-based eviction composing correctly with the existing count-based limit
- Added `storage_type` column (`'s3'` or `'local'`) to `VideoCache` to track which storage backend each cached entry was written under; stale entries from a previous storage config are detected on access — treated as a cache miss and marked for eviction rather than causing a failed file lookup
- Moved download backoff tracking and failure queue management from `music.py` into `DownloadClient`
- Added `PlaylistAddRequest` / `PlaylistAddResult` types to consolidate playlist-add handling
- Added `CleanupReason` type to unify player shutdown/cleanup paths
- Improved serialization of `MediaRequest` bundles and `DistributedQueue` items
- Made `MessageDispatcher` context bits more mutable and serializable; refactored dispatch logic
- Fixed log levels across music, markov, dispatcher, and utility modules
- Migrated remaining internal types (`DownloadStatus`, `CatalogResponse`) to Pydantic
- Moved ready-file and file-removal operations from `MusicPlayer` into `MediaBroker`
- Increased test coverage to 96%
- Moved KNOWN-ISSUES content into DEVELOPMENT.md
- Bumped yt-dlp from 2026.3.3 to 2026.3.17
- Bumped boto3 from 1.42.67 to 1.42.71
- Bumped google-api-python-client from 2.192.0 to 2.193.0
- Bumped croniter from 6.0.0 to 6.2.2
- Bumped tox from 4.49.1 to 4.50.0

## [2.5.2] - 2026-03-13

### Changed

- Added healthcheck server endpoint for container health monitoring
- Consolidated all Discord API calls into a single per-guild `MessageDispatcher` queue to reduce rate-limit contention
- Simplified message dispatch logic and removed partial function wrappers in dispatch calls
- Added regex support to the spam filter
- Set Spotipy token cache to in-memory to avoid writing credentials to disk
- Added OTel span filter to reduce high-volume trace noise
- Fixed async retry usage in role cog send messages
- Fixed cache directory creation for Discord user runtime
- Added `MediaBroker` — aggregate in-process lifecycle tracker for all media through three zones: `IN_FLIGHT` → `AVAILABLE` → `CHECKED_OUT`
- Added `MediaRequestStateMachine` for per-bundle state tracking and message update logic
- Added `SearchCollection` / `BundledMediaRequest` classes to better handle multi-track search inputs
- Separated `DownloadResult` from `DownloadClient` methods for a cleaner handoff to `MediaBroker`
- Moved most cache lookup/eviction operations into `MediaBroker`
- Made YouTube Music search the default path, removing conditional logic around it
- Fixed 429 throttling from YouTube Music API with retry and backoff
- Fixed race condition in music player cleanup
- Fixed bug in media request bundles when all items were already cached
- Fixed search bundle flow for multi-track inputs
- Fixed single message processing in dispatcher
- Fixed backoff minimum calculation bug and updated backoff multiplier
- Improved retry message display: show full error cause and retry count to users
- Removed older yt-dlp match generator (superseded by YouTube Music pre-check)
- Added better return validation for third-party search results
- Cleaned up YouTube Music search queue logic
- Updated post-processing function naming
- Extracted all dataclasses into a dedicated `discord_bot/types/` package (`search`, `download`, `catalog`, `media_request`, `media_download`, `history_playlist_item`)
- Removed Twitter/fxtwitter URL handling (no longer supported)
- Simplified logging logic and fixed logging levels
- Added additional OTel spans for high-volume operations; limited trace length
- Bumped discord.py from 2.6.4 to 2.7.1
- Bumped yt-dlp to 2026.3.3 (nightly build)
- Bumped dappertable to 1.0.0
- Bumped opentelemetry-sdk from 1.39.1 to 1.40.0
- Bumped spotipy from 2.25.2 to 2.26.0
- Bumped ytmusicapi from 1.11.4 to 1.11.5
- Bumped pytz from 2025.2 to 2026.1.post1
- Bumped sqlalchemy from 2.0.45 to 2.0.48
- Bumped alembic from 1.17.2 to 1.18.4
- Bumped google-api-python-client to 2.192.0
- Bumped psutil from 7.2.1 to 7.2.2
- Bumped pylint from 4.0.4 to 4.0.5
- Bumped boto3 to 1.42.67
- Bumped setuptools to 82.0.1
- Bumped tox to 4.49.1

## [2.5.1] - 2026-01-04

### Changed

- Added support for running as non-root user
- Added log level configuration for 3rd party libraries
- Fixed discord.py logger level configuration
- Fixed third party logging config
- Simplified init config options
- Added better typing to music classes
- Added logging to help diagnose extra character messages
- Fixed handling of exit exceptions gracefully
- Updated to Python 3.14
- Fleshed out retry logic in download client
- Added retryable exceptions to download client
- Simplified retry backoff implementation
- Fixed ytdlp build path configuration
- Fixed deno path in environment
- Updated to use nightly build of yt-dlp
- Updated to DapperTable v0.2.4
- Added lockfile fixes and additional tests
- Added text validation checks
- Sleep and asyncio updates
- Bumped pynacl from 1.6.1 to 1.6.2
- Bumped boto3 from 1.42.12 to 1.42.20
- Bumped psutil from 7.1.3 to 7.2.1
- Bumped pydantic from 2.10.6 to 2.12.5
- Bumped pydantic-yaml from 1.5.0 to 1.6.0
- Bumped ytmusicapi from 1.11.3 to 1.11.4

## [2.5.0] - 2025-12-17

### Changed

- **Migration to Pydantic v2**: Replaced jsonschema with Pydantic v2 for configuration validation
- **Discord IDs now integers**: Changed all Discord IDs (guild, channel, role, user, message) from strings to integers
- Refactored media request bundle to use dataclass instead of dictionaries for better type safety
- Added `BundledMediaRequest` dataclass for cleaner request tracking
- Added comprehensive type hints to test helper functions
- Improved test coverage for configuration validation
- Cleaned up distributed queue implementation
- Extracted duplicate counter logic in media request bundle
- Improved code organization and maintainability
- Guild/Server IDs
- Channel IDs
- Role IDs
- User IDs
- Message IDs

## [2.4.5] - 2025-12-01

### Changed

- Attempt to handle sigterm better for docker compatability
- Add memory profiler log file to help diagnose issues
- Remove need to for checkfile in loop heartbeat metrics
- Attempt to combine common database functions into common file
- Use PaginationLength instead of number of line pagination in outputs
- Added deno to base install for yt-dlp compatability
- Moved youtube music search to separate queue to speed up time to first download
- Add table to guild analytics, not used in commands yet
- Database cleanup, remove unused tables
- Optimize media request bundle print statements to optimize for discord API calls

## [2.4.4] - 2025-09-17

### Changed

- Update dependabot to run daily checks instead of weekly
- Add KNOWN-ISSUES.md documentation file
- Add support for DEVELOPMENT.md documentation
- Complete overhaul from single mutable to multi-mutable message architecture
- Remove configurable `number_shuffles`, implement single shuffle with proper random seeding
- Update to v0.1.3 with zero-padding support for position display
- Add message not found error handling and HTTP server disconnect retries
- Optimize message dispatch logic to delete removed messages in middle rather than editing all subsequent messages
- Rework media request lifecycle to use DapperTable, maintaining message order consistency
- Improve search result handling and message queue integration
- Expose history playlist in commands, fix various playlist-related issues
- Enhanced cache cleanup and backup storage handling
- Remove search cache client functionality (migrated database schema)
- Fix voice client checks on stop operations
- Improved iterative message deletion on errors

## [2.4.3] - 2025-07-08

### Changed

- Fixups for OTLP setup, added heartbeat metrics to multiple cogs
- Add alembic database migration support
- Add s3 backups to cached files

## [2.4.2] - 2025-06-10

### Changed

- Added support for OTLP logging, traces, and metrics
- Move downloads to tmpfile in Music
- Move player files to tmpfile
- In general isolated cache files

## [2.4.1] - 2025-04-13

### Changed

- Split up logging into one file per cog

## [2.4.0] - 2025-04-13

### Changed

- Added more test coverage, up to 90%
- Changed up common cog to not return a db session, but added function to yield one
- Added function to retry db statements
- Added a "message queue" to handle all message requests. Helps from reaching rate limiting too often
- Removed unused `video_id` field from `PlaylistItem` table
- Added proper index on `video_url` to `PlaylistItem` table
- Updated logic to use db retries
- Updated config args to be a bit more readable
- Updated to use db retries

## [2.3.0] - 2025-01-05

### Changed

- Added more test coverage, up to 60%
- Major rework of music cog
- Replace elasticcache search with generic db cache for spotify playlists
- Add support for spotify tracks
- Add search for youtube music urls
- Remove bug where files were double downloaded
- Add cache check to file downloads pre-download
- Add variance to periodic yt-dlp backoff from youtube extractor
- Adding message queue to handle all discord related messages, remove lockfiles
- Add better messages for users on download errors
- Use display name instead of auth name in most places

## [2.2.0] - 2024-12-28

### Changed

- Added test cases, bring test coverage to near 40%
- Add command `!markov list-channels` to show where server is active in that server
- Rework config options to be more straight forward
- Update README to reflect those changes

## [2.1.0] - 2024-12-15

### Changed

- Removed unused `allowed_roles` functions
- Removed plugin support, not necessary as much anymore
- Fixed bug with discord retry rate limited wait time
- Fixups to cog stop (unload/remove) that will log errors
- Add command to remove bot from reject list of guilds
- Add log on startup showing what guilds bot is currently in
- Add regexes to twitter/youtube links to catch slightly different urls
- Add in elasticsearch cache on top of video cache
- Check results to see if any search strings passed in match
- Add in `!random-play cache` for only cached files
- Have cached videos skip download queue entirely
- Add better options for youtube download backoff
- Move any yt-dlp logic to download queue, helps with backoff
- Add more tests for utils

## [2.0.9] - 2024-08-26

### Changed

- Move `cache.json` data to new table called `VideoCache`
- Adding lookup of urls to check `VideoCache` before attempting yt-dlp calls
- Adding wait time between each yt-dlp download
- Adding `SearchCache` table to cache youtube string lookups to video urls
- Adding check to see if download was unavailable or private before removing from PlaylistItems
- Fix downloading of non-youtube video extractors
