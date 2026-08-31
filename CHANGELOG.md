# Changelog

All notable changes to the Discord bot will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.5.120] - 2026-08-31

### Changed

- Move the guild analytics tables behind a `GuildAnalyticsStore` protocol, the fourth and last table-scoped slice of the persistence-tier extraction. `!music-stats` was reading six columns off a live `GuildVideoAnalytics` row inside the session block that loaded it, and the post-play loop was doing a read-modify-write over four counters in a session it held open across a Discord dispatch -- two players finishing a track at once could read the same totals and write back the same increment. Both are now single calls that open, commit and close their own transaction, and `!music-stats` gets a serializable entry instead of a row. With this in, `cogs/music.py` opens no database session at all, so the eighteen `database_functions` helpers that no longer had a caller are deleted along with it; what remains in that module is the `video_cache` catalog, which stays put because the broker's cleanup loop interleaves it with Redis and S3.

## [2.5.119] - 2026-08-29

### Changed

- Restores the public playlist index to newest-first. It was changed to oldest-first on the finding that `created_at` was NULL on every row, so `ORDER BY created_at DESC` had never taken effect and heap order was what servers actually saw. That was measured against a fresh database. It held for `playlist_item`, where 1463 production rows genuinely had no timestamp, but not for `playlist`: all 32 production rows already carried distinct timestamps, so the DESC had been in effect the whole time and the index has always been newest-first. Shipping the reversal renumbered every guild with more than one playlist, one of which has sixteen. The item-level ordering is unaffected and stays oldest-first, which is what `!playlist show` positions and the history playlist's delete-the-oldest eviction need. Four tests that asserted insertion order as index order have been flipped: they passed only because the ordering they were asserting never ran.

## [2.5.118] - 2026-08-28

### Changed

- Bumped pydantic to v2.13.5
- Bumped click to v8.5.0

## [2.5.117] - 2026-08-28

### Changed

- The playlist tables move behind a `PlaylistStore` protocol, the third and largest group of the persistence seam (`projects/discord-db-tier-extraction`). A new `PlaylistClient` owns every session and transaction boundary, `types/playlist.py` adds the entries and write types that cross it, and `cogs/music.py` no longer opens a session for anything but guild analytics. Three compound operations collapse into single calls rather than becoming round trips per row: `add_items` takes a batch and reports per item whether it was added, was a duplicate or hit the ceiling; `record_history_item` replaces the post-play sequence of a delete-by-url, a count, a conditional bulk delete and an insert; and `ensure_history_playlist` is one get-or-create where a read plus a conditional write would race two players starting at once. The item-count ceiling is now enforced inside the same transaction as the insert instead of being a check-then-act around it. Two paths also stop holding a postgres connection across network I/O: `!playlist queue` held one open while dispatching searches and enqueuing downloads, and `!playlist merge` held one open while sending a Discord message per copied item.

## [2.5.116] - 2026-08-28

### Changed

- Two playlist defects found while scoping the persistence seam, both confirmed against a real postgres rather than by reading. `!playlist queue` never recorded a time: `update_playlist_queued_at` assigned `last_queued_at`, which is not a mapped attribute and not a column, so SQLAlchemy accepted it silently, the commit emitted no UPDATE, and the "Last Queued" column of `!playlist list` has always shown N/A. And `created_at` was NULL on every `playlist` and `playlist_item` row, because no construction site passed it and the column carried no default - so the playlist index order, `!playlist show`'s item order and the history playlist's delete-the-oldest eviction were all returning postgres heap order, which drifts as rows are deleted and reinserted. The column is defaulted at the model now, every ordering carries an `id` tiebreak so a tie stops meaning heap order, and a migration backfills existing rows from their id so they keep insertion order. Playlist indexes stay oldest-first, which is what servers see today.

## [2.5.115] - 2026-08-27

### Changed

- The markov tables move behind a `MarkovStore` protocol, the second group of the persistence seam (`projects/discord-db-tier-extraction`). A new `MarkovClient` owns every session and transaction boundary the cog used to hold open, and the cog reaches persistence only through the protocol: `types/markov.py` adds `MarkovChannelEntry` and `MarkovMessageWrite` so nothing session-bound crosses the boundary, the module-level query helpers in `cogs/markov.py` are gone, and the cog no longer mutates or deletes ORM rows. Two costs went with them - the producer loop held one postgres connection open across the entire Discord dispatch fan-out, and `!markov on` held one across a `fetch_channel` round trip. Methods are sized per unit of work rather than per row (`save_messages` takes a batch, `generate_words` returns a whole sentence), so the eventual HTTP store is one request per call rather than one per message or one per word.

## [2.5.114] - 2026-08-27

### Changed

- The video-cache catalog now has a Protocol, the first of four groups in the persistence-tier extraction. `VideoCacheStore` (`interfaces/database_protocols.py`) declares the six methods `VideoCacheClient` actually provides, and `get_deletable_entries` now returns `VideoCacheEntry` models rather than live SQLAlchemy rows. That is the whole point of the seam: an ORM instance is bound to the session that loaded it, so a signature naming one can only ever be satisfied by the in-process implementation — `cache_cleanup` already reads `base_path` and `id` from those rows *after* the loading session has closed, which works today only by accident of eager loading. The entries are plain pydantic, `model_dump`/`model_validate` clean. `generate_download_from_existing` was deleted instead of being declared: it took a `VideoCache` row as an argument, which nothing remote could ever pass, and it had no caller outside its own test. `MusicVideoCacheNaming` went with it as its only user. Annotating `MediaBrokerBase.video_cache` against the Protocol also drops **sqlalchemy out of `broker_protocols`' import chain** — the dependency that forced `CheckoutResult`, `DownloadResultQueue` and `BrokerClient` each into their own module — leaving only boto3 via `delete_file`. A subprocess import measurement now guards that. No behavior change; the video-cache suite passes unchanged against a real postgres.

## [2.5.113] - 2026-08-27

### Changed

- The `video_cache_backup` table and its helpers are gone. The object-storage backup table landed in migration `0f696315a882` and never gained a caller: `VideoCacheBackup`, `list_video_cache_where_no_backup`, `get_video_cache_backup` and `delete_video_cache_backup` had no references anywhere outside their own unit tests, so nothing has ever written a row to it. Two more `database_functions` went with them for the same reason — `list_video_cache` and `get_video_cache_by_id` — taking the module from 29 functions to 24. Their tests are deleted rather than kept: they only ever exercised code that production could not reach, which is how five dead functions held 100% coverage. Migration `c3f1a7d20b45` drops the table, verified in both directions against a real postgres. Applying it stays manual, as every migration in this repo is.

## [2.5.112] - 2026-08-27

### Changed

- `!markov speak` now asks postgres for one word at a time instead of pulling the guild's whole relation table into the bot. `get_possible_words` selected **every** `MarkovRelation.id` for the guild, `choice()`d one in python, then fetched that row back by id — and repeated the entire select once per word of the sentence. Measured on a 1000-relation guild with 25 followers per word, a 32-word sentence cost **64 SELECTs returning 832 rows**; it is now **32 SELECTs returning 32 rows**, and the row count no longer grows with the size of the guild's history. The selection moved into `ORDER BY random() LIMIT 1` behind two helpers, `random_leader_word` and `random_follower_word`. A dead end is also no longer a crash: retention deletes relations by age, so it can remove every relation in which a word leads while keeping one where it follows, and the old code handed that empty id list straight to `choice()` and raised `IndexError`. The sentence now simply ends there. The four speak tests stopped patching `markov.choice` to force determinism — the first one gets it from a corpus where every word has exactly one follower, so the assertion describes the query rather than the mock.

## [2.5.111] - 2026-08-27

### Changed

- Markov relation writes are now staged on the caller's session instead of opening a new one per word pair. `build_and_save_relations` used to open its own `with_db_session()` and commit for *every* pair in a message, and the engine is built with `poolclass=NullPool`, so nothing is reused — a twenty-word message opened twenty postgres connections on top of the one its caller was already holding. It now takes the caller's session and only `add()`s, and `_apply_history_result`'s existing per-message commit covers the relations too. The connection cost of a message drops from `len(corpus) + 1` to 1. That also makes a message atomic, which fixes a quieter bug: the old split committed the relations first and the channel's `last_message_id` second, so a failure between the two left the relations saved while the channel still pointed at the previous message — the next cycle re-fetched that message and added its relations a second time, silently doubling them. A failure now rolls the whole message back and the retry is clean. No behavior change to what gets stored: the existing markov suite, which runs against a real postgres, passes unchanged.

## [2.5.110] - 2026-08-26

### Changed

- A resolved search is no longer destroyed when the downloader is unreachable. `broker_client.next_search_result()` is a destructive pop, and `process_search_results` caught only `PutsBlocked` and `QueueFull` around the download submit — both of which are the downloader *answering*. Anything else, in practice a `ClientConnectorError` against a downloader pod mid-`Recreate`, escaped with the resolution already popped and gone: the media request was never downloaded, and its bundle row stranded on `QUEUED` with no error the user could see. The resolution now goes back on the broker's queue before the exception propagates, so the loop runner's capped backoff applies and the request is picked up once the new pod is serving. When the requeue itself fails the request really is lost, so that branch says so in the log and lets the original submit failure propagate rather than masking it. `downloader-app.yaml` calls the `Recreate` gap harmless because "downloads queue in Redis and resume when the new pod's tunnel is up" — true for work already on the downloader's queue, and this is what makes it true for work still in the handoff. The same shape is fixed at the other end of the pipeline: `_generate_media_requests_from_search` creates its bundle before calling `check_source`, and caught only `SearchException` — airtight while `check_source` was in-process, but since the media_search cutover it is an HTTP round trip that can raise out of aiohttp. An uncaught one left a rendered placeholder row that never resolved; the bundle is now torn down and the user told the search backend is unavailable.

## [2.5.109] - 2026-08-26

### Changed

- Source expansion now runs in the search pod. The cog builds an `HttpMediaSearchClient` against the new required `music.media_search_client.url` and holds no provider client of its own, so **`spotipy` and `google-api-python-client` leave `[bot]`** — measured, not asserted: `discord_bot.cli.bot` imports neither, and the `discord-bot` row of `docs/image-dependencies.md` moves with the measurement. `SearchClient` is untouched on both sides of the cutover, which is what MR 1's `MediaSearchClient` Protocol was for, and a provider failure still arrives as `MediaSearchError` — raised by the remote client out of the pod's typed error body — so the user-facing rendering does not know the difference. `media_search_client` is required from its first release rather than optional-then-un-defaulted like the broker, download and ytmusic seams before it: the only fallback a missing url could select is the in-process client, and importing that would drag both SDKs back onto the image this change exists to slim. `music.download.spotify_credentials` and `music.download.youtube_api_key` are dropped from the cog's config model — the search pod reads both out of its own config, and pydantic's `extra='ignore'` means a `discord.bot.conf` still carrying them through the cutover is harmless. `[bot]` keeps `scraping`: `beautifulsoup4` belongs to the urban cog, which the cog registry imports unconditionally, and it has nothing to do with media_search.

## [2.5.108] - 2026-08-26

### Changed

- The search pod now fronts two route families from one bind. `CompositeHttpServer` merges `YoutubeMusicSearchHttpServer` (`/search/ytmusic*`) and `MediaSearchHttpServer` (`/search/spotify`, `/search/youtube`) into a single aiohttp Application, which is what the per-provider route namespacing was reserved for: one image, one pin, one netpol tier, and no extra Deployment whose pin a revert-then-auto-bump can strand out of step with the bot. `[search]` gains `search-providers` accordingly, and `tests/cli/_image_deps.py` now declares `spotipy` and `googleapiclient` on the search image — measured, so the `discord-search` row of `docs/image-dependencies.md` moves with it. **The trap this closes:** `AiohttpServerBase.serve()` is what sets a server's `_serving` flag, and a child whose routes are merged into a composite never runs its own `serve()`. Left alone every child would report `is_serving` False forever, so `youtube_music_search_server` — a series on the Discord Health dashboard and in the `DiscordHeartbeat` alert — would have read a flat 0 while the listener was up and healthy. `set_serving` is now a public method on the base, called by `serve()` for a server that owns its site and by the composite for each child it fronts; `start_draining` propagates the same way. Provider credentials (`music.download.spotify_credentials`, `music.download.youtube_api_key`) are read by the pod and are optional: an absent one yields a `None` provider client whose route answers `MISSING_CREDENTIALS`, which the cog renders into the same message a credential-less bot shows today. That keeps "are credentials configured" a question the pod answers rather than one the bot has to guess about. Building the provider clients from raw values moved into a shared `build_media_search_client`, since the cog and the pod read the same three settings out of differently-shaped config and two copies of that constructor is what the duplicate-code check exists to catch.

## [2.5.107] - 2026-08-25

### Changed

- The media-search providers gain an HTTP surface: `MediaSearchHttpServer` serves `POST /search/spotify` and `POST /search/youtube` on the search pod's reserved namespace, and `HttpMediaSearchClient` is the bot-side `MediaSearchClient` that calls them. Nothing constructs either yet — the cog picks the HTTP client up at the cutover. Unlike every other pod route in the repo this is not a `QueueWorkerHttpServer`: source expansion is request/response inside `!play`, with no queue, no worker and no consumer loop, so the submit/clear/block/status shape has nothing to describe. A provider failure comes back as **HTTP 200 with a typed error body** rather than a status code, which is the deliberate part: a provider saying "no such playlist" is an answer the pod produced successfully, not a fault of the pod, so encoding it as 5xx would make the client's retry wrapper re-run a lookup whose answer cannot change — and would lose the body, since `HttpClientMixin._http` calls `raise_for_status()`. Non-2xx stays reserved for the pod itself being broken, which is worth a retry. The client rebuilds the same `MediaSearchError` the in-process client raises, so `except MediaSearchError` behaves identically on both sides of the split. `HttpMediaSearchClient` gets its own module rather than sharing `clients/media_search_client.py`, because the in-memory client imports `spotipy` and `googleapiclient` at module scope and importing the HTTP client must not drag them into the bot — the same split, and the same reason, as `HttpBrokerClient` moving out of `clients/broker_client.py`. That property is asserted by measuring the import in a clean interpreter rather than by reading the source. The server publishes no heartbeat gauge on purpose: it will run behind a composite app alongside the ytmusic server, and `AiohttpServerBase` sets `_serving` in `serve()`, so a merged server would report a permanent 0 while serving fine — whoever owns the composite owns the listener heartbeat.

## [2.5.106] - 2026-08-25

### Changed

- Source expansion moves behind a `MediaSearchClient` Protocol. `SearchClient` took a `SpotifyClient` and a `YoutubeClient` and called them directly; it now takes one client with two methods — `spotify_source` and `youtube_source`, both returning a `CatalogResponse` — and `InMemoryMediaSearchClient` supplies the in-process implementation the bot runs today. This is the seam the media_search extraction needs, placed around the two provider calls rather than around `check_source` as a whole: only two of `check_source`'s six input shapes reach a provider at all, and the other four, including the plain-text search that is the common `!play`, are `re.match` and nothing more. Two consequences make the later cutover possible. The executor offload moved into the client, so the Protocol takes no event loop and `check_source` no longer has a `loop` parameter — the HTTP implementation does no offloading and would have carried a dead argument forever. And the provider SDKs are now absorbed rather than passed through: `spotipy` and `googleapiclient` failures become a `MediaSearchError` carrying `(provider, reason, http_status)`, and the cog renders that into the user-facing message. Keeping the Discord copy in the cog is deliberate — when these calls move to the search pod, the pod returns the reason and the bot still writes what a user reads. `SearchException`, `ThirdPartyException` and `InvalidSearchURL` move to `discord_bot/exceptions.py` (which imports nothing) so that `clients/` can raise them without importing a cogs module, and re-export from their old home. One user-visible fix rode along: the non-404 Spotify failure message was a plain string containing a literal `{search}` with no `f` prefix, so it told users the problem was with a url called `{search}`. The existing test asserted only the message prefix, which is how a one-character bug survived in user-facing copy; the new tests assert the interpolation.

## [2.5.105] - 2026-08-25

### Changed

- `beautifulsoup4` moves out of the `search-providers` capability group into its own `[scraping]` group. It was never a search provider: its only import site is `cogs/urban.py`, which `cli/_lib/cog_registry` imports unconditionally, so the bot image needs it whatever the search providers do. The grouping mattered because the media_search extraction plans to move `search-providers` to `[search]` — moving it by name would have carried `beautifulsoup4` out of `[bot]` and broken the bot at import. That would have failed loudly rather than silently, since `test_import_boundaries` asserts declared and measured imports agree in both directions, but only after somebody spent the MR finding out. `search-providers` is now exactly the two packages that leave `[bot]` at the fold-in, `spotipy` and `google-api-python-client`. This is a pure regrouping: all five images resolve to identical package sets before and after, so no image changes and `docs/image-dependencies.md` is untouched. The `[search]` comment that described the pending move also spelled the group `search_providers` with an underscore — the PEP 685 silent-resolution trap documented thirty lines above it in the same file, in the one line a fold-in MR would most likely copy.

## [2.5.104] - 2026-08-25

### Changed

- The player loop now waits for a voice client to appear instead of destroying the player and dropping the queue. `Music.get_player` starts the player loop *before* it awaits `join_voice`, so a resumed session — which re-queues its whole backlog the moment the player exists — can reach `voice_client.play()` while the voice handshake is still in flight. That raised `AttributeError` on `None`, and the handler tore the player down: prod lost a 15-track queue this way during a rollout, with the voice client arriving 29 seconds later and nothing left to play. The loop now waits up to 60s (sized against the ~45s handshake measured in that incident), polling for the client; a player that genuinely never gets voice still gets torn down, so the give-up path is unchanged. Tests in `test_music_player.py` shrink the wait through an autouse fixture — at the real value, any test reaching `play()` without a voice client becomes a one-minute test, which two existing ones silently became when the wait was first added.

## [2.5.103] - 2026-08-24

### Changed

- Dependencies are now declared as **capability groups** — `[gateway]`, `[voice]`, `[database]`, `[storage]`, `[rendering]`, `[downloading]`, `[search_providers]`, `[youtube_music]` — with each per-image extra composed from them by self-reference rather than by listing packages again. Every package is pinned exactly once. The first cut of this split repeated the pins per image, and within a single renovate bump the copies had already drifted: `[bot]` moved to yt-dlp 2026.8.19 while the downloader, the image that actually runs yt-dlp, was left on 2026.7.4. `discord.py` also leaves the core dependencies for `[gateway]`, so it is paid for by the bot and dispatcher rather than by all five images, and the dispatcher gains a `[dispatcher]` extra instead of running on the core set by habit — all five images now name what they install. Dependency closure, measured per image: bot 466 MB → 273 MB, broker 466 MB → 130 MB, downloader 466 MB → 110 MB (broker and downloader both installed `[bot]` before), with dispatcher at 71 MB and search at 64 MB against a 64 MB core.
- `moviepy` is no longer installed. It sat in the `[bot]` extra with **no import site anywhere in the tree** — the docstrings that claimed `interfaces/download_protocols` pulled "moviepy via utils/audio" were stale, since `utils/audio` shells out to the `ffmpeg` binary and never imported the package. Because `[bot]` is what the bot, broker and downloader images all install, that dead dependency was being carried three times over, and it is not small: moviepy drags in numpy, pillow, imageio and imageio-ffmpeg, ~172 MB of installed size that exists solely for it. Verified by removing it outright: all five entrypoints import and the full suite passes with moviepy, numpy, imageio, imageio-ffmpeg, pillow and proglog absent. The import-boundary constant is kept so re-adding the dependency still meets an explicit refusal.
- `docs/image-dependencies.md` is a generated, asserted answer to "what is used strictly by what": the tier-defining packages each image imports, how much of the module tree each one loads, and how much of that tree is shared. It is rendered from a live measurement — each entrypoint imported in a clean interpreter — and a test fails if the checked-in file drifts, so it cannot become confidently wrong. Regenerate with `UPDATE_IMAGE_DEPS=1 pytest tests/cli/test_import_boundaries.py`. The import boundaries behind it changed shape too: each image now declares the packages it *does* import, and the forbidden set is derived from that rather than hand-maintained alongside it. That makes the assertion an equality, which catches the failure a forbidden-list structurally cannot — an extra gone over-broad because the code needing it was deleted, which is how `yt_dlp` stayed in `[bot]` and `moviepy` stayed installed with no import site at all.
- The broker and downloader images install their own dependency sets instead of `[bot]`. Both used to install the bot's full extra, so the broker carried yt-dlp and all three search clients despite never downloading or searching, and the downloader carried SQLAlchemy, dappertable and the search clients despite never touching the database or searching. The new `[broker]` (SQLAlchemy, asyncpg, alembic, boto3, dappertable) and `[downloader]` (yt-dlp, yt-dlp-ejs, boto3) extras are derived from each entrypoint's measured import set, and each was verified by building a venv containing only that extra and importing the entrypoint — the same thing the pod does at start. Site-packages goes 333 MB → 169 MB for the broker and 333 MB → 154 MB for the downloader. Two packages are in `[broker]` with no import site and are annotated as such in pyproject: asyncpg is SQLAlchemy's driver, named in the URL `cli/_lib/db` builds, and alembic is the migration CLI whose files this image already COPYs.

## [2.5.101] - 2026-08-24

### Changed

- Bumped yt-dlp to v2026.8.19

## [2.5.100] - 2026-08-24

### Changed

- The deprecated `discord-bot-min` console script is gone; `discord-bot` is the only name for the gateway entrypoint. The alias was kept transitionally because the bot Deployment set `DISCORD_BOT_CMD=discord-bot-min`, so removing the name first would have CrashLooped the pod on exec. docker-apps!1288 dropped that override and the pod has been running on the image default since, which closes the three-step sequence started in !233. `test_every_published_image_has_a_boundary` now reads `[project.scripts]` from pyproject instead of restating the list — it was a hardcoded literal that agreed with the real script list only as long as someone kept both in step by hand, and removing a script is precisely the event it claims to notice.

## [2.5.99] - 2026-08-24

### Changed

- The broker, downloader and search pods no longer import `discord.py`. Four routes reached them, all through modules every entrypoint loads: `utils/otel` imported `Context` at module scope for a runtime `isinstance` in `command_wrapper`; `utils/common` and `cli/_lib/common` named `Bot` in annotations; and `utils/discord_retry` held both the discord-message retry helper and the transport-level ones that every HTTP client uses. `command_wrapper` moved to `utils/otel_command`, `build_bot` to `cli/_lib/gateway`, the annotations moved under `TYPE_CHECKING`, and the discord-free retries moved to `utils/retry` (re-exported from their old home). Measured on the entrypoints: `discord` is gone from all three worker pods. The dispatcher keeps it deliberately — it sends and edits real messages. The per-image import boundaries now assert this, so it cannot regress.

## [2.5.98] - 2026-08-23

### Changed

- Broker `/results/next` and `/search-results/next` no longer emit a span on an empty (204) poll. Both endpoints are polled roughly once a second per bot pod even while idle, and with no active span on the client to attach to, every empty poll was landing in Tempo as its own single-span root trace — 2 spans/s between them, 22% of all trace volume, none of it attached to a real request. The span is now opened only once a result is actually being handed back, matching what `BrokerClient.next_result` has always done on the client side. Hit/empty accounting is unchanged.

## [2.5.97] - 2026-08-23

### Changed

- ### Changed
- **The bot image no longer rewrites 474 MB of dependencies on every commit.** The final stage copied the builder's entire `/install` prefix — third-party dependencies *and* our own package — as a single 474 MB layer, so any source change invalidated it and buildkit recompressed the lot. Measured on a code-change CI build, `exporting layers` was **270.9 s of a 283.3 s** export step, and the export phase was ~345–426 s of a ~530–600 s job. The builder now installs dependencies to `/install` and the package to `/install-app`, and the final stage copies them as separate layers: the dependency layer is keyed on `pyproject.toml` alone and stays cached, while the only layer that changes per commit is **2.47 MB**. Together with the `chown` fix below, the per-commit layer churn goes from ~665 MB to ~2.5 MB.
- **Removed a 95.6 MB layer that was a byte-for-byte duplicate of the layer above it.** `RUN chmod +x /entrypoint.sh && chown -R discord:discord "${WORKDIR}"` ran *after* the Deno runtime was copied into `${WORKDIR}/.deno`, so it rewrote all 95.6 MB of it into a new layer. The directories are now chowned while still empty, in the same `RUN` that creates them, and the Deno copy carries `--chown` instead. Image size drops 1.3 GB → 1.2 GB.

## [2.5.96] - 2026-08-23

### Changed

- ### Changed
- **Removed a stale test marker that was the entire contents of `discord_bot/__init__.py`.** The two comment lines were added by !68 in June 2026 to generate a source change that would exercise the then-new buildkit S3 layer cache, and were meant to be reverted before that MR merged. They weren't, and have sat on `main` since. No behaviour change — the file stays as the package marker, now empty.

## [2.5.95] - 2026-08-23

### Changed

- ### Changed
- **The bot image installs dependencies before copying source, so a code change no longer rebuilds them.** `docker/Dockerfile` copied `discord_bot/` above `pip install "${APPDIR}[bot]"`, so every source edit invalidated the dependency install *and* the Deno fetch below it. Measured across 44 successful `pr-check:build-bot` runs (2026-08-15→22) the job was sharply bimodal: 6 runs at 60–80s (identical-source rebuild, full cache hit) against 34 runs at 450–880s, median 496s — the S3 layer cache only paid on rebuilds of unchanged source. The dependency set now installs against a stub package keyed on `pyproject.toml` alone, and the real package installs `--no-deps` in a layer below the source copy; the Deno install moves above every `COPY`, since it depends on nothing in this repo. Locally, a source change plus a `VERSION` bump goes from 39.4s (dependency install and Deno both re-running) to 13.7s with both `CACHED`, against a ~44s cold build in each case.
- `VERSION` is deliberately kept out of the dependency layer. setuptools resolves the version from it via `[tool.setuptools.dynamic]`, but the changelog workflow bumps it on every MR, so copying the real file into that layer would bust the dependency install on every change and defeat the split. The layer writes a `0.0.0` placeholder and tears the stub back out, leaving `/install` holding only third-party dependencies; the source layer installs the real version. The teardown matters: pip cannot reliably uninstall from a `--prefix` tree, and leaving the stub in place produced both a `discord_bot-0.0.0.dist-info` and the real one, with `importlib.metadata.version()` resolving to `0.0.0`. Nothing reads the version at runtime today, but the image reported the wrong one.

## [2.5.94] - 2026-08-22

### Changed

- ### Changed
- **Fixed-interval background pollers no longer emit spans.** Three loops re-run on a timer whether or not anything has happened, so the spans they produced were emitted at the poll rate rather than per unit of work. Measured in prod over 30m: `utils.retry_broker_command` **8,137/hour** on the bot (the client half of the 1 Hz status poll, one per worker client), `downloader.status` **3,563/hour** and `youtube_music_search.status` **3,565/hour** (the server halves), against 4/hour for `music.play` and 10/hour for `broker.checkout` — the spans that describe real work. Roughly 15k spans/hour, ~99% of the bot's total volume, to report that a cache refresh found nothing new. `async_retry_broker_command` takes `traced=False` (opt-in, and only the status poller passes it), and `QueueWorkerHttpServer._handle_status` drops its span wrapper. Routes that mutate queue state, and every caller that loses work when it fails, are unchanged.
- **A worker pod being rescheduled no longer reports a fault.** While a downloader or search pod is replaced its `/status` route is unreachable, and each 1 Hz poll tick burned its four attempts and landed an ERROR span — thirteen of them during a default-pool node replacement on 2026-08-22 (01:34–01:40 UTC), for a failure `_poll_status_loop_once` already handles by keeping the cached values and logging a WARNING. Retry and timeout behaviour is unchanged; only the span is gone. The WARNING is the signal.
- **The pool exit-IP probe stops stamping ERROR spans for tolerated relay failures.** `PoolExitIpProbe._fetch_ip` now runs its request inside `suppress_instrumentation()`. yt-dlp's client is requests-backed, so auto-instrumentation emitted a client span per exit per tick — ~207/hour at the default 300s interval across the configured pool — and any relay that failed to connect stamped that span ERROR after the 20s SOCKS connect timeout. Those were **21 of the 39 error spans** in a representative six-hour window, more than half of them describing a condition `refresh()` exists to absorb: it keeps the last-known IP and warns. Suppression is scoped to the probe request, so the download spans this attribution gets stamped onto are unaffected.

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
