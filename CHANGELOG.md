# Changelog

## 2.5.3

Music:
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

Code Quality:
- Migrated remaining internal types (`DownloadStatus`, `CatalogResponse`) to Pydantic
- Moved ready-file and file-removal operations from `MusicPlayer` into `MediaBroker`
- Increased test coverage to 96%
- Moved KNOWN-ISSUES content into DEVELOPMENT.md

Dependencies:
- Bumped yt-dlp from 2026.3.3 to 2026.3.17
- Bumped boto3 from 1.42.67 to 1.42.71
- Bumped google-api-python-client from 2.192.0 to 2.193.0
- Bumped croniter from 6.0.0 to 6.2.2
- Bumped tox from 4.49.1 to 4.50.0

## 2.5.2

General:
- Added healthcheck server endpoint for container health monitoring
- Consolidated all Discord API calls into a single per-guild `MessageDispatcher` queue to reduce rate-limit contention
- Simplified message dispatch logic and removed partial function wrappers in dispatch calls
- Added regex support to the spam filter
- Set Spotipy token cache to in-memory to avoid writing credentials to disk
- Added OTel span filter to reduce high-volume trace noise
- Fixed async retry usage in role cog send messages
- Fixed cache directory creation for Discord user runtime

Music:
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

Code Quality:
- Extracted all dataclasses into a dedicated `discord_bot/types/` package (`search`, `download`, `catalog`, `media_request`, `media_download`, `history_playlist_item`)
- Removed Twitter/fxtwitter URL handling (no longer supported)
- Simplified logging logic and fixed logging levels
- Added additional OTel spans for high-volume operations; limited trace length

Dependencies:
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

## 2.5.1

General:
- Added support for running as non-root user
- Added log level configuration for 3rd party libraries
- Fixed discord.py logger level configuration
- Fixed third party logging config
- Simplified init config options
- Added better typing to music classes
- Added logging to help diagnose extra character messages
- Fixed handling of exit exceptions gracefully
- Updated to Python 3.14

Music:
- Fleshed out retry logic in download client
- Added retryable exceptions to download client
- Simplified retry backoff implementation
- Fixed ytdlp build path configuration
- Fixed deno path in environment
- Updated to use nightly build of yt-dlp
- Updated to DapperTable v0.2.4

Testing:
- Added lockfile fixes and additional tests
- Added text validation checks
- Sleep and asyncio updates

Dependencies:
- Bumped pynacl from 1.6.1 to 1.6.2
- Bumped boto3 from 1.42.12 to 1.42.20
- Bumped psutil from 7.1.3 to 7.2.1
- Bumped pydantic from 2.10.6 to 2.12.5
- Bumped pydantic-yaml from 1.5.0 to 1.6.0
- Bumped ytmusicapi from 1.11.3 to 1.11.4

## 2.5.0

**BREAKING CHANGES:**

General:
- **Migration to Pydantic v2**: Replaced jsonschema with Pydantic v2 for configuration validation
  - All configuration validation now uses Pydantic models
  - Better error messages when configuration is invalid
  - Type-safe configuration throughout the codebase
- **Discord IDs now integers**: Changed all Discord IDs (guild, channel, role, user, message) from strings to integers
  - **Database migration required**: Run `alembic upgrade head` to migrate existing databases
  - YAML configuration should use unquoted integers for IDs (e.g., `12345` not `"12345"`)
  - See migration guide below for more details

Music:
- Refactored media request bundle to use dataclass instead of dictionaries for better type safety
- Added `BundledMediaRequest` dataclass for cleaner request tracking

Testing:
- Added comprehensive type hints to test helper functions
- Improved test coverage for configuration validation

Code Quality:
- Cleaned up distributed queue implementation
- Extracted duplicate counter logic in media request bundle
- Improved code organization and maintainability

### Migration Guide for 2.5.0

#### Database Migration
**Required**: This release includes a database migration to convert Discord IDs from VARCHAR to Integer. Run the following command before starting the bot:

```bash
alembic upgrade head
```

The migration handles both SQLite and PostgreSQL databases automatically.

#### Configuration Updates
Update your YAML configuration to use integer IDs instead of string IDs:

**Before (2.4.x):**
```yaml
role:
  "123456789":  # String key (quoted)
    "987654321":  # String key (quoted)
      manages_roles:
        - "111111111"  # String value (quoted)
```

**After (2.5.0):**
```yaml
role:
  123456789:  # Integer key (unquoted)
    987654321:  # Integer key (unquoted)
      manages_roles:
        - 111111111  # Integer value (unquoted)
```

The same applies to all Discord IDs in configuration including:
- Guild/Server IDs
- Channel IDs
- Role IDs
- User IDs
- Message IDs

## 2.4.5

General:
- Attempt to handle sigterm better for docker compatability
- Add memory profiler log file to help diagnose issues
- Remove need to for checkfile in loop heartbeat metrics
- Attempt to combine common database functions into common file
- Use PaginationLength instead of number of line pagination in outputs

Docker:
- Added deno to base install for yt-dlp compatability

Music:
- Moved youtube music search to separate queue to speed up time to first download
- Add table to guild analytics, not used in commands yet
- Database cleanup, remove unused tables
- Optimize media request bundle print statements to optimize for discord API calls

## 2.4.4

General:
- Update dependabot to run daily checks instead of weekly
- Add KNOWN-ISSUES.md documentation file
- Add support for DEVELOPMENT.md documentation

Music:
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

## 2.4.3

General:
- Fixups for OTLP setup, added heartbeat metrics to multiple cogs
- Add alembic database migration support

Music:
- Add s3 backups to cached files

## 2.4.2

General:
- Added support for OTLP logging, traces, and metrics

Music:
- Move downloads to tmpfile in Music
- Move player files to tmpfile
- In general isolated cache files

## 2.4.1

General:
- Split up logging into one file per cog

## 2.4.0

General:
- Added more test coverage, up to 90%
- Changed up common cog to not return a db session, but added function to yield one
- Added function to retry db statements

Music:
- Added a "message queue" to handle all message requests. Helps from reaching rate limiting too often
- Removed unused `video_id` field from `PlaylistItem` table
- Added proper index on `video_url` to `PlaylistItem` table
- Updated logic to use db retries
- Updated config args to be a bit more readable

Markov:
- Updated to use db retries

## 2.3.0

General:
- Added more test coverage, up to 60%

Music:
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

## 2.2.0

General:
- Added test cases, bring test coverage to near 40%

Markov:
- Add command `!markov list-channels` to show where server is active in that server

Role:
- Rework config options to be more straight forward
- Update README to reflect those changes

## 2.1.0

General:
- Removed unused `allowed_roles` functions
- Removed plugin support, not necessary as much anymore
- Fixed bug with discord retry rate limited wait time
- Fixups to cog stop (unload/remove) that will log errors
- Add command to remove bot from reject list of guilds
- Add log on startup showing what guilds bot is currently in

Music:
- Add regexes to twitter/youtube links to catch slightly different urls
- Add in elasticsearch cache on top of video cache
- Check results to see if any search strings passed in match
- Add in `!random-play cache` for only cached files
- Have cached videos skip download queue entirely
- Add better options for youtube download backoff
- Move any yt-dlp logic to download queue, helps with backoff

Testing:
- Add more tests for utils

## 2.0.9

Music:
- Move `cache.json` data to new table called `VideoCache`
-- Track VideoUnavaiable errors and VideoTooLong errors in `VideoCache`
- Adding lookup of urls to check `VideoCache` before attempting yt-dlp calls
- Adding wait time between each yt-dlp download
- Adding `SearchCache` table to cache youtube string lookups to video urls
- Adding check to see if download was unavailable or private before removing from PlaylistItems
- Fix downloading of non-youtube video extractors