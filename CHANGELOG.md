# Changelog

## 2.4.3

General:
- Fixups for OTLP setup, added heartbeat metrics to multiple cogs
- Add alembic database migration support

Music
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