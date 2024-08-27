# Changelog

## 2.0.9

Music:
- Move `cache.json` data to new table called `VideoCache`
-- Track VideoUnavaiable errors and VideoTooLong errors in `VideoCache`
- Adding lookup of urls to check `VideoCache` before attempting yt-dlp calls
- Adding wait time between each yt-dlp download
- Adding `SearchCache` table to cache youtube string lookups to video urls
- Adding check to see if download was unavailable or private before removing from PlaylistItems
- Fix downloading of non-youtube video extractors