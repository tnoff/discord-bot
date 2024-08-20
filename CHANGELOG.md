# Changelog

## 2.0.9

Music:
- Adding `webpage_url` and other args to local cache JSON
- Adding lookup of urls to check local cache before attempting yt-dlp calls
- Adding wait time between each yt-dlp download
- Adding `SearchCache` table to cache youtube string lookups to video urls
- Adding check to see if download was unavailable or private before removing from PlaylistItems