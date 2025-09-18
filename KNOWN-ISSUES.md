# Known Issues

Known issues to track

## Playlist Item-Adds Can Be Broken By Stopping Bot

### Repro Steps

- Call `!play <input-string>`
- Bot downloads and plays media
- Call `!playlist item-add <input-string>`
- Stop bot `!stop`

### Behavior

Bot stops all downloads, which includes purging the items from the playlist item add

### Ideal Behavior

Bot removes downloads intended for player, but continues on with playlist item adds.