# Music Cog

Play audio from Youtube videos in voice chat. The bot can be called to join voice chat, and Youtube videos can be requested to be downloaded, processed, and played in the voice chat. Video audio will play one at a time and can be added to a queue to be played after the current video audio is played or skipped.

The bot is designed to be able to run on multiple servers at the same time.

## Inputs

Potential input includes
- A string that will be searched in Youtube and download the first result
- A direct Youtube link that will be downloaded
- A Spotify playlist or album (if Spotify credentials are given in the config)
- A Youtube playlist (if Youtube credentials are given in the config)

## Basic Usage

Once joined to a voice chat channel, call `!join` or `!awaken` to have the bot join the same voice channel you are in. Then enter `!play` with the input of the video you would like to listen to.

![](./images/basic_play.png)

If multiple videos are requested, they will be downloaded and placed into the queue, which will be posted to the channel.

![](./images/queue.png)

There is some logic to ensure that the queue messages are the most recent messages shown in the channel, for readability.

## Basic Functions

Other basic functions of music playing include

Skipping a video

```
!skip
```

Remove video from the queue
```
!remove <queue-position>
```

Bump video to the top of the queue

```
!bump <queue-position>
```

Stop the bot from playing audio and delete the queue

```
!stop
```

Show history of videos played by the bot during its current session

```
!history
```


If you would like to move the queue messages to a different channel, call `!move-messages` from the channel you'd like to move the messages to.

## Playlist Functions

If database creds are given to the bot, playlists can be created and updated to the server. This will allow you to save lists of Youtube videos to be played within the server. By default, a "history" playlist is created which adds previously played videos from the server. Anyone can queue these videos up in the server using:

```
!random-play
```

To list the current playlists

```
!playlist list
```

To create a custom playlist use:

```
!playlist create <name>
```

To add a specific video to a playlist:

```
!playlist item-add <playlist id> <video input>
```

To show the videos saved to a playlist

```
!playlist show
```

To queue up all vidoes in a playlist

```
!playlist queue <playlist id>
```

These can also be shuffled

```
!playlist queue <playlist id> shuffle
```


### Advance Playlist Features


To save all videos currently in the queue to a playlist

```
!playlist save-queue <playlist name>
```

To save all videos currently in the history to a playlist

```
!playlist save-history <playlist name>
```

To merge two playlists

```
!playlist merge <playlist one id> <playlist two id>
```

Remove an item from a playlist

```
!playlist item-remove <playlist id> <item id>
```

## Spotify Enablement

You can pass [Spotify API](https://developer.spotify.com/) credentials to the config to allow for Spotify playlists and albums to be given as input. This will request the track information from Spotify, then the bot will run a Youtube search for "`<artist name>` `<song name>`" in Youtube, and download the first result.

You can pass the Spotify credentials into the config:

```
music:
  spotify_client_id: secret-spotify-client
  spotify_client_secret: secret-spotify-client-secret
```

## Youtube Playlist Enablement

You can pass [Youtube API Key](https://developers.google.com/Youtube/v3/getting-started) credentials to the config to allow for Youtube playlists to be given as input. This will request all of the video Ids in the playlist and download all of them to the queue.

You can pass Youtube API credentials into the config:

```
music:
  youtube_api_key: secret-Youtube-api-key
```

## Multi Video Input Shuffles

Note that with either Spotify playlists/albums or Youtube playlist input, you can pass `shuffle` to the play input to have the videos shuffled.

```
!play <spotify-playlist-link> shuffle
```

## Under the Hood

All Youtube videos are downloaded by the bot via [yt-dlp](https://github.com/yt-dlp/yt-dlp). The video audio is then left on disk and deleted after the video is played. You can specify what directory the videos are downloaded to in the config:

```
music:
  download_dir: /tmp/discord
```

Specifically when videos are downloaded, they go to the base directory of the download dir. A subdirectory is then created matching the server id, and a symlink is created between the video file and the server subdirectory, with the symlink endpoint given a random UUID. When a video is deleted, the symlink is deleted, and when the bot has not actively being used in any server, the download directory is cleared.

This is to ensure:
- A video can be played within a queue multiple times, and deleting it when the first iteration is over does not delete all files for the same video
- If the same video is downloaded by multiple servers, there is not contention over which player uses which file

Do to disk limitation you may wish to limit the queue size, max length of a video that can be played, max playlist size that can be used.

```
music:
  queue_max_size: 256
  max_song_length: 3600 # In seconds
  server_playlist_max: 64
```


### Caching

You can enable caching so that videos are not deleted automatically when all players are stopped on the server. The bot then has logic to use the previous download when the same video is then downloaded again. There can be a max cache number given that limits the number of videos downloaded at a time, which older/less played videos will be deleted.


```
music:
  download_dir: /tmp/discord
  enable_cache_files: true
```

There is a cache JSON file, stored in the `VideoCache` table with the files kept in the download_dir, that will contain the video url that was gathered, as long as metadata about the video. If a video url is given to download, it will first check for an entry that matches that url in the cache before a download.

You can set the number of local cache entries you want stored in the download dir via `max_cache_files`.


```
music:
  max_cache_files: 2048
```

Additionally if the cache is set there is a `SearchCache` table that is used to map "search strings" to video urls gathered through [yt-dlp](https://github.com/yt-dlp/yt-dlp). For example, if the following command is given

```
!play john prine paradise
```

There will be a `SearchCache` entry that maps the `search_string` "john prine paradise" to the Youtube url that was downloaded. If a cache entry is discovered, this video url will be passed to the download client instead of the search string. This way at the next step when the local cache JSON is searched for the video url, it will return immediately and not need to make a call to [yt-dlp](https://github.com/yt-dlp/yt-dlp).

You can set how many entries you want in this table via `max_search_cache_entries`, it will default to double the size of the cache files option

```
music:
  max_search_cache_entries: 1024
```

Here is a diagram of how the layers of caching interact with each other:

![](./images/caching.png)

Explainers of each item:

- *A* Check for the type of input
- *B/E* If youtube playlist, generate list of full urls
- *C/F* If spotify playlist/album, generate list of "<artist name> <song name>" pairs
- *D* Generic input passed
- *G* Check if `https://` is passed in generic input, and determine if full url or search string
- *H* If a search string, check `SearchCache` for an existing result
- *J* If cache hit, pass full url down
- *K* If full url given, check `VideoCache` for existing items
- *L* If video found in `VideoCache`, return that existing item
- *M* If `SearchCache` has no existing items, pass to download
- *N* If `VideoCache` has no existing items, pass to download
- *O* If original input was a search string, add search string/full url pair to `SearchCache`
- *P* If item downloaded, add new item to cache


### Audio Processing

You can also choose to enable audio processing, which will use FFMPEG to normalize the audio of all videos downloaded, since some Youtube videos have different volumes to them. This will also remove "dead air" from the start and end of videos after they are downloaded.


`ffmpeg` must be installed on the machine is using to be able to use this feature.

```
music:
  enable_audio_processing: true
```

Note that if audio processing is enabled alongside the cache, then two copies of each video will be stored. One for the original download, and another for the processed file.

### Extra YT-DLP Options

You can pass in extra options for the [yt-dlp](https://github.com/yt-dlp/yt-dlp/) client. These should be inputted as a dictionary/hash and will be passed in to the YTDLP client when the download client is created.

```
music:
  extra_ytdlp_options:
    proxy: http://localhost:8888
```

### YTDLP Wait Time

Add a wait time between [yt-dlp](https://github.com/yt-dlp/yt-dlp/) downloads. Defaults to 30 seconds if not given.

```
music:
  ytdlp_wait_period: 50 # Value in seconds
```