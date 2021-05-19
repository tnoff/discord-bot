Bot for discord servers. Includes functions for playing music in voice chats, music playlists, twitter, and a markov chat bot.

h1. Setup

To install the python package, install the pip file within the repo

```
$ git clone https://github.com/tnoff/discord-bot.git
$ pip install discord-bot/discord_bot/
```

To run the bot via the command line

```
discord-bot -c /path/to/config/file
```

h1. Config

Two main arguments are required
- A discord authentication token, you can read more about that [here|https://discord.com/developers/docs/topics/oauth2]
- A log file

```
[general]
discord_token=blah-blah-blah-discord-token
log_file=/logs/discord.log
```



h2. Database
The music playlist, twitter, and markov functions all use a database. Sqlite3 and mysql databases are current supported.


h3. Sqlite
The simpliest database type is a sqlite file
```
[general]
db_type=sqlite

[sqlite]
file=/path/to/sqlite/file
```


h3. Mysql
A mysql server can be used instead of sqlite

```
[general]
db_type=mysql

[mysql]
user=discord
password=example-password
database=discord
host=172.19.0.2
```


h2. Twitter

For twitter functions, you'll need the following config

```
[twitter]
api_key=super-secret
api_key_secret=super-secret
access_token=super-secret
access_token_secret=super-secret
```

h1. Commands

```
General:
    hello        Say hello to the server
    meta         Get meta information for channel and server
    roll         Get a random number between 1 and number given
    windows      Get an inspirational note about your operating system
Markov:
    markov       Markov functions
Music:
    bump         Bump item to top of queue
    clear        Clear all items from queue
    join         Connect to voice channel.
    pause        Pause the currently playing song.
    play         Request a song and add it to the queue.
    playlist     Playlist functions.
    queue        Show current song queue
    remove       Remove item from queue.
    resume       Resume the currently paused song.
    shuffle      Shuffle song queue.
    skip         Skip the song.
    stop         Stop the currently playing song and disconnect bot from voice ...
Planner:
    planner      Planner functions
RoleAssign:
    assign-roles Generate message with all roles.
Twitter:
    twitter      Planner functions
No Category:
    help         Shows this message
```

h2. Music Functions
Use the bot for basic music functions, with a queue to track requested songs.

The main functions:

Have bot join requested channel

```
!join <channel>
```

Have bot add requested song to the queue

```
!play <search input>
```

```
!skip
```

Show the current song queue

```
!queue
```

Shuffle the queue

```
!shuffle
```

Bump item to top of queue

```
!bump <queue_index>
```

Remove item from queue

```
!remove <queue_index>
```

Bot will stop and disconnect from server

```
!stop
```

h2. Playlist functions

Bot allows saving songs to a playlist to use later

The main functions:

List all playlists

```
!playlist list
````

Create new playlist

```
!playlist create <name>
```

Add item to playlist

```
!playlist add <playlist_index> <item>
```

Show songs in a playlist

```
!playlist show <playlist_index>
```

Add songs from playlist to the queue

````
!playlist queue <playlist_index>
```

h2. Role Assignment Bot

Easily assign roles users in server by having them add reaction emojis to a bot message.

Run the roll assignment command

```
!assign-roles
```

A message will be sent to the channel prompting users to add an emoji if they want a given role.

```
For role @rocket-league reply with emoji :zero:
```

The bot will check every minute or so to see if any new roles should be added.

A couple of notes

- The bot will require permissions to add users to roles for this to work
- The bot will only run assign roles with zero permissions. The thinking here is to use these roles as more of a type of mailing list.


h2. Twitter

With twitter api credentials specified in the config file, subscribe channels to twitter feeds. The bot will check every few minutes for new posts,
and then add a message in the channel for each new post.

Subscribe to a given twitter feed ( is specified to the channel where this command is run )

```
!twitter subscribe YakuzaFriday
```

List channel subscriptions

```
!twitter list-subscriptions
```

Unsubscribe from a twitter feed

```
!twitter unsubscribe
```


h2. Markov

Very basic implementation of a Markov Chain of chat history. Turn markov on for a channel and the bot will read all the chat history from the channel, after which is can generate text.

Markov channels are aggregated across the server, meaning that the chat of all channels on a server are used to generate the markov speak commands.


Turn on markov for a channel

```
!markov on
```

Turn off markov for a channel

```
!markov off
```

Have markov generate random text

```
!markov speak
```

Have markov generate random text starting with a given word

```
!markov speak first_word
```

Have markov generate text 64 words long starting with a given phrase

```
!markov speak "starting phrase" 64
```