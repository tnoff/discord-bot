###########
Discord Bot
###########
Discord bot for general use, mostly includes music functions.

You can either use the docker install or install the python package directly.


=====
Setup
=====
Where you install via docker or via the python package directly, you'll need to get a key for the bot.
You can find more info about this on the discord site: https://discord.com/developers/docs/topics/oauth2

----------
Python cli
----------
To install the python package, install the pip file within the repo

.. code::

    git clone https://github.com/tnoff/discord-bot.git
    pip install discord-bot/discord_bot/

To run via the python cli, simply point at a config file.

.. code::

    discord-bot -c /path/to/config/file

------------
Docker Setup
------------
The docker image will install the python package and place in a supervisord job to run automatically.

You'll want to setup two volumes for the docker, one for secrets and one for logs.

The startup script will assume your config is located at `/secret/discord.conf` within the container.

To startup, use something like

.. code::

    docker run -d -v /var/secret/discord:/secret /var/log/discord:/logs/ <image id>

------
Config
------
The config has a number of options, here are the main ones

.. code::

    [general]
    discord_token=blah-blah-blah-discord-token
    log_file=/logs/discord.log
    db_type=sqlite

    [sqlite]
    file=/opt/discord/.discord.sql


--------
Database
--------
For playlist functionality you'll need to have a database in use. If you don't plan on anything heavy, just use a sqlite from the previous example.

-----
Mysql
-----
A mysql server can be used instead of sqlite

.. code::

    [general]
    db_type=mysql

    [mysql]
    user=discord
    password=example-password
    database=discord
    host=172.19.0.2

-------
Twitter
-------
For twitter functions, you'll need the following config

.. code::

    [twitter]
    api_key=super-secret
    api_key_secret=super-secret
    access_token=super-secret
    access_token_secret=super-secret

========
Commands
========

.. code::

    General:
      hello        Say hello to the server
      roll         Get a random number between 1 and number given
      windows      Get an inspirational note about your operating system
    Music:
      bump         Bump item to top of queue
      clear        Clear all items from queue
      join         Connect to voice channel.
      now_playing  Display information about the currently playing song.
      pause        Pause the currently playing song.
      play         Request a song and add it to the queue.
      playlist     Playlist functions.
      queue        Show the queue of upcoming songs.
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


===============
Music functions
===============
Use the bot for basic music functions, with a queue to track requested songs.

The main functions:

Have bot join requested channel

.. code::

    !join <channel>

Have bot add requested song to the queue

.. code::

    !play <song title>

Skip the current song

.. code::

    !skip

Show the current song queue

.. code::

    !queue

Shuffle the queue

.. code::

    !shuffle

Bump item to top of queue

.. code::

    !bump <queue_index>

Remove item from queue

.. code::

    !remove <queue_index>

Bot will stop and disconnect from server

.. code::

    !stop

------------------
Playlist functions
------------------
Bot allows saving songs to a playlist to use later

The main functions:

List all playlists

.. code::

    !playlist list

Create new playlist

.. code::

    !playlist create <name>

Add item to playlist

.. code::

    !playlist add <playlist_index> <item>

Show songs in a playlist

.. code::

    !playlist show <playlist_index>

Add songs from playlist to the queue

.. code::

    !playlist queue <playlist_index>

-------------------
Role Assignment Bot
-------------------
Easily assign roles users in server by having them add reaction emojis to a bot message.

Run the roll assignment command

..code::

    !assign-roles

A message will be sent to the channel prompting users to add an emoji if they want a given role.

.. code::

    For role @rocket-league reply with emoji :zero:

The bot will check every minute or so to see if any new roles should be added.

A couple of notes

- The bot will require permissions to add users to roles for this to work
- The bot will only run assign roles with zero permissions. The thinking here is to use these roles as more of a type of mailing list.

-------
Twitter
-------
With twitter api credentials specified in the config file, subscribe channels to twitter feeds. The bot will check every few minutes for new posts,
and then add a message in the channel for each new post.

Subscribe to a given twitter feed ( is specified to the channel where this command is run )

..code::

    !twitter subscribe ootthursday

List channel subscriptions

..code::

    !twitter list-subscriptions

Unsubscribe from a twitter feed

.. code::

    !twitter unsubscribe

=====
TODOs
=====

------
Markov
------

- Add "private" option to channels
