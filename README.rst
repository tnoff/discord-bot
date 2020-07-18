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
      hello       Say hello to the server
      roll        Get a random number between 1 and number given
      windows     Get an inspirational note about your operating system
    Music:
      bump        Bump item to top of queue
      join        Connect to voice.
      now_playing Display information about the currently playing song.
      pause       Pause the currently playing song.
      play        Request a song and add it to the queue.
      playlist    Playlist functions
      queue       Show the queue of upcoming songs.
      remove      Remove item from queue
      resume      Resume the currently paused song.
      shuffle     Shuffle song queue
      skip        Skip the song.
      stop        Stop the currently playing song and destroy the player.
    Planner:
      planner     Planner functions

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
