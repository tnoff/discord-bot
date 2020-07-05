###########
Discord Bot
###########

Discord bot for music and a few other functions



=====
Setup
=====

You'll need at the very least a discord bot token to setup.
You can find more info about this on the discord site: https://discord.com/developers/docs/topics/oauth2

======
Config
======
The config has a number of options, here are the main ones

.. code::

    [general]
    discord_token=blah-blah-blah-discord-token
    log_file=/logs/discord.log
    db_type=sqlite

    [sqlite]
    file=/opt/discord/.discord.sql

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

The main functions

.. code::

    !join <channel>

Have bot join requested channel

.. code::

    !play <song title>

Have bot add requested song to the queue

.. code::

    !stop

Bot will stop and disconnect from server

------------------
Playlist functions
------------------
Bot allows saving songs to a playlist to use later

The main functions

.. code::

    !playlist list

List all playlists

.. code::

    !playlist create <name>

Create new playlist

.. code::

    !playlist show <playlist_index>

Show songs in a playlist

.. code::

    !playlist queue <playlist_index>

Add songs from playlist to the queue
