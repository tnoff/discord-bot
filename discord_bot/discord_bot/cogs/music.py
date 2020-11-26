import asyncio
import random
import sys
import traceback
import typing

from async_timeout import timeout
import discord
from discord.ext import commands
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from discord_bot.database import Playlist, PlaylistItem, PlaylistMembership
from discord_bot.youtube import YTDLClient

# Max title length for table views
MAX_TITLE_LENGTH = 64

# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

class VoiceConnectionError(commands.CommandError):
    '''
    Custom Exception class for connection errors.
    '''
    pass

class InvalidVoiceChannel(VoiceConnectionError):
    '''
    Exception for cases of invalid Voice Channels.
    '''
    pass

class MyQueue(asyncio.Queue):
    '''
    Custom implementation of asyncio Queue
    '''
    def shuffle(self):
        '''
        Shuffle queue
        '''
        random.shuffle(self._queue)
        return True

    def clear(self):
        '''
        Remove all items from queue
        '''
        while self.qsize():
            self._queue.popleft()

    def remove_item(self, queue_index):
        '''
        Remove item from queue
        '''
        if queue_index < 1 or queue_index > self.qsize():
            return None
        # Rotate, remove top, then remove
        for _ in range(1, queue_index):
            self._queue.rotate(-1)
        item = self._queue.popleft()
        for _ in range(1, queue_index):
            self._queue.rotate(1)
        return item

    def bump_item(self, queue_index):
        '''
        Bump item to top of queue
        '''
        item = self.remove_item(queue_index)
        self._queue.appendleft(item)
        return item


def clean_title(stringy, max_length=MAX_TITLE_LENGTH):
    '''
    Make sure title is not longer than max string
    '''
    if len(stringy) > max_length:
        stringy = f'{stringy[0:max_length-3]}...'
    return stringy

def get_table_view(items, max_rows=15):
    '''
    Common function for queue printing
    max_rows    :   Only show max rows in a single print
    '''
    current_index = 0
    table_strings = []

    if not items:
        return None

    # Assume first column is short index name
    # Second column is longer title name
    while True:
        table = ''
        for (count, item) in enumerate(items[current_index:]):
            table = f'{table}\n{count + current_index + 1:3} || {item:64}'
            if count >= max_rows - 1:
                break
        table_strings.append(f'```\n{table}\n```')
        current_index += max_rows
        if current_index >= len(items):
            break
    return table_strings

def get_queue_message(queue):
    '''
    Get full queue message
    '''
    items = [clean_title(item['data']['title']) for item in queue._queue] #pylint:disable=protected-access
    table_strings = get_table_view(items)
    if table_strings is None:
        return None
    return table_strings

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    __slots__ = ('bot', '_guild', '_channel',
                 '_cog', 'queue', 'next',
                 'current', 'np', 'np_string', 'queue_messages', 'queue_strings',
                 'volume', 'logger', 'max_song_length', 'sticky_queue')

    def __init__(self, ctx, logger, queue_max_size, max_song_length):
        self.bot = ctx.bot
        self.logger = logger
        self._guild = ctx.guild
        self._channel = ctx.channel
        self._cog = ctx.cog

        self.logger.info(f'Max length for music queue in guild {self._guild} is {queue_max_size}')
        self.queue = MyQueue(maxsize=queue_max_size)
        self.next = asyncio.Event()

        self.np = None  # Now playing message
        self.queue_messages = [] # Show current queue
        self.np_string = None # Keep np message here in case we pause
        self.queue_strings = None # Keep string here in case we pause
        self.sticky_queue = False # Continually show queue as music plays
        self.volume = .75
        self.current = None
        # Max song length in seconds
        self.max_song_length = max_song_length

        ctx.bot.loop.create_task(self.player_loop())

    async def player_loop(self):
        '''
        Our main player loop.
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            self.next.clear()

            try:
                # Wait for the next song. If we timeout cancel the player and disconnect...
                async with timeout(600):  # 10 minutes...
                    source_dict = await self.queue.get()
            except asyncio.TimeoutError:
                self.logger.error(f'Music bot reached timeout on queue in guild {self._guild}')
                return self.destroy(self._guild)

            source_dict['source'].volume = self.volume
            self.current = source_dict['source']
            try:
                self._guild.voice_client.play(source_dict['source'], after=lambda _: self.bot.loop.call_soon_threadsafe(self.next.set)) #pylint:disable=line-too-long
            except AttributeError:
                self.logger.info('No voice client found, disconnecting')
                return self.destroy(self._guild)
            self.logger.info(f'Music bot now playing {source_dict["data"]["title"]} requested '
                             f'by {source_dict["requester"]}, url '
                             f'{source_dict["data"]["webpage_url"]}')
            message = f'Now playing {source_dict["data"]["webpage_url"]} ' \
                      f'requested by {source_dict["requester"].name}'
            self.np_string = message
            self.np = await self._channel.send(message)

            self.queue_messages = []
            self.queue_strings = get_queue_message(self.queue)
            if self.queue_strings is not None and self.sticky_queue:
                for table in self.queue_strings:
                    self.queue_messages.append(await self._channel.send(table))

            await self.next.wait()

            # Make sure the FFmpeg process is cleaned up.
            source_dict['source'].cleanup()
            self.current = None

            try:
                # We are no longer playing this song...
                await self.np.delete()
                for queue_message in self.queue_messages:
                    await queue_message.delete()
            except discord.HTTPException:
                pass

    def destroy(self, guild):
        '''
        Disconnect and cleanup the player.
        '''
        self.logger.info(f'Removing music bot from guild {self._guild}')
        return self.bot.loop.create_task(self._cog.cleanup(guild))


class Music(commands.Cog): #pylint:disable=too-many-public-methods
    '''
    Music related commands
    '''

    __slots__ = ('bot', 'players', 'db_session', 'logger', 'ytdl', 'delete_after',
                 'queue_max_size', 'max_song_length', 'trim_audio')

    def __init__(self, bot, db_session, logger, ytdl,
                 delete_after, queue_max_size, max_song_length, trim_audio):
        self.bot = bot
        self.db_session = db_session
        self.logger = logger
        self.ytdl = YTDLClient(ytdl, logger)
        self.players = {}
        self.logger.info(f'Will delete all messages after {delete_after} seconds')
        self.delete_after = delete_after # Delete messages after N seconds
        self.queue_max_size = queue_max_size
        self.max_song_length = max_song_length
        self.trim_audio = trim_audio

    async def cleanup(self, guild):
        '''
        Cleanup guild player
        '''
        try:
            await guild.voice_client.disconnect()
        except AttributeError:
            pass

        try:
            del self.players[guild.id]
        except KeyError:
            pass

    async def __local_check(self, ctx):
        '''
        A local check which applies to all commands in this cog.
        '''
        if not ctx.guild:
            raise commands.NoPrivateMessage
        return True

    async def __error(self, ctx, error):
        '''
        A local error handler for all errors arising from commands in this cog.
        '''
        if isinstance(error, commands.NoPrivateMessage):
            try:
                return await ctx.send('This command can not be used in Private Messages.')
            except discord.HTTPException:
                pass
        elif isinstance(error, InvalidVoiceChannel):
            await ctx.send('Error connecting to Voice Channel. '
                           'Please make sure you are in a valid channel or provide me with one')

        print('Ignoring exception in command {}:'.format(ctx.command), file=sys.stderr)
        traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)

    def __get_playlist(self, playlist_index, guild_id): #pylint:disable=no-self-use
        try:
            index = int(playlist_index)
        except ValueError:
            return False, None
        try:
            playlist = self.db_session.query(Playlist)#pylint:disable=no-member
            playlist = playlist.filter(Playlist.server_id == guild_id).\
                            filter(Playlist.server_index == index).one()
        except NoResultFound:
            return False, None
        return True, playlist

    def __delete_playlist_item(self, membership, item):#pylint:disable=no-self-use
        '''
        Delete playlist membership, and check if playlist item is not
        used anymore and should be removed
        '''
        self.db_session.delete(membership)
        self.db_session.commit() #pylint:disable=no-member
        check_query = self.db_session.query(PlaylistMembership) #pylint:disable=no-member
        check_query = check_query.filter(PlaylistMembership.playlist_item_id == item.id)
        check_query = check_query.first()
        if not check_query:
            # Assume we can remove item
            self.db_session.delete(item)
            self.db_session.commit() #pylint:disable=no-member
            return True
        return False

    def get_player(self, ctx):
        '''
        Retrieve the guild player, or generate one.
        '''
        try:
            player = self.players[ctx.guild.id]
        except KeyError:
            player = MusicPlayer(ctx, self.logger, queue_max_size=self.queue_max_size,
                                 max_song_length=self.max_song_length)
            self.players[ctx.guild.id] = player

        return player

    @commands.command(name='join', aliases=['awaken'])
    async def connect_(self, ctx, *, channel: discord.VoiceChannel=None):
        '''
        Connect to voice channel.

        channel: discord.VoiceChannel [Optional]
            The channel to connect to. If a channel is not specified, an attempt
            to join the voice channel you are in will be made.

        This command also handles moving the bot to different channels.
        '''
        if not channel:
            try:
                channel = ctx.author.voice.channel
            except AttributeError as e:
                raise InvalidVoiceChannel('No channel to join. Please either '
                                          'specify a valid channel or join one.') from e

        vc = ctx.voice_client

        if vc:
            if vc.channel.id == channel.id:
                return
            try:
                self.logger.info(f'Music bot moving to channel {channel.id}')
                await vc.move_to(channel)
            except asyncio.TimeoutError as e:
                self.logger.error(f'Moving to channel {channel.id} timed out')
                raise VoiceConnectionError(f'Moving to channel: <{channel}> timed out.') from e
        else:
            try:
                await channel.connect()
            except asyncio.TimeoutError as e:
                self.logger.error(f'Connecting to channel {channel.id} timed out')
                raise VoiceConnectionError(f'Connecting to channel: <{channel}> timed out.') from e

        await ctx.send(f'Connected to: {channel}', delete_after=self.delete_after)

    @commands.command(name='play')
    async def play_(self, ctx, *, search: str):
        '''
        Request a song and add it to the queue.

        search: str [Required]
            The song to search and retrieve from youtube.
            This could be a simple search, an ID or URL.
        '''
        await ctx.trigger_typing()

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)

        player = self.get_player(ctx)

        if player.queue.full():
            return await ctx.send('Queue is full, cannot add more songs',
                                  delete_after=self.delete_after)

        source_dict = await self.ytdl.create_source(ctx, search, loop=self.bot.loop,
                                                    max_song_length=self.max_song_length,
                                                    trim_audio=self.trim_audio)
        try:
            if source_dict['data']['duration'] > self.max_song_length:
                return await ctx.send(f'Unable to add <{source_dict["data"]["webpage_url"]}>'
                                      f' to queue, exceeded max length '
                                      f'{self.max_song_length} seconds')
        except TypeError:
            # Data likely is None
            if source_dict['source'] is None:
                return await ctx.send(f'Unable to find youtube source for "{search}"',
                                      delete_after=self.delete_after)

        try:
            player.queue.put_nowait(source_dict)
            self.logger.info(f'Adding {source_dict["data"]["title"]} to queue')
            await ctx.send(f'Added "{source_dict["data"]["title"]}" to queue. '
                           f'<{source_dict["data"]["webpage_url"]}>',
                           delete_after=self.delete_after)
        except asyncio.QueueFull:
            await ctx.send('Queue is full, cannot add more songs',
                           delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except discord.HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None and player.sticky_queue:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

    @commands.command(name='pause')
    async def pause_(self, ctx):
        '''
        Pause the currently playing song.
        '''
        vc = ctx.voice_client

        player = self.get_player(ctx)
        if not player.current or not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)
        if vc.is_paused():
            return
        vc.pause()
        try:
            # Remove our previous now_playing message.
            await player.np.delete()
            for queue_message in player.queue_messages:
                await queue_message.delete()
        except discord.HTTPException:
            pass

        player.np = await ctx.send('Player paused')
        player.queue_messages = []

    @commands.command(name='resume')
    async def resume_(self, ctx):
        '''
        Resume the currently paused song.
        '''
        vc = ctx.voice_client

        player = self.get_player(ctx)
        if not player.current or not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)
        if not vc.is_paused():
            return
        vc.resume()
        try:
            # Remove our previous now_playing message.
            await player.np.delete()
            for queue_message in player.queue_messages:
                await queue_message.delete()
        except discord.HTTPException:
            pass

        player.np = await ctx.send(player.np_string)
        if player.queue_strings is not None and player.sticky_queue:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

    @commands.command(name='skip')
    async def skip_(self, ctx):
        '''
        Skip the song.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        if not vc.is_paused() and not vc.is_playing():
            return
        vc.stop()
        await ctx.send('Skipping song',
                       delete_after=self.delete_after)

        player = self.get_player(ctx)
        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except discord.HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None and player.sticky_queue:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

    @commands.command(name='clear')
    async def clear(self, ctx):
        '''
        Clear all items from queue
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)
        player.queue.clear()
        await ctx.send('Cleared all items from queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            await queue_message.delete()

    @commands.command(name='queue')
    async def queue_(self, ctx, sub_command: typing.Optional[str] = ''):
        '''
        Show current song queue

        sub_command is optional, but can be used to turn off/on a "sticky" queue.
        Sticky queues will not be deleted like other bot output.

        command: "on", turn on sticky queue
        command: "off", turn off sticky queue
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)
        exit_early = False
        if sub_command:
            if sub_command.lower() == 'on':
                player.sticky_queue = True
            elif sub_command.lower() == 'off':
                player.sticky_queue = False
                exit_early = True
            else:
                return await ctx.send(f'Invalid sub_command {sub_command}',
                                      delete_after=self.delete_after)

        # Delete any old queue message regardless of on/off
        for queue_message in player.queue_messages:
            await queue_message.delete()
        player.queue_messages = []

        # If you turned the queue off, exit now
        if exit_early:
            return

        player.queue_strings = get_queue_message(player.queue)
        if player.queue_strings is not None:
            if player.sticky_queue:
                for table in player.queue_strings:
                    player.queue_messages.append(await ctx.send(table))
            else:
                for table in player.queue_strings:
                    await ctx.send(f'{table}', delete_after=self.delete_after)

    @commands.command(name='shuffle')
    async def shuffle_(self, ctx):
        '''
        Shuffle song queue.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)
        player.queue.shuffle()

        # Reset queue messages
        for queue_message in player.queue_messages:
            await queue_message.delete()

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None:
            if player.sticky_queue:
                for table in player.queue_strings:
                    player.queue_messages.append(await ctx.send(table))
            else:
                for table in player.queue_strings:
                    await ctx.send(f'{table}', delete_after=self.delete_after)

    @commands.command(name='remove')
    async def remove_item(self, ctx, queue_index):
        '''
        Remove item from queue.

        queue_index: integer [Required]
            Position in queue of song that will be removed.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            self.logger.info(f'Queue entered was invalid {queue_index}')
            return await ctx.send(f'Invalid queue index {queue_index}',
                                  delete_after=self.delete_after)

        item = player.queue.remove_item(queue_index)
        if item is None:
            self.logger.info(f'Unable to remove queue index {queue_index}')
            return ctx.send(f'Unable to remove queue index {queue_index}',
                            delete_after=self.delete_after)
        await ctx.send(f'Removed item {item["data"]["title"]} from queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except discord.HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None and player.sticky_queue:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

    @commands.command(name='bump')
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue

        queue_index: integer [Required]
            Position in queue of song that will be removed.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            self.logger.info(f'Queue entered was invalid {queue_index}')
            return await ctx.send(f'Invalid queue index {queue_index}',
                                  delete_after=self.delete_after)

        item = player.queue.bump_item(queue_index)
        if item is None:
            self.logger.info(f'Unable to remove queue index {queue_index}')
            return ctx.send(f'Unable to bump queue index {queue_index}',
                            delete_after=self.delete_after)
        await ctx.send(f'Bumped item {item["data"]["title"]} to top of queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except discord.HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None and player.sticky_queue:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))

    @commands.command(name='stop')
    async def stop_(self, ctx):
        '''
        Stop the currently playing song and disconnect bot from voice chat.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        await self.cleanup(ctx.guild)

    @commands.group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions.
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...', delete_after=self.delete_after)

    @playlist.command(name='create')
    async def playlist_create(self, ctx, *, name: str):
        '''
        Create new playlist.

        name: str [Required]
            Name of new playlist to create
        '''
        try:
            playlist = self.db_session.query(Playlist) #pylint:disable=no-member
            playlist = playlist.filter(func.lower(Playlist.name) == func.lower(name),
                                       Playlist.server_id == ctx.guild.id).one()
        except NoResultFound:
            self.logger.info(f'No playlist with name {name} in '
                             f'server {ctx.guild.id} found, continuing')
        # Grab latest server_index that matches server_id
        query = self.db_session.query(Playlist) #pylint:disable=no-member
        query = query.filter(Playlist.server_id == ctx.guild.id).\
                    order_by(Playlist.server_index.desc()).first()
        if query:
            server_index = query.server_index + 1
        else:
            # If none found, assume 1 is fine
            server_index = 1

        playlist = Playlist(
            name=name,
            server_id=ctx.guild.id,
            server_index=server_index,
        )
        self.db_session.add(playlist) #pylint:disable=no-member
        self.db_session.commit() #pylint:disable=no-member
        self.logger.info(f'Playlist created {playlist.id}')
        return await ctx.send(f'Created playlist {playlist.server_index}',
                              delete_after=self.delete_after)

    @playlist.command(name='list')
    async def playlist_list(self, ctx):
        '''
        List playlists.
        '''
        self.logger.info(f'Playlist list called for server {ctx.guild.id}')
        playlist_items = self.db_session.query(Playlist)
        playlist_items = playlist_items.\
            filter(Playlist.server_id == ctx.guild.id)
        playlist_items = [p for p in playlist_items]

        if not playlist_items:
            return await ctx.send('No playlists in database',
                                  delete_after=self.delete_after)
        table = ''
        for playlist in playlist_items:
            table = f'{table}{playlist.server_index:3} || {clean_title(playlist.name):64}\n'
        return await ctx.send(f'```{table}```', delete_after=self.delete_after)

    @playlist.command(name='add')
    async def playlist_add(self, ctx, playlist_index, *, search: str):
        '''
        Add item to playlist.

        playlist_index: integer [Required]
            ID of playlist
        search: str [Required]
            The song to search and retrieve from youtube.
            This could be a simple search, an ID or URL.
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            self.logger.info(f'Invalid playlist index {playlist_index} given')
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        data = await self.ytdl.run_search(search, loop=self.bot.loop)
        if data is None:
            return await ctx.send(f'Unable to find video for search {search}')
        if data['duration'] > self.max_song_length:
            return await ctx.send(f'Unable to add <{data["webpage_url"]}>'
                                  f' to queue, exceeded max length '
                                  f'{self.max_song_length} seconds')
        try:
            playlist_item = self.db_session.query(PlaylistItem) #pylint:disable=no-member
            playlist_item = playlist_item.filter(PlaylistItem.video_id == data['id']).one()
        except NoResultFound:
            playlist_item = PlaylistItem(title=data['title'], video_id=data['id'])
            self.db_session.add(playlist_item) #pylint:disable=no-member
            self.db_session.commit() #pylint:disable=no-member
        try:
            playlist_membership = PlaylistMembership(playlist_id=playlist.id,
                                                     playlist_item_id=playlist_item.id)
            self.db_session.add(playlist_membership) #pylint:disable=no-member
            self.db_session.commit() #pylint:disable=no-member
            return await ctx.send(f'Added "{playlist_item.title}" '
                                  f'to playlist "{playlist.name}"', delete_after=self.delete_after)
        except IntegrityError:
            self.db_session.rollback() #pylint:disable=no-member
            return await ctx.send(f'Unable to add "{playlist_item.title}" '
                                  f'to playlist "{playlist.name}', delete_after=self.delete_after)

    @playlist.command(name='item-remove')
    async def playlist_item_remove(self, ctx, playlist_index, song_index):
        '''
        Add item to playlist

        playlist_index: integer [Required]
            ID of playlist
        song_index: integer [Required]
            ID of song to remove
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            self.logger.info(f'Invalid playlist index {playlist_index} given')
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        try:
            song_index = int(song_index)
        except ValueError:
            return await ctx.send(f'Invalid item index {song_index}',
                                  delete_after=self.delete_after)
        if song_index < 1:
            return await ctx.send(f'Invalid item index {song_index}',
                                  delete_after=self.delete_after)

        query = self.db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
        query = query.join(PlaylistMembership).\
            filter(PlaylistMembership.playlist_id == playlist.id)
        query_results = [item for item in query]
        try:
            item, membership = query_results[song_index - 1]
            title = item.title
            self.__delete_playlist_item(membership, item)
            return await ctx.send(f'Removed item {title} from playlist',
                                  delete_after=self.delete_after)
        except IndexError:
            return await ctx.send(f'Unable to find item {song_index}',
                                  delete_after=self.delete_after)

    @playlist.command(name='show')
    async def playlist_show(self, ctx, playlist_index):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)

        query = self.db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
        query = query.join(PlaylistMembership).\
            filter(PlaylistMembership.playlist_id == playlist.id)
        items = [clean_title(item.title) for (item, _membership) in query]

        if not items:
            return await ctx.send('No playlist items in database',
                                  delete_after=self.delete_after)

        tables = get_table_view(items)
        for table in tables:
            await ctx.send(table, delete_after=self.delete_after)

    def __playlist_update_server_indexes(self, server_id):
        '''
        Once playlist is deleted, update server indexes so that values
        re now incremental
        '''
        playlists = self.db_session.query(Playlist)
        playlists = playlists.filter(Playlist.server_id == server_id).\
                    order_by(Playlist.server_index)
        for (current_index, playlist) in enumerate(playlists):
            if (current_index + 1) != playlist.server_index:
                playlist.server_index = current_index + 1
                self.db_session.commit()

    @playlist.command(name='delete')
    async def playlist_delete(self, ctx, playlist_index):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        playlist_name = playlist.name
        old_server_id = playlist.server_id
        self.logger.debug(f'Deleting all playlist items for {playlist.id}')
        query = self.db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
        query = query.join(PlaylistMembership).\
            filter(PlaylistMembership.playlist_id == playlist.id)
        for item, membership in query:
            self.__delete_playlist_item(membership, item)
        self.logger.info(f'Deleting playlist {playlist.id}')
        self.db_session.delete(playlist)
        self.db_session.commit()
        self.__playlist_update_server_indexes(old_server_id)
        return await ctx.send(f'Deleted playlist {playlist_name}',
                              delete_after=self.delete_after)

    @playlist.command(name='rename')
    async def playlist_rename(self, ctx, playlist_index, *, playlist_name: str):
        '''
        Rename playlist to new name

        playlist_index: integer [Required]
            ID of playlist
        playlist_name: str [Required]
            New name of playlist
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        playlist.name = playlist_name
        self.db_session.commit()
        return await ctx.send(f'Renamed playlist {playlist_index} to name {playlist_name}')

    @playlist.command(name='queue')
    async def playlist_queue(self, ctx, playlist_index, sub_command: typing.Optional[str] = ''): #pylint:disable=too-many-branches
        '''
        Add playlist to queue

        playlist_index: integer [Required]
            ID of playlist
        Sub commands - [shuffle]
            shuffle - Shuffle playlist when entering it into queue
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        shuffle = False
        # Make sure sub command is valid
        if sub_command:
            if sub_command.lower() == 'shuffle':
                shuffle = True
            else:
                return await ctx.send(f'Invalid sub command {sub_command}',
                                      delete_after=self.delete_after)

        vc = ctx.voice_client
        if not vc:
            await ctx.invoke(self.connect_)
        player = self.get_player(ctx)

        query = self.db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
        query = query.join(PlaylistMembership).\
            filter(PlaylistMembership.playlist_id == playlist.id)
        playlist_items = [item for (item, _membership) in query]

        if shuffle:
            await ctx.send('Shuffling playlist items',
                           delete_after=self.delete_after)
            random.shuffle(playlist_items)

        for item in playlist_items:
            if player.queue.full():
                return await ctx.send('Queue is full, cannot add more songs',
                                      delete_after=self.delete_after)

            source_dict = await self.ytdl.create_source(ctx,
                                                        f'{item.video_id}',
                                                        loop=self.bot.loop, exact_match=True,
                                                        max_song_length=self.max_song_length,
                                                        trim_audio=self.trim_audio)
            try:
                if source_dict['data']['duration'] > self.max_song_length:
                    await ctx.send(f'Unable to add <{source_dict["data"]["webpage_url"]}>'
                                   f' to queue, exceeded max length '
                                   f'{self.max_song_length} seconds')
                    continue
            except TypeError:
                # Data dict is likely none
                if source_dict['source'] is None:
                    await ctx.send(f'Unable to find youtube source ' \
                                   f'for "{item.title}", "{item.video_id}"',
                                   delete_after=self.delete_after)
                    continue
            try:
                player.queue.put_nowait(source_dict)
                await ctx.send(f'Added "{source_dict["data"]["title"]}" to queue. '
                               f'<{source_dict["data"]["webpage_url"]}>',
                               delete_after=self.delete_after)
            except asyncio.QueueFull:
                await ctx.send('Queue is full, cannot add more songs',
                               delete_after=self.delete_after)
                break

        await ctx.send(f'Added all songs in playlist {playlist.name} to Queue',
                       delete_after=self.delete_after)

        # Reset queue messages
        for queue_message in player.queue_messages:
            try:
                await queue_message.delete()
            except discord.HTTPException:
                pass

        player.queue_strings = get_queue_message(player.queue)
        player.queue_messages = []
        if player.queue_strings is not None and player.sticky_queue:
            for table in player.queue_strings:
                player.queue_messages.append(await ctx.send(table))
