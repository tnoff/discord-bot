import asyncio
import random
import sys
import traceback
import typing

from async_timeout import timeout
import discord
from discord.ext import commands
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

class MusicPlayer:
    '''
    A class which is assigned to each guild using the bot for Music.

    This class implements a queue and loop, which allows for different guilds
    to listen to different playlists simultaneously.

    When the bot disconnects from the Voice it's instance will be destroyed.
    '''

    __slots__ = ('bot', '_guild', '_channel',
                 '_cog', 'queue', 'next',
                 'current', 'np', 'np_message', 'volume', 'logger')

    def __init__(self, ctx, logger, queue_max_size):
        self.bot = ctx.bot
        self.logger = logger
        self._guild = ctx.guild
        self._channel = ctx.channel
        self._cog = ctx.cog

        self.logger.info(f'Max length for music queue in guild {self._guild} is {queue_max_size}')
        self.queue = MyQueue(maxsize=queue_max_size)
        self.next = asyncio.Event()

        self.np = None  # Now playing message
        self.np_message = None # Keep np message here in case we pause
        self.volume = .75
        self.current = None

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

            self._guild.voice_client.play(source_dict['source'], after=lambda _: self.bot.loop.call_soon_threadsafe(self.next.set)) #pylint:disable=line-too-long
            self.logger.info(f'Music bot now playing {source_dict["data"]["title"]} requested '
                             f'by {source_dict["requester"]}, url '
                             f'{source_dict["data"]["webpage_url"]}')
            message = f'Now playing {source_dict["data"]["webpage_url"]} ' \
                      f'requested by {source_dict["requester"].name}'
            self.np_message = message
            self.np = await self._channel.send(message)

            await self.next.wait()

            # Make sure the FFmpeg process is cleaned up.
            source_dict['source'].cleanup()
            self.current = None

            try:
                # We are no longer playing this song...
                await self.np.delete()
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
                 'queue_max_size')

    def __init__(self, bot, db_session, logger, ytdl,
                 delete_after, queue_max_size):
        self.bot = bot
        self.db_session = db_session
        self.logger = logger
        self.ytdl = YTDLClient(ytdl, logger)
        self.players = {}
        self.logger.info(f'Will delete all messages after {delete_after} seconds')
        self.delete_after = delete_after # Delete messages after N seconds
        self.queue_max_size = queue_max_size

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
            player = MusicPlayer(ctx, self.logger, queue_max_size=self.queue_max_size)
            self.players[ctx.guild.id] = player

        return player

    @commands.command(name='join', aliases=['awaken'])
    async def connect_(self, ctx, *, channel: discord.VoiceChannel=None): #pylint:disable=bad-whitespace
        '''
        Connect to voice.

        Parameters
        ------------
        channel: discord.VoiceChannel [Optional]
            The channel to connect to. If a channel is not specified, an attempt
            to join the voice channel you are in will be made.

        This command also handles moving the bot to different channels.
        '''
        if not channel:
            try:
                channel = ctx.author.voice.channel
            except AttributeError:
                raise InvalidVoiceChannel('No channel to join. Please either '
                                          'specify a valid channel or join one.')

        vc = ctx.voice_client

        if vc:
            if vc.channel.id == channel.id:
                return
            try:
                self.logger.info(f'Music bot moving to channel {channel.id}')
                await vc.move_to(channel)
            except asyncio.TimeoutError:
                self.logger.error(f'Moving to channel {channel.id} timed out')
                raise VoiceConnectionError(f'Moving to channel: <{channel}> timed out.')
        else:
            try:
                await channel.connect()
            except asyncio.TimeoutError:
                self.logger.error(f'Connecting to channel {channel.id} timed out')
                raise VoiceConnectionError(f'Connecting to channel: <{channel}> timed out.')

        await ctx.send(f'Connected to: {channel}', delete_after=self.delete_after)

    @commands.command(name='play')
    async def play_(self, ctx, *, search: str):
        '''
        Request a song and add it to the queue.

        This command attempts to join a valid voice channel if the bot is not already in one.
        Uses YTDL to automatically search and retrieve a song.

        Parameters
        ------------
        search: str [Required]
            The song to search and retrieve using YTDL. This could be a
            simple search, an ID or URL.
        '''
        await ctx.trigger_typing()

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)

        player = self.get_player(ctx)

        if player.queue.full():
            return await ctx.send('Queue is full, cannot add more songs',
                                  delete_after=self.delete_after)

        source_dict = await self.ytdl.create_source(ctx, search, loop=self.bot.loop)
        if source_dict['source'] is None:
            return await ctx.send(f'Unable to find youtube source for "{search}"',
                                  delete_after=self.delete_after)

        try:
            player.queue.put_nowait(source_dict)
            self.logger.info(f'Adding {source_dict["data"]["title"]} to queue')
            return await ctx.send(f'Added "{source_dict["data"]["title"]}" to queue. '
                                  f'<{source_dict["data"]["webpage_url"]}>',
                                  delete_after=self.delete_after)
        except asyncio.QueueFull:
            return await ctx.send('Queue is full, cannot add more songs',
                                  delete_after=self.delete_after)

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
        except discord.HTTPException:
            pass

        player.np = await ctx.send('Song paused')

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
        except discord.HTTPException:
            pass

        player.np = await ctx.send(player.np_message)

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

    @commands.command(name='shuffle')
    async def shuffle_(self, ctx):
        '''
        Shuffle song queue
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

        items = [clean_title(item['data']['title']) for item in player.queue._queue] #pylint:disable=protected-access
        table_strings = get_table_view(items)
        for table in table_strings:
            await ctx.send(table, delete_after=self.delete_after)


    @commands.command(name='queue')
    async def queue_info(self, ctx):
        '''
        Show the queue of upcoming songs.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if player.queue.empty():
            return await ctx.send('There are currently no more queued songs.',
                                  delete_after=self.delete_after)

        items = [clean_title(item['data']['title']) for item in player.queue._queue] #pylint:disable=protected-access
        table_strings = get_table_view(items)
        for table in table_strings:
            await ctx.send(table, delete_after=self.delete_after)

    @commands.command(name='remove')
    async def remove_item(self, ctx, queue_index):
        '''
        Remove item from queue
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
        return await ctx.send(f'Removed item {item["data"]["title"]} from queue',
                              delete_after=self.delete_after)

    @commands.command(name='bump')
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue
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
        return await ctx.send(f'Bumped item {item["data"]["title"]} to top of queue',
                              delete_after=self.delete_after)


    @commands.command(name='now_playing')
    async def now_playing_(self, ctx):
        '''
        Display information about the currently playing song.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently connected to voice',
                                  delete_after=self.delete_after)

        player = self.get_player(ctx)
        if not player.current:
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        try:
            # Remove our previous now_playing message.
            await player.np.delete()
        except discord.HTTPException:
            pass

        player.np = await ctx.send(player.np_message)

    @commands.command(name='stop')
    async def stop_(self, ctx):
        '''
        Stop the currently playing song and destroy the player.

        !Warning!
            This will destroy the player assigned to your guild,
            also deleting any queued songs and settings.
        '''
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await ctx.send('I am not currently playing anything',
                                  delete_after=self.delete_after)

        await self.cleanup(ctx.guild)

    @commands.group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...', delete_after=self.delete_after)

    @playlist.command(name='create')
    async def playlist_create(self, ctx, *, name: str):
        '''
        Create new playlist
        '''
        try:
            playlist = self.db_session.query(Playlist) #pylint:disable=no-member
            playlist = playlist.filter(Playlist.name == name,
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
        List playlists
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
        Add item to playlist
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            self.logger.info(f'Invalid playlist index {playlist_index} given')
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        title, video_id = await self.ytdl.run_search(search, loop=self.bot.loop)
        try:
            playlist_item = self.db_session.query(PlaylistItem) #pylint:disable=no-member
            playlist_item = playlist_item.filter(PlaylistItem.web_url == video_id).one()
        except NoResultFound:
            playlist_item = PlaylistItem(title=title, web_url=video_id)
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
    async def playlist_item_remove(self, ctx, playlist_index, item_index):
        '''
        Add item to playlist
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            self.logger.info(f'Invalid playlist index {playlist_index} given')
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        try:
            item_index = int(item_index)
        except ValueError:
            return await ctx.send(f'Invalid item index {item_index}',
                                  delete_after=self.delete_after)
        if item_index < 1:
            return await ctx.send(f'Invalid item index {item_index}',
                                  delete_after=self.delete_after)

        query = self.db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
        query = query.join(PlaylistMembership).\
            filter(PlaylistMembership.playlist_id == playlist.id)
        query_results = [item for item in query]
        try:
            item, membership = query_results[item_index - 1]
            title = item.title
            self.__delete_playlist_item(membership, item)
            return await ctx.send(f'Removed item {title} from playlist',
                                  delete_after=self.delete_after)
        except IndexError:
            return await ctx.send(f'Unable to find item {item_index}',
                                  delete_after=self.delete_after)

    @playlist.command(name='show')
    async def playlist_show(self, ctx, playlist_index):
        '''
        Show Items in playlist
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

    @playlist.command(name='delete')
    async def playlist_delete(self, ctx, playlist_index):
        '''
        Delete playlist
        '''
        result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
        if not result:
            return await ctx.send(f'Unable to find playlist {playlist_index}',
                                  delete_after=self.delete_after)
        playlist_name = playlist.name

        self.logger.debug(f'Deleting all playlist items for {playlist.id}')
        query = self.db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
        query = query.join(PlaylistMembership).\
            filter(PlaylistMembership.playlist_id == playlist.id)
        for item, membership in query:
            self.__delete_playlist_item(membership, item)
        self.logger.info(f'Deleting playlist {playlist.id}')
        self.db_session.delete(playlist)
        self.db_session.commit()
        return await ctx.send(f'Deleted playlist {playlist_name}',
                              delete_after=self.delete_after)


    @playlist.command(name='queue')
    async def playlist_queue(self, ctx, playlist_index, sub_command: typing.Optional[str] = ''):
        '''
        Add playlist to queue

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
                                                        f'{item.web_url} {item.title}',
                                                        loop=self.bot.loop, exact_match=True)
            if source_dict is None:
                await ctx.send(f'Unable to find youtube source ' \
                               f'for "{item.web_url} {item.title}"',
                               delete_after=self.delete_after)

            try:
                player.queue.put_nowait(source_dict)
                await ctx.send(f'Added "{source_dict["data"]["title"]}" to queue. '
                               f'<{source_dict["data"]["webpage_url"]}>',
                               delete_after=self.delete_after)
            except asyncio.QueueFull:
                return await ctx.send('Queue is full, cannot add more songs',
                                      delete_after=self.delete_after)

        return await ctx.send(f'Added all songs in playlist {playlist.name} to Queue',
                              delete_after=self.delete_after)
