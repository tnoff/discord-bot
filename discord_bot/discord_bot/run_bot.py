import argparse
import asyncio
from functools import partial
import os
import random
import re
import sys
import traceback
import typing

from async_timeout import timeout
import discord
from discord.ext import commands
from moviepy.editor import AudioFileClip
from numpy import sqrt
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from youtube_dl import YoutubeDL


from discord_bot import functions
from discord_bot.database import Playlist, PlaylistItem, PlaylistMembership
from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.utils import get_logger, load_args, get_db_session

# Delete messages after N seconds
DELETE_AFTER = 120
# Max queue size
QUEUE_MAX_SIZE = 35
# Max title length for table views
MAX_TITLE_LENGTH = 64
# Only trim audio if buffer exceeding in start or end
AUDIO_BUFFER = 30

YOUTUBE_URL_REGEX = r'https://www.youtube.com/watch[\?]v=(?P<video_id>.*)'

def parse_args():
    '''
    Basic cli arg parser
    '''
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT,
                        help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")
    parser.add_argument("--discord-token", "-t",
                        help="Discord token, defaults to DISCORD_TOKEN env arg")
    parser.add_argument("--download-dir", "-d", default="/tmp/",
                        help="Directory for downloading youtube files")
    return parser.parse_args()


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

def main(): #pylint:disable=too-many-statements
    '''
    Main loop
    '''
    settings = load_args(vars(parse_args()))

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
    bot = commands.Bot(command_prefix='!')
    # Setup database
    db_session = get_db_session(settings)

    ytdlopts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(settings['download_dir'],
                                '%(extractor)s-%(id)s-%(title)s.%(ext)s'),
        'restrictfilenames': True,
        'noplaylist': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
        'logtostderr': False,
        'logger': logger,
        'default_search': 'auto',
        'source_address': '0.0.0.0'  # ipv6 addresses cause issues sometimes
    }
    ytdl = YoutubeDL(ytdlopts)


    def trim_audio(video_file):
        '''
        Trim dead audio from beginning and end of file
        video_file      :   Path to video file
        '''
        # Basic logic adapted from http://zulko.github.io/blog/2014/07/04/automatic-soccer-highlights-compilations-with-python/ pylint:disable=line-too-long
        # First get an array of volume at each second of file
        clip = AudioFileClip(video_file)
        total_length = clip.duration
        cut = lambda i: clip.subclip(i, i+1).to_soundarray(fps=1)
        volume = lambda array: sqrt(((1.0*array)**2).mean())
        volumes = [volume(cut(i)) for i in range(0, int(clip.duration-1))]

        # Get defaults
        volume_length = len(volumes)
        start_index = 0
        end_index = volume_length - 1

        # Remove all dead audio from front
        for (index, vol) in enumerate(volumes):
            if vol != 0:
                start_index = index
                break
        # Remove dead audio from back
        for (index, vol) in enumerate(reversed(volumes[start_index:])):
            if vol != 0:
                end_index = volume_length - index
                break

        # Remove one second from start, and add one to back, for safety
        if start_index != 0:
            start_index -= 1
        end_index += 1

        logger.debug(f'Audio trim: Total file length {total_length}, start {start_index}, end {end_index}')
        if start_index < AUDIO_BUFFER and end_index > (int(total_length) - AUDIO_BUFFER):
            logger.info('Not enough dead audio at beginning or end of file, skipping audio trim')
            return False, video_file

        logger.info(f'Trimming file to start at {start_index} and end at {end_index}')
        # Write file with changes, then overwrite
        # Use mp3 by default for easier encoding
        file_name, _ext = os.path.splitext(video_file)
        new_path = f'{file_name}-edited.mp3'
        new_clip = clip.subclip(t_start=start_index, t_end=end_index)
        new_clip.write_audiofile(new_path)
        logger.info('Removing old file {video_file}')
        os.remove(video_file)
        return True, new_path

    # Music bot setup
    # Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34
    class YTDLSource(discord.PCMVolumeTransformer):
        '''
        Youtube DL Source
        '''
        def __init__(self, source, *, data, requester):
            super().__init__(source)
            self.requester = requester

            self.title = data.get('title')
            self.webpage_url = data.get('webpage_url')
            logger.info(f'Music bot adding new source: {self.webpage_url}, '
                        f'requested by {self.requester}')

            # YTDL info dicts (data) have other useful information you might want
            # https://github.com/rg3/youtube-dl/blob/master/README.md

        def __getitem__(self, item: str):
            '''
            Allows us to access attributes similar to a dict.

            This is only useful when you are NOT downloading.
            '''
            return self.__getattribute__(item)

        @classmethod
        async def run_search(cls, search: str, *, loop):
            '''
            Run search and return url
            '''
            loop = loop or asyncio.get_event_loop()

            to_run = partial(ytdl.extract_info, url=search, download=False)
            data = await loop.run_in_executor(None, to_run)

            if 'entries' in data:
                # take first item from a playlist
                data = data['entries'][0]
            return data['title'], data['webpage_url']

        @classmethod
        async def create_source(cls, ctx, search: str, *, loop):
            '''
            Create source from youtube search
            '''
            loop = loop or asyncio.get_event_loop()

            to_run = partial(ytdl.extract_info, url=search, download=True)
            data = await loop.run_in_executor(None, to_run)

            if 'entries' in data:
                # take first item from a playlist
                data = data['entries'][0]

            logger.info(f'Starting download of video {data["title"]}, url {data["webpage_url"]}')
            source = ytdl.prepare_filename(data)
            logger.info(f'Downloaded file {source} from youtube url {data["webpage_url"]}')
            to_run = partial(trim_audio, video_file=source)
            changed, file_name = await loop.run_in_executor(None, to_run)
            if changed:
                logger.info(f'Created trimmed audio file to {file_name}')
            logger.info(f'Music bot adding {data["title"]} to the queue {data["webpage_url"]}')
            return cls(discord.FFmpegPCMAudio(file_name), data=data, requester=ctx.author)


    class MusicPlayer:
        '''
        A class which is assigned to each guild using the bot for Music.

        This class implements a queue and loop, which allows for different guilds
        to listen to different playlists simultaneously.

        When the bot disconnects from the Voice it's instance will be destroyed.
        '''

        __slots__ = ('bot', '_guild', '_channel',
                     '_cog', 'queue', 'next',
                     'current', 'np', 'volume')

        def __init__(self, ctx):
            logger.info(f'Adding music bot to guild {ctx.guild}')
            self.bot = ctx.bot
            self._guild = ctx.guild
            self._channel = ctx.channel
            self._cog = ctx.cog

            self.queue = MyQueue(maxsize=QUEUE_MAX_SIZE)
            self.next = asyncio.Event()

            self.np = None  # Now playing message
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
                        source = await self.queue.get()
                except asyncio.TimeoutError:
                    logger.error(f'Music bot reached timeout on queue in guild {self._guild}')
                    return self.destroy(self._guild)

                source.volume = self.volume
                self.current = source

                self._guild.voice_client.play(source, after=lambda _: self.bot.loop.call_soon_threadsafe(self.next.set)) #pylint:disable=line-too-long
                logger.info(f'Music bot now playing {source.title} requested '
                            f'by {source.requester}, url {source.webpage_url}')
                message = f'```Now playing "{source.title}" ' \
                          f'requested by "{source.requester.name}"```'
                self.np = await self._channel.send(message)

                await self.next.wait()

                # Make sure the FFmpeg process is cleaned up.
                source.cleanup()
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
            logger.info(f'Removing music bot from guild {self._guild}')
            return self.bot.loop.create_task(self._cog.cleanup(guild))


    class Music(commands.Cog): #pylint:disable=too-many-public-methods
        '''
        Music related commands
        '''

        __slots__ = ('bot', 'players')

        def __init__(self, bot):
            self.bot = bot
            self.players = {}

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
                playlist = db_session.query(Playlist)# #pylint:disable=no-member
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
            db_session.delete(membership)
            db_session.commit() #pylint:disable=no-member
            check_query = db_session.query(PlaylistMembership) #pylint:disable=no-member
            check_query = check_query.filter(PlaylistMembership.playlist_item_id == item.id)
            check_query = check_query.first()
            if not check_query:
                # Assume we can remove item
                db_session.delete(item)
                db_session.commit() #pylint:disable=no-member
                return True
            return False

        def get_player(self, ctx):
            '''
            Retrieve the guild player, or generate one.
            '''
            try:
                player = self.players[ctx.guild.id]
            except KeyError:
                player = MusicPlayer(ctx)
                self.players[ctx.guild.id] = player

            return player

        @commands.command(name='join')
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
                    logger.info(f'Music bot moving to channel {channel.id}')
                    await vc.move_to(channel)
                except asyncio.TimeoutError:
                    logger.error(f'Moving to channel {channel.id} timed out')
                    raise VoiceConnectionError(f'Moving to channel: <{channel}> timed out.')
            else:
                try:
                    await channel.connect()
                except asyncio.TimeoutError:
                    logger.error(f'Connecting to channel {channel.id} timed out')
                    raise VoiceConnectionError(f'Connecting to channel: <{channel}> timed out.')

            await ctx.send(f'```Connected to: {channel}```', delete_after=DELETE_AFTER)

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
                                      delete_after=DELETE_AFTER)

            source = await YTDLSource.create_source(ctx, search, loop=self.bot.loop)

            try:
                player.queue.put_nowait(source)
                logger.info(f'Adding {source.title} to queue')
                return await ctx.send(f'```ini\n[Added {source.title} to the Queue '
                                      f'{source.webpage_url}\n```',
                                      delete_after=DELETE_AFTER)
            except asyncio.QueueFull:
                return await ctx.send('Queue is full, cannot add more songs',
                                      delete_after=DELETE_AFTER)

        @commands.command(name='pause')
        async def pause_(self, ctx):
            '''
            Pause the currently playing song.
            '''
            vc = ctx.voice_client

            player = self.get_player(ctx)
            if not player.current or not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)
            if vc.is_paused():
                return
            logger.info(f'Song paused by {ctx.author.name}')
            vc.pause()
            try:
                # Remove our previous now_playing message.
                await player.np.delete()
            except discord.HTTPException:
                pass

            player.np = await ctx.send(f'```Song paused: "{vc.source.title}", '
                                       f'requested by "{vc.source.requester.name}"```')

        @commands.command(name='resume')
        async def resume_(self, ctx):
            '''
            Resume the currently paused song.
            '''
            vc = ctx.voice_client

            player = self.get_player(ctx)
            if not player.current or not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)
            if not vc.is_paused():
                return
            logger.info(f'Song resumed by {ctx.author.name}')
            vc.resume()
            try:
                # Remove our previous now_playing message.
                await player.np.delete()
            except discord.HTTPException:
                pass

            player.np = await ctx.send(f'```Now Playing: "{vc.source.title}", '
                                       f'requested by "{vc.source.requester.name}"```')

        @commands.command(name='skip')
        async def skip_(self, ctx):
            '''
            Skip the song.
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)

            if not vc.is_paused() and not vc.is_playing():
                return
            logger.info(f'Song skipped by {ctx.author.name}')
            vc.stop()
            await ctx.send(f'```"{ctx.author.name}": Skipped the song```',
                           delete_after=DELETE_AFTER)

        @commands.command(name='shuffle')
        async def shuffle_(self, ctx):
            '''
            Shuffle song queue
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)

            player = self.get_player(ctx)
            if player.queue.empty():
                return await ctx.send('There are currently no more queued songs.',
                                      delete_after=DELETE_AFTER)
            player.queue.shuffle()
            logger.info(f'Queue shuffled by {ctx.author.name}')

            items = [clean_title(item['title']) for item in player.queue._queue] #pylint:disable=protected-access
            table_strings = get_table_view(items)
            for table in table_strings:
                await ctx.send(table, delete_after=DELETE_AFTER)


        @commands.command(name='queue')
        async def queue_info(self, ctx):
            '''
            Show the queue of upcoming songs.
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently connected to voice',
                                      delete_after=DELETE_AFTER)

            player = self.get_player(ctx)
            if player.queue.empty():
                return await ctx.send('There are currently no more queued songs.',
                                      delete_after=DELETE_AFTER)
            logger.info(f'Queue called by {ctx.author.name}')

            items = [clean_title(item['title']) for item in player.queue._queue] #pylint:disable=protected-access
            table_strings = get_table_view(items)
            for table in table_strings:
                await ctx.send(table, delete_after=DELETE_AFTER)

        @commands.command(name='remove')
        async def remove_item(self, ctx, queue_index):
            '''
            Remove item from queue
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently connected to voice',
                                      delete_after=DELETE_AFTER)

            player = self.get_player(ctx)
            if player.queue.empty():
                return await ctx.send('There are currently no more queued songs.',
                                      delete_after=DELETE_AFTER)

            try:
                queue_index = int(queue_index)
            except ValueError:
                logger.info(f'Queue entered was invalid {queue_index}')
                return await ctx.send(f'Invalid queue index {queue_index}',
                                      delete_after=DELETE_AFTER)

            item = player.queue.remove_item(queue_index)
            if item is None:
                logger.info(f'Unable to remove queue index {queue_index}')
                return ctx.send(f'Unable to remove queue index {queue_index}',
                                delete_after=DELETE_AFTER)
            logger.info(f'Removed item {item["title"]} from queue')
            return await ctx.send(f'Removed item {item["title"]} from queue',
                                  delete_after=DELETE_AFTER)

        @commands.command(name='bump')
        async def bump_item(self, ctx, queue_index):
            '''
            Bump item to top of queue
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently connected to voice',
                                      delete_after=DELETE_AFTER)

            player = self.get_player(ctx)
            if player.queue.empty():
                return await ctx.send('There are currently no more queued songs.',
                                      delete_after=DELETE_AFTER)

            try:
                queue_index = int(queue_index)
            except ValueError:
                logger.info(f'Queue entered was invalid {queue_index}')
                return await ctx.send(f'Invalid queue index {queue_index}',
                                      delete_after=DELETE_AFTER)

            item = player.queue.bump_item(queue_index)
            if item is None:
                logger.info(f'Unable to remove queue index {queue_index}')
                return ctx.send(f'Unable to bump queue index {queue_index}',
                                delete_after=DELETE_AFTER)
            logger.info(f'Bumped item {item["title"]} to top of the queue')
            return await ctx.send(f'Bumped item {item["title"]} to top of queue',
                                  delete_after=DELETE_AFTER)


        @commands.command(name='now_playing')
        async def now_playing_(self, ctx):
            '''
            Display information about the currently playing song.
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently connected to voice',
                                      delete_after=DELETE_AFTER)

            player = self.get_player(ctx)
            if not player.current:
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)

            try:
                # Remove our previous now_playing message.
                await player.np.delete()
            except discord.HTTPException:
                pass

            player.np = await ctx.send(f'```Now Playing: "{vc.source.title}", '
                                       f'requested by "{vc.source.requester.name}"```')

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
                                      delete_after=DELETE_AFTER)

            await self.cleanup(ctx.guild)

        @commands.group(name='playlist', invoke_without_command=False)
        async def playlist(self, ctx):
            '''
            Playlist functions
            '''
            if ctx.invoked_subcommand is None:
                await ctx.send('Invalid sub command passed...', delete_after=DELETE_AFTER)

        @playlist.command(name='create')
        async def playlist_create(self, ctx, *, name: str):
            '''
            Create new playlist
            '''
            try:
                playlist = db_session.query(Playlist) #pylint:disable=no-member
                playlist = playlist.filter(Playlist.name == name,
                                           Playlist.server_id == ctx.guild.id).one()
            except NoResultFound:
                logger.info(f'No playlist with name {name} in '
                            f'server {ctx.guild.id} found, continuing')
            # Grab latest server_index that matches server_id
            query = db_session.query(Playlist) #pylint:disable=no-member
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
            db_session.add(playlist) #pylint:disable=no-member
            db_session.commit() #pylint:disable=no-member
            logger.info(f'Playlist created {playlist.id}')
            return await ctx.send(f'Created playlist {playlist.server_index}',
                                  delete_after=DELETE_AFTER)

        @playlist.command(name='list')
        async def playlist_list(self, ctx):
            '''
            List playlists
            '''
            logger.info(f'Playlist list called for server {ctx.guild.id}')
            table = ''
            playlist_items = db_session.query(Playlist)
            playlist_items = playlist_items.\
                filter(Playlist.server_id == ctx.guild.id)
            playlist_items = [p for p in playlist_items]

            if not playlist_items:
                return await ctx.send('No playlists in database',
                                      delete_after=DELETE_AFTER)

            for playlist in playlist_items:
                table = f'{table}{playlist.server_index:3} || {clean_title(playlist.name):64}\n'
            return await ctx.send(f'```\n{table}```', delete_after=DELETE_AFTER)

        @playlist.command(name='add')
        async def playlist_add(self, ctx, playlist_index, *, search: str):
            '''
            Add item to playlist
            '''
            result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
            if not result:
                logger.info(f'Invalid playlist index {playlist_index} given')
                return await ctx.send(f'Unable to find playlist {playlist_index}',
                                      delete_after=DELETE_AFTER)
            title, url = await YTDLSource.run_search(search, loop=self.bot.loop)
            try:
                playlist_item = db_session.query(PlaylistItem) #pylint:disable=no-member
                playlist_item = playlist_item.filter(PlaylistItem.web_url == url).one()
            except NoResultFound:
                playlist_item = PlaylistItem(title=title, web_url=url)
                db_session.add(playlist_item) #pylint:disable=no-member
                db_session.commit() #pylint:disable=no-member
            try:
                playlist_membership = PlaylistMembership(playlist_id=playlist.id,
                                                         playlist_item_id=playlist_item.id)
                db_session.add(playlist_membership) #pylint:disable=no-member
                db_session.commit() #pylint:disable=no-member
                return await ctx.send(f'Added "{playlist_item.title}" '
                                      f'to playlist "{playlist.name}"', delete_after=DELETE_AFTER)
            except IntegrityError:
                db_session.rollback() #pylint:disable=no-member
                return await ctx.send(f'Unable to add "{playlist_item.title}" '
                                      f'to playlist "{playlist.name}', delete_after=DELETE_AFTER)

        @playlist.command(name='item-remove')
        async def playlist_item_remove(self, ctx, playlist_index, item_index):
            '''
            Add item to playlist
            '''
            result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
            if not result:
                logger.info(f'Invalid playlist index {playlist_index} given')
                return await ctx.send(f'Unable to find playlist {playlist_index}',
                                      delete_after=DELETE_AFTER)
            try:
                item_index = int(item_index)
            except ValueError:
                return await ctx.send(f'Invalid item index {item_index}',
                                      delete_after=DELETE_AFTER)
            if item_index < 1:
                return await ctx.send(f'Invalid item index {item_index}',
                                      delete_after=DELETE_AFTER)

            query = db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
            query = query.join(PlaylistMembership).\
                filter(PlaylistMembership.playlist_id == playlist.id)
            query_results = [item for item in query]
            try:
                item, membership = query_results[item_index - 1]
                title = item.title
                self.__delete_playlist_item(membership, item)
                return await ctx.send(f'Removed item {title} from playlist',
                                      delete_after=DELETE_AFTER)
            except IndexError:
                return await ctx.send(f'Unable to find item {item_index}',
                                      delete_after=DELETE_AFTER)

        @playlist.command(name='show')
        async def playlist_show(self, ctx, playlist_index):
            '''
            Show Items in playlist
            '''
            result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
            if not result:
                return await ctx.send(f'Unable to find playlist {playlist_index}',
                                      delete_after=DELETE_AFTER)

            query = db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
            query = query.join(PlaylistMembership).\
                filter(PlaylistMembership.playlist_id == playlist.id)
            items = [clean_title(item.title) for (item, _membership) in query]

            if not items:
                return await ctx.send('No playlist items in database',
                                      delete_after=DELETE_AFTER)

            tables = get_table_view(items)
            for table in tables:
                await ctx.send(table, delete_after=DELETE_AFTER)

        @playlist.command(name='delete')
        async def playlist_delete(self, ctx, playlist_index):
            '''
            Delete playlist
            '''
            result, playlist = self.__get_playlist(playlist_index, ctx.guild.id)
            if not result:
                return await ctx.send(f'Unable to find playlist {playlist_index}',
                                      delete_after=DELETE_AFTER)
            playlist_name = playlist.name

            logger.debug(f'Deleting all playlist items for {playlist.id}')
            query = db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
            query = query.join(PlaylistMembership).\
                filter(PlaylistMembership.playlist_id == playlist.id)
            for item, membership in query:
                self.__delete_playlist_item(membership, item)
            logger.info(f'Deleting playlist {playlist.id}')
            db_session.delete(playlist)
            db_session.commit()
            return await ctx.send(f'Deleted playlist {playlist_name}',
                                  delete_after=DELETE_AFTER)


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
                                      delete_after=DELETE_AFTER)
            shuffle = False
            # Make sure sub command is valid
            if sub_command:
                if sub_command.lower() == 'shuffle':
                    shuffle = True
                else:
                    return await ctx.send(f'Invalid sub command {sub_command}',
                                          delete_after=DELETE_AFTER)



            vc = ctx.voice_client
            if not vc:
                await ctx.invoke(self.connect_)
            player = self.get_player(ctx)

            query = db_session.query(PlaylistItem, PlaylistMembership)#pylint:disable=no-member
            query = query.join(PlaylistMembership).\
                filter(PlaylistMembership.playlist_id == playlist.id)
            playlist_items = [item for (item, _membership) in query]

            if shuffle:
                await ctx.send('Shuffling playlist items',
                               delete_after=DELETE_AFTER)
                random.shuffle(playlist_items)

            for item in playlist_items:
                if player.queue.full():
                    return await ctx.send('Queue is full, cannot add more songs',
                                          delete_after=DELETE_AFTER)

                match = re.match(YOUTUBE_URL_REGEX, item.web_url)
                if not match:
                    await ctx.send(f'Cannot add invalid url {item.web_url}',
                                   delete_after=DELETE_AFTER)
                    continue

                source = await YTDLSource.create_source(ctx,
                                                        f'{match.group("video_id")} {item.title}',
                                                        loop=self.bot.loop)

                try:
                    player.queue.put_nowait(source)
                    await ctx.send(f'```ini\n[Added {source.title} to the Queue '
                                   f'{source.webpage_url}\n```', delete_after=DELETE_AFTER)
                except asyncio.QueueFull:
                    return await ctx.send('Queue is full, cannot add more songs',
                                          delete_after=DELETE_AFTER)

            return await ctx.send(f'Added all songs in playlist {playlist.name} to Queue',
                                  delete_after=DELETE_AFTER)


    class General(commands.Cog):
        '''
        General use commands
        '''
        @commands.command(name='hello')
        async def hello(self, ctx):
            '''
            Say hello to the server
            '''
            _, message = functions.hello(ctx, logger)
            await ctx.send(message)

        @commands.command(name='roll')
        async def roll(self, ctx, *, number):
            '''
            Get a random number between 1 and number given
            '''
            _status, message = functions.roll(ctx, logger, number)
            await ctx.send(message)

        @commands.command(name='windows')
        async def windows(self, ctx):
            '''
            Get an inspirational note about your operating system
            '''
            _, message = functions.windows(ctx, logger)
            await ctx.send(message)


    class Planner(commands.Cog):
        '''
        Assistant for planning events
        '''
        @commands.group(name='planner', invoke_without_command=False)
        async def planner(self, ctx):
            '''
            Planner functions
            '''
            if ctx.invoked_subcommand is None:
                await ctx.send('Invalid sub command passed...')

        @planner.command(name='register')
        async def register(self, ctx):
            '''
            Register yourself with planning service
            '''
            _, message = functions.planner_register(ctx, logger, db_session)
            await ctx.send(message)

    # Run bot
    bot.add_cog(Music(bot))
    bot.add_cog(General(bot))
    bot.add_cog(Planner(bot))
    bot.run(settings['discord_token'])
