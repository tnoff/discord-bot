import argparse
import asyncio
from functools import partial
import os
import random
import sys
import traceback

from async_timeout import timeout
import discord
from discord.ext import commands
from moviepy.editor import VideoFileClip
from numpy import sqrt
from prettytable import PrettyTable
from youtube_dl import YoutubeDL


from discord_bot import functions
from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils import get_logger, get_database_session, read_config


# Delete messages after N seconds
DELETE_AFTER = 20


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
        if queue_index < 1 or queue_index > self.qsize():
            return None
        # Rotate, remove top, then remove
        for _ in range(1, queue_index):
            self._queue.rotate(-1)
        item = self._queue.popleft()
        for _ in range(1, queue_index):
            self._queue.rotate(1)
        self._queue.appendleft(item)
        return item

def get_queue_string(queue):
    '''
    Common function for queue printing
    '''
    table = PrettyTable()
    table.field_names = ["Queue Order", "Title"]

    for (count, item) in enumerate(queue):
        table.add_row([count + 1, item["title"]])
    return f'```\n{table.get_string()}\n```'

def main(): #pylint:disable=too-many-statements
    '''
    Main loop
    '''
    # First get cli args
    args = vars(parse_args())
    # Load settings
    settings = read_config(args.pop('config_file'))
    # Override settings if cli args passed
    for key, item in args.items():
        if item is not None:
            settings[key] = item
    # Check for token
    if settings['discord_token'] is None:
        raise DiscordBotException('No discord token given')

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
    bot = commands.Bot(command_prefix='!')
    # Setup database
    db_session = get_database_session(settings['mysql_user'],
                                      settings['mysql_password'],
                                      settings['mysql_database'],
                                      settings['mysql_host'])

    ytdlopts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(args['download_dir'],
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
        clip = VideoFileClip(video_file)
        total_length = clip.duration
        cut = lambda i: clip.audio.subclip(i, i+1).to_soundarray(fps=1)
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

        if start_index == 0 and end_index == total_length:
            logger.info('No dead audio at beginning or end of file, skipping')
            return

        logger.info(f'Trimming file to start {start_index} and end {end_index}')
        # Write file with changes, then overwrite
        new_path = os.path.join(video_file, '.edited')
        new_clip = clip.audio.subclip(t_start=start_index, end_index=end_index)
        new_clip.write_audiofile(new_path)
        os.rename(new_path, video_file)

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

            logger.info(f'Music bot adding {data["title"]} to the queue {data["webpage_url"]}')
            await ctx.send(f'```ini\n[Added {data["title"]} to the Queue '
                           f'{data["webpage_url"]}]\n```', delete_after=DELETE_AFTER)

            source = ytdl.prepare_filename(data)
            logger.info(f'Downloaded file {source} from youtube url {data["webpage_url"]}')
            logger.info(f'Attempting to trim audio on {source}')
            trim_audio(source)
            logger.info(f'Trimmed audio on {source}')
            return cls(discord.FFmpegPCMAudio(source), data=data, requester=ctx.author)


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

            self.queue = MyQueue(maxsize=20)
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
                self.np = await self._channel.send(message, delete_after=DELETE_AFTER)

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


    class Music(commands.Cog):
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
                return await ctx.send('Queue is full, cannot add more songs')

            source = await YTDLSource.create_source(ctx, search, loop=self.bot.loop)

            try:
                player.queue.put_nowait(source)
            except asyncio.QueueFull:
                await ctx.send('Queue is full, cannot add more songs')

        @commands.command(name='pause')
        async def pause_(self, ctx):
            '''
            Pause the currently playing song.
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_playing():
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)
            if vc.is_paused():
                return

            vc.pause()
            await ctx.send(f'```{ctx.author.name}`: Paused the song```')

        @commands.command(name='resume')
        async def resume_(self, ctx):
            '''
            Resume the currently paused song.
            '''
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything',
                                      delete_after=DELETE_AFTER)
            if not vc.is_paused():
                return

            vc.resume()
            await ctx.send(f'```{ctx.author.name}`: Resumed the song')

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

            vc.stop()
            await ctx.send(f'```{ctx.author.name}```: Skipped the song', delete_after=DELETE_AFTER)


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

            await ctx.send(get_queue_string(player.queue._queue), delete_after=DELETE_AFTER) #pylint:disable=protected-access


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

            await ctx.send(get_queue_string(player.queue._queue)) #pylint:disable=protected-access

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
            except AttributeError:
                return await ctx.send(f'Invalid queue index {queue_index}')

            item = player.queue.remove_item(queue_index)
            if item is None:
                return ctx.send(f'Unable to remove queue index {queue_index}')
            return await ctx.send(f'Removed item {item["title"]} from queue')

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
            except AttributeError:
                return await ctx.send(f'Invalid queue index {queue_index}')

            item = player.queue.bump_item(queue_index)
            if item is None:
                return ctx.send(f'Unable to remove queue index {queue_index}')
            return await ctx.send(f'Bumped item {item["title"]} to top of queue')


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
                                       f'requested by "{vc.source.requester.name}"```',
                                       delete_after=DELETE_AFTER)

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
