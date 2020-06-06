import argparse
import asyncio
from async_timeout import timeout
from functools import partial
import itertools
from prettytable import PrettyTable
import re
import random
import sys
import traceback
from youtube_dl import YoutubeDL

import discord
from discord.ext import commands


from discord_bot import functions
from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils import get_logger, get_database_session, read_config


def parse_args():
    parser = argparse.ArgumentParser(description="Discord Bot Runner")
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT, help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")
    parser.add_argument("--discord-token", "-t", help="Discord token, defaults to DISCORD_TOKEN env arg")
    return parser.parse_args()


class VoiceConnectionError(commands.CommandError):
    """Custom Exception class for connection errors."""


class InvalidVoiceChannel(VoiceConnectionError):
    """Exception for cases of invalid Voice Channels."""


class MyQueue(asyncio.Queue):
    def shuffle(self):
        random.shuffle(self._queue)


def main():
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
        # TODO Allow arg for changing dir
        'outtmpl': '/tmp/%(extractor)s-%(id)s-%(title)s.%(ext)s',
        'restrictfilenames': True,
        'noplaylist': True,
        'nocheckcertificate': True,
        'ignoreerrors': False,
        'logtostderr': False,
        'quiet': True,
        'no_warnings': True,
        'default_search': 'auto',
        'source_address': '0.0.0.0'  # ipv6 addresses cause issues sometimes
    }

    ffmpegopts = {
        'before_options': '-nostdin',
        'options': '-vn'
    }

    ytdl = YoutubeDL(ytdlopts)

    # Music bot setup
    # Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34
    class YTDLSource(discord.PCMVolumeTransformer):
        def __init__(self, source, *, data, requester):
            super().__init__(source)
            self.requester = requester

            self.title = data.get('title')
            self.webpage_url = data.get('webpage_url')
            logger.info(f'Music bot adding new source: {self.webpage_url}, requested by {self.requester}')

            # YTDL info dicts (data) have other useful information you might want
            # https://github.com/rg3/youtube-dl/blob/master/README.md

        def __getitem__(self, item: str):
            """Allows us to access attributes similar to a dict.

            This is only useful when you are NOT downloading.
            """
            return self.__getattribute__(item)

        @classmethod
        async def create_source(cls, ctx, search: str, *, loop, download=True):
            loop = loop or asyncio.get_event_loop()

            to_run = partial(ytdl.extract_info, url=search, download=download)
            data = await loop.run_in_executor(None, to_run)

            if 'entries' in data:
                # take first item from a playlist
                data = data['entries'][0]

            logger.info(f'Music bot adding {data["title"]} to the queue {data["webpage_url"]}')
            await ctx.send(f'```ini\n[Added {data["title"]} to the Queue {data["webpage_url"]}]\n```', delete_after=15)

            if download:
                source = ytdl.prepare_filename(data)
            else:
                return {'webpage_url': data['webpage_url'], 'requester': ctx.author, 'title': data['title']}

            return cls(discord.FFmpegPCMAudio(source), data=data, requester=ctx.author)

        @classmethod
        async def regather_stream(cls, data, *, loop):
            """Used for preparing a stream, instead of downloading.

            Since Youtube Streaming links expire."""
            loop = loop or asyncio.get_event_loop()
            requester = data['requester']

            to_run = partial(ytdl.extract_info, url=data['webpage_url'], download=True)
            data = await loop.run_in_executor(None, to_run)

            return cls(discord.FFmpegPCMAudio(data['url']), data=data, requester=requester)


    class MusicPlayer:
        """A class which is assigned to each guild using the bot for Music.

        This class implements a queue and loop, which allows for different guilds to listen to different playlists
        simultaneously.

        When the bot disconnects from the Voice it's instance will be destroyed.
        """

        __slots__ = ('bot', '_guild', '_channel', '_cog', 'queue', 'next', 'current', 'np', 'volume')

        def __init__(self, ctx):
            logger.info(f'Adding music bot to guild {ctx.guild}')
            self.bot = ctx.bot
            self._guild = ctx.guild
            self._channel = ctx.channel
            self._cog = ctx.cog

            self.queue = MyQueue()
            self.next = asyncio.Event()

            self.np = None  # Now playing message
            self.volume = .5
            self.current = None

            ctx.bot.loop.create_task(self.player_loop())

        async def player_loop(self):
            """Our main player loop."""
            await self.bot.wait_until_ready()

            while not self.bot.is_closed():
                self.next.clear()

                try:
                    # Wait for the next song. If we timeout cancel the player and disconnect...
                    async with timeout(600):  # 10 minutes...
                        source = await self.queue.get()
                except asyncio.TimeoutError:
                    logger.error(f'Music bot reached timeout on queue')
                    return self.destroy(self._guild)

                if not isinstance(source, YTDLSource):
                    # Source was probably a stream (not downloaded)
                    # So we should regather to prevent stream expiration
                    try:
                        source = await YTDLSource.regather_stream(source, loop=self.bot.loop)
                    except Exception as e:
                        await self._channel.send(f'There was an error processing your song.\n'
                                                 f'```css\n[{e}]\n```')
                        continue

                source.volume = self.volume
                self.current = source

                self._guild.voice_client.play(source, after=lambda _: self.bot.loop.call_soon_threadsafe(self.next.set))
                logger.info(f'Music bot now playing {source.title} requested by {source.requester}, url {source.webpage_url}')
                self.np = await self._channel.send(f'**Now Playing:** `{source.title}` requested by '
                                                   f'`{source.requester}` `{source.webpage_url}`')
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
            """Disconnect and cleanup the player."""
            return self.bot.loop.create_task(self._cog.cleanup(guild))


    class Music(commands.Cog):
        """Music related commands."""

        __slots__ = ('bot', 'players')

        def __init__(self, bot):
            self.bot = bot
            self.players = {}

        async def cleanup(self, guild):
            try:
                await guild.voice_client.disconnect()
            except AttributeError:
                pass

            try:
                del self.players[guild.id]
            except KeyError:
                pass

        async def __local_check(self, ctx):
            """A local check which applies to all commands in this cog."""
            if not ctx.guild:
                raise commands.NoPrivateMessage
            return True

        async def __error(self, ctx, error):
            """A local error handler for all errors arising from commands in this cog."""
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
            """Retrieve the guild player, or generate one."""
            try:
                player = self.players[ctx.guild.id]
            except KeyError:
                player = MusicPlayer(ctx)
                self.players[ctx.guild.id] = player

            return player

        @commands.command(name='connect', aliases=['join'])
        async def connect_(self, ctx, *, channel: discord.VoiceChannel=None):
            """Connect to voice.

            Parameters
            ------------
            channel: discord.VoiceChannel [Optional]
                The channel to connect to. If a channel is not specified, an attempt to join the voice channel you are in
                will be made.

            This command also handles moving the bot to different channels.
            """
            if not channel:
                try:
                    channel = ctx.author.voice.channel
                except AttributeError:
                    raise InvalidVoiceChannel('No channel to join. Please either specify a valid channel or join one.')

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

            await ctx.send(f'Connected to: **{channel}**', delete_after=20)

        @commands.command(name='play', aliases=['sing'])
        async def play_(self, ctx, *, search: str):
            """Request a song and add it to the queue.

            This command attempts to join a valid voice channel if the bot is not already in one.
            Uses YTDL to automatically search and retrieve a song.

            Parameters
            ------------
            search: str [Required]
                The song to search and retrieve using YTDL. This could be a simple search, an ID or URL.
            """
            await ctx.trigger_typing()

            vc = ctx.voice_client

            if not vc:
                await ctx.invoke(self.connect_)

            player = self.get_player(ctx)

            # If download is False, source will be a dict which will be used later to regather the stream.
            # If download is True, source will be a discord.FFmpegPCMAudio with a VolumeTransformer.
            source = await YTDLSource.create_source(ctx, search, loop=self.bot.loop, download=True)

            await player.queue.put(source)

        @commands.command(name='pause')
        async def pause_(self, ctx):
            """Pause the currently playing song."""
            vc = ctx.voice_client

            if not vc or not vc.is_playing():
                return await ctx.send('I am not currently playing anything!', delete_after=20)
            elif vc.is_paused():
                return

            vc.pause()
            await ctx.send(f'**`{ctx.author}`**: Paused the song!')

        @commands.command(name='resume')
        async def resume_(self, ctx):
            """Resume the currently paused song."""
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything!', delete_after=20)
            elif not vc.is_paused():
                return

            vc.resume()
            await ctx.send(f'**`{ctx.author}`**: Resumed the song!')

        @commands.command(name='skip')
        async def skip_(self, ctx):
            """Skip the song."""
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything!', delete_after=20)

            if vc.is_paused():
                pass
            elif not vc.is_playing():
                return

            vc.stop()
            await ctx.send(f'**`{ctx.author}`**: Skipped the song!')


        @commands.command(name='shuffle')
        async def shuffle_(self, ctx):
            """Shuffle song queue ."""
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything!', delete_after=20)

            player = self.get_player(ctx)
            if player.queue.empty():
                return await ctx.send('There are currently no more queued songs.')
            player.queue.shuffle()
 
            table = PrettyTable()
            table.field_names = ["Queue Position", "Title"]

            for (count, item) in enumerate(player.queue._queue):
                table.add_row([count + 1, item["title"]])

            await ctx.send(f'Queue Shuffled\n{table}')


        @commands.command(name='queue', aliases=['q', 'playlist'])
        async def queue_info(self, ctx):
            """Retrieve a basic queue of upcoming songs."""
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently connected to voice!', delete_after=20)

            player = self.get_player(ctx)
            if player.queue.empty():
                return await ctx.send('There are currently no more queued songs.')

            table = PrettyTable()
            table.field_names = ["Queue Position", "Title"]

            for (count, item) in enumerate(player.queue._queue):
                table.add_row([count + 1, item["title"]])

            await ctx.send(f'{table}')


        @commands.command(name='now_playing', aliases=['np', 'current', 'currentsong', 'playing'])
        async def now_playing_(self, ctx):
            """Display information about the currently playing song."""
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently connected to voice!', delete_after=20)

            player = self.get_player(ctx)
            if not player.current:
                return await ctx.send('I am not currently playing anything!')

            try:
                # Remove our previous now_playing message.
                await player.np.delete()
            except discord.HTTPException:
                pass

            player.np = await ctx.send(f'**Now Playing:** `{vc.source.title}`'
                                       f'requested by `{vc.source.requester}` `{vc.source.webpage_url}`')

        @commands.command(name='stop')
        async def stop_(self, ctx):
            """Stop the currently playing song and destroy the player.

            !Warning!
                This will destroy the player assigned to your guild, also deleting any queued songs and settings.
            """
            vc = ctx.voice_client

            if not vc or not vc.is_connected():
                return await ctx.send('I am not currently playing anything!', delete_after=20)

            await self.cleanup(ctx.guild)


    class General(commands.Cog):
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
