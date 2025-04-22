# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

from asyncio import sleep
from asyncio import QueueEmpty, QueueFull, TimeoutError as async_timeout
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from random import shuffle as random_shuffle, randint
from re import match as re_match
from tempfile import TemporaryDirectory
from typing import Callable, Optional, List

from dappertable import shorten_string_cjk, DapperTable
from discord.ext.commands import Bot, Context, group, command
from discord import VoiceChannel
from discord.errors import NotFound
from sqlalchemy import asc
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from yt_dlp import YoutubeDL
from yt_dlp.postprocessor import PostProcessor
from yt_dlp.utils import DownloadError

from discord_bot.cogs.common import CogHelper
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.download_client import DownloadClient, DownloadClientException
from discord_bot.cogs.music_helpers.download_client import ExistingFileException, VideoBanned, VideoTooLong, BotDownloadFlagged
from discord_bot.cogs.music_helpers.message_queue import MessageQueue, SourceLifecycleStage, MessageType
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.cogs.music_helpers.search_cache_client import SearchCacheClient
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

from discord_bot.database import Playlist, PlaylistItem, Guild, VideoCacheGuild, VideoCache
from discord_bot.exceptions import CogMissingRequiredArg, ExitEarlyException
from discord_bot.cogs.schema import SERVER_ID
from discord_bot.utils.common import retry_discord_message_command, rm_tree, return_loop_runner
from discord_bot.utils.audio import edit_audio_file
from discord_bot.utils.queue import PutsBlocked
from discord_bot.utils.distributed_queue import DistributedQueue
from discord_bot.utils.clients.spotify import SpotifyClient
from discord_bot.utils.clients.youtube import YoutubeClient
from discord_bot.utils.clients.youtube_music import YoutubeMusicClient
from discord_bot.utils.sql_retry import retry_database_commands

# GLOBALS
PLAYHISTORY_PREFIX = '__playhistory__'

# Find numbers in strings
NUMBER_REGEX = r'.*(?P<number>[0-9]+).*'

# YTDLP OUTPUT TEMPLATE
YTDLP_OUTPUT_TEMPLATE = '%(extractor)s.%(id)s.%(ext)s'

# Music config schema
MUSIC_SECTION_SCHEMA = {
    'type': 'object',
    'properties': {
        'general': {
            'type': 'object',
            'properties': {
                # Message delete after (seconds)
                # Delete all messages after interval
                'message_delete_after': {
                    'type': 'number',
                },
                # Number of shuffles by default
                'number_shuffles': {
                    'type': 'number',
                    'minimum': 1,
                },
            }
        },
        'player': {
            'type': 'object',
            'properties': {
                # Max size of player queue
                # Also applied to download queue
                # Since items will be passed through
                'queue_max_size': {
                    'type': 'number',
                    'minimum': 1,
                },
                # Disconnect timeout (seconds)
                # How long player should wait in server with no data
                # Before disconnecting
                'disconnect_timeout': {
                    'type': 'number',
                    'minimum': 1,
                },
            }
        },
        'playlist': {
            'type': 'object',
            'properties': {
                # Max size of server playlists
                'server_playlist_max_size': {
                    'type': 'number',
                    'minimum': 1,
                },
            }
        },
        'download': {
            'type': 'object',
            'properties': {
                # Max video length seconds
                # YTDLP with not allow videos longer than this
                'max_video_length': {
                    'type': 'number',
                    'minimum': 1,
                },
                # Enable audio processing on downloaded files
                # Removes dead air and normalizes volume
                'enable_audio_processing': {
                    'type': 'boolean',
                },
                # Extra options to pass to ytdlp clients
                'extra_ytdlp_options': {
                    'type': 'object',
                },
                # List of banned video urls
                'banned_videos_list': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                },
                # Enable youtube music search before attempting download
                'enable_youtube_music_search': {
                    'type': 'boolean',
                },
                # Min wait time between ytdlp downloads
                # In seconds
                'youtube_wait_period_minimum': {
                    'type': 'number',
                    'minimum': 1,
                },
                # Max variance time to add between ytdlp downloads
                # In seconds
                'youtube_wait_period_max_variance': {
                    'type': 'number',
                    'minimum': 1,
                },
                # Spotify api credentials
                # To allow for spotify urls
                'spotify_credentials': {
                    'type': 'object',
                    'properties': {
                        'client_id': {
                            'type': 'string',
                        },
                        'client_secret': {
                            'type': 'string',
                        },
                    },
                    'required': [
                        'client_id',
                        'client_secret'
                    ]
                },
                # Youtube api key to grab playlists with
                'youtube_api_key': {
                    'type': 'string',
                },
                # Priority servers should be placed in download queues
                'server_queue_priority': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'server_id': SERVER_ID, #pylint:disable=duplicate-code
                            'priority': {
                                'type': 'number'
                            },
                        },
                        'required': ['server_id', 'priority'],
                        'additionalProperties': False,
                    }
                },
                'cache': {
                    'type': 'object',
                    'properties': {
                        # Download dir for local files
                        'download_dir_path': {
                            'type': 'string',
                        },
                        # Keep local files on disk and don't cleanup after stopping
                        # Also enables search cache for spotify
                        'enable_cache_files': {
                            'type': 'boolean',
                        },
                        # Max cache files to keep on disk and in db
                        'max_cache_files': {
                            'type': 'number',
                            'minimum': 1,
                        },
                        # Max to keep in search cache
                        # Mostly used to keep spotify resuls around
                        'max_search_cache_entries': {
                            'type': 'number',
                            'minimum': 1,
                        },
                    }
                },

            }
        },
    }
}

#
# Exceptions
#

class PlaylistMaxLength(Exception):
    '''
    Playlist hit max length
    '''

#
# Common Functions
#

def match_generator(max_video_length: int, banned_videos_list: List[str], video_cache_search: Callable = None):
    '''
    Generate filters for yt-dlp
    '''
    def filter_function(info, *, incomplete): #pylint:disable=unused-argument
        '''
        Throw errors if filters dont match
        '''
        duration = info.get('duration')
        if duration and max_video_length and duration > max_video_length:
            raise VideoTooLong('Video Too Long', user_message=f'Video duration {duration} exceeds max length of {max_video_length}, skipping')
        vid_url = info.get('webpage_url')
        if vid_url and banned_videos_list:
            for banned_url in banned_videos_list:
                if vid_url == banned_url:
                    raise VideoBanned('Video Banned', user_message=f'Video url "{vid_url}" is banned, skipping')
        # Check if video exists within cache, and raise
        extractor = info.get('extractor')
        vid_id = info.get('id')
        if video_cache_search:
            result = video_cache_search(extractor, vid_id)
            if result:
                raise ExistingFileException('File already downloaded', video_cache=result)

    return filter_function

#
# YTDL Post Processor
#


class VideoEditing(PostProcessor):
    '''
    Run post processing on downloaded videos
    '''
    def run(self, information):
        '''
        Run post processing editing
        Get filename, edit with moviepy, and update dict
        '''
        file_path = Path(information['_filename'])
        edited_path = edit_audio_file(file_path)
        if edited_path:
            information['_filename'] = str(edited_path)
            information['filepath'] = str(edited_path)
        else:
            information['_filename'] = str(file_path)
            information['filepath'] = str(file_path)
        return [], information

class Music(CogHelper): #pylint:disable=too-many-public-methods
    '''
    Music related commands
    '''

    def __init__(self, bot: Bot, settings: dict, db_engine: Engine): #pylint:disable=too-many-statements
        super().__init__(bot, settings, db_engine, settings_prefix='music', section_schema=MUSIC_SECTION_SCHEMA)
        if not self.settings.get('general', {}).get('include', {}).get('music', False):
            raise CogMissingRequiredArg('Music not enabled')

        self.players = {}
        self._cleanup_task = None
        self._download_task = None
        self._cache_cleanup_task = None
        self._message_task = None

        # Keep track of when bot is in shutdown mode
        self.bot_shutdown = False
        # Message queue bits
        self.message_queue = MessageQueue()
        self.player_messages = {}
        # General options
        self.delete_after = self.settings.get('music', {}).get('general', {}).get('message_delete_after', 300) # seconds
        self.number_shuffles = self.settings.get('music', {}).get('general', {}).get('number_shuffles', 5)
        # Player options
        self.queue_max_size = self.settings.get('music', {}).get('player', {}).get('queue_max_size', 128)
        self.disconnect_timeout = self.settings.get('music', {}).get('player', {}).get('disconnect_timeout', 60 * 15) # seconds

        self.download_queue = DistributedQueue(self.queue_max_size, number_shuffles=self.number_shuffles)

        # Playlist options
        self.server_playlist_max_size = self.settings.get('music', {}).get('playlist', {}).get('server_playlist_max_size', 64)

        # Download options
        max_video_length = self.settings.get('music', {}).get('download', {}).get('max_video_length', 60 * 15) # seconds
        enable_audio_processing = self.settings.get('music', {}).get('download', {}).get('enable_audio_processing', False)
        download_dir_path = self.settings.get('music', {}).get('download', {}).get('cache', {}).get('download_dir_path', None)
        self.enable_cache = self.settings.get('music', {}).get('download', {}).get('cache', {}).get('enable_cache_files', False)
        max_cache_files = self.settings.get('music', {}).get('download', {}).get('cache', {}).get('max_cache_files', 2048)
        max_search_cache_entries = self.settings.get('music', {}).get('download', {}).get('cache', {}).get('max_search_cache_entries', 4096)
        ytdlp_options = self.settings.get('music', {}).get('download', {}).get('extra_ytdlp_options', {})
        banned_videos_list = self.settings.get('music', {}).get('download', {}).get('banned_videos_list', [])


        self.youtube_wait_period_min = self.settings.get('music', {}).get('download', {}).get('youtube_wait_period_minimum', 30) # seconds
        self.youtube_wait_period_max_variance = self.settings.get('music', {}).get('download', {}).get('youtube_wait_period_max_variance', 10) # seconds

        self.spotify_client = None
        spotify_credentails = self.settings.get('music', {}).get('download', {}).get('spotify_credentials', {})
        if spotify_credentails:
            self.spotify_client = SpotifyClient(spotify_credentails.get('client_id'), spotify_credentails.get('client_secret'))

        self.youtube_client = None
        youtube_api_key = self.settings.get('music', {}).get('download', {}).get('youtube_api_key', None)
        if youtube_api_key:
            self.youtube_client = YoutubeClient(youtube_api_key)

        enable_youtube_music_search = self.settings.get('music', {}).get('download', {}).get('enable_youtube_music_search', True)
        self.youtube_music_client = None
        if enable_youtube_music_search:
            self.youtube_music_client = YoutubeMusicClient()

        server_queue_priority_input = self.settings.get('music', {}).get('download', {}).get('server_queue_priority', [])
        self.server_queue_priority = {}
        for item in server_queue_priority_input:
            self.server_queue_priority[int(item['server_id'])] = item['priority']

        # Setup rest of client
        if download_dir_path is not None:
            self.download_dir = Path(download_dir_path)
            if not self.download_dir.exists():
                self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        self.video_cache = None
        self.search_string_cache = None
        if self.enable_cache and self.db_engine:
            self.video_cache = VideoCacheClient(self.download_dir, max_cache_files, partial(self.with_db_session))
            self.video_cache.verify_cache()
            self.search_string_cache = SearchCacheClient(partial(self.with_db_session), max_search_cache_entries)

        self.last_download_lockfile = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with


        ytdlopts = {
            'format': 'bestaudio/best',
            'restrictfilenames': True,
            'noplaylist': True,
            'nocheckcertificate': True,
            'ignoreerrors': False,
            'logtostderr': False,
            'logger': self.logger,
            'default_search': 'auto',
            'source_address': '0.0.0.0',  # ipv6 addresses cause issues sometimes
            'outtmpl': str(self.download_dir / f'{YTDLP_OUTPUT_TEMPLATE}'),
        }
        for key, val in ytdlp_options.items():
            ytdlopts[key] = val
        # Add any filter functions, do some logic so we only pass a single function into the processor
        if max_video_length or banned_videos_list or self.video_cache:
            callback_function = None
            if self.video_cache:
                callback_function = partial(self.video_cache.search_existing_file)
            ytdlopts['match_filter'] = match_generator(max_video_length, banned_videos_list, video_cache_search=callback_function)
        ytdl = YoutubeDL(ytdlopts)
        if enable_audio_processing:
            ytdl.add_post_processor(VideoEditing(), when='post_process')
        self.download_client = DownloadClient(ytdl, self.message_queue, spotify_client=self.spotify_client, youtube_client=self.youtube_client,
                                              youtube_music_client=self.youtube_music_client,
                                              search_cache_client=self.search_string_cache,
                                              number_shuffles=self.number_shuffles)

    async def cog_load(self):
        '''
        When cog starts
        '''
        self._cleanup_task = self.bot.loop.create_task(return_loop_runner(self.cleanup_players, self.bot, self.logger)())
        self._download_task = self.bot.loop.create_task(return_loop_runner(self.download_files, self.bot, self.logger)())
        self._message_task = self.bot.loop.create_task(return_loop_runner(self.send_messages, self.bot, self.logger)())
        if self.enable_cache:
            self._cache_cleanup_task = self.bot.loop.create_task(return_loop_runner(self.cache_cleanup, self.bot, self.logger)())

    async def cog_unload(self):
        '''
        Run when cog stops
        '''
        self.logger.debug('Music :: Calling shutdown on Music')

        self.bot_shutdown = True

        guilds = list(self.players.keys())
        self.logger.debug(f'Music :: Calling shutdown on guild players {guilds}')
        for guild_id in guilds:
            self.logger.info(f'Music :: Calling shutdown on player in guild {guild_id}')
            guild = await self.bot.fetch_guild(guild_id)
            await self.cleanup(guild, external_shutdown_called=True)

        self.logger.debug('Music :: Cancelling main tasks')
        if self._cleanup_task:
            self._cleanup_task.cancel()
        if self._download_task:
            self._download_task.cancel()
        if self._cache_cleanup_task:
            self._cache_cleanup_task.cancel()
        if self._message_task:
            self._message_task.cancel()
        self.last_download_lockfile.unlink(missing_ok=True)

        if self.download_dir.exists() and not self.enable_cache:
            rm_tree(self.download_dir)

    async def player_should_update_queue_order(self, player: MusicPlayer):
        '''
        Check if known queue messages match whats in channel history
        This is so the queue order is the last message in the text channel
        If it isn't we want to delete the current messages and resend

        player: Music player to check for updates
        '''
        queue_messages = self.player_messages.get(player.guild.id, [])
        if len(queue_messages) < 1:
            return False
        history = [message async for message in retry_discord_message_command(player.text_channel.history, limit=len(queue_messages))]
        for (count, hist_item) in enumerate(history):
            index = len(queue_messages) - 1 - count
            mess = queue_messages[index]
            if mess.id != hist_item.id:
                return True
        return False

    async def clear_player_queue(self, guild_id: int):
        '''
        Delete player queue messages
        '''
        queue_messages = self.player_messages.get(guild_id, [])
        for queue_message in queue_messages:
            try:
                await retry_discord_message_command(queue_message.delete)
            except NotFound:
                pass
        self.player_messages[guild_id] = []
        return True

    async def player_update_queue_order(self, guild_id: int):
        '''
        Update queue message in channel

        player: Music player to update for
        '''
        player = await self.get_player(guild_id, create_player=False)
        if not player:
            return False
        self.logger.debug(f'Music :: Updating queue messages in channel {player.text_channel.id} in guild {player.guild.id}')
        new_queue_strings = player.get_queue_order_messages()
        if await self.player_should_update_queue_order(player):
            await self.clear_player_queue(player.guild.id)
        queue_messages = self.player_messages.get(player.guild.id, [])
        if len(queue_messages) > len(new_queue_strings):
            for _ in range(len(queue_messages) - len(new_queue_strings)):
                queue_message = queue_messages.pop(-1)
                await retry_discord_message_command(queue_message.delete)
        for (count, queue_message) in enumerate(queue_messages):
            # Check if queue message is the same before updating
            if queue_message.content == new_queue_strings[count]:
                continue
            await retry_discord_message_command(queue_message.edit, content=new_queue_strings[count])
        if len(queue_messages) < len(new_queue_strings):
            for table in new_queue_strings[-(len(new_queue_strings) - len(queue_messages)):]:
                self.player_messages[guild_id].append(await retry_discord_message_command(player.text_channel.send, table))
        return True

    async def send_messages(self):
        '''
        Send messages runner
        '''
        await sleep(.01)


        source_type, item = self.message_queue.get_next_message()

        if not source_type:
            if self.bot_shutdown:
                raise ExitEarlyException('Bot in shutdown and i dont have any more messages, exiting early')
            return True

        if source_type == MessageType.SINGLE_MESSAGE:
            for func in item:
                try:
                    await retry_discord_message_command(func)
                except NotFound:
                    self.logger.warning(f'Unable to run single message func {func}, assuming was deletion and no longer exists')
                    return False
            return True
        if source_type == MessageType.SOURCE_LIFECYCLE:
            try:
                result = await retry_discord_message_command(item.function, item.message_content, delete_after=item.delete_after)
                if item.lifecycle_stage == SourceLifecycleStage.SEND:
                    item.source_dict.set_message(result)
                return True
            except NotFound:
                if item.lifecycle_stage == SourceLifecycleStage.DELETE:
                    self.logger.warning(f'Unable to find message for deletion for source {item}')
                    return False
                raise
        if source_type == MessageType.PLAY_ORDER:
            await self.player_update_queue_order(item)
            return True
        return False

    async def cleanup_players(self):
        '''
        Check for players with no members, cleanup bot in channels that do
        '''
        await sleep(30)
        if self.bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        guilds = []
        for _guild_id, player in self.players.items():
            if not player.voice_channel_active():
                self.message_queue.iterate_single_message([partial(player.text_channel.send, content='No members in guild, removing myself',
                                                                   delete_after=self.delete_after)])
                player.shutdown_called = True
                self.logger.warning(f'No members connected to voice channel {player.guild.id}, stopping bot')
                guilds.append(player.guild)
        # Run in separate loop since the cleanup function removes items form self.players
        # And you might hit issues where dict size changes during iteration
        for guild in guilds:
            await self.cleanup(guild)

    async def cache_cleanup(self):
        '''
        Cache cleanup runner

        After cache files marked for deletion, check if they are in use before deleting
        '''
        def list_ready_cache_files(db_session: Session):
            return db_session.query(VideoCache).\
                filter(VideoCache.ready_for_deletion == True).all()

        await sleep(60)
        if self.bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        base_paths = set()
        for _guild_id, player in self.players.items():
            for path in player.get_file_paths():
                base_paths.add(str(path))
        delete_videos = []
        with self.with_db_session() as db_session:
            for video_cache in retry_database_commands(db_session, partial(list_ready_cache_files, db_session)):
                # Check if video cache in use
                if str(video_cache.base_path) in base_paths:
                    continue
                delete_videos.append(video_cache.id)
            if not delete_videos:
                return False

            self.logger.debug(f'Music :: Identified cache videos ready for deletion {delete_videos}')
            self.video_cache.remove_video_cache(delete_videos)
            return True

    async def youtube_backoff_time(self, minimum_wait_time: int, max_variance: int):
        '''
        Wait for next youtube download time
        Wait for minimum time plus a random interval, where max is set by max variance

        minimum_wait_time : Wait at least this amount of time
        max_variance : Max variance to add from random value
        '''
        try:
            last_updated_at = self.last_download_lockfile.read_text()
        except (FileNotFoundError, ValueError):
            self.logger.debug('Music:: No youtube backoff timestamp found, continuing')
            # If file doesn't exist or no value, assume we dont need to wait
            return True
        wait_until = int(last_updated_at) + minimum_wait_time + randint(0, max_variance)
        self.logger.debug(f'Music :: Waiting on backoff in youtube, waiting until {wait_until}')
        while True:
            # If bot exited, return now
            if self.bot_shutdown:
                raise ExitEarlyException('Exiting bot wait loop')
            now = int(datetime.now(timezone.utc).timestamp())
            if now > wait_until:
                return True
            await sleep(1)

    async def __cache_search(self, source_download: SourceDownload):
        '''
        Cache search string in db session
        source_download     : Source Download from DownloadClient
        '''
        if not self.search_string_cache:
            return False
        self.logger.info(f'Music :: Search cache enabled, attempting to add webpage "{source_download.webpage_url}"')
        self.search_string_cache.iterate(source_download)
        return True

    async def add_source_to_player(self, source_download: SourceDownload, player: MusicPlayer, skip_update_queue_strings: bool = False):
        '''
        Add source to player queue

        source_dict : Standard source_dict for pre-download
        source_download : Standard SourceDownload for post download
        player : MusicPlayer
        skiP_update_queue_strings : Skip queue string update
        '''
        try:
            source_download.ready_file(file_dir=player.file_dir, move_file=not self.enable_cache)
            player.add_to_play_queue(source_download)
            self.logger.info(f'Music :: Adding "{source_download.webpage_url}" '
                             f'to queue in guild {source_download.source_dict.guild_id}')
            if not skip_update_queue_strings:
                self.message_queue.iterate_play_order(player.guild.id)
            self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.DELETE,
                                                        partial(source_download.source_dict.delete_message), '')
            if self.video_cache:
                self.logger.info(f'Music :: Iterating file on base path {str(source_download.base_path)}')
                self.video_cache.iterate_file(source_download)
            # If we have a result, add to search cache
            await self.__cache_search(source_download)
            return True
        except QueueFull:
            self.logger.warning(f'Music ::: Play queue full, aborting download of item "{str(source_download.source_dict)}"')
            self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.EDIT,
                                                        partial(source_download.source_dict.edit_message),
                                                        f'Play queue is full, cannot add "{str(source_download.source_dict)}"',
                                                        delete_after=self.delete_after)
            source_download.delete()
            return False
            # Dont return to loop, file was downloaded so we can iterate on cache at least
        except PutsBlocked:
            self.logger.warning(f'Music :: Puts Blocked on queue in guild "{source_download.source_dict.guild_id}", assuming shutdown')
            self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.DELETE,
                                                        partial(source_download.source_dict.delete_message), '')
            source_download.delete()
            return False

    def update_download_lockfile(self, source_download: SourceDownload,
                                 add_additional_backoff: int=None) -> bool:
        '''
        Update the download lockfile

        source_download : Source Download
        add_additional_backoff : Add more backoff time to existing timestamp

        '''
        if source_download and source_download.extractor != 'youtube':
            return False
        new_timestamp = int(datetime.now(timezone.utc).timestamp())
        if add_additional_backoff:
            new_timestamp += add_additional_backoff
        self.last_download_lockfile.write_text(str(new_timestamp))
        return True

    # Take both source dict and source download
    # Since source download might be none
    async def __ensure_video_download_result(self, source_dict: SourceDict, source_download: SourceDownload):
        if source_download is None:
            self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.EDIT,
                                                        partial(source_dict.edit_message),
                                                        f'Issue downloading video "{str(source_dict)}", skipping', delete_after=self.delete_after)
            return False
        return True

    async def __return_bad_video(self, source_dict: SourceDict, exception: DownloadClientException,
                                 skip_callback_functions: bool=False):
        message = exception.user_message
        self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.EDIT,
                                                    partial(source_dict.edit_message), message, delete_after=self.delete_after)
        if not skip_callback_functions:
            for func in source_dict.video_non_exist_callback_functions:
                await func()
        return

    async def __check_video_cache(self, source_dict: SourceDict):
        if not self.video_cache:
            return None
        return self.video_cache.get_webpage_url_item(source_dict)

    async def download_files(self): #pylint:disable=too-many-statements
        '''
        Main runner
        '''
        await sleep(.01)
        if self.bot_shutdown:
            raise ExitEarlyException('Bot shutdown called, exiting early')
        try:
            source_dict = self.download_queue.get_nowait()
        except QueueEmpty:
            return
        # If not meant to download, dont check for player
        # Check for player, if doesn't exist return
        player = None
        if source_dict.download_file:
            player = await self.get_player(source_dict.guild_id, create_player=False)
            if not player:
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.DELETE,
                                                            partial(source_dict.delete_message), '')
                return

            # Check if queue in shutdown, if so return
            if player.shutdown_called:
                self.logger.warning(f'Music ::: Play queue in shutdown, skipping downloads for guild {player.guild.id}')
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.DELETE,
                                                            partial(source_dict.delete_message), '')
                return
        self.logger.debug(f'Music ::: Gathered new item to download "{str(source_dict)}", guild "{source_dict.guild_id}"')
        # If cache enabled and search string with 'https://' given, try to grab this first
        source_download = await self.__check_video_cache(source_dict)
        # Else grab from ytdlp
        if not source_download:
            # Make sure we wait for next video download
            # Dont spam the video client
            await self.youtube_backoff_time(self.youtube_wait_period_min, self.youtube_wait_period_max_variance)
            try:
                source_download = await self.download_client.create_source(source_dict, self.bot.loop)
                self.update_download_lockfile(source_download)
            except ExistingFileException as e:
                # File exists on disk already, create again from cache
                self.logger.debug(f'Music :: Existing file found for download {str(source_dict)}, using existing file from url "{e.video_cache.video_url}"')
                source_download = self.video_cache.generate_download_from_existing(source_dict, e.video_cache)
                self.update_download_lockfile(source_download)
            except (BotDownloadFlagged) as e:
                self.logger.warning(f'Music :: Bot flagged while downloading video "{str(source_dict)}", {str(e)}')
                await self.__return_bad_video(source_dict, e, skip_callback_functions=True)
                self.logger.warning(f'Music :: Adding additional time {self.youtube_wait_period_min} to usual youtube backoff since bot was flagged')
                self.update_download_lockfile(source_download, add_additional_backoff=self.youtube_wait_period_min)
                return
            except (DownloadClientException) as e:
                self.logger.warning(f'Music :: Known error while downloading video "{str(source_dict)}", {str(e)}')
                await self.__return_bad_video(source_dict, e)
                self.update_download_lockfile(source_download)
                return
            except DownloadError as e:
                self.logger.error(f'Music :: Unknown error while downloading video "{str(source_dict)}", {str(e)}')
                source_download = None

        # Final none check in case we couldn't download video
        if not await self.__ensure_video_download_result(source_dict, source_download):
            return
        # Callback functions if given
        for func in source_dict.post_download_callback_functions:
            await func(source_download)

        if source_dict.download_file and player:
            # Add sources to players
            if not await self.add_source_to_player(source_download, player):
                return
            # Remove from cache file if exists
            if self.video_cache:
                self.logger.debug('Music ::: Checking cache files to remove in music player')
                self.video_cache.ready_remove()

    def __update_history_playlist(self, playlist_id: int, history_items: List[SourceDownload]):
        '''
        Add history items to playlist
        playlist        : Playlist history object
        history_items   : List of history items
        '''

        def delete_existing_item(db_session: Session, webpage_url: str, playlist_id: int):
            existing_history_item = db_session.query(PlaylistItem).\
                filter(PlaylistItem.video_url == webpage_url).\
                filter(PlaylistItem.playlist_id == playlist_id).first()
            if existing_history_item:
                self.logger.debug(f'Music ::: New history item {webpage_url} already exists, deleting this first')
                db_session.delete(existing_history_item)
                db_session.commit()

        def get_playlist_size(db_session: Session, playlist_id: int):
            return db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_id).count()

        def delete_extra_items(db_session: Session, playlist_id: int, delta: int):
            for existing_item in db_session.query(PlaylistItem).\
                    filter(PlaylistItem.playlist_id == playlist_id).\
                    order_by(asc(PlaylistItem.created_at)).limit(delta):
                self.logger.debug(f'Music ::: Deleting older history playlist item {existing_item.video_url} from playlist {playlist_id}, created on {existing_item.created_at}')
                db_session.delete(existing_item)
            db_session.commit()

        if not self.db_engine:
            return None
        # Delete existing items first
        with self.with_db_session() as db_session:
            for item in history_items:
                self.logger.info(f'Music ::: Attempting to add url {item.webpage_url} to history playlist {playlist_id}')
                retry_database_commands(db_session, partial(delete_existing_item, db_session, item.webpage_url, playlist_id))

            # Delete number of rows necessary to add list
            existing_items = retry_database_commands(db_session, partial(get_playlist_size, db_session, playlist_id))
            delta = (existing_items + len(history_items)) - self.server_playlist_max_size
            if delta > 0:
                self.logger.info(f'Need to delete {delta} items from history playlist {delta}')
                retry_database_commands(db_session, partial(delete_extra_items, db_session, playlist_id, delta))
            for item in history_items:
                self.logger.info(f'Music ::: Adding new history item "{item.webpage_url}" to playlist {playlist_id}')
                self.__playlist_insert_item(playlist_id, item.webpage_url, item.title, item.uploader)

    def __get_history_playlist(self, guild_id: int):
        '''
        Get history playlist for guild

        guild_id : Guild id
        '''
        def find_history_playlist(db_session: Session, guild_id: str):
            return db_session.query(Playlist).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == True).first()

        def create_history_playlist(db_session: Session, guild_id: str):
            history_playlist = Playlist(name=f'{PLAYHISTORY_PREFIX}{guild_id}',
                                        server_id=str(guild_id),
                                        created_at=datetime.now(timezone.utc),
                                        is_history=True)
            db_session.add(history_playlist)
            db_session.commit()
            return history_playlist

        if not self.db_engine:
            return None
        with self.with_db_session() as db_session:
            history_playlist = retry_database_commands(db_session, partial(find_history_playlist, db_session, guild_id))
            if history_playlist:
                return history_playlist.id
            history_playlist = retry_database_commands(db_session, partial(create_history_playlist, db_session, guild_id))
            return history_playlist.id

    async def cleanup(self, guild, external_shutdown_called=False):
        '''
        Cleanup guild player

        guild : Guild object
        external_shutdown_called: Whether called by something other than a user
        '''
        self.logger.info(f'Music :: Starting cleanup on guild {guild.id}')
        player = await self.get_player(guild.id, create_player=False)
        # Set external shutdown so this doesnt happen twice
        player.shutdown_called = True
        if external_shutdown_called and player:
            self.message_queue.iterate_single_message([partial(player.text_channel.send, content='External shutdown called on bot, please contact admin for details',
                                                        delete_after=self.delete_after)])
        try:
            await guild.voice_client.disconnect()
        except AttributeError:
            pass

        try:
            await guild.voice_client.cleanup()
        except AttributeError:
            pass

        # Block download queue for later
        self.download_queue.block(guild.id)
        # Clear play queue if that didnt happen
        try:
            player = self.players.pop(guild.id)
        except KeyError:
            return

        # Clear downloaded items
        player.np_message = ''
        player.clear_queue()
        # Cleanup queue messages if they still exist
        self.logger.info(f'Music :: Clearing queue message for guild {guild.id}')
        await self.clear_player_queue(guild.id)

        self.logger.debug(f'Music :: Starting cleaning tasks on player for guild {guild.id}')
        history_items = await player.cleanup()
        self.logger.debug(f'Music :: Grabbing {len(history_items)} history items for {guild.id}')
        history_playlist_id = self.__get_history_playlist(guild.id)
        if history_playlist_id and history_items:
            self.__update_history_playlist(history_playlist_id, history_items)

        self.logger.debug(f'Music :: Clearing download queue for guild {guild.id}')
        pending_items = self.download_queue.clear_queue(guild.id)
        self.logger.debug(f'Music :: Found existing download items {pending_items}')
        for source in pending_items:
            self.message_queue.iterate_source_lifecycle(source, SourceLifecycleStage.DELETE, source.delete_message, '')

        self.logger.debug(f'Music :: Deleting download dir for guild {guild.id}')
        guild_path = self.download_dir / f'{guild.id}'
        if guild_path.exists():
            rm_tree(guild_path)

    async def get_player(self, guild_id: int,
                         join_channel = None,
                         create_player: bool=True,
                         ctx: Context = None,
                         check_voice_client_active: bool=False):
        '''
        Retrieve the guild player, or generate one.

        guild_id : Guild id for player
        join_channel: Turn on voice client while we're here
        create_player : Create player if doesn't exist yet
        ctx: Original context call
        check_voice_client_active: Check if we're currently playing anything
        '''
        try:
            player = self.players[guild_id]
        except KeyError:
            if check_voice_client_active:
                self.message_queue.iterate_single_message([partial(ctx.send, 'I am not currently playing anything',
                                                           delete_after=self.delete_after)])
                return None
            if not create_player:
                return None
            # Make directory for guild specific files
            guild_path = self.download_dir / f'{ctx.guild.id}'
            guild_path.mkdir(exist_ok=True, parents=True)
            # Generate and start player
            player = MusicPlayer(self.logger, ctx, [partial(self.cleanup, ctx.guild)], self.queue_max_size, self.disconnect_timeout, guild_path, self.message_queue)
            await player.start_tasks()
            self.players[guild_id] = player
            self.player_messages.setdefault(guild_id, [])
        if check_voice_client_active:
            if not player.guild.voice_client or (not player.guild.voice_client.is_playing() and not self.download_queue.get_queue_size(guild_id)):
                self.message_queue.iterate_single_message([partial(player.text_channel.send, 'I am not currently playing anything',
                                                           delete_after=self.delete_after)])
                return None
        # Check if we should join voice
        if not player.guild.voice_client and join_channel:
            await player.join_voice(join_channel)
        return player

    async def __check_author_voice_chat(self, ctx: Context, check_voice_chats: bool = True):
        '''
        Check that command author in proper voice chat
        '''
        try:
            channel = ctx.author.voice.channel
        except AttributeError:
            self.message_queue.iterate_single_message([partial(ctx.send, f'{ctx.author.display_name} not in voice chat channel. Please join one and try again',
                                                      delete_after=self.delete_after)])
            return None

        if not check_voice_chats:
            return channel

        if channel.guild.id is not ctx.guild.id:
            self.message_queue.iterate_single_message([partial(ctx.send, 'User not joined to channel bot is in, ignoring command',
                                                      delete_after=self.delete_after)])
            return None
        return channel

    async def __ensure_player(self, ctx: Context, channel: VoiceChannel) -> MusicPlayer:
        try:
            return await self.get_player(ctx.guild.id, join_channel=channel, ctx=ctx)
        except async_timeout as e:
            self.logger.error(f'Reached async timeout error on bot joining channel, {str(e)}')
            self.message_queue.iterate_single_message([partial(ctx.send, f'Bot cannot join channel {channel}', delete_after=self.delete_after)])
        return None

    @command(name='join', aliases=['awaken'])
    async def connect_(self, ctx: Context):
        '''
        Connect to voice channel.
        '''
        channel = await self.__check_author_voice_chat(ctx, check_voice_chats=False)
        if not channel:
            return

        await self.__ensure_player(ctx, channel)

        self.message_queue.iterate_single_message([partial(ctx.send, f'Connected to: {channel}', delete_after=self.delete_after)])

    @command(name='play')
    async def play_(self, ctx, *, search: str):
        '''
        Request a video and add it to the download queue, which will then play after the download

        search: str [Required]
            The video to search and retrieve from youtube.
            This could be a string to search in youtube, an video id, or a direct url.

            If spotify credentials are passed to the bot it can also be a spotify album or playlist.
            If youtube api credentials are passed to the bot it can also be a youtube playlsit.
        
        shuffle: boolean [Optional]
            If the search input is a spotify url or youtube api playlist, it will shuffle the results from the api before passing it into the download queue
        '''
        channel = await self.__check_author_voice_chat(ctx)
        if not channel:
            return

        player = await self.__ensure_player(ctx, channel)
        if not player:
            return

        try:
            entries = await self.download_client.check_source(search, ctx.guild.id, ctx.author.display_name, ctx.author.id, self.bot.loop,
                                                              self.queue_max_size, ctx.channel)
        except DownloadClientException as exc:
            self.logger.warning(f'Received download client exception for search "{search}", {str(exc)}')
            self.message_queue.iterate_single_message([partial(ctx.send, f'{exc.user_message}', delete_after=self.delete_after)])
            return
        for source_dict in entries:
            try:
                # Check cache first
                source_download = await self.__check_video_cache(source_dict)
                if source_download:
                    self.logger.debug(f'Music :: Search "{str(source_dict)}" found in cache, placing in player queue')
                    await self.add_source_to_player(source_download, player)
                    continue
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.SEND,
                                                            partial(ctx.send),
                                                            f'Downloading and processing "{str(source_dict)}"')
                self.logger.debug(f'Music :: Handing off source_dict {str(source_dict)} to download queue')
                self.download_queue.put_nowait(source_dict.guild_id, source_dict, priority=self.server_queue_priority.get(ctx.guild.id, None))
            except PutsBlocked:
                self.logger.warning(f'Music :: Puts to queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.DELETE,
                                                            partial(source_dict.delete_message), '')
                return
            except QueueFull:
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.EDIT,
                                                            partial(source_dict.edit_message),
                                                            f'Unable to add "{str(source_dict)}" to queue, download queue is full',
                                                            delete_after=self.delete_after)
                return

    @command(name='skip')
    async def skip_(self, ctx):
        '''
        Skip the video.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return

        if not player.guild.voice_client.is_playing():
            return
        current_title = player.current_source.title
        player.video_skipped = True
        player.guild.voice_client.stop()
        self.message_queue.iterate_single_message([partial(ctx.send, f'Skipping video "{current_title}"',
                                                           delete_after=self.delete_after)])

    @command(name='clear')
    async def clear(self, ctx):
        '''
        Clear all items from queue
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return

        if player.check_queue_empty():
            self.message_queue.iterate_single_message([partial(ctx.send, 'There are currently no more queued videos.',
                                                               delete_after=self.delete_after)])
            return
        self.logger.info(f'Music :: Player clear called in guild {ctx.guild.id}')
        player.clear_queue()
        self.message_queue.iterate_play_order(player.guild.id)
        self.message_queue.iterate_single_message([partial(ctx.send, 'Cleared player queue', delete_after=self.delete_after)])
        return

    @command(name='history')
    async def history_(self, ctx: Context):
        '''
        Show recently played videos
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx)

        if player.check_history_empty():
            self.message_queue.iterate_single_message([partial(ctx.send, 'There have been no videos played.',
                                                               delete_after=self.delete_after)])
            return

        headers = [
            {
                'name': 'Pos',
                'length': 3,
            },
            {
                'name': 'Title /// Uploader',
                'length': 80,
            },
        ]
        table = DapperTable(headers, rows_per_message=15)
        table_items = player.get_history_items()
        for (count, item) in enumerate(table_items):
            uploader = item.uploader or ''
            table.add_row([
                f'{count + 1}',
                f'{item.title} /// {uploader}'
            ])
        messages = [f'```{t}```' for t in table.print()]
        message_funcs = []
        for mess in messages:
            message_funcs.append(partial(ctx.send, mess, delete_after=self.delete_after))
        self.message_queue.iterate_single_message(message_funcs)

    @command(name='shuffle')
    async def shuffle_(self, ctx):
        '''
        Shuffle video queue.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return

        if player.check_queue_empty():
            self.message_queue.iterate_single_message([partial(ctx.send, 'There are currently no more queued videos.',
                                                               delete_after=self.delete_after)])
            return
        player.shuffle_queue()
        self.message_queue.iterate_play_order(player.guild.id)

    @command(name='remove')
    async def remove_item(self, ctx, queue_index):
        '''
        Remove item from queue.

        queue_index: integer [Required]
            Position in queue of video that will be removed.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return

        if player.check_queue_empty():
            self.message_queue.iterate_single_message([partial(ctx.send, 'There are currently no more queued videos.',
                                                               delete_after=self.delete_after)])
            return

        try:
            queue_index = int(queue_index)
        except ValueError:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid queue index {queue_index}',
                                                               delete_after=self.delete_after)])
            return

        item = player.remove_queue_item(queue_index)
        if item is None:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to remove queue index {queue_index}',
                                                               delete_after=self.delete_after)])
            return
        self.message_queue.iterate_single_message([partial(ctx.send, f'Removed item {item.title} from queue',
                                                           delete_after=self.delete_after)])
        item.delete()
        self.message_queue.iterate_play_order(player.guild.id)

    @command(name='bump')
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue

        queue_index: integer [Required]
            Position in queue of video that will be removed.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return

        if player.check_queue_empty():
            self.message_queue.iterate_single_message([partial(ctx.send, 'There are currently no more queued videos.',
                                                               delete_after=self.delete_after)])
            return
        try:
            queue_index = int(queue_index)
        except ValueError:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid queue index {queue_index}',
                                                               delete_after=self.delete_after)])
            return

        item = player.bump_queue_item(queue_index)
        if item is None:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to bump queue index {queue_index}',
                                                               delete_after=self.delete_after)])
            return
        self.message_queue.iterate_single_message([partial(ctx.send, f'Bumped item {item.title} to top of queue',
                                                           delete_after=self.delete_after)])

        self.message_queue.iterate_play_order(player.guild.id)

    @command(name='stop')
    async def stop_(self, ctx):
        '''
        Stop the currently playing video and disconnect bot from voice chat.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return
        self.logger.info(f'Music :: Calling stop for guild {ctx.guild.id}')
        await self.cleanup(ctx.guild)

    @command(name='move-messages')
    async def move_messages_here(self, ctx):
        '''
        Move queue messages to this text chanel
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx, check_voice_client_active=True)
        if not player:
            return

        if ctx.channel.id == player.text_channel.id:
            self.message_queue.iterate_single_message([partial(ctx.send, f'I am already sending messages to channel {ctx.channel.name}',
                                                               delete_after=self.delete_after)])
            return
        player.text_channel = ctx.channel
        # Since the first step in update player order strings checks the text channel for the last message
        # This will set the messages to delete, and then the rest will happen
        self.message_queue.iterate_play_order(ctx.guild.id)

    async def __get_playlist(self, playlist_index: int, ctx: Context):
        def check_playlist_count(db_session: Session, guild_id: str):
            return db_session.query(Playlist.id).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == False).count()

        def list_non_history_playlists(db_session: Session, guild_id: str, offset: int):
            return db_session.query(Playlist.id).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == False).\
                order_by(Playlist.created_at.asc()).offset(offset).first()

        try:
            index = int(playlist_index)
        except ValueError:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)])
            return None

        with self.with_db_session() as db_session:
            if not retry_database_commands(db_session, partial(check_playlist_count, db_session, str(ctx.guild.id))):
                self.message_queue.iterate_single_message([partial(ctx.send, 'No playlists in database',
                                                                delete_after=self.delete_after)])
                return None

            playlist_id = retry_database_commands(db_session, partial(list_non_history_playlists, db_session, str(ctx.guild.id), (index - 1)))
            if not playlist_id:
                self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)])
                return None
            return playlist_id[0]

    async def __check_database_session(self, ctx: Context):
        '''
        Check if database session is in use
        '''
        if not self.db_engine:
            self.message_queue.iterate_single_message([partial(ctx.send, 'Functionality not available, database is not enabled')])
            return False
        return True

    @group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions.
        '''
        if ctx.invoked_subcommand is None:
            self.message_queue.iterate_single_message([partial(ctx.send, 'Invalid sub command passed...', delete_after=self.delete_after)])

    async def __playlist_create(self, ctx: Context, name: str):
        def check_for_playlist(db_session: Session, name: str, guild_id: str):
            return db_session.query(Playlist).\
                filter(Playlist.name == name).\
                filter(Playlist.server_id == guild_id).first()

        def create_playlist(db_session: Session, name: str, guild_id: str):
            playlist = Playlist(name=name,
                                server_id=guild_id,
                                created_at=datetime.now(timezone.utc),
                                is_history=False)
            db_session.add(playlist)
            db_session.commit()
            return playlist

        if not await self.__check_database_session(ctx):
            return
        # Check name doesn't conflict with history
        playlist_name = shorten_string_cjk(name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to create playlist "{name}", name cannot contain {PLAYHISTORY_PREFIX}')])
            return None
        with self.with_db_session() as db_session:
            existing_playlist = retry_database_commands(db_session, partial(check_for_playlist, db_session, playlist_name, str(ctx.guild.id)))
            if existing_playlist:
                self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to create playlist "{name}", name likely already exists')])
                return None

            playlist = retry_database_commands(db_session, partial(create_playlist, db_session, playlist_name, str(ctx.guild.id)))
            self.logger.info(f'Music :: Playlist created "{playlist_name}" with ID {playlist.id} in guild {ctx.guild.id}')
            self.message_queue.iterate_single_message([partial(ctx.send, f'Created playlist "{playlist_name}"',
                                                               delete_after=self.delete_after)])
            return playlist.id

    @playlist.command(name='create')
    async def playlist_create(self, ctx: Context, *, name: str):
        '''
        Create new playlist.

        name: str [Required]
            Name of new playlist to create
        '''
        await self.__playlist_create(ctx, name)

    @playlist.command(name='list')
    async def playlist_list(self, ctx: Context):
        '''
        List playlists.
        '''
        def get_playlist_items(db_session: Session, guild_id: str):
            return db_session.query(Playlist).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == False).\
                order_by(Playlist.created_at.asc())

        if not await self.__check_database_session(ctx):
            return
        with self.with_db_session() as db_session:
            playlist_items = retry_database_commands(db_session, partial(get_playlist_items, db_session, str(ctx.guild.id)))

            if not playlist_items:
                self.message_queue.iterate_single_message([partial(ctx.send, 'No playlists in database',
                                                                delete_after=self.delete_after)])
                return

            headers = [
                {
                    'name': 'ID',
                    'length': 3,
                },
                {
                    'name': 'Playlist Name',
                    'length': 64,
                },
                {
                    'name': 'Last Queued',
                    'length': 20,
                }
            ]
            table = DapperTable(headers, rows_per_message=15)
            for (count, item) in enumerate(playlist_items):
                last_queued = 'N/A'
                if item.last_queued:
                    last_queued = item.last_queued.strftime('%Y-%m-%d %H:%M:%S')
                table.add_row([
                    f'{count + 1}',
                    item.name,
                    last_queued,
                ])
            messages = [f'```{t}```' for t in table.print()]
            message_funcs = []
            for mess in messages:
                message_funcs.append(partial(ctx.send, mess, delete_after=self.delete_after))
            self.message_queue.iterate_single_message(message_funcs)

    def __playlist_insert_item(self, playlist_id: int, video_url: str, video_title: str, video_uploader: str):
        def get_item_count(db_session: Session, playlist_id: int):
            return db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_id).count()

        def check_existing_item(db_session: Session, playlist_id: int, video_url: str):
            return db_session.query(PlaylistItem).\
                filter(PlaylistItem.playlist_id == playlist_id).\
                filter(PlaylistItem.video_url == video_url).first()

        def create_new_item(db_session: Session, video_title: str, video_url: str, video_uploader: str, playlist_id: int):
            playlist_item = PlaylistItem(title=shorten_string_cjk(video_title, 256),
                                        video_url=video_url,
                                        uploader=shorten_string_cjk(video_uploader, 256),
                                        playlist_id=playlist_id,
                                        created_at=datetime.now(timezone.utc))
            db_session.add(playlist_item)
            db_session.commit()
            return playlist_item.id

        with self.with_db_session() as db_session:
            self.logger.info(f'Music :: Adding video "{video_url}" to playlist {playlist_id}')
            item_count = retry_database_commands(db_session, partial(get_item_count, db_session, playlist_id))
            if item_count >= self.server_playlist_max_size:
                raise PlaylistMaxLength(f'Playlist {playlist_id} greater to or equal to max length {self.server_playlist_max_size}')

            existing_item = retry_database_commands(db_session, partial(check_existing_item, db_session, playlist_id, video_url))
            if existing_item:
                return None

            playlist_item_id = retry_database_commands(db_session, partial(create_new_item, db_session, video_title,
                                                                           video_url, video_uploader, playlist_id))
            return playlist_item_id

    async def __add_playlist_item_function(self, ctx: Context, playlist_id: int, source_download: SourceDownload):
        '''
        Call this when the source download eventually completes
        source_download : Source Download from download client
        '''
        if source_download is None:
            self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.EDIT,
                                                        partial(source_download.source_dict.edit_message),
                                                        f'Unable to add playlist item "{str(source_download.source_dict)}", issue generating source',
                                                        delete_after=self.delete_after)
            return
        self.logger.info(f'Music :: Adding video_url "{source_download.webpage_url}" to playlist "{playlist_id}" '
                         f' in guild {ctx.guild.id}')
        try:
            playlist_item_id = self.__playlist_insert_item(playlist_id, source_download.webpage_url, source_download.title, source_download.uploader)
        except PlaylistMaxLength:
            self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.EDIT,
                                                        partial(source_download.source_dict.edit_message),
                                                        'Cannot add more items to playlist, already max size',
                                                        delete_after=self.delete_after)
            return
        if playlist_item_id:
            self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.EDIT,
                                                        partial(source_download.source_dict.edit_message),
                                                        f'Added item "{source_download.title}" to playlist',
                                                        delete_after=self.delete_after)
            return
        self.message_queue.iterate_source_lifecycle(source_download.source_dict, SourceLifecycleStage.EDIT,
                                                    partial(source_download.source_dict.edit_message),
                                                    f'Unable to add playlist item "{str(source_download.source_dict)}", likely already exists',
                                                    delete_after=self.delete_after)
        return

    @playlist.command(name='item-add')
    async def playlist_item_add(self, ctx, playlist_index, *, search: str):
        '''
        Add item to playlist.

        playlist_index: integer [Required]
            ID of playlist
        search: str [Required]
            The video to search and retrieve from youtube.
            This could be a simple search, an ID or URL.
        '''
        if not await self.__check_database_session(ctx):
            return

        playlist_id = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None

        try:
            source_entries = await self.download_client.check_source(search, ctx.guild.id, ctx.author.display_name, ctx.author.id, self.bot.loop,
                                                                     self.queue_max_size, ctx.channel)
        except DownloadClientException as exc:
            self.logger.warning(f'Received download client exception for search "{search}", {str(exc)}')
            self.message_queue.iterate_single_message([partial(ctx.send, f'{exc.user_message}', delete_after=self.delete_after)])
            return
        for source_dict in source_entries:
            source_dict.download_file = False
            self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.SEND, partial(ctx.send),
                                                        f'Downloading and processing "{str(source_dict)}" to add to playlist')
            source_dict.post_download_callback_functions = [partial(self.__add_playlist_item_function, ctx, playlist_id)] #pylint: disable=no-value-for-parameter
            self.download_queue.put_nowait(source_dict.guild_id, source_dict, priority=self.server_queue_priority.get(ctx.guild.id, None))

    @playlist.command(name='item-remove')
    async def playlist_item_remove(self, ctx: Context, playlist_index: int, video_index: int):
        '''
        Add item to playlist

        playlist_index: integer [Required]
            ID of playlist
        video_index: integer [Required]
            ID of video to remove
        '''
        def remove_playlist_item_remove(db_session: Session, playlist_id: int, index_id: int):
            query = db_session.query(PlaylistItem).\
                filter(PlaylistItem.playlist_id == playlist_id).\
                order_by(PlaylistItem.created_at.asc()).offset(index_id).first()
            if query:
                db_session.delete(query)
                db_session.commit()
                return True
            return False

        if not await self.__check_database_session(ctx):
            return

        playlist_id = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None
        try:
            video_index = int(video_index)
        except ValueError:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid item index {video_index}',
                                                               delete_after=self.delete_after)])
            return
        if video_index < 1:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid item index {video_index}',
                                                               delete_after=self.delete_after)])
            return

        with self.with_db_session() as db_session:
            if retry_database_commands(db_session, partial(remove_playlist_item_remove, db_session, playlist_id, (video_index - 1))):
                self.message_queue.iterate_single_message([partial(ctx.send, f'Removed item "{video_index}" from playlist',
                                                                delete_after=self.delete_after)])
                return
            self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to find item {video_index}',
                                                               delete_after=self.delete_after)])
            return

    @playlist.command(name='show')
    async def playlist_show(self, ctx: Context, playlist_index: int):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        def get_playlist_items(db_session: Session, playlist_id: int):
            return db_session.query(PlaylistItem).\
                filter(PlaylistItem.playlist_id == playlist_id).\
                order_by(PlaylistItem.created_at.asc())

        if not await self.__check_database_session(ctx):
            return

        playlist_id = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None

        with self.with_db_session() as db_session:
            headers = [
                {
                    'name': 'Pos',
                    'length': 3,
                },
                {
                    'name': 'Title /// Uploader',
                    'length': 64,
                },
            ]
            table = DapperTable(headers, rows_per_message=15)
            for (count, item) in enumerate(retry_database_commands(db_session, partial(get_playlist_items, db_session, playlist_id))): #pylint:disable=protected-access
                uploader = item.uploader or ''
                table.add_row([
                    f'{count + 1}',
                    f'{item.title} /// {uploader}',
                ])
            messages = [f'```{t}```' for t in table.print()]
            message_funcs = []
            for mess in messages:
                message_funcs.append(partial(ctx.send, mess, delete_after=self.delete_after))
            self.message_queue.iterate_single_message(message_funcs)

    @playlist.command(name='delete')
    async def playlist_delete(self, ctx: Context, playlist_index: int):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        if not await self.__check_database_session(ctx):
            return

        playlist_id = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None
        await self.__playlist_delete(playlist_id)
        self.message_queue.iterate_single_message([partial(ctx.send, f'Deleted playlist {playlist_index}',
                                                   delete_after=self.delete_after)])
        return

    async def __playlist_delete(self, playlist_id: int):
        def delete_playlist(db_session: Session, playlist_id: int):
            db_session.query(PlaylistItem).\
                filter(PlaylistItem.playlist_id == playlist_id).delete()
            query = db_session.query(Playlist).get(playlist_id)
            if query:
                db_session.delete(query)
            db_session.commit()
        self.logger.info(f'Music :: Deleting playlist items "{playlist_id}"')
        with self.with_db_session() as db_session:
            retry_database_commands(db_session, partial(delete_playlist, db_session, playlist_id))
            return

    @playlist.command(name='rename')
    async def playlist_rename(self, ctx: Context, playlist_index: int, *, playlist_name: str):
        '''
        Rename playlist to new name

        playlist_index: integer [Required]
            ID of playlist
        playlist_name: str [Required]
            New name of playlist
        '''
        def rename_playlist(db_session: Session, playlist_id: int, playlist_name: str):
            query = db_session.query(Playlist).get(playlist_id)
            query.name = playlist_name
            db_session.commit()

        if not await self.__check_database_session(ctx):
            return

        playlist_id = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None

        playlist_name = shorten_string_cjk(playlist_name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to create playlist "{playlist_name}", name cannot contain {PLAYHISTORY_PREFIX}')])
            return None

        self.logger.info(f'Music :: Renaming playlist {playlist_id} to name "{playlist_name}"')
        with self.with_db_session() as db_session:
            retry_database_commands(db_session, partial(rename_playlist, db_session, playlist_id, playlist_name))
            self.message_queue.iterate_single_message([partial(ctx.send, f'Renamed playlist {playlist_index} to name "{playlist_name}"',
                                                    delete_after=self.delete_after)])
            return

    @playlist.command(name='save-queue')
    async def playlist_queue_save(self, ctx: Context, *, name: str):
        '''
        Save contents of queue to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.__playlist_queue_save(ctx, name)

    @playlist.command(name='save-history')
    async def playlist_history_save(self, ctx: Context, *, name: str):
        '''
        Save contents of history to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.__playlist_queue_save(ctx, name, is_history=True)

    async def __playlist_queue_save(self, ctx: Context, name: str, is_history=False):
        playlist_id = await self.__playlist_create(ctx, name)
        if not playlist_id:
            return None

        player = await self.get_player(ctx.guild.id, create_player=False)
        if not player:
            self.message_queue.iterate_single_message([partial(ctx.send, 'No player connected, no queue to save',
                                                               delete_after=self.delete_after)])
            return

        # Do a deepcopy here so list doesn't mutate as we iterate
        if is_history:
            queue_copy = player.get_history_items()
        else:
            queue_copy = player.get_queue_items()

        self.logger.info(f'Music :: Saving queue contents to playlist "{name}", is_history? {is_history}')

        if len(queue_copy) == 0:
            self.message_queue.iterate_single_message([partial(ctx.send, 'There are no videos to add to playlist',
                                                               delete_after=self.delete_after)])
            return

        for data in queue_copy:
            try:
                playlist_item_id = self.__playlist_insert_item(playlist_id, data.webpage_url, data.title, data.uploader)
            except PlaylistMaxLength:
                self.message_queue.iterate_single_message([partial(ctx.send, 'Cannot add more items to playlist, already max size',
                                                           delete_after=self.delete_after)])
                break
            if playlist_item_id:
                self.message_queue.iterate_single_message([partial(ctx.send, f'Added item "{data.title}" to playlist', delete_after=self.delete_after)])
                continue
            self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to add playlist item "{data.title}", likely already exists', delete_after=self.delete_after)])
        self.message_queue.iterate_single_message([partial(ctx.send, f'Finished adding items to playlist "{name}"', delete_after=self.delete_after)])
        return

    async def __delete_non_existing_item(self, item_id: int, item_video_url: str, ctx: Context):
        self.logger.warning(f'Unable to find playlist item {item_id} in playlist, deleting')
        self.message_queue.iterate_single_message([partial(ctx.send, content=f'Unable to find video "{item_video_url}" in playlist, deleting',
                                                           delete_after=self.delete_after)])
        with self.with_db_session() as db_session:
            item = db_session.query(PlaylistItem).get(item_id)
            db_session.delete(item)
            db_session.commit()

    async def __playlist_enqueue_items(self, ctx: Context, source_dicts: List[SourceDict], player: MusicPlayer):
        '''
        Enqueue items from a playlist
        ctx: Standard discord context
        source dicts: Source dicts to hand off
        is_history: Is this a history playlist, pass into entries
        player: MusicPlayer
        '''
        # Track if we broke early for eventual return block
        broke_early = False
        for source_dict in source_dicts:
            try:
                # Just add directly to download queue here, since we already know the video id
                source_download = await self.__check_video_cache(source_dict)
                if source_download:
                    self.logger.debug(f'Music :: Search "{source_download}" found in cache, placing in player queue')
                    await self.add_source_to_player(source_download, player)
                    continue
                self.logger.debug(f'Music :: Handing off "{source_download}" to download queue')
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.SEND,
                                                            partial(ctx.send),
                                                            f'Downloading and processing "{source_dict}"')
                self.download_queue.put_nowait(source_dict.guild_id, source_dict, priority=self.server_queue_priority.get(ctx.guild.id, None))
            except QueueFull:
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.EDIT, partial(source_dict.edit_message),
                                                            f'Unable to add item "{source_dict}" to queue, queue is full',
                                                            delete_after=self.delete_after)
                broke_early = True
                break
            except PutsBlocked:
                self.logger.warning(f'Music :: Puts to queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                self.message_queue.iterate_source_lifecycle(source_dict, SourceLifecycleStage.DELETE, partial(source_dict.delete_message), '')
                break
        # Update queue strings finally just to be safe
        self.message_queue.iterate_play_order(player.guild.id)
        return broke_early

    async def __playlist_queue(self, ctx: Context, player: MusicPlayer, playlist_id: int, shuffle: bool, max_num: int, is_history: bool = False):
        def list_playlist_items(db_session: Session, playlist_id: int):
            return db_session.query(PlaylistItem).\
                filter(PlaylistItem.playlist_id == playlist_id)

        def get_playlist_name(db_session: Session, playlist_id: int):
            item = db_session.query(Playlist).get(playlist_id)
            return item.name

        def playlist_update_queued(db_session: Session, playlist_id: int):
            item = db_session.query(Playlist).get(playlist_id)
            item.last_queued = datetime.now(timezone.utc)
            db_session.commit()

        self.logger.info(f'Music :: Playlist queue called for playlist {playlist_id} in server "{ctx.guild.id}"')

        with self.with_db_session() as db_session:
            playlist_items = []
            for item in retry_database_commands(db_session, partial(list_playlist_items, db_session, playlist_id)):
                source_dict = SourceDict(ctx.guild.id,
                                         ctx.author.display_name,
                                         ctx.author.id,
                                         item.video_url,
                                         SearchType.DIRECT,
                                         added_from_history=is_history,
                                         video_non_exist_callback_functions=[partial(self.__delete_non_existing_item, item.id, item.video_url, ctx)] if is_history else [])
                playlist_items.append(source_dict)

            if shuffle:
                for _ in range(self.number_shuffles):
                    random_shuffle(playlist_items)

            if max_num:
                if max_num < 0:
                    self.message_queue.iterate_single_message([partial(ctx.send, f'Invalid number of videos {max_num}',
                                                            delete_after=self.delete_after)])
                    return
                if max_num < len(playlist_items):
                    playlist_items = playlist_items[:max_num]
                else:
                    max_num = 0

            broke_early = await self.__playlist_enqueue_items(ctx, playlist_items, player)

            playlist_name = retry_database_commands(db_session, partial(get_playlist_name, db_session, playlist_id))
            if is_history:
                playlist_name = 'Channel History'
            if broke_early:
                self.message_queue.iterate_single_message([partial(ctx.send, f'Added as many videos in playlist "{playlist_name}" to queue as possible, but hit limit',
                                                        delete_after=self.delete_after)])
            elif max_num:
                self.message_queue.iterate_single_message([partial(ctx.send, f'Added {max_num} videos from "{playlist_name}" to queue',
                                                        delete_after=self.delete_after)])
            else:
                self.message_queue.iterate_single_message([partial(ctx.send, f'Added all videos in playlist "{playlist_name}" to queue',
                                                        delete_after=self.delete_after)])
            retry_database_commands(db_session, partial(playlist_update_queued, db_session, playlist_id))

    @playlist.command(name='queue')
    async def playlist_queue(self, ctx: Context, playlist_index: int, sub_command: Optional[str] = ''):
        '''
        Add playlist to queue

        playlist_index: integer [Required]
            ID of playlist
        Sub commands - [shuffle] [max_number]
            shuffle - Shuffle playlist when entering it into queue
            max_num - Only add this number of videos to the queue
        '''
        channel = await self.__check_author_voice_chat(ctx)
        if not channel:
            return
        if not await self.__check_database_session(ctx):
            return

        player = await self.__ensure_player(ctx, channel)
        if not player:
            return

        # Make sure sub command is valid
        playlist_id = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None
        shuffle = False
        max_num = None
        if sub_command:
            if 'shuffle' in sub_command.lower():
                shuffle = True
            number_matcher = re_match(NUMBER_REGEX, sub_command.lower())
            if number_matcher:
                max_num = int(number_matcher.group('number'))
        return await self.__playlist_queue(ctx, player, playlist_id, shuffle, max_num)

    @command(name='random-play')
    async def playlist_random_play(self, ctx: Context, sub_command: Optional[str] = ''):
        '''
        Play random videos from history

        Sub commands - [cache] [max_num]
            max_num - Number of videos to add to the queue at maximum
            cache   - Play videos that are available in cache
        '''
        def get_video_cache_items(db_session: Session, guild_id: str, max_num: int):
            return db_session.query(VideoCache).\
                        join(VideoCacheGuild).\
                        join(Guild).\
                        filter(Guild.server_id == str(guild_id)).limit(max_num)
        channel = await self.__check_author_voice_chat(ctx)
        if not channel:
            return
        if not await self.__check_database_session(ctx):
            return

        player = await self.__ensure_player(ctx, channel)
        if not player:
            return

        max_num = 32 # Default
        from_cache = False
        if sub_command:
            sub_commands = sub_command.split(' ')
            for item in sub_commands:
                if item.lower() == 'cache':
                    from_cache = True
                    continue
                try:
                    max_num = int(item)
                except ValueError:
                    continue
        # If not from cache, play from history playlist
        if not from_cache:
            history_playlist_id = self.__get_history_playlist(ctx.guild.id)

            if not history_playlist_id:
                self.message_queue.iterate_single_message([partial(ctx.send, 'Unable to find history for server', delete_after=self.delete_after)])
                return
            return await self.__playlist_queue(ctx, player, history_playlist_id, True, max_num, is_history=True)

        with self.with_db_session() as db_session:
            playlist_items = []
            for item in retry_database_commands(db_session, partial(get_video_cache_items, db_session, ctx.guild.id, max_num)):
                source_dict = SourceDict(ctx.guild.id,
                                         ctx.author.display_name,
                                         ctx.author.id,
                                         item.video_url,
                                         SearchType.DIRECT,
                                         added_from_history=True)
                playlist_items.append(source_dict)

        for _ in range(self.number_shuffles):
            random_shuffle(playlist_items)

        broke_early = await self.__playlist_enqueue_items(ctx, playlist_items, player)
        if broke_early:
            self.message_queue.iterate_single_message([partial(ctx.send, 'Added as many videos in cache to queue as possible, but hit limit',
                                                               delete_after=self.delete_after)])
        elif max_num:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Added {max_num} videos from cache to queue',
                                                               delete_after=self.delete_after)])
        else:
            self.message_queue.iterate_single_message([partial(ctx.send, 'Added all videos in playlist cache to queue',
                                                               delete_after=self.delete_after)])
        return

    @playlist.command(name='merge')
    async def playlist_merge(self, ctx: Context, playlist_index_one: str, playlist_index_two: str):
        '''
        Merge second playlist into first playlist, deletes second playlist

        playlist_index_one: integer [Required]
            ID of playlist to be merged, will be kept
        playlist_index_two: integer [Required]
            ID of playlist to be merged, will be deleted
        '''
        def get_playlist_items(db_session: Session, playlist_id: int):
            return db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_id)

        if not await self.__check_database_session(ctx):
            return

        self.logger.info(f'Music :: Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{ctx.guild.id}"')
        playlist_one_id = await self.__get_playlist(playlist_index_one, ctx)
        playlist_two_id = await self.__get_playlist(playlist_index_two, ctx)
        if not playlist_one_id:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Cannot find playlist {playlist_index_one}', delete_after=self.delete_after)])
            return
        if not playlist_two_id:
            self.message_queue.iterate_single_message([partial(ctx.send, f'Cannot find playlist {playlist_index_two}', delete_after=self.delete_after)])
            return
        with self.with_db_session() as db_session:
            for item in retry_database_commands(db_session, partial(get_playlist_items, db_session, playlist_two_id)):
                try:
                    playlist_item_id = self.__playlist_insert_item(playlist_one_id, item.video_url, item.title, item.uploader)
                except PlaylistMaxLength:
                    self.message_queue.iterate_single_message([partial(ctx.send, f'Cannot add more items to playlist "{playlist_one_id}", already max size', delete_after=self.delete_after)])
                    return
                if playlist_item_id:
                    self.message_queue.iterate_single_message([partial(ctx.send, f'Added item "{item.title}" to playlist {playlist_index_one}',
                                                            delete_after=self.delete_after)])
                    continue
                self.message_queue.iterate_single_message([partial(ctx.send, f'Unable to add playlist item "{item.title}", likely already exists',
                                                        delete_after=self.delete_after)])
        await self.__playlist_delete(playlist_index_two)
