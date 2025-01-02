# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

from asyncio import sleep
from asyncio import QueueEmpty, QueueFull, TimeoutError as asyncio_timeout
from datetime import datetime, timezone
from functools import partial
from logging import RootLogger
from pathlib import Path
from random import shuffle as random_shuffle
from re import match as re_match
from tempfile import TemporaryDirectory
from traceback import format_exc
from typing import Callable, Optional, List

from dappertable import shorten_string_cjk, DapperTable
from discord.ext.commands import Bot, Context, group, command
from sqlalchemy import asc
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import IntegrityError
from yt_dlp import YoutubeDL
from yt_dlp.postprocessor import PostProcessor

from discord_bot.cogs.common import CogHelper
from discord_bot.cogs.music_helpers.common import SearchType
from discord_bot.cogs.music_helpers.download_client import DownloadClient, DownloadClientException, ExistingFileException, VideoBanned, VideoTooLong
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.source_dict import SourceDict
from discord_bot.cogs.music_helpers.source_download import SourceDownload
from discord_bot.cogs.music_helpers.search_cache_client import SearchCacheClient
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

from discord_bot.database import Playlist, PlaylistItem, Guild, VideoCacheGuild, VideoCache
from discord_bot.exceptions import CogMissingRequiredArg, ExitEarlyException
from discord_bot.utils.common import retry_discord_message_command, rm_tree
from discord_bot.utils.audio import edit_audio_file
from discord_bot.utils.queue import Queue, PutsBlocked
from discord_bot.utils.distributed_queue import DistributedQueue
from discord_bot.utils.clients.spotify import SpotifyClient
from discord_bot.utils.clients.youtube import YoutubeClient

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
        'message_delete_after': {
            'type': 'number',
        },
        'queue_max_size': {
            'type': 'number',
        },
        'server_playlist_max_size': {
            'type': 'number',
        },
        'max_video_length': {
            'type': 'number',
        },
        'disconnect_timeout': {
            'type': 'number',
        },
        'download_dir': {
            'type': 'string',
        },
        'enable_audio_processing': {
            'type': 'boolean',
        },
        'enable_cache_files': {
            'type': 'boolean',
        },
        'max_cache_files': {
            'type': 'number',
        },
        'spotify_client_id': {
            'type': 'string',
        },
        'spotify_client_secret': {
            'type': 'string',
        },
        'youtube_api_key': {
            'type': 'string',
        },
        'extra_ytdlp_options': {
            'type': 'object',
        },
        'banned_videos_list': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'url': {
                        'type': 'string',
                    },
                    'message': {
                        'type': 'string'
                    },
                },
            },
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

class LockfileException(Exception):
    '''
    Lock file Exceptions
    '''

#
# Common Functions
#

def match_generator(max_video_length, banned_videos_list, video_cache_search: Callable = None):
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
            for banned_video_dict in banned_videos_list:
                if vid_url == banned_video_dict['url']:
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

    def __init__(self, bot: Bot, logger: RootLogger, settings: dict, db_engine: Engine): #pylint:disable=too-many-statements
        super().__init__(bot, logger, settings, db_engine, settings_prefix='music', section_schema=MUSIC_SECTION_SCHEMA)
        if not self.settings.get('general', {}).get('include', {}).get('music', False):
            raise CogMissingRequiredArg('Music not enabled')

        self.players = {}
        self._cleanup_task = None
        self._download_task = None
        self._cache_cleanup_task = None
        self._message_task = None

        # Keep track of when bot is in shutdown mode
        self.bot_shutdown = False
        self.final_bot_shutdown = False

        # TODO make this configurable
        self.number_shuffles = 5


        self.queue_max_size = self.settings.get('music', {}).get('queue_max_size', 128)
        self.download_queue = DistributedQueue(self.queue_max_size, number_shuffles=self.number_shuffles)

        self.message_queue = Queue()

        self.delete_after = self.settings.get('music', {}).get('message_delete_after', 300) # seconds
        self.server_playlist_max_size = self.settings.get('music', {}).get('server_playlist_max_size', 64)
        self.max_video_length = self.settings.get('music', {}).get('max_video_length', 60 * 15) # seconds
        self.disconnect_timeout = self.settings.get('music', {}).get('disconnect_timeout', 60 * 15) # seconds
        self.download_dir = self.settings.get('music', {}).get('download_dir', None)
        self.enable_audio_processing = self.settings.get('music', {}).get('enable_audio_processing', False)
        self.enable_cache = self.settings.get('music', {}).get('enable_cache_files', False)
        self.max_cache_files = self.settings.get('music', {}).get('max_cache_files', 2048)
        self.max_search_cache_entries = self.settings.get('music', {}).get('max_search_cache_entries', 4096)
        self.banned_videos_list = self.settings.get('music', {}).get('banned_videos_list', [])

        spotify_client_id = self.settings.get('music', {}).get('spotify_client_id', None)
        spotify_client_secret = self.settings.get('music', {}).get('spotify_client_secret', None)
        youtube_api_key = self.settings.get('music', {}).get('youtube_api_key', None)

        ytdlp_options = self.settings.get('music', {}).get('extra_ytdlp_options', {})
        self.ytdlp_wait_period = self.settings.get('music', {}).get('ytdlp_wait_period', 30) # seconds

        self.spotify_client = None
        if spotify_client_id and spotify_client_secret:
            self.spotify_client = SpotifyClient(spotify_client_id, spotify_client_secret)

        self.youtube_client = None
        if youtube_api_key:
            self.youtube_client = YoutubeClient(youtube_api_key)

        if self.download_dir is not None:
            self.download_dir = Path(self.download_dir)
            if not self.download_dir.exists():
                self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        self.video_cache = None
        if self.enable_cache:
            self.video_cache = VideoCacheClient(self.download_dir, self.max_cache_files, self.db_session)
            self.video_cache.remove_extra_files()

        self.search_string_cache = None
        if self.db_session:
            self.search_string_cache = SearchCacheClient(self.db_session, self.max_search_cache_entries)

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
        if self.max_video_length or self.banned_videos_list or self.video_cache:
            callback_function = None
            if self.video_cache:
                callback_function = partial(self.video_cache.search_existing_file)
            ytdlopts['match_filter'] = match_generator(self.max_video_length, self.banned_videos_list, video_cache_search=callback_function)
        ytdl = YoutubeDL(ytdlopts)
        if self.enable_audio_processing:
            ytdl.add_post_processor(VideoEditing(), when='post_process')
        self.download_client = DownloadClient(ytdl, spotify_client=self.spotify_client, youtube_client=self.youtube_client,
                                              number_shuffles=self.number_shuffles)

    async def cog_load(self):
        '''
        When cog starts
        '''
        self._cleanup_task = self.bot.loop.create_task(self.cleanup_players())
        self._download_task = self.bot.loop.create_task(self.download_files())
        self._message_task = self.bot.loop.create_task(self.send_messages())
        if self.enable_cache:
            self._cache_cleanup_task = self.bot.loop.create_task(self.cache_cleanup())

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
        # Wait till final shutdown for messages
        # This way we have time to cleanup any messages left hanging around
        # TODO something should have a wait here to see how much we can cleanup
        self.final_bot_shutdown = True

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

    async def send_messages(self):
        '''
        Send queued messages
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__send_messages()
            except ExitEarlyException:
                return
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(format_exc())
                self.logger.error(str(e))
                print(f'Cleanup players exception {str(e)}')
                print('Formatted exception:', format_exc())

    async def __send_messages(self):
        '''
        Send messages runner
        '''
        await sleep(.01)
        if self.final_bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        try:
            message_func = self.message_queue.get_nowait()
        except QueueEmpty:
            return
        await retry_discord_message_command(message_func)
        return True

    async def cleanup_players(self):
        '''
        Cleanup players with no members in the channel
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__cleanup_players()
            except ExitEarlyException:
                return
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(format_exc())
                self.logger.error(str(e))
                print(f'Cleanup players exception {str(e)}')
                print('Formatted exception:', format_exc())

    async def __cleanup_players(self):
        '''
        Check for players with no members, cleanup bot in channels that do
        '''
        await sleep(60)
        if self.bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        guilds = []
        for _guild_id, player in self.players.items():
            has_members = False
            for member in player.voice_channel.members:
                if member.id != self.bot.user.id:
                    has_members = True
                    break
            if not has_members:
                guilds.append(player.voice_channel.guild)
        for guild in guilds:
            self.logger.warning(f'No members connected to voice channel {guild.id}, stopping bot')
            await self.cleanup(guild, external_shutdown_called=True, no_members_present=True)

    async def cache_cleanup(self):
        '''
        Cleanup cache and remove items marked for deletion
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__cache_cleanup()
            except ExitEarlyException:
                return
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(format_exc())
                self.logger.error(str(e))
                print(f'Cleanup players exception {str(e)}')
                print('Formatted exception:', format_exc())

    async def __cache_cleanup(self):
        '''
        Cache cleanup runner

        After cache files marked for deletion, check if they are in use before deleting
        '''
        await sleep(60)
        if self.bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        base_paths = set()
        for _guild_id, player in self.players.items():
            for path in player.get_symlinks():
                base_paths.add(str(path))
        delete_videos = []
        for video_cache in self.db_session.query(VideoCache).\
            filter(VideoCache.ready_for_deletion == True).all():
            # Check if video cache in use
            if str(video_cache.base_path) in base_paths:
                continue
            delete_videos.append(video_cache.id)
        if not delete_videos:
            return False
        self.logger.debug(f'Music :: Identified cache videos ready for deletion {delete_videos}')
        self.video_cache.remove_video_cache(delete_videos)
        return True

    async def download_files(self):
        '''
        Go through download loop and download all files
        '''
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                await self.__download_files()
            except ExitEarlyException:
                return
            except Exception as e:
                # New discord.py version doesn't seem to pick up task exceptions as well as I'd like
                # So catch all exceptions here, log a traceback and exit
                self.logger.exception(e)
                self.logger.error(format_exc())
                self.logger.error(str(e))
                print(f'Download files exception {str(e)}')
                print('Formatted exception:', format_exc())

    def wait_for_download_time(self, wait: int = 10):
        '''
        Whether or not to continue waiting for next download
        wait        : How long we should wait between next download

        Returns how long we need to wait for next run
        '''
        try:
            last_updated_at = self.last_download_lockfile.read_text()
            now = int(datetime.now(timezone.utc).timestamp())
            total_diff = now - int(last_updated_at)
            # Make sure if value is negative we default to 0 here
            return max((wait - total_diff), 0)
        except (FileNotFoundError, ValueError):
            return 0

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

    async def __return_bad_video(self, source_dict: SourceDict, exception: DownloadClientException):
        message = exception.user_message
        self.message_queue.put_nowait(partial(source_dict.message.edit, content=message, delete_after=self.delete_after))
        for func in source_dict.video_non_exist_callback_functions:
            await func()
        return

    async def __ensure_video_download_result(self, source_download: SourceDownload):
        if source_download is None:
            self.message_queue.put_nowait(partial(source_download.source_dict.message.edit, content=f'Issue downloading video "{str(source_download.source_dict)}", skipping',
                                                  delete_after=self.delete_after))
            return False
        return True

    async def __check_video_cache(self, source_dict: SourceDict):
        if not self.video_cache:
            return None
        if 'https://' not in source_dict.search_string:
            return None
        return self.video_cache.get_webpage_url_item(source_dict)

    async def __add_source_to_player(self, source_download: SourceDownload, player: MusicPlayer, skip_update_queue_strings: bool = False):
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
                await player.update_queue_strings()
            self.message_queue.put_nowait(partial(source_download.source_dict.delete_message))
            if self.video_cache:
                self.logger.info(f'Music :: Iterating file on base path {str(source_download.base_path)}')
                self.video_cache.iterate_file(source_download)
            return True
        except QueueFull:
            self.logger.warning(f'Music ::: Play queue full, aborting download of item "{source_download.source_dict.search_string}"')
            self.message_queue.put_nowait(partial(source_download.source_dict.message.edit,
                                            content=f'Play queue is full, cannot add "{str(source_download.source_dict)}"',
                                            delete_after=self.delete_after))
            source_download.delete()
            return False
            # Dont return to loop, file was downloaded so we can iterate on cache at least
        except PutsBlocked:
            self.logger.warning(f'Music :: Puts Blocked on queue in guild "{source_download.source_dict.guild_id}", assuming shutdown')
            self.message_queue.put_nowait(partial(source_download.source_dict.delete_message))
            source_download.delete()
            return False

    def __update_download_lockfile(self, source_download: SourceDownload) -> bool:
        '''
        Update the download lockfile
        '''
        if source_download and source_download.extractor != 'youtube':
            return False
        self.last_download_lockfile.write_text(str(int(datetime.now(timezone.utc).timestamp())))
        return True

    async def __download_files(self): #pylint:disable=too-many-statements
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
            try:
                player = self.players[source_dict.guild_id]
            except KeyError:
                self.message_queue.put_nowait(partial(source_dict.delete_message))
                return

            # Check if queue in shutdown, if so return
            if player.play_queue.shutdown:
                self.logger.warning(f'Music ::: Play queue in shutdown, skipping downloads for guild {player.guild.id}')
                self.message_queue.put_nowait(partial(source_dict.delete_message))
                return

        self.logger.debug(f'Music ::: Gathered new item to download "{source_dict.search_string}", guild "{source_dict.guild_id}"')

        # If cache enabled and search string with 'https://' given, try to grab this first
        source_download = await self.__check_video_cache(source_dict)
        # Else grab from ytdlp
        if not source_download:
            # Make sure we wait for next video download
            # Dont spam the video client
            wait_time = self.wait_for_download_time(wait=self.ytdlp_wait_period)
            if wait_time:
                self.logger.debug(f'Music ::: Waiting {wait_time} seconds until next video download')
                await sleep(wait_time)

            try:
                source_download = await self.download_client.create_source(source_dict, self.bot.loop)
                self.__update_download_lockfile(source_download)
            except ExistingFileException as e:
                # File exists on disk already, create again from cache
                self.logger.debug(f'Music :: Existing file found for download {str(source_dict)}, using existing file from url "{e.video_cache.video_url}"')
                source_download = self.video_cache.generate_download_from_existing(source_dict, e.video_cache)
                self.__update_download_lockfile(source_download)
            except (DownloadClientException) as e:
                self.logger.error(f'Error downloading video "{source_dict.search_string}", {str(e)}')
                # Try to mark search as unavailable for later
                await self.__return_bad_video(source_dict, e)
                self.__update_download_lockfile(source_download)
                return
            # TODO handle regular DownloadError here
        # Final none check in case we couldn't download video
        if not await self.__ensure_video_download_result(source_download):
            return

        # If we have a result, add to search cache
        await self.__cache_search(source_download)

        for func in source_dict.post_download_callback_functions:
            await func(source_download)

        if source_dict.download_file:
            # Add sources to players
            if not await self.__add_source_to_player(source_download, player):
                return
            # Remove from cache file if exists
            if self.video_cache:
                self.logger.debug('Music ::: Checking cache files to remove in music player')
                self.video_cache.ready_remove()

    async def __check_database_session(self, ctx: Context):
        '''
        Check if database session is in use
        '''
        if not self.db_session:
            self.message_queue.put_nowait(partial(ctx.send, 'Functionality not available, database is not enabled'))
            return False
        return True

    def __update_history_playlist(self, playlist: Playlist, history_items: List[SourceDownload]):
        '''
        Add history items to playlist
        playlist        : Playlist history object
        history_items   : List of history items
        '''
        # Delete existing items first
        for item in history_items:
            self.logger.info(f'Music ::: Attempting to add url {item.webpage_url} to history playlist {playlist.id}')
            existing_history_item = self.db_session.query(PlaylistItem).\
                filter(PlaylistItem.video_url == item.webpage_url).\
                filter(PlaylistItem.playlist_id == playlist.id).first()
            if existing_history_item:
                self.logger.debug(f'Music ::: New history item {item.webpage_url} already exists, deleting this first')
                self.db_session.delete(existing_history_item)
                self.db_session.commit()

        # Delete number of rows necessary to add list
        existing_items = self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist.id).count()
        if (existing_items + len(history_items)) > self.server_playlist_max_size:
            delta = (existing_items + len(history_items)) - self.server_playlist_max_size
            for existing_item in self.db_session.query(PlaylistItem).\
                    filter(PlaylistItem.playlist_id == playlist.id).\
                    order_by(asc(PlaylistItem.created_at)).limit(delta):
                self.logger.debug(f'Music ::: Deleting older history playlist item {existing_item.video_url} from playlist {playlist.id}')
                self.db_session.delete(existing_item)
                self.db_session.commit()
        for item in history_items:
            self.logger.info(f'Music ::: Adding new history item "{item.webpage_url}" to playlist {playlist.id}')
            self.__playlist_add_item(playlist, item.id, item.webpage_url, item.title, item.uploader)

    def __get_history_playlist(self, guild_id: str):
        '''
        Get history playlist for guild

        guild_id : Guild id
        '''
        history_playlist_id = None
        if self.db_session:
            history_playlist = self.db_session.query(Playlist).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == True).first()

            if not history_playlist:
                history_playlist = Playlist(name=f'{PLAYHISTORY_PREFIX}{guild_id}',
                                            server_id=guild_id,
                                            created_at=datetime.now(timezone.utc),
                                            is_history=True)
                self.db_session.add(history_playlist)
                self.db_session.commit()
            history_playlist_id = history_playlist.id
        return history_playlist_id

    async def cleanup(self, guild, external_shutdown_called=False, no_members_present=False):
        '''
        Cleanup guild player

        guild : Guild object
        external_shutdown_called: Whether called by something other than a user
        no_members_present: Called when no members present in server
        '''
        self.logger.info(f'Music :: Starting cleanup on guild {guild.id}')
        self.download_queue.block(guild.id)
        try:
            player = self.players[guild.id]
        except KeyError:
            return
        # Set external shutdown so we know not to call this twice
        player.shutdown_called = True
        try:
            await guild.voice_client.disconnect()
        except AttributeError:
            pass
        # Send messages if this was called externally somehow
        if external_shutdown_called:
            if no_members_present:
                self.message_queue.put_nowait(partial(player.text_channel.send, content='No members in guild, removing myself',
                                                      delete_after=self.delete_after))
            else:
                self.message_queue.put_nowait(partial(player.text_channel.send, content='External shutdown called on bot, please contact admin for details',
                                                      delete_after=self.delete_after))

        self.logger.debug(f'Music :: Starting cleaning tasks on player for guild {guild.id}')
        history_items = await player.cleanup()
        self.logger.debug(f'Music :: Grabbing history items {history_items} for {guild.id}')
        history_playlist_id = self.__get_history_playlist(guild.id)
        if history_playlist_id:
            playlist = self.db_session.query(Playlist).get(history_playlist_id)
            self.__update_history_playlist(playlist, history_items)

        self.logger.debug(f'Music :: Clearing download queue for guild {guild.id}')
        download_items = self.download_queue.clear_queue(guild.id)
        for source_dict in download_items:
            self.message_queue.put_nowait(partial(source_dict.delete_message))

        guild_path = self.download_dir / f'{guild.id}'
        if guild_path.exists():
            rm_tree(guild_path)

        # See if we need to delete
        try:
            del self.players[guild.id]
        except KeyError:
            pass

    async def get_player(self, ctx, voice_channel):
        '''
        Retrieve the guild player, or generate one.
        '''
        try:
            player = self.players[ctx.guild.id]
        except KeyError:
            # Make directory for guild specific files
            guild_path = self.download_dir / f'{ctx.guild.id}'
            guild_path.mkdir(exist_ok=True, parents=True)
            # Generate and start player
            player = MusicPlayer(ctx.bot, ctx.guild, partial(self.cleanup, ctx.guild), ctx.channel, voice_channel,
                                 self.logger, self.queue_max_size, self.disconnect_timeout, guild_path)
            await player.start_tasks()
            self.players[ctx.guild.id] = player

        return player

    async def __check_author_voice_chat(self, ctx: Context, check_voice_chats: bool = True):
        '''
        Check that command author in proper voice chat
        '''
        try:
            channel = ctx.author.voice.channel
        except AttributeError:
            self.message_queue.put_nowait(partial(ctx.send, f'"{ctx.author.display_name}" not in voice chat channel. Please join one and try again',
                                                  delete_after=self.delete_after))
            return None

        if not check_voice_chats:
            return channel

        if channel.guild.id is not ctx.guild.id:
            self.message_queue.put_nowait(partial(ctx.send, 'User not joined to channel bot is in, ignoring command',
                                                  delete_after=self.delete_after))
            return False
        return channel

    @command(name='join', aliases=['awaken'])
    async def connect_(self, ctx: Context):
        '''
        Connect to voice channel.
        '''
        channel = await self.__check_author_voice_chat(ctx, check_voice_chats=False)
        vc = ctx.voice_client

        if vc:
            if vc.channel.id == channel.id:
                return
            try:
                self.logger.info(f'Music :: bot moving to channel {channel.id} '
                                 f'in guild {ctx.guild.id}')
                await vc.move_to(channel)
            except asyncio_timeout:
                self.logger.warning(f'Music :: Moving to channel {channel.id} timed out')
                self.message_queue.put_nowait(partial(ctx.send, f'Moving to channel: <{channel}> timed out.'))
                return
        else:
            try:
                await channel.connect()
            except asyncio_timeout:
                self.logger.warning(f'Music :: Connecting to channel {channel.id} timed out')
                self.message_queue.put_nowait(partial(ctx.send, f'Connecting to channel: <{channel}> timed out.'))
                return

        await retry_discord_message_command(ctx.send, f'Connected to: {channel}', delete_after=self.delete_after)

    async def __check_search_cache(self, source_dict: SourceDict):
        '''
        Check search string cache for item
        source_dict : Standard source dict object
        '''
        if not self.search_string_cache:
            return None
        self.logger.info(f'Music ::: Checking search cache for string "{source_dict.search_string}"')
        return self.search_string_cache.check_cache(source_dict)

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
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client

        player = await self.get_player(ctx, vc.channel)

        try:
            entries = await self.download_client.check_source(search, ctx.guild.id, ctx.author.display_name, ctx.author.id, self.bot.loop)
        except DownloadClientException as exc:
            self.logger.warning(f'Received download client exception for search "{search}", {str(exc)}')
            self.message_queue.put_nowait(partial(ctx.send, f'{exc.user_message}', delete_after=self.delete_after))
            return
        for (count, source_dict) in enumerate(entries):
            try:
                # Check if item is already search cache
                search_video_url = await self.__check_search_cache(source_dict)
                if search_video_url:
                    self.logger.debug(f'Music :: Original search "{str(source_dict)} found with search video url "{search_video_url}"')
                    source_dict.search_string = search_video_url
                # Check cache first
                source_download = await self.__check_video_cache(source_dict)
                if source_download:
                    self.logger.debug(f'Music :: Search "{str(source_dict)}" found in cache, placing in player queue')
                    # Skip queue strings for every cahced result except the last one
                    skip_queue_strings = not count == (len(entries) - 1)
                    await self.__add_source_to_player(source_download, player, skip_update_queue_strings=skip_queue_strings)
                    continue
                source_dict.set_message(await retry_discord_message_command(ctx.send, f'Downloading and processing "{str(source_dict)}"'))
                self.logger.debug(f'Music :: Handing off source_dict {str(source_dict)} to download queue')
                self.download_queue.put_nowait(source_dict.guild_id, source_dict)
            except PutsBlocked:
                self.logger.warning(f'Music :: Puts to queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                self.message_queue.put_nowait(partial(source_dict.delete_message))
                break
            except QueueFull:
                if source_dict.message:
                    self.message_queue.put_nowait(partial(source_dict.message.edit, content=f'Unable to add "{str(source_dict)}" to queue, download queue is full',
                                                          delete_after=self.delete_after))
                else:
                    # Message not passed, sent to channel instead
                    self.message_queue.put_nowait(partial(ctx.send, content=f'Unable to add "{str(source_dict)}" to queue, download queue is full', delete_after=self.delete_after))
                break
        # Update queue strings finally just to be safe
        await player.update_queue_strings()

    @command(name='skip')
    async def skip_(self, ctx):
        '''
        Skip the video.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if not vc.is_paused() and not vc.is_playing():
            return
        player.video_skipped = True
        vc.stop()
        await retry_discord_message_command(ctx.send, 'Skipping video',
                                            delete_after=self.delete_after)

    @command(name='clear')
    async def clear(self, ctx):
        '''
        Clear all items from queue
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.check_queue_empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued videos.',
                                                      delete_after=self.delete_after)
        self.logger.info(f'Music :: Clear called in guild {ctx.guild.id}, first stopping tasks')
        for item in player.clear_queue():
            item.delete()
        await player.update_queue_strings()
        return await retry_discord_message_command(ctx.send, 'Cleared player queue', delete_after=self.delete_after)

    @command(name='history')
    async def history_(self, ctx):
        '''
        Show recently played videos
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.history.empty():
            return await retry_discord_message_command(ctx.send, 'There have been no videos played.',
                                                       delete_after=self.delete_after)

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
        table_items = player.history.items()
        for (count, item) in enumerate(table_items):
            uploader = item.uploader or ''
            table.add_row([
                f'{count + 1}',
                f'{item.title} /// {uploader}'
            ])
        messages = [f'```{t}```' for t in table.print()]
        for mess in messages:
            await retry_discord_message_command(ctx.send, mess, delete_after=self.delete_after)

    @command(name='shuffle')
    async def shuffle_(self, ctx):
        '''
        Shuffle video queue.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.check_queue_empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued videos.',
                                                       delete_after=self.delete_after)
        player.play_queue.shuffle()
        await player.update_queue_strings()

    @command(name='remove')
    async def remove_item(self, ctx, queue_index):
        '''
        Remove item from queue.

        queue_index: integer [Required]
            Position in queue of video that will be removed.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently connected to voice',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.check_queue_empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued videos.',
                                            delete_after=self.delete_after)

        try:
            queue_index = int(queue_index)
        except ValueError:
            return await retry_discord_message_command(ctx.send, f'Invalid queue index {queue_index}',
                                            delete_after=self.delete_after)

        item = player.play_queue.remove_item(queue_index)
        if item is None:
            return retry_discord_message_command(ctx.send, f'Unable to remove queue index {queue_index}',
                            delete_after=self.delete_after)
        await retry_discord_message_command(ctx.send, f'Removed item {item["title"]} from queue',
                                 delete_after=self.delete_after)
        item.delete()
        await player.update_queue_strings()

    @command(name='bump')
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue

        queue_index: integer [Required]
            Position in queue of video that will be removed.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently connected to voice',
                                            delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if player.check_queue_empty():
            return await retry_discord_message_command(ctx.send, 'There are currently no more queued videos.',
                                            delete_after=self.delete_after)
        try:
            queue_index = int(queue_index)
        except ValueError:
            return await retry_discord_message_command(ctx.send, f'Invalid queue index {queue_index}',
                                            delete_after=self.delete_after)

        item = player.play_queue.bump_item(queue_index)
        if item is None:
            return await retry_discord_message_command(ctx.send, f'Unable to bump queue index {queue_index}',
                                            delete_after=self.delete_after)
        await retry_discord_message_command(ctx.send, f'Bumped item {item["title"]} to top of queue',
                                 delete_after=self.delete_after)

        await player.update_queue_strings()

    @command(name='stop')
    async def stop_(self, ctx):
        '''
        Stop the currently playing video and disconnect bot from voice chat.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                            delete_after=self.delete_after)
        self.logger.info(f'Music :: Calling stop for guild {ctx.guild.id}')
        player = await self.get_player(ctx, vc.channel)
        player.shutdown_called = True
        await self.cleanup(ctx.guild)

    @command(name='move-messages')
    async def move_messages_here(self, ctx):
        '''
        Move queue messages to this text chanel
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        vc = ctx.voice_client

        if not vc or not vc.is_connected():
            return await retry_discord_message_command(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)

        player = await self.get_player(ctx, vc.channel)
        if ctx.channel.id == player.text_channel.id:
            return await retry_discord_message_command(ctx.send, f'I am already sending messages to channel {ctx.channel.name}',
                                                       delete_after=self.delete_after)
        await player.move_queue_message_channel(ctx.channel)

    async def __get_playlist(self, playlist_index, ctx):
        try:
            index = int(playlist_index)
        except ValueError:
            await retry_discord_message_command(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id)).order_by(Playlist.created_at.asc())
        playlist_items = [p for p in playlist_items if PLAYHISTORY_PREFIX not in p.name]

        if not playlist_items:
            await retry_discord_message_command(ctx.send, 'No playlists in database',
                                                delete_after=self.delete_after)
            return None
        try:
            return playlist_items[index - 1]
        except IndexError:
            await retry_discord_message_command(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            return None

    @group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions.
        '''
        if ctx.invoked_subcommand is None:
            await retry_discord_message_command(ctx.send, 'Invalid sub command passed...', delete_after=self.delete_after)

    async def __playlist_create(self, ctx: Context, name: str):
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return
        # Check name doesn't conflict with history
        playlist_name = shorten_string_cjk(name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            await retry_discord_message_command(ctx.send, f'Unable to create playlist "{name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return None
        playlist = Playlist(name=playlist_name,
                            server_id=ctx.guild.id,
                            created_at=datetime.now(timezone.utc),
                            is_history=False)
        try:
            self.db_session.add(playlist)
            self.db_session.commit()
        except IntegrityError:
            self.db_session.rollback()
            self.db_session.commit()
            await retry_discord_message_command(ctx.send, f'Unable to create playlist "{name}", name likely already exists')
            return None
        self.logger.info(f'Music :: Playlist created "{playlist.name}" with ID {playlist.id} in guild {ctx.guild.id}')
        await retry_discord_message_command(ctx.send, f'Created playlist "{name}"',
                                            delete_after=self.delete_after)
        return playlist

    @playlist.command(name='create')
    async def playlist_create(self, ctx, *, name: str):
        '''
        Create new playlist.

        name: str [Required]
            Name of new playlist to create
        '''
        await self.__playlist_create(ctx, name)

    @playlist.command(name='list')
    async def playlist_list(self, ctx):
        '''
        List playlists.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return
        playlist_items = self.db_session.query(Playlist).\
            filter(Playlist.server_id == str(ctx.guild.id)).order_by(Playlist.created_at.asc())
        playlist_items = [p for p in playlist_items if PLAYHISTORY_PREFIX not in p.name]

        if not playlist_items:
            return await retry_discord_message_command(ctx.send, 'No playlists in database',
                                            delete_after=self.delete_after)

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
        for mess in messages:
            await retry_discord_message_command(ctx.send, mess, delete_after=self.delete_after)

    def __playlist_add_item(self, playlist, data_id, data_url, data_title, data_uploader):
        self.logger.info(f'Music :: Adding video {data_url} to playlist {playlist.id}')
        item_count = self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist.id).count()
        if item_count >= (self.server_playlist_max_size):
            raise PlaylistMaxLength(f'Playlist {playlist.id} greater to or equal to max length {self.server_playlist_max_size}')

        playlist_item = PlaylistItem(title=shorten_string_cjk(data_title, 256),
                                     video_id=data_id,
                                     video_url=data_url,
                                     uploader=shorten_string_cjk(data_uploader, 256),
                                     playlist_id=playlist.id,
                                     created_at=datetime.utcnow())
        try:
            self.db_session.add(playlist_item)
            self.db_session.commit()
            return playlist_item
        except IntegrityError:
            self.db_session.rollback()
            self.db_session.commit()
            return None

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
        return await self.__playlist_item_add(ctx, playlist_index, search)

    async def __add_playlist_item_function(self, ctx, search, playlist, source_download: SourceDownload):
        '''
        Call this when the source download eventually completes
        source_download : Source Download from download client
        '''
        if source_download is None:
            await retry_discord_message_command(ctx.send, f'Unable to find video for search {search}')
        self.logger.info(f'Music :: Adding video_id {source_download.webpage_url} to playlist "{playlist.name}" '
                         f' in guild {ctx.guild.id}')
        try:
            playlist_item = self.__playlist_add_item(playlist, source_download.id, source_download.webpage_url, source_download.title, source_download.uploader)
        except PlaylistMaxLength:
            retry_discord_message_command(ctx.send, f'Cannot add more items to playlist "{playlist.name}", already max size', delete_after=self.delete_after)
            return
        if playlist_item:
            await retry_discord_message_command(ctx.send, f'Added item "{source_download.title}" to playlist {playlist.name}', delete_after=self.delete_after)
        else:
            await retry_discord_message_command(ctx.send, content=f'Unable to add playlist item "{search}" , likely already exists', delete_after=self.delete_after)
        return await retry_discord_message_command(source_download.source_dict.delete_message)

    async def __playlist_item_add(self, ctx, playlist_index, search):

        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return

        vc = ctx.voice_client

        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        try:
            source_entries = await self.download_client.check_source(search, ctx.guild.id, ctx.author.display_name, ctx.author.id, self.bot.loop)
        except DownloadClientException as exc:
            self.logger.warning(f'Received download client exception for search "{search}", {str(exc)}')
            return await retry_discord_message_command(ctx.send, f'{exc.user_message}', delete_after=self.delete_after)
        for source_dict in source_entries:
            source_dict.download_file = False
            # Pylint disable as gets injected later
            source_dict.set_message(await retry_discord_message_command(ctx.send, f'Downloading and processing "{str(source_dict)}" to add to playlist'))
            source_dict.post_download_callback_functions = [partial(self.__add_playlist_item_function, ctx, search, playlist)] #pylint: disable=no-value-for-parameter
            self.download_queue.put_nowait(source_dict.guild_id, source_dict)

    @playlist.command(name='item-remove')
    async def playlist_item_remove(self, ctx: Context, playlist_index: int, video_index: int):
        '''
        Add item to playlist

        playlist_index: integer [Required]
            ID of playlist
        video_index: integer [Required]
            ID of video to remove
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        try:
            video_index = int(video_index)
        except ValueError:
            return await retry_discord_message_command(ctx.send, f'Invalid item index {video_index}',
                                            delete_after=self.delete_after)
        if video_index < 1:
            return await retry_discord_message_command(ctx.send, f'Invalid item index {video_index}',
                                            delete_after=self.delete_after)

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        query_results = [item for item in query]
        try:
            item = query_results[video_index - 1]
            self.db_session.delete(item)
            self.db_session.commit()
            return await retry_discord_message_command(ctx.send, f'Removed item {video_index} from playlist',
                                            delete_after=self.delete_after)
        except IndexError:
            return await retry_discord_message_command(ctx.send, f'Unable to find item {video_index}',
                                            delete_after=self.delete_after)

    @playlist.command(name='show')
    async def playlist_show(self, ctx: Context, playlist_index: int):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None

        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
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
        for (count, item) in enumerate(query): #pylint:disable=protected-access
            uploader = item.uploader or ''
            table.add_row([
                f'{count + 1}',
                f'{item.title} /// {uploader}',
            ])
            # Backwards compat for new field
            if not item.created_at:
                item.created_at = datetime.utcnow()
                self.db_session.add(item)
                self.db_session.commit()
        messages = [f'```{t}```' for t in table.print()]
        for mess in messages:
            self.message_queue.put_nowait(partial(ctx.send, mess, delete_after=self.delete_after))

    @playlist.command(name='delete')
    async def playlist_delete(self, ctx: Context, playlist_index: int):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        return await self.__playlist_delete(ctx, playlist_index)

    async def __playlist_delete(self, ctx: Context, playlist_index: int):
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        self.logger.info(f'Music :: Deleting playlist items "{playlist.name}"')
        self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id).delete()
        self.db_session.delete(playlist)
        self.db_session.commit()
        self.message_queue.put_nowait(partial(ctx.send, f'Deleted playlist {playlist_index}',
                                              delete_after=self.delete_after))
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
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return

        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        self.logger.info(f'Music :: Renaming playlist {playlist.id} to name "{playlist_name}"')
        playlist.name = playlist_name
        self.db_session.commit()
        self.message_queue.put_nowait(partial(ctx.send, f'Renamed playlist {playlist_index} to name "{playlist_name}"',
                                              delete_after=self.delete_after))
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
        playlist = await self.__playlist_create(ctx, name)
        if not playlist:
            return None

        try:
            player = self.players[ctx.guild.id]
        except KeyError:
            return await retry_discord_message_command(ctx.send, 'No player connected, no queue to save',
                                                       delete_after=self.delete_after)
        # Do a deepcopy here so list doesn't mutate as we iterate
        if is_history:
            queue_copy = player.history.items()
        else:
            queue_copy = player.play_queue.items()

        self.logger.info(f'Music :: Saving queue contents to playlist "{name}", is_history? {is_history}')

        if len(queue_copy) == 0:
            self.message_queue.put_nowait(partial(ctx.send, 'There are no videos to add to playlist',
                                                  delete_after=self.delete_after))
            return

        for data in queue_copy:
            try:
                playlist_item = self.__playlist_add_item(playlist, data.id, data.webpage_url, data.title, data.uploader)
            except PlaylistMaxLength:
                self.message_queue.put_nowait(partial(ctx.send, f'Cannot add more items to playlist "{playlist.name}", already max size',
                                                      delete_after=self.delete_after))
                break
            if playlist_item:
                self.message_queue.put_nowait(partial(ctx.send, f'Added item "{data.title}" to playlist', delete_after=self.delete_after))
                continue
            self.message_queue.put_nowait(partial(ctx.send, f'Unable to add playlist item "{data.title}", likely already exists', delete_after=self.delete_after))
        self.message_queue.put_nowait(partial(ctx.send, f'Finished adding items to playlist "{name}"', delete_after=self.delete_after))
        if is_history:
            player.history.clear()
            self.message_queue.put_nowait(partial(ctx.send, 'Cleared history', delete_after=self.delete_after))
        return

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
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return
        # Make sure sub command is valid
        playlist = await self.__get_playlist(playlist_index, ctx)
        if not playlist:
            return None
        shuffle = False
        max_num = None
        if sub_command:
            if 'shuffle' in sub_command.lower():
                shuffle = True
            number_matcher = re_match(NUMBER_REGEX, sub_command.lower())
            if number_matcher:
                max_num = int(number_matcher.group('number'))
        return await self.__playlist_queue(ctx, playlist, shuffle, max_num)

    @command(name='random-play')
    async def playlist_random_play(self, ctx: Context, sub_command: Optional[str] = ''):
        '''
        Play random videos from history

        Sub commands - [cache] [max_num]
            max_num - Number of videos to add to the queue at maximum
            cache   - Play videos that are available in cache
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
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
            history_playlist = self.db_session.query(Playlist).\
                filter(Playlist.server_id == str(ctx.guild.id)).\
                filter(Playlist.is_history == True).first()

            if not history_playlist:
                self.message_queue.put_nowait(partial(ctx.send, 'Unable to find history for server', delete_after=self.delete_after))
                return
            return await self.__playlist_queue(ctx, history_playlist, True, max_num, is_history=True)
        # Turn this into a list so it can do the shuffle functions and other things
        cache_items = [i for i in self.db_session.query(VideoCache).\
            join(VideoCacheGuild).\
            join(Guild).\
            filter(Guild.server_id == str(ctx.guild.id)).limit(max_num)]

        for _ in range(self.number_shuffles):
            random_shuffle(cache_items)

        vc = ctx.voice_client
        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client
        # Get player in case we dont have one already
        player = await self.get_player(ctx, vc.channel)
        broke_early = await self.__playlist_enqueue_items(ctx, cache_items, True, player)
        if broke_early:
            self.message_queue.put_nowait(partial(ctx.send, 'Added as many videos in cache to queue as possible, but hit limit',
                                                  delete_after=self.delete_after))
        elif max_num:
            self.message_queue.put_nowait(partial(ctx.send, f'Added {max_num} videos from cache to queue',
                                                  delete_after=self.delete_after))
        else:
            self.message_queue.put_nowait(partial(ctx.send, 'Added all videos in playlist cache to queue',
                                                  delete_after=self.delete_after))
        return

    async def __delete_non_existing_item(self, item: PlaylistItem, ctx: Context):
        self.logger.warning(f'Unable to find video "{item.video_id}" in playlist {item.playlist_id}, deleting')
        self.message_queue.put_nowait(partial(ctx.send, content=f'Unable to find video "{item.video_id}" in playlist, deleting',
                                              delete_after=self.delete_after))
        self.db_session.delete(item)
        self.db_session.commit()

    async def __playlist_enqueue_items(self, ctx: Context, playlist_items: List[PlaylistItem], is_history: bool, player: MusicPlayer):
        '''
        Enqueue items from a playlist
        ctx: Standard discord context
        playlist_items: Playlist item objects to iterate over
        is_history: Is this a history playlist, pass into entries
        player: MusicPlayer
        '''
        # Track if we broke early for eventual return block
        broke_early = False
        for (count, item) in enumerate(playlist_items):
            try:
                # Just add directly to download queue here, since we already know the video id
                source_dict = SourceDict(ctx.guild.id,
                                   ctx.author.display_name,
                                   ctx.author.id,
                                   item.video_url,
                                   SearchType.DIRECT,
                                   added_from_history=is_history,
                                   video_non_exist_callback_functions=[partial(self.__delete_non_existing_item, item, ctx)])
                source_download = await self.__check_video_cache(source_dict)
                if source_download:
                    self.logger.debug(f'Music :: Search "{item.video_url}" found in cache, placing in player queue')
                    # Skip queue strings for every cahced result except the last one
                    skip_queue_strings = not count == (len(playlist_items) - 1)
                    await self.__add_source_to_player(source_download, player, skip_update_queue_strings=skip_queue_strings)
                    continue
                self.logger.debug(f'Music :: Handing off "{item.video_url}" to download queue')
                source_dict.set_message(await retry_discord_message_command(ctx.send, f'Downloading and processing "{item.title}"'))
                self.download_queue.put_nowait(source_dict.guild_id, source_dict)
            except QueueFull:
                if source_dict.message:
                    self.message_queue.put_nowait(partial(source_dict.message.edit, content=f'Unable to add item "{item.title}" with id "{item.video_id}" to queue, queue is full',
                                                          delete_after=self.delete_after))
                else:
                    # Message not send originally
                    self.message_queue.put_nowait(partial(ctx.send, content=f'Unable to add item "{item.title}" with id "{item.video_id}" to queue, queue is full',
                                                          delete_after=self.delete_after))
                broke_early = True
                break
            except PutsBlocked:
                self.logger.warning(f'Music :: Puts to queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                self.message_queue.put_nowait(partial(source_dict.delete_message))
                break
        # Update queue strings finally just to be safe
        await player.update_queue_strings()
        return broke_early

    async def __playlist_queue(self, ctx: Context, playlist: Playlist, shuffle: bool, max_num: int, is_history: bool = False):
        vc = ctx.voice_client
        if not vc:
            await ctx.invoke(self.connect_)
            vc = ctx.voice_client

        self.logger.info(f'Music :: Playlist queue called for playlist "{playlist.name}" in server "{ctx.guild.id}"')
        query = self.db_session.query(PlaylistItem).\
            filter(PlaylistItem.playlist_id == playlist.id)
        playlist_items = []
        # Backwards compat for new field
        for item in query:
            playlist_items.append(item)
            if not item.created_at:
                item.created_at = datetime.now(timezone.utc)
                self.db_session.add(item)
                self.db_session.commit()

        if shuffle:
            self.message_queue.put_nowait(partial(ctx.send, 'Shuffling playlist items',
                                                  delete_after=self.delete_after))
            for _ in range(self.number_shuffles):
                random_shuffle(playlist_items)

        if max_num:
            if max_num < 0:
                self.message_queue.put_nowait(partial(ctx.send, f'Invalid number of videos {max_num}',
                                                      delete_after=self.delete_after))
                return
            if max_num < len(playlist_items):
                playlist_items = playlist_items[:max_num]
            else:
                max_num = 0

        # Get player in case we dont have one already
        player = await self.get_player(ctx, vc.channel)
        broke_early = await self.__playlist_enqueue_items(ctx, playlist_items, is_history, player)

        playlist_name = playlist.name
        if PLAYHISTORY_PREFIX in playlist_name:
            playlist_name = 'Channel History'
        if broke_early:
            self.message_queue.put_nowait(partial(ctx.send, f'Added as many videos in playlist "{playlist_name}" to queue as possible, but hit limit',
                                                  delete_after=self.delete_after))
        elif max_num:
            self.message_queue.put_nowait(partial(ctx.send, f'Added {max_num} videos from "{playlist_name}" to queue',
                                                  delete_after=self.delete_after))
        else:
            self.message_queue.put_nowait(partial(ctx.send, f'Added all videos in playlist "{playlist_name}" to queue',
                                                  delete_after=self.delete_after))
        playlist.last_queued = datetime.now(timezone.utc)
        self.db_session.commit()

    @playlist.command(name='merge')
    async def playlist_merge(self, ctx: Context, playlist_index_one: str, playlist_index_two: str):
        '''
        Merge second playlist into first playlist, deletes second playlist

        playlist_index_one: integer [Required]
            ID of playlist to be merged, will be kept
        playlist_index_two: integer [Required]
            ID of playlist to be merged, will be deleted
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        if not await self.__check_database_session(ctx):
            return

        self.logger.info(f'Music :: Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{ctx.guild.id}"')
        playlist_one = await self.__get_playlist(playlist_index_one, ctx)
        playlist_two = await self.__get_playlist(playlist_index_two, ctx)
        if not playlist_one:
            self.message_queue.put_nowait(partial(ctx.send, f'Cannot find playlist {playlist_index_one}', delete_after=self.delete_after))
            return
        if not playlist_two:
            self.message_queue.put_nowait(partial(ctx.send, f'Cannot find playlist {playlist_index_two}', delete_after=self.delete_after))
            return
        query = self.db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_two.id)
        for item in query:
            try:
                playlist_item = self.__playlist_add_item(playlist_one, item.video_id, item.video_url, item.title, item.uploader)
            except PlaylistMaxLength:
                self.message_queue.put_nowait(partial(ctx.send, f'Cannot add more items to playlist "{playlist_one.name}", already max size', delete_after=self.delete_after))
                return
            if playlist_item:
                self.message_queue.put_nowait(partial(ctx.send, f'Added item "{item.title}" to playlist {playlist_index_one}',
                                                      delete_after=self.delete_after))
                continue
            self.message_queue.put_nowait(partial(ctx.send, f'Unable to add playlist item "{item.title}", likely already exists',
                                                  delete_after=self.delete_after))
        await self.__playlist_delete(ctx, playlist_index_two)
