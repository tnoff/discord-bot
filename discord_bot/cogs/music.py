# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

from asyncio import sleep
from asyncio import QueueEmpty, QueueFull, TimeoutError as async_timeout
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from random import shuffle as random_shuffle, randint
from re import match as re_match
from shutil import disk_usage
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import Optional, List

from dappertable import shorten_string_cjk, DapperTable
from discord.ext.commands import Bot, Context, group, command
from discord.errors import DiscordServerError, Forbidden
from discord import VoiceChannel
from discord.errors import NotFound
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode
from opentelemetry.metrics import Observation
from sqlalchemy import asc
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session
from yt_dlp import YoutubeDL
from yt_dlp.postprocessor import PostProcessor
from yt_dlp.utils import DownloadError

from discord_bot.cogs.common import CogHelper
from discord_bot.cogs.music_helpers.common import SearchType, MessageLifecycleStage, MessageType, MultipleMutableType
from discord_bot.cogs.music_helpers.message_context import MessageContext
from discord_bot.cogs.music_helpers.download_client import DownloadClient, DownloadClientException
from discord_bot.cogs.music_helpers.download_client import ExistingFileException, BotDownloadFlagged, match_generator
from discord_bot.cogs.music_helpers.message_formatter import MessageFormatter
from discord_bot.cogs.music_helpers.message_queue import MessageQueue
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.search_client import SearchClient, SearchException, check_youtube_video
from discord_bot.cogs.music_helpers.media_request import MediaRequest, media_request_attributes
from discord_bot.cogs.music_helpers.media_download import MediaDownload, media_download_attributes
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient

from discord_bot.database import Playlist, PlaylistItem, VideoCache, VideoCacheBackup
from discord_bot.exceptions import CogMissingRequiredArg, ExitEarlyException
from discord_bot.cogs.schema import SERVER_ID
from discord_bot.utils.common import async_retry_discord_message_command, rm_tree, return_loop_runner, get_logger, create_observable_gauge
from discord_bot.utils.audio import edit_audio_file
from discord_bot.utils.queue import PutsBlocked
from discord_bot.utils.distributed_queue import DistributedQueue
from discord_bot.utils.clients.spotify import SpotifyClient
from discord_bot.utils.clients.youtube import YoutubeClient
from discord_bot.utils.clients.youtube_music import YoutubeMusicClient
from discord_bot.utils.sql_retry import retry_database_commands
from discord_bot.utils.queue import Queue
from discord_bot.utils.otel import otel_span_wrapper, command_wrapper, AttributeNaming, MetricNaming, DiscordContextNaming, METER_PROVIDER

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
                    }
                },
                'storage': {
                    'type': 'object',
                    'properties': {
                        'backend': {
                            'type': 'string',
                            "enum": ['s3'],
                        },
                        'bucket_name': {
                            'type': 'string',
                        }
                    },
                    'required': ['backend', 'bucket_name'],
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

OTEL_SPAN_PREFIX = 'music'

VIDEOS_PLAYED_COUNTER = METER_PROVIDER.create_counter(MetricNaming.VIDEOS_PLAYED.value, unit='number', description='Number of videos played')


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
        self._history_playlist_task = None

        # Keep track of when bot is in shutdown mode
        self.bot_shutdown = False
        # Message queue bits
        self.message_queue = MessageQueue()
        # History Playlist Queue
        self.history_playlist_queue = None
        if self.db_engine:
            self.history_playlist_queue = Queue()
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

        self.backup_storage_options = self.settings.get('music', {}).get('download', {}).get('storage', {})

        # Setup rest of client
        if download_dir_path is not None:
            self.download_dir = Path(download_dir_path)
            if not self.download_dir.exists():
                self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        # Tempdir used in downloads
        self.temp_download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        # Tempdir for players
        self.player_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        self.video_cache = None
        if self.enable_cache and self.db_engine:
            self.video_cache = VideoCacheClient(self.download_dir, max_cache_files, partial(self.with_db_session),
                                                self.backup_storage_options.get('backend', None), self.backup_storage_options.get('bucket_name', None))
            self.video_cache.verify_cache()


        self.last_download_lockfile = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with

        # Use this to track the files being copied over currently
        # So we dont delete them as they are in flight
        self.sources_in_transit = {}

        ytdlopts = {
            'format': 'bestaudio/best',
            'restrictfilenames': True,
            'noplaylist': True,
            'nocheckcertificate': True,
            'ignoreerrors': False,
            'logtostderr': False,
            'logger': get_logger('ytdlp', settings.get('general', {}).get('logging', {})),
            'default_search': 'auto',
            'source_address': '0.0.0.0',  # ipv6 addresses cause issues sometimes
            'outtmpl': str(self.temp_download_dir / f'{YTDLP_OUTPUT_TEMPLATE}'),
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
        self.search_client = SearchClient(self.message_queue, spotify_client=self.spotify_client, youtube_client=self.youtube_client,
                                          youtube_music_client=self.youtube_music_client,
                                          number_shuffles=self.number_shuffles)
        self.download_client = DownloadClient(ytdl, self.download_dir)

        # Callback functions
        create_observable_gauge(METER_PROVIDER, MetricNaming.ACTIVE_PLAYERS.value, self.__active_players_callback, 'Active music players')
        create_observable_gauge(METER_PROVIDER, MetricNaming.CACHE_FILE_COUNT.value, self.__cache_count_callback, 'Number of cache files in use')
        # Cache file count callback
        if self.download_dir and self.download_dir.is_mount():
            # Cache stats
            create_observable_gauge(METER_PROVIDER, MetricNaming.CACHE_FILESYSTEM_MAX.value, self.__cache_filestats_callback_total, 'Max size of cache filesystem', unit='bytes')
            create_observable_gauge(METER_PROVIDER, MetricNaming.CACHE_FILESYSTEM_USED.value, self.__cache_filestats_callback_used, 'Used size of cache filesystem', unit='bytes')
        # Timestamps for heartbeat gauges
        #self.playlist_history_timestamp = None
        self.send_message_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__send_message_loop_active_callback, 'Send message loop heartbeat')
        self.cleanup_player_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__cleanup_player_loop_active_callback, 'Cleanup player loop heartbeat')
        self.cache_cleanup_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__cache_cleanup_loop_active_callback, 'Cache cleanup loop heartbeat')
        self.download_file_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__download_file_loop_active_callback, 'Download files loop heartbeat')
        self.playlist_history_checkfile = Path(NamedTemporaryFile(delete=False).name) #pylint:disable=consider-using-with
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__playlist_history_loop_active_callback, 'Playlist update loop heartbeat')

    # Metric callback functons
    def __playlist_history_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.playlist_history_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'playlist_history'
            })
        ]
    def __download_file_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.download_file_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'download_files'
            })
        ]

    def __cache_cleanup_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.send_message_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'cache_cleanup'
            })
        ]
    def __send_message_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.send_message_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'send_messages'
            })
        ]

    def __cleanup_player_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = int(self.cleanup_player_checkfile.read_text())
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'cleanup_players'
            })
        ]

    def __active_players_callback(self, _options):
        '''
        Get active players
        '''
        items = []
        for key in self.players:
            items.append(Observation(1, attributes={
                DiscordContextNaming.GUILD.value: key,
            }))
        return items

    def __cache_count_callback(self, _options):
        '''
        Cache count observer
        '''
        def get_cache_file_count(db_session: Session):
            return db_session.query(VideoCache).count()
        with self.with_db_session() as db_session:
            cache_file_count = retry_database_commands(db_session, partial(get_cache_file_count, db_session))
            return [
                Observation(cache_file_count)
            ]

    def __cache_filestats_callback_used(self, _options):
        '''
        Cache stats observer
        '''
        _, used, _ = disk_usage(str(self.download_dir))
        return [
            Observation(used)
        ]

    def __cache_filestats_callback_total(self, _options):
        '''
        Cache stats observer
        '''
        total, _, _ = disk_usage(str(self.download_dir))
        return [
            Observation(total)
        ]

    async def cog_load(self):
        '''
        When cog starts
        '''
        self._cleanup_task = self.bot.loop.create_task(return_loop_runner(self.cleanup_players, self.bot, self.logger, self.cleanup_player_checkfile)())
        self._download_task = self.bot.loop.create_task(return_loop_runner(self.download_files, self.bot, self.logger, self.download_file_checkfile)())
        self._message_task = self.bot.loop.create_task(return_loop_runner(self.send_messages, self.bot, self.logger, self.send_message_checkfile, continue_exceptions=DiscordServerError)())
        if self.enable_cache:
            self._cache_cleanup_task = self.bot.loop.create_task(return_loop_runner(self.cache_cleanup, self.bot, self.logger, self.cache_cleanup_checkfile)())
        if self.db_engine:
            self._history_playlist_task = self.bot.loop.create_task(return_loop_runner(self.playlist_history_update, self.bot, self.logger, self.playlist_history_checkfile)())

    async def cog_unload(self):
        '''
        Run when cog stops
        '''
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cog_unload', kind=SpanKind.INTERNAL):
            self.logger.debug('Calling shutdown on Music')

            self.bot_shutdown = True

            guilds = list(self.players.keys())
            self.logger.debug(f'Calling shutdown on guild players {guilds}')
            for guild_id in guilds:
                self.logger.info(f'Calling shutdown on player in guild {guild_id}')
                guild = await self.bot.fetch_guild(guild_id)
                await self.cleanup(guild, external_shutdown_called=True)

            self.logger.debug('Cancelling main tasks')
            if self._cleanup_task:
                self._cleanup_task.cancel()
            if self._download_task:
                self._download_task.cancel()
            if self._cache_cleanup_task:
                self._cache_cleanup_task.cancel()
            if self._message_task:
                self._message_task.cancel()
            if self._history_playlist_task:
                self._history_playlist_task.cancel()
            self.last_download_lockfile.unlink(missing_ok=True)

            if self.download_dir.exists() and not self.enable_cache:
                rm_tree(self.download_dir)
            if self.temp_download_dir.exists():
                rm_tree(self.temp_download_dir)
            if self.player_dir.exists():
                rm_tree(self.player_dir)
            # Delete loop checkfiles
            if self.cleanup_player_checkfile.exists():
                self.cleanup_player_checkfile.unlink()
            if self.send_message_checkfile.exists():
                self.send_message_checkfile.unlink()
            if self.cache_cleanup_checkfile.exists():
                self.cache_cleanup_checkfile.unlink()
            if self.download_file_checkfile.exists():
                self.download_file_checkfile.unlink()
            if self.playlist_history_checkfile.exists():
                self.playlist_history_checkfile.unlink()


            return True


    async def playlist_history_update(self):
        '''
        Update history playlists
        '''
        def delete_existing_item(db_session: Session, webpage_url: str, playlist_id: int):
            existing_history_item = db_session.query(PlaylistItem).\
                filter(PlaylistItem.video_url == webpage_url).\
                filter(PlaylistItem.playlist_id == playlist_id).first()
            if existing_history_item:
                self.logger.debug(f'New history item {webpage_url} already exists, deleting this first')
                db_session.delete(existing_history_item)
                db_session.commit()

        def get_playlist_size(db_session: Session, playlist_id: int):
            return db_session.query(PlaylistItem).filter(PlaylistItem.playlist_id == playlist_id).count()

        def delete_extra_items(db_session: Session, playlist_id: int, delta: int):
            for existing_item in db_session.query(PlaylistItem).\
                    filter(PlaylistItem.playlist_id == playlist_id).\
                    order_by(asc(PlaylistItem.created_at)).limit(delta):
                self.logger.debug(f'Deleting older history playlist item {existing_item.video_url} from playlist {playlist_id}, created on {existing_item.created_at}')
                db_session.delete(existing_item)
            db_session.commit()

        await sleep(.01)
        try:
            history_item = self.history_playlist_queue.get_nowait()
        except QueueEmpty:
            if self.bot_shutdown:
                raise ExitEarlyException('Exiting history cleanup') #pylint:disable=raise-missing-from
            return

        # Add all videos to metrics
        VIDEOS_PLAYED_COUNTER.add(1, attributes={
            DiscordContextNaming.GUILD.value: history_item.media_download.media_request.guild_id
        })
        # Skip if added from history
        if history_item.media_download.media_request.added_from_history:
            self.logger.info(f'Played video "{history_item.media_download.webpage_url}" was original played from history, skipping history add')
            return

        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.playlist_history_update', kind=SpanKind.CONSUMER):
            with self.with_db_session() as db_session:
                self.logger.info(f'Attempting to add url "{history_item.media_download.webpage_url}" to history playlist {history_item.playlist_id} for server {history_item.media_download.media_request.guild_id}')
                retry_database_commands(db_session, partial(delete_existing_item, db_session, history_item.media_download.webpage_url, history_item.playlist_id))

                # Delete number of rows necessary to add list
                existing_items = retry_database_commands(db_session, partial(get_playlist_size, db_session, history_item.playlist_id))
                delta = (existing_items + 1) - self.server_playlist_max_size
                if delta > 0:
                    self.logger.info(f'Need to delete {delta} items from history playlist {history_item.playlist_id}')
                    retry_database_commands(db_session, partial(delete_extra_items, db_session, history_item.playlist_id, delta))
                self.logger.info(f'Adding new history item "{history_item.media_download.webpage_url}" to playlist {history_item.playlist_id}')
                self.__playlist_insert_item(history_item.playlist_id, history_item.media_download.webpage_url, history_item.media_download.title, history_item.media_download.uploader)
                # Update metrics
                VIDEOS_PLAYED_COUNTER.add(1, attributes={
                    DiscordContextNaming.GUILD.value: history_item.media_download.media_request.guild_id
                })

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

        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.send_messages', kind=SpanKind.CONSUMER) as span:
            if source_type == MessageType.SINGLE_IMMUTABLE:
                for message_context in item:
                    await async_retry_discord_message_command(message_context.function, allow_404=True)
                return True
            if source_type == MessageType.SINGLE_MUTABLE:
                try:
                    result = await async_retry_discord_message_command(partial(item.function, item.message_content, delete_after=item.delete_after))
                    if item.lifecycle_stage == MessageLifecycleStage.SEND:
                        item.set_message(result)
                    return True
                except Forbidden as e:
                    # Add some extra context so we can debug when this happens
                    span.set_attributes({
                        DiscordContextNaming.GUILD.value: item.guild_id,
                        DiscordContextNaming.CHANNEL.value: item.channel_id,
                    })
                    raise e
                except NotFound:
                    if item.lifecycle_stage == MessageLifecycleStage.DELETE:
                        self.logger.warning(f'Unable to find message for deletion for source {item}')
                        return False
                    raise
            if source_type == MessageType.MULTIPLE_MUTABLE:
                if MultipleMutableType.PLAY_ORDER.value in item:
                    guild_id = item.replace(f'{MultipleMutableType.PLAY_ORDER.value}-', '')
                    player = self.players.get(int(guild_id), None)
                    message_content = player.get_queue_order_messages() if player else []
                    funcs = await self.message_queue.update_mutable_bundle_content(item, message_content)
                    results = []
                    for func in funcs:
                        result = await async_retry_discord_message_command(func)
                        results.append(result)
                    # Update message references for new messages (only pass Message objects, not delete results)
                    message_results = [r for r in results if r and hasattr(r, 'id')]
                    await self.message_queue.update_mutable_bundle_references(item, message_results)
                    return True
            return False

    async def cleanup_players(self):
        '''
        Check for players with no members, cleanup bot in channels that do
        '''
        if self.bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        await sleep(1)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cleanup_players', kind=SpanKind.CONSUMER):
            guilds = []
            for _guild_id, player in self.players.items():
                if not player.voice_channel_inactive():
                    message_context = MessageContext(player.guild.id, player.text_channel.id)
                    message_context.function = partial(player.text_channel.send, content='No members in guild, removing myself',
                                                       delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
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

        def list_non_backup_files(db_session: Session):
            return db_session.query(VideoCache).\
                outerjoin(VideoCacheBackup, VideoCache.id == VideoCacheBackup.video_cache_id).\
                filter(VideoCacheBackup.id == None).\
                all()

        if self.bot_shutdown:
            raise ExitEarlyException('Bot in shutdown, exiting early')
        await sleep(1)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cache_cleanup', kind=SpanKind.CONSUMER):
            # Get metric data first
            delete_videos = []
            self.video_cache.ready_remove()
            with self.with_db_session() as db_session:
                # Then check for deleted videos
                for video_cache in retry_database_commands(db_session, partial(list_ready_cache_files, db_session)):
                    # Check if video cache in use
                    if str(video_cache.base_path) in self.sources_in_transit.values():
                        continue
                    delete_videos.append(video_cache.id)

                if delete_videos:
                    self.logger.debug(f'Identified cache videos ready for deletion {delete_videos}')
                    self.video_cache.remove_video_cache(delete_videos)

                # Check for pending backup files
                for video_cache in retry_database_commands(db_session, partial(list_non_backup_files, db_session)):
                    self.logger.info(f'Backing up video cache file {video_cache.id} to object storage')
                    self.video_cache.object_storage_backup(video_cache.id)

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
        self.logger.debug(f'Waiting on backoff in youtube, waiting until {wait_until}')
        while True:
            # If bot exited, return now
            if self.bot_shutdown:
                raise ExitEarlyException('Exiting bot wait loop')
            now = int(datetime.now(timezone.utc).timestamp())
            if now > wait_until:
                return True
            await sleep(1)

    async def add_source_to_player(self, media_download: MediaDownload, player: MusicPlayer, skip_update_queue_strings: bool = False):
        '''
        Add source to player queue

        media_request : Standard media_request for pre-download
        media_download : Standard MediaDownload for post download
        player : MusicPlayer
        skiP_update_queue_strings : Skip queue string update
        '''
        attributes = media_download_attributes(media_download)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.add_source_to_player', kind=SpanKind.INTERNAL, attributes=attributes):
            try:
                if self.video_cache:
                    self.logger.info(f'Iterating file on base path {str(media_download.base_path)}')
                    self.video_cache.iterate_file(media_download)
                self.sources_in_transit[media_download.media_request.uuid] = str(media_download.base_path)
                media_download.ready_file(guild_path=player.file_dir)
                self.sources_in_transit.pop(media_download.media_request.uuid)
                player.add_to_play_queue(media_download)
                self.logger.info(f'Adding "{media_download.webpage_url}" '
                                f'to queue in guild {media_download.media_request.guild_id}')
                if not skip_update_queue_strings:
                    self.message_queue.update_multiple_mutable(
                        f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}',
                        player.text_channel,
                    )
                self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.DELETE,
                                                         partial(media_download.media_request.message_context.delete_message), '')

                return True
            except QueueFull:
                self.logger.warning(f'Play queue full, aborting download of item "{str(media_download.media_request)}"')
                self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.EDIT,
                                                         partial(media_download.media_request.message_context.edit_message),
                                                         MessageFormatter.format_play_queue_full_message(str(media_download.media_request)),
                                                         delete_after=self.delete_after)
                media_download.delete()
                return False
                # Dont return to loop, file was downloaded so we can iterate on cache at least
            except PutsBlocked:
                self.logger.warning(f'Puts Blocked on queue in guild "{media_download.media_request.guild_id}", assuming shutdown')
                self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.DELETE,
                                                         partial(media_download.media_request.message_context.delete_message), '')
                media_download.delete()
                return False

    def update_download_lockfile(self, media_download: MediaDownload,
                                 add_additional_backoff: int=None) -> bool:
        '''
        Update the download lockfile

        media_download : Media Download
        add_additional_backoff : Add more backoff time to existing timestamp

        '''
        if media_download and media_download.extractor != 'youtube':
            return False
        new_timestamp = int(datetime.now(timezone.utc).timestamp())
        if add_additional_backoff:
            new_timestamp += add_additional_backoff
        self.last_download_lockfile.write_text(str(new_timestamp))
        return True

    # Take both source dict and media download
    # Since media download might be none
    async def __ensure_video_download_result(self, media_request: MediaRequest, media_download: MediaDownload):
        if media_download is None:
            self.message_queue.update_single_mutable(media_request.message_context, MessageLifecycleStage.EDIT,
                                                     partial(media_request.message_context.edit_message),
                                                     MessageFormatter.format_video_download_issue_message(str(media_request)), delete_after=self.delete_after)
            return False
        return True

    async def __return_bad_video(self, media_request: MediaRequest, exception: DownloadClientException,
                                 skip_callback_functions: bool=False):
        message = exception.user_message
        self.message_queue.update_single_mutable(media_request.message_context, MessageLifecycleStage.EDIT,
                                                 partial(media_request.message_context.edit_message), message, delete_after=self.delete_after)
        if not skip_callback_functions:
            for func in media_request.video_non_exist_callback_functions:
                await func()
        return

    async def __check_video_cache(self, media_request: MediaRequest):
        if not self.video_cache:
            return None
        return self.video_cache.get_webpage_url_item(media_request)


    async def download_files(self): #pylint:disable=too-many-statements
        '''
        Main runner
        '''
        if self.bot_shutdown:
            raise ExitEarlyException('Bot shutdown called, exiting early')

        await sleep(.01)
        try:
            media_request = self.download_queue.get_nowait()
        except QueueEmpty:
            return

        attributes = media_request_attributes(media_request)
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.download_files', kind=SpanKind.CONSUMER, attributes=attributes) as span:
            # If not meant to download, dont check for player
            # Check for player, if doesn't exist return
            player = None
            if media_request.download_file:
                player = await self.get_player(media_request.guild_id, create_player=False)
                if not player:
                    self.message_queue.update_single_mutable(media_request.message_context, MessageLifecycleStage.DELETE,
                                                             partial(media_request.message_context.delete_message), '')
                    return

                # Check if queue in shutdown, if so return
                if player.shutdown_called:
                    self.logger.warning(f'Play queue in shutdown, skipping downloads for guild {player.guild.id}')
                    self.message_queue.update_single_mutable(media_request.message_context, MessageLifecycleStage.DELETE,
                                                             partial(media_request.message_context.delete_message), '')
                    return
            self.logger.debug(f'Gathered new item to download "{str(media_request)}", guild "{media_request.guild_id}"')
            # If cache enabled and search string with 'https://' given, try to grab this first
            media_download = await self.__check_video_cache(media_request)
            # Else grab from ytdlp
            if not media_download:
                # Make sure we wait for next video download
                # Dont spam the video client
                await self.youtube_backoff_time(self.youtube_wait_period_min, self.youtube_wait_period_max_variance)
                try:
                    media_download = await self.download_client.create_source(media_request, self.bot.loop)
                    self.update_download_lockfile(media_download)
                except ExistingFileException as e:
                    # File exists on disk already, create again from cache
                    self.logger.debug(f'Existing file found for download {str(media_request)}, using existing file from url "{e.video_cache.video_url}"')
                    media_download = self.video_cache.generate_download_from_existing(media_request, e.video_cache)
                    self.update_download_lockfile(media_download)
                    span.set_status(StatusCode.OK)
                except (BotDownloadFlagged) as e:
                    self.logger.warning(f'Bot flagged while downloading video "{str(media_request)}", {str(e)}')
                    await self.__return_bad_video(media_request, e, skip_callback_functions=True)
                    self.logger.warning(f'Adding additional time {self.youtube_wait_period_min} to usual youtube backoff since bot was flagged')
                    self.update_download_lockfile(media_download, add_additional_backoff=self.youtube_wait_period_min)
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(e)
                    return
                except (DownloadClientException) as e:
                    self.logger.warning(f'Known error while downloading video "{str(media_request)}", {str(e)}')
                    await self.__return_bad_video(media_request, e)
                    self.update_download_lockfile(media_download)
                    span.set_status(StatusCode.OK)
                    return
                except DownloadError as e:
                    self.logger.error(f'Unknown error while downloading video "{str(media_request)}", {str(e)}')
                    media_download = None
                    self.message_queue.update_single_mutable(media_request.message_context, MessageLifecycleStage.EDIT,
                                                             partial(media_request.message_context.edit_message),
                                                             MessageFormatter.format_video_download_issue_message(str(media_request), str(e)),
                                                             delete_after=self.delete_after)
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(e)

            # Final none check in case we couldn't download video
            if not await self.__ensure_video_download_result(media_request, media_download):
                span.set_status(StatusCode.ERROR)
                return
            span.set_status(StatusCode.OK)
            # Check if we need to add to a playlist
            if media_request.add_to_playlist:
                await self.__add_playlist_item_function(media_request.add_to_playlist, media_download)

            if media_request.download_file and player:
                # Add sources to players
                if not await self.add_source_to_player(media_download, player):
                    return

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
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cleanup', kind=SpanKind.CONSUMER, attributes={DiscordContextNaming.GUILD.value: guild.id}):
            self.logger.info(f'Starting cleanup on guild {guild.id}')
            player = await self.get_player(guild.id, create_player=False)
            # Set external shutdown so this doesnt happen twice
            player.shutdown_called = True
            if external_shutdown_called and player:
                message_context = MessageContext(player.guild.id, player.text_channel.id)
                message_context.function = partial(player.text_channel.send, content='External shutdown called on bot, please contact admin for details',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
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
            self.logger.info(f'Clearing queue message for guild {guild.id}')
            self.message_queue.update_multiple_mutable(
                f'{MultipleMutableType.PLAY_ORDER.value}-{guild.id}',
                player.text_channel,
            )

            self.logger.debug(f'Starting cleaning tasks on player for guild {guild.id}')
            await player.cleanup()

            self.logger.debug(f'Clearing download queue for guild {guild.id}')
            pending_items = self.download_queue.clear_queue(guild.id)
            self.logger.debug(f'Found {len(pending_items)} existing download items')
            for source in pending_items:
                self.message_queue.update_single_mutable(source.message_context, MessageLifecycleStage.DELETE, source.message_context.delete_message, '')

            self.logger.debug(f'Deleting download dir for guild {guild.id}')
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
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_player', kind=SpanKind.INTERNAL, attributes={DiscordContextNaming.GUILD.value: guild_id}):
            try:
                player = self.players[guild_id]
            except KeyError:
                if check_voice_client_active:
                    message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                    message_context.function = partial(ctx.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
                    return None
                if not create_player:
                    return None
                # Make directory for guild specific files
                guild_path = self.player_dir / f'{ctx.guild.id}'
                guild_path.mkdir(exist_ok=True, parents=True)
                # Generate and start player
                history_playlist_id = self.__get_history_playlist(ctx.guild.id)
                player = MusicPlayer(self.logger, ctx, [partial(self.cleanup, ctx.guild)],
                                     self.queue_max_size, self.disconnect_timeout,
                                     guild_path, self.message_queue,
                                     history_playlist_id, self.history_playlist_queue)
                await player.start_tasks()
                self.players[guild_id] = player
            if check_voice_client_active:
                if not player.guild.voice_client or (not player.guild.voice_client.is_playing() and not self.download_queue.get_queue_size(guild_id)):
                    message_context = MessageContext(player.guild.id, player.text_channel.id)
                    message_context.function = partial(player.text_channel.send, 'I am not currently playing anything',
                                                       delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'{ctx.author.display_name} not in voice chat channel. Please join one and try again',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return None

        if not check_voice_chats:
            return channel

        if channel.guild.id is not ctx.guild.id:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'User not joined to channel bot is in, ignoring command',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return None
        return channel

    async def __ensure_player(self, ctx: Context, channel: VoiceChannel) -> MusicPlayer:
        with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ensure_player', kind=SpanKind.INTERNAL, attributes={DiscordContextNaming.GUILD.value: ctx.guild.id}):
            try:
                return await self.get_player(ctx.guild.id, join_channel=channel, ctx=ctx)
            except async_timeout as e:
                self.logger.error(f'Reached async timeout error on bot joining channel, {str(e)}')
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Bot cannot join channel {channel}', delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
            return None

    @command(name='join', aliases=['awaken'])
    @command_wrapper
    async def connect_(self, ctx: Context):
        '''
        Connect to voice channel.
        '''
        channel = await self.__check_author_voice_chat(ctx, check_voice_chats=False)
        if not channel:
            return

        await self.__ensure_player(ctx, channel)

        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, f'Connected to: {channel}', delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])

    async def enqueue_media_requests(self, ctx: Context, player: MusicPlayer, entries: List[MediaRequest]) -> bool:
        '''
        Enqueue source dicts to a player or download queue

        ctx: Discord Context
        player: Music Player
        entries: List of source dicts

        Returns true if all items added, false if some were not
        '''
        for media_request in entries:
            try:
                # Check cache first
                media_download = await self.__check_video_cache(media_request)
                if media_download:
                    self.logger.debug(f'Search "{str(media_request)}" found in cache, placing in player queue')
                    await self.add_source_to_player(media_download, player)
                    continue
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                media_request.message_context = message_context
                self.message_queue.update_single_mutable(message_context, MessageLifecycleStage.SEND,
                                                         partial(ctx.send),
                                                         MessageFormatter.format_downloading_message(str(media_request)))
                self.logger.debug(f'Handing off media_request {str(media_request)} to download queue')
                self.download_queue.put_nowait(media_request.guild_id, media_request, priority=self.server_queue_priority.get(ctx.guild.id, None))
            except PutsBlocked:
                self.logger.warning(f'Puts to queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                self.message_queue.update_single_mutable(message_context, MessageLifecycleStage.DELETE,
                                                        partial(message_context.delete_message), '')
                return False
            except QueueFull:
                self.message_queue.update_single_mutable(message_context, MessageLifecycleStage.EDIT,
                                                         partial(message_context.edit_message),
                                                         MessageFormatter.format_download_queue_full_message(str(media_request)),
                                                         delete_after=self.delete_after)
                return False
        self.message_queue.update_multiple_mutable(
            f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}',
            player.text_channel,
        )
        return True

    @command(name='play')
    @command_wrapper
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
            entries = await self.search_client.check_source(search, ctx.guild.id, ctx.channel.id, ctx.author.display_name, ctx.author.id, self.bot.loop,
                                                              self.queue_max_size, ctx.channel)
        except SearchException as exc:
            self.logger.warning(f'Received download client exception for search "{search}", {str(exc)}')
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'{exc.user_message}', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        await self.enqueue_media_requests(ctx, player, entries)

    @command(name='skip')
    @command_wrapper
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
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, f'Skipping video "{current_title}"',
                                           delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])

    @command(name='clear')
    @command_wrapper
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'There are currently no more queued videos.',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        self.logger.info(f'Player clear called in guild {ctx.guild.id}')
        player.clear_queue()
        self.message_queue.update_multiple_mutable(
            f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}',
            player.text_channel,
        )
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, 'Cleared player queue', delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])
        return

    @command(name='history')
    @command_wrapper
    async def history_(self, ctx: Context):
        '''
        Show recently played videos
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx)

        if player.check_history_empty():
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'There have been no videos played.',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
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
        message_contexts = []
        for mess in messages:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, mess, delete_after=self.delete_after)
            message_contexts.append(message_context)
        self.message_queue.send_single_immutable(message_contexts)

    @command(name='shuffle')
    @command_wrapper
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'There are currently no more queued videos.',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        player.shuffle_queue()
        self.message_queue.update_multiple_mutable(
            f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}',
            player.text_channel,
        )

    @command(name='remove')
    @command_wrapper
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'There are currently no more queued videos.',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        try:
            queue_index = int(queue_index)
        except ValueError:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Invalid queue index {queue_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        item = player.remove_queue_item(queue_index)
        if item is None:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to remove queue index {queue_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, f'Removed item {item.title} from queue',
                                           delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])
        item.delete()
        self.message_queue.update_multiple_mutable(
            f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}',
            player.text_channel,
        )

    @command(name='bump')
    @command_wrapper
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'There are currently no more queued videos.',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        try:
            queue_index = int(queue_index)
        except ValueError:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Invalid queue index {queue_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        item = player.bump_queue_item(queue_index)
        if item is None:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to bump queue index {queue_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, f'Bumped item {item.title} to top of queue',
                                           delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])

        self.message_queue.update_multiple_mutable(
            f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}',
            player.text_channel,
        )

    @command(name='stop')
    @command_wrapper
    async def stop_(self, ctx):
        '''
        Stop the currently playing video and disconnect bot from voice chat.
        '''
        if not await self.__check_author_voice_chat(ctx):
            return
        player = await self.get_player(ctx.guild.id, ctx=ctx)
        if not player:
            return
        self.logger.info(f'Calling stop for guild {ctx.guild.id}')
        await self.cleanup(ctx.guild)

    @command(name='move-messages')
    @command_wrapper
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'I am already sending messages to channel {ctx.channel.name}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        # Update the bundle to move to the new channel (this deletes old messages)
        bundle_index = f'{MultipleMutableType.PLAY_ORDER.value}-{ctx.guild.id}'
        await self.message_queue.update_mutable_bundle_channel(bundle_index, ctx.channel)

        # Update the player's text channel reference
        player.text_channel = ctx.channel

        # Queue an update to send new messages in the new channel
        self.message_queue.update_multiple_mutable(
            bundle_index,
            ctx.channel,
        )

    async def __get_playlist(self, playlist_index: int, ctx: Context):
        def check_playlist_count(db_session: Session, guild_id: str):
            return db_session.query(Playlist.id).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == False).count()

        def get_history_playlist(db_session: Session, guild_id: str):
            return db_session.query(Playlist.id).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == True).first()

        def list_non_history_playlists(db_session: Session, guild_id: str, offset: int):
            return db_session.query(Playlist.id).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == False).\
                order_by(Playlist.created_at.asc()).offset(offset).first()

        try:
            index = int(playlist_index)
        except ValueError:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return None, False

        with self.with_db_session() as db_session:
            if index > 0:
                if not retry_database_commands(db_session, partial(check_playlist_count, db_session, str(ctx.guild.id))):
                    message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                    message_context.function = partial(ctx.send, 'No playlists in database',
                                                       delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
                    return None, False

            is_history = False
            if index == 0:
                playlist = retry_database_commands(db_session, partial(get_history_playlist, db_session, str(ctx.guild.id)))[0]
                is_history = True
            else:
                playlist = retry_database_commands(db_session, partial(list_non_history_playlists, db_session, str(ctx.guild.id), (index - 1)))[0]
            if not playlist:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Invalid playlist index {playlist_index}', delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
                return None, False
            return playlist, is_history

    async def __check_database_session(self, ctx: Context):
        '''
        Check if database session is in use
        '''
        if not self.db_engine:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'Functionality not available, database is not enabled')
            self.message_queue.send_single_immutable([message_context])
            return False
        return True

    @group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions.
        '''
        if ctx.invoked_subcommand is None:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'Invalid sub command passed...', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])

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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to create playlist "{name}", name cannot contain {PLAYHISTORY_PREFIX}')
            self.message_queue.send_single_immutable([message_context])
            return None
        with self.with_db_session() as db_session:
            existing_playlist = retry_database_commands(db_session, partial(check_for_playlist, db_session, playlist_name, str(ctx.guild.id)))
            if existing_playlist:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Unable to create playlist "{name}", name likely already exists')
                self.message_queue.send_single_immutable([message_context])
                return None

            playlist = retry_database_commands(db_session, partial(create_playlist, db_session, playlist_name, str(ctx.guild.id)))
            self.logger.info(f'Playlist created "{playlist_name}" with ID {playlist.id} in guild {ctx.guild.id}')
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Created playlist "{playlist_name}" with id {playlist.id}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return playlist.id

    @playlist.command(name='create')
    @command_wrapper
    async def playlist_create(self, ctx: Context, *, name: str):
        '''
        Create new playlist.

        name: str [Required]
            Name of new playlist to create
        '''
        await self.__playlist_create(ctx, name)

    @playlist.command(name='list')
    @command_wrapper
    async def playlist_list(self, ctx: Context):
        '''
        List playlists.
        '''
        def get_history_playlist(db_session: Session, guild_id: str):
            return db_session.query(Playlist).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == True).\
                first()

        def get_playlist_items(db_session: Session, guild_id: str):
            return db_session.query(Playlist).\
                filter(Playlist.server_id == str(guild_id)).\
                filter(Playlist.is_history == False).\
                order_by(Playlist.created_at.asc())

        if not await self.__check_database_session(ctx):
            return
        with self.with_db_session() as db_session:
            history_playlist = retry_database_commands(db_session, partial(get_history_playlist, db_session, str(ctx.guild.id)))
            playlist_items = retry_database_commands(db_session, partial(get_playlist_items, db_session, str(ctx.guild.id)))

            if not playlist_items and not history_playlist:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, 'No playlists in database',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
                return

            if history_playlist:
                playlist_items = [history_playlist] + [i for i in playlist_items]

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
                name = item.name
                if item.is_history:
                    name = 'History Playlist'
                table.add_row([
                    f'{count}',
                    name,
                    last_queued,
                ])
            messages = [f'```{t}```' for t in table.print()]
            message_contexts = []
            for mess in messages:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, mess, delete_after=self.delete_after)
                message_contexts.append(message_context)
            self.message_queue.send_single_immutable(message_contexts)

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
            self.logger.info(f'Adding video "{video_url}" to playlist {playlist_id}')
            item_count = retry_database_commands(db_session, partial(get_item_count, db_session, playlist_id))
            if item_count >= self.server_playlist_max_size:
                raise PlaylistMaxLength(f'Playlist {playlist_id} greater to or equal to max length {self.server_playlist_max_size}')

            existing_item = retry_database_commands(db_session, partial(check_existing_item, db_session, playlist_id, video_url))
            if existing_item:
                return None

            playlist_item_id = retry_database_commands(db_session, partial(create_new_item, db_session, video_title,
                                                                           video_url, video_uploader, playlist_id))
            return playlist_item_id

    async def __add_playlist_item_function(self, playlist_id: int, media_download: MediaDownload):
        '''
        Call this when the media download eventually completes
        media_download : Media Download from download client
        '''
        if media_download is None:
            self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.EDIT,
                                                     partial(media_download.media_request.message_context.edit_message),
                                                     MessageFormatter.format_playlist_generation_issue_message(str(media_download.media_request)),
                                                     delete_after=self.delete_after)
            return
        self.logger.info(f'Adding video_url "{media_download.webpage_url}" to playlist "{playlist_id}" '
                         f' in guild {media_download.media_request.guild_id}')
        try:
            playlist_item_id = self.__playlist_insert_item(playlist_id, media_download.webpage_url, media_download.title, media_download.uploader)
        except PlaylistMaxLength:
            self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.EDIT,
                                                     partial(media_download.media_request.message_context.edit_message),
                                                     MessageFormatter.format_playlist_max_length_message(),
                                                     delete_after=self.delete_after)
            return
        if playlist_item_id:
            self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.EDIT,
                                                     partial(media_download.media_request.message_context.edit_message),
                                                     MessageFormatter.format_playlist_item_added_message(media_download.title),
                                                     delete_after=self.delete_after)
            return
        self.message_queue.update_single_mutable(media_download.media_request.message_context, MessageLifecycleStage.EDIT,
                                                 partial(media_download.media_request.message_context.edit_message),
                                                 MessageFormatter.format_playlist_item_add_failed_message(str(media_download.media_request)),
                                                 delete_after=self.delete_after)
        return

    @playlist.command(name='item-add')
    @command_wrapper
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

        playlist_id, is_history = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None

        if is_history:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to add "{search}" to history playlist, is reserved and cannot be added to manually', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        try:
            source_entries = await self.search_client.check_source(search, ctx.guild.id, ctx.channel.id, ctx.author.display_name, ctx.author.id, self.bot.loop,
                                                                     self.queue_max_size, ctx.channel)
        except SearchException as exc:
            self.logger.warning(f'Received download client exception for search "{search}", {str(exc)}')
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'{exc.user_message}', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        for media_request in source_entries:
            media_request.download_file = False
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            media_request.message_context = message_context
            self.message_queue.update_single_mutable(message_context, MessageLifecycleStage.SEND, partial(ctx.send),
                                                     MessageFormatter.format_downloading_for_playlist_message(str(media_request)))
            media_download = await self.__check_video_cache(media_request)
            if media_download:
                self.logger.debug(f'Search "{str(media_request)}" found in cache, placing in playlist item')
                await self.__add_playlist_item_function(playlist_id, media_download)
                continue
            media_request.add_to_playlist = playlist_id
            self.download_queue.put_nowait(media_request.guild_id, media_request, priority=self.server_queue_priority.get(ctx.guild.id, None))

    @playlist.command(name='item-remove')
    @command_wrapper
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

        playlist_id, _is_history  = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None
        try:
            video_index = int(video_index)
        except ValueError:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Invalid item index {video_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        if video_index < 1:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Invalid item index {video_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        with self.with_db_session() as db_session:
            if retry_database_commands(db_session, partial(remove_playlist_item_remove, db_session, playlist_id, (video_index - 1))):
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Removed item "{video_index}" from playlist',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
                return
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to find item {video_index}',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

    @playlist.command(name='show')
    @command_wrapper
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

        playlist_id, _is_history = await self.__get_playlist(playlist_index, ctx)
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
            total = 0
            for (count, item) in enumerate(retry_database_commands(db_session, partial(get_playlist_items, db_session, playlist_id))): #pylint:disable=protected-access
                uploader = item.uploader or ''
                table.add_row([
                    f'{count + 1}',
                    f'{item.title} /// {uploader}',
                ])
                total += 1
            if not total:
                self.message_queue.send_single_immutable(f'No items in playlist {playlist_id}')
                return
            messages = [f'```{t}```' for t in table.print()]
            message_contexts = []
            for mess in messages:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, mess, delete_after=self.delete_after)
                message_contexts.append(message_context)
            self.message_queue.send_single_immutable(message_contexts)

    @playlist.command(name='delete')
    @command_wrapper
    async def playlist_delete(self, ctx: Context, playlist_index: int):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        if not await self.__check_database_session(ctx):
            return

        playlist_id, is_history  = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None
        if is_history:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'Cannot delete history playlist, is reserved', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        await self.__playlist_delete(playlist_id)
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, f'Deleted playlist {playlist_index}',
                                           delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])
        return

    async def __playlist_delete(self, playlist_id: int):
        def delete_playlist(db_session: Session, playlist_id: int):
            db_session.query(PlaylistItem).\
                filter(PlaylistItem.playlist_id == playlist_id).delete()
            query = db_session.query(Playlist).get(playlist_id)
            if query:
                db_session.delete(query)
            db_session.commit()
        self.logger.info(f'Deleting playlist items "{playlist_id}"')
        with self.with_db_session() as db_session:
            retry_database_commands(db_session, partial(delete_playlist, db_session, playlist_id))
            return

    @playlist.command(name='rename')
    @command_wrapper
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

        playlist_id, is_history = await self.__get_playlist(playlist_index, ctx)
        if is_history:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'Cannot rename history playlist, is reserved', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        if not playlist_id:
            return None

        playlist_name = shorten_string_cjk(playlist_name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to create playlist "{playlist_name}", name cannot contain {PLAYHISTORY_PREFIX}')
            self.message_queue.send_single_immutable([message_context])
            return None

        self.logger.info(f'Renaming playlist {playlist_id} to name "{playlist_name}"')
        with self.with_db_session() as db_session:
            retry_database_commands(db_session, partial(rename_playlist, db_session, playlist_id, playlist_name))
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Renamed playlist {playlist_index} to name "{playlist_name}"',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

    @playlist.command(name='save-queue')
    @command_wrapper
    async def playlist_queue_save(self, ctx: Context, *, name: str):
        '''
        Save contents of queue to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        return await self.__playlist_queue_save(ctx, name)

    @playlist.command(name='save-history')
    @command_wrapper
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
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'No player connected, no queue to save',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        # Do a deepcopy here so list doesn't mutate as we iterate
        if is_history:
            queue_copy = player.get_history_items()
        else:
            queue_copy = player.get_queue_items()

        self.logger.info(f'Saving queue contents to playlist "{name}", is_history? {is_history}')

        if len(queue_copy) == 0:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'There are no videos to add to playlist',
                                               delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return

        for data in queue_copy:
            try:
                playlist_item_id = self.__playlist_insert_item(playlist_id, data.webpage_url, data.title, data.uploader)
            except PlaylistMaxLength:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, 'Cannot add more items to playlist, already max size',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
                break
            if playlist_item_id:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Added item "{data.title}" to playlist', delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
                continue
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Unable to add playlist item "{data.title}", likely already exists', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, f'Finished adding items to playlist "{name}"', delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])
        return

    async def __delete_non_existing_item(self, item_id: int, item_video_url: str, ctx: Context):
        self.logger.warning(f'Unable to find playlist item {item_id} in playlist, deleting')
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, content=f'Unable to find video "{item_video_url}" in playlist, deleting',
                                           delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])
        with self.with_db_session() as db_session:
            item = db_session.query(PlaylistItem).get(item_id)
            db_session.delete(item)
            db_session.commit()

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

        self.logger.info(f'Playlist queue called for playlist {playlist_id} in server "{ctx.guild.id}"')

        with self.with_db_session() as db_session:
            playlist_items = []
            for item in retry_database_commands(db_session, partial(list_playlist_items, db_session, playlist_id)):
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                media_request = MediaRequest(ctx.guild.id,
                                         ctx.channel.id,
                                         ctx.author.display_name,
                                         ctx.author.id,
                                         item.video_url,
                                         SearchType.YOUTUBE if check_youtube_video(item.video_url) else SearchType.DIRECT,
                                         added_from_history=is_history,
                                         video_non_exist_callback_functions=[partial(self.__delete_non_existing_item, item.id, item.video_url, ctx)] if is_history else [],
                                         message_context=message_context)
                playlist_items.append(media_request)

            if shuffle:
                for _ in range(self.number_shuffles):
                    random_shuffle(playlist_items)

            if max_num:
                if max_num < 0:
                    message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                    message_context.function = partial(ctx.send, f'Invalid number of videos {max_num}',
                                                       delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
                    return
                if max_num < len(playlist_items):
                    playlist_items = playlist_items[:max_num]
                else:
                    max_num = 0

            broke_early = await self.enqueue_media_requests(ctx, player, playlist_items)

            playlist_name = retry_database_commands(db_session, partial(get_playlist_name, db_session, playlist_id))
            if is_history:
                playlist_name = 'Channel History'
            if broke_early:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Added as many videos in playlist "{playlist_name}" to queue as possible, but hit limit',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
            elif max_num:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Added {max_num} videos from "{playlist_name}" to queue',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
            else:
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Added all videos in playlist "{playlist_name}" to queue',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
            retry_database_commands(db_session, partial(playlist_update_queued, db_session, playlist_id))

    @playlist.command(name='queue')
    @command_wrapper
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
        playlist_id, is_history = await self.__get_playlist(playlist_index, ctx)
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
        return await self.__playlist_queue(ctx, player, playlist_id, shuffle, max_num, is_history=is_history)

    @playlist.command(name='merge')
    @command_wrapper
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

        self.logger.info(f'Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{ctx.guild.id}"')
        playlist_one_id, is_history1 = await self.__get_playlist(playlist_index_one, ctx)
        playlist_two_id, is_history2  = await self.__get_playlist(playlist_index_two, ctx)
        if is_history1 or is_history2:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, 'Cannot merge history playlist, is reserved', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        if not playlist_one_id:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Cannot find playlist {playlist_index_one}', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        if not playlist_two_id:
            message_context = MessageContext(ctx.guild.id, ctx.channel.id)
            message_context.function = partial(ctx.send, f'Cannot find playlist {playlist_index_two}', delete_after=self.delete_after)
            self.message_queue.send_single_immutable([message_context])
            return
        with self.with_db_session() as db_session:
            for item in retry_database_commands(db_session, partial(get_playlist_items, db_session, playlist_two_id)):
                try:
                    playlist_item_id = self.__playlist_insert_item(playlist_one_id, item.video_url, item.title, item.uploader)
                except PlaylistMaxLength:
                    message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                    message_context.function = partial(ctx.send, f'Cannot add more items to playlist "{playlist_one_id}", already max size', delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
                    return
                if playlist_item_id:
                    message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                    message_context.function = partial(ctx.send, f'Added item "{item.title}" to playlist {playlist_index_one}',
                                                       delete_after=self.delete_after)
                    self.message_queue.send_single_immutable([message_context])
                    continue
                message_context = MessageContext(ctx.guild.id, ctx.channel.id)
                message_context.function = partial(ctx.send, f'Unable to add playlist item "{item.title}", likely already exists',
                                                   delete_after=self.delete_after)
                self.message_queue.send_single_immutable([message_context])
        await self.__playlist_delete(playlist_index_two)

    @command(name='random-play')
    @command_wrapper
    async def playlist_random_play(self, ctx: Context):
        '''
        Deprecated, please use !playlist queue 0
        '''
        message_context = MessageContext(ctx.guild.id, ctx.channel.id)
        message_context.function = partial(ctx.send, 'Function deprecated, please use `!playlist queue 0 shuffle`', delete_after=self.delete_after)
        self.message_queue.send_single_immutable([message_context])
        return
