# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

import asyncio
from asyncio import sleep, create_task
from asyncio import QueueEmpty, QueueFull, TimeoutError as async_timeout
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
import random
from shutil import disk_usage
from tempfile import TemporaryDirectory
from time import time
from typing import List, Optional

from dappertable import shorten_string, DapperTable, Columns, Column, PaginationLength
from discord.ext.commands import Bot, Context, group, command
from discord import VoiceChannel, TextChannel
from discord.errors import ClientException
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode
from opentelemetry.metrics import Observation
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.engine.base import Engine

from discord_bot.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_bot.cogs.cog_helper import CogHelper
from discord_bot.cogs.music_helpers.common import SearchType, MultipleMutableType, PLAYHISTORY_PREFIX
from discord_bot.clients.download_client import InMemoryDownloadClient
from discord_bot.workers.asyncio_download_worker import AsyncioDownloadWorker
from discord_bot.workers.redis_download_worker import RedisDownloadWorker
from discord_bot.interfaces.download_protocols import DownloadClient
from discord_bot.types.cleanup_reason import CleanupReason
from discord_bot.types.download import LifecycleEvent, LifecycleStatusUpdate
from discord_bot.utils.failure_queue import FailureStatus, FailureQueue
from discord_bot.workers.asyncio_broker import AsyncioBroker
from discord_bot.servers.broker_server import BrokerHttpServer
from discord_bot.clients.broker_client import HttpBrokerClient, InMemoryBrokerClient
from discord_bot.cogs.music_helpers.music_player import MusicPlayer
from discord_bot.cogs.music_helpers.search_client import SearchClient, SearchException, check_youtube_video
from discord_bot.types.search import SearchResult
from discord_bot.types.media_request import MediaRequest, media_request_attributes
from discord_bot.types.playlist_add_request import PlaylistAddRequest
from discord_bot.types.playlist_add_result import PlaylistAddResult
from discord_bot.types.media_download import MediaDownload, media_download_attributes
from discord_bot.types.history_playlist_item import HistoryPlaylistItem
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient, MusicCacheConfig
from discord_bot.cogs.music_helpers import database_functions

from discord_bot.database import PlaylistItem, Playlist
from discord_bot.exceptions import CogMissingRequiredArg, ExitEarlyException
from discord_bot.utils.common import rm_tree, return_loop_runner
from discord_bot.types.queue import PutsBlocked
from discord_bot.utils.distributed_queue import DistributedQueue
from discord_bot.utils.integrations.spotify import SpotifyClient
from discord_bot.utils.integrations.youtube import YoutubeClient
from discord_bot.utils.integrations.youtube_music import YoutubeMusicClient, YoutubeMusicRetryException
from discord_bot.utils.sql_retry import async_retry_database_commands
from discord_bot.types.queue import Queue
from discord_bot.utils.otel import async_otel_span_wrapper, capture_span_context, command_wrapper, AttributeNaming, MetricNaming, DiscordContextNaming, METER_PROVIDER, create_observable_gauge, span_links_from_context
from discord_bot.utils.integrations.common import YOUTUBE_VIDEO_PREFIX
from discord_bot.clients.dispatch_client_base import DispatchClientBase

# GLOBALS

PLAYHISTORY_NAME = 'Channel History'

# Find numbers in strings
NUMBER_REGEX = r'.*(?P<number>[0-9]+).*'

# Pydantic config models
class MusicGeneralConfig(BaseModel):
    '''General music configuration'''
    message_delete_after: int = 300

class MusicPlayerConfig(BaseModel):
    '''Music player configuration'''
    queue_max_size: int = Field(default=128, ge=1)
    disconnect_timeout: int = Field(default=900, ge=1)
    inactive_voice_channel_timeout: int = Field(default=180, ge=1)
    player_dir_path: Optional[str] = None

class MusicPlaylistConfig(BaseModel):
    '''Music playlist configuration'''
    server_playlist_max_size: int = Field(default=64, ge=1)

class SpotifyCredentialsConfig(BaseModel):
    '''Spotify API credentials configuration'''
    client_id: str
    client_secret: str

class ServerQueuePriorityConfig(BaseModel):
    '''Server queue priority configuration'''
    server_id: int
    priority: int

class MusicStorageConfig(BaseModel):
    '''Music storage backend configuration'''
    bucket_name: str
    prefetch_limit: int = Field(default=5, ge=0)

class MusicDownloadConfig(BaseModel):
    '''Music download configuration'''
    download_dir_path: Optional[str] = None
    max_video_length: int = Field(default=900, ge=1)
    extra_ytdlp_options: dict = Field(default_factory=dict)
    banned_videos_list: list[str] = Field(default_factory=list)
    youtube_wait_period_minimum: int = Field(default=30, ge=1)
    youtube_wait_period_max_variance: int = Field(default=10, ge=1)
    # Number of concurrent download loops. Defaults to 1: yt-dlp/YouTube
    # rate-limits per source IP, so a single downloader per egress IP is the
    # safe default. Raise only when downloads egress over distinct IPs.
    max_concurrent_downloads: int = Field(default=1, ge=1)
    # Back the download queue with Redis (RedisDownloadWorker) instead of the
    # in-process AsyncioDownloadWorker, so downloads can be shared across pods.
    # Requires a redis_manager; falls back to in-process if unset.
    redis_backed: bool = False
    # Per-egress bucket for the shared YouTube backoff/failure keys. Pods behind
    # distinct egress IPs should use distinct keys so their rate-limits don't couple.
    youtube_egress_key: str = 'default'
    spotify_credentials: Optional[SpotifyCredentialsConfig] = None
    youtube_api_key: Optional[str] = None
    server_queue_priority: list[ServerQueuePriorityConfig] = Field(default_factory=list)
    cache: MusicCacheConfig = Field(default_factory=MusicCacheConfig)
    storage: Optional[MusicStorageConfig] = None
    normalize_audio: bool = False
    max_download_retries: int = Field(default=3, ge=1)
    max_youtube_music_search_retries: int = Field(default=3, ge=1)
    # Mostly to keep a cap on the queue to avoid issues
    failure_tracking_max_size: int = Field(default=100, ge=1)
    # Recommended to be at least an hour
    failure_tracking_max_age_seconds: int = Field(default=600, ge=1)

    @model_validator(mode='after')
    def validate_cache_requires_storage(self) -> 'MusicDownloadConfig':
        '''Require storage when enable_cache_files is set.'''
        if self.cache.enable_cache_files and self.storage is None:  #pylint:disable=no-member
            raise ValueError('enable_cache_files requires storage to be configured')
        return self

class BrokerServerConfig(BaseModel):
    '''Config for running a broker HTTP server on this process.'''
    # bandit B104: '0.0.0.0' default is intentional — worker pods reach the broker across the docker/k8s network; operators override via music.broker_server.host in config
    host: str = '0.0.0.0'  # nosec B104
    port: int = Field(default=8081, ge=1, le=65535)

class BrokerClientConfig(BaseModel):
    '''Config for connecting to a remote broker HTTP server.'''
    url: str

class MusicConfig(BaseModel):
    '''Top-level music cog configuration'''
    general: MusicGeneralConfig = Field(default_factory=MusicGeneralConfig)
    player: MusicPlayerConfig = Field(default_factory=MusicPlayerConfig)
    playlist: MusicPlaylistConfig = Field(default_factory=MusicPlaylistConfig)
    download: MusicDownloadConfig = Field(default_factory=MusicDownloadConfig)
    broker_server: BrokerServerConfig | None = None
    broker_client: BrokerClientConfig | None = None

#
# Exceptions
#

class PlaylistMaxLength(Exception):
    '''
    Playlist hit max length
    '''

OTEL_SPAN_PREFIX = 'music'
# Idle backoff for process_download_results when the broker has no finished
# result ready — in HA this paces the remote GET /results/next poll.
_BROKER_POLL_INTERVAL_SECONDS = 1.0

#
class Music(CogHelper): #pylint:disable=too-many-public-methods
    '''
    Music related commands
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 db_engine: Engine = None, redis_manager=None): #pylint:disable=too-many-statements
        super().__init__(bot, settings, dispatcher, db_engine,
                         settings_prefix='music', config_model=MusicConfig,
                         redis_manager=redis_manager)
        if not self.settings.get('general', {}).get('include', {}).get('music', False):
            raise CogMissingRequiredArg('Music not enabled')

        self.players = {}
        self._cleanup_task = None
        self._download_tasks = []
        self._result_task = None
        self._post_play_processing_task = None
        self._youtube_search_task = None
        self._init_task = None

        # Keep track of when bot is in shutdown mode
        self.bot_shutdown_event = asyncio.Event()
        self._message_delete_after = self.config.general.message_delete_after
        # History Playlist Queue
        self.history_playlist_queue: Queue[HistoryPlaylistItem] | None = None
        if self.db_engine:
            self.history_playlist_queue = Queue()

        # Queues for youtube music search; download queue is owned by download_client
        # Search queue can be larger since search requests are lightweight
        self.youtube_music_search_queue: DistributedQueue[tuple[MediaRequest, TextChannel]] = DistributedQueue(self.config.player.queue_max_size * 2)

        self.spotify_client = None
        if self.config.download.spotify_credentials:
            self.spotify_client = SpotifyClient(
                self.config.download.spotify_credentials.client_id,
                self.config.download.spotify_credentials.client_secret
            )

        self.youtube_client = None
        if self.config.download.youtube_api_key:
            self.youtube_client = YoutubeClient(self.config.download.youtube_api_key)

        self.youtube_music_client = YoutubeMusicClient()

        self.server_queue_priority = {}
        if self.config.download and self.config.download.server_queue_priority:
            for item in self.config.download.server_queue_priority:
                self.server_queue_priority[int(item.server_id)] = item.priority

        storage_bucket_name = self.config.download.storage.bucket_name if self.config.download.storage else None

        # Dir for player working files; use configured path if set, otherwise a temp dir
        if self.config.player.player_dir_path is not None:
            self.player_dir = Path(self.config.player.player_dir_path)
            self.player_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.player_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with
            self.player_dir.mkdir(exist_ok=True, parents=True)

        # Set download dir for download client
        # If not given assume its a tmpdir
        self.download_dir: Path | None = None
        if self.config.download.download_dir_path is not None:
            self.download_dir = Path(self.config.download.download_dir_path)
            self.download_dir.mkdir(exist_ok=True, parents=True)
        else:
            self.download_dir = Path(TemporaryDirectory().name) #pylint:disable=consider-using-with
            self.download_dir.mkdir(exist_ok=True, parents=True)

        self.video_cache = None
        if self.config.download.cache.enable_cache_files and self.db_engine and storage_bucket_name:
            self.video_cache = VideoCacheClient(
                self.config.download.cache.max_cache_files,
                partial(self.with_db_session),
                max_cache_size_bytes=(
                    self.config.download.cache.max_cache_size_mb * 1024 * 1024
                    if self.config.download.cache.max_cache_size_mb else None
                ),
                storage_type='s3',
            )

        self.media_broker = AsyncioBroker(
            video_cache=self.video_cache,
            bucket_name=storage_bucket_name,
            dispatcher=self.dispatcher,
            download_max_retries=self.config.download.max_download_retries,
            search_max_retries=self.config.download.max_youtube_music_search_retries,
            message_delete_after=self.config.general.message_delete_after,
        )

        if self.config.broker_client:
            # bucket_name is load-bearing in HA: an HA broker's checkout returns
            # CheckoutResult(s3_key=...), and the client stamps bucket_name onto
            # it so MusicPlayer knows where to fetch the file from S3. Without it
            # the player falls through to open() the raw s3_key and 404s.
            self.broker_client = HttpBrokerClient(
                self.config.broker_client.url, bucket_name=storage_bucket_name)
        else:
            self.broker_client = InMemoryBrokerClient(self.media_broker)

        self.search_client = SearchClient(spotify_client=self.spotify_client, youtube_client=self.youtube_client)
        # Add any filter functions, do some logic so we only pass a single function into the processor
        failure_queue = FailureQueue(
            max_size=self.config.download.failure_tracking_max_size,
            max_age_seconds=self.config.download.failure_tracking_max_age_seconds,
        )
        download_worker_kwargs = {
            'extra_ytdlp_options': self.config.download.extra_ytdlp_options,
            'max_video_length': self.config.download.max_video_length,
            'banned_video_list': self.config.download.banned_videos_list,
            'failure_queue': failure_queue,
            'wait_period_minimum': self.config.download.youtube_wait_period_minimum,
            'wait_period_max_variance': self.config.download.youtube_wait_period_max_variance,
            'bucket_name': storage_bucket_name,
            'normalize_audio': self.config.download.normalize_audio,
            'broker': self.broker_client,
            'max_retries': self.config.download.max_download_retries,
        }
        if self.config.download.redis_backed and self.redis_manager:
            # Redis-backed queue: shareable across downloader pods (MR 2b runs it
            # in-process; the download_server/HttpDownloadClient split is later).
            download_worker = RedisDownloadWorker(
                self.logging_config,
                self.download_dir,
                redis_manager=self.redis_manager,
                youtube_egress_key=self.config.download.youtube_egress_key,
                **download_worker_kwargs,
            )
        else:
            download_worker = AsyncioDownloadWorker(
                self.logging_config,
                self.download_dir,
                queue_max_size=self.config.player.queue_max_size,
                **download_worker_kwargs,
            )
        self.download_client: DownloadClient = InMemoryDownloadClient(download_worker)
        self.youtube_music_failure_queue = FailureQueue(
            max_size=self.config.download.failure_tracking_max_size,
            max_age_seconds=self.config.download.failure_tracking_max_age_seconds,
        )
        self.youtube_music_wait_timestamp: float | None = None

        # Callback functions
        create_observable_gauge(METER_PROVIDER, MetricNaming.ACTIVE_PLAYERS.value, self.__active_players_callback, 'Active music players')
        # Cache filesystem stats — only meaningful in local mode with a dedicated mount
        if not storage_bucket_name and self.download_dir and self.download_dir.is_mount():
            # Cache stats
            create_observable_gauge(METER_PROVIDER, MetricNaming.CACHE_FILESYSTEM_MAX.value, self.__cache_filestats_callback_total, 'Max size of cache filesystem', unit='bytes')
            create_observable_gauge(METER_PROVIDER, MetricNaming.CACHE_FILESYSTEM_USED.value, self.__cache_filestats_callback_used, 'Used size of cache filesystem', unit='bytes')
        # Timestamps for heartbeat gauges
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__cleanup_player_loop_active_callback, 'Cleanup player loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__download_file_loop_active_callback, 'Download files loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__result_task_loop_active_callback, 'Download result processing loop heartbeat')
        create_observable_gauge(METER_PROVIDER, MetricNaming.DOWNLOAD_RESULT_QUEUE_DEPTH.value, self.__download_result_queue_depth_callback, 'Pending download results awaiting processing')
        create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__post_play_processing_loop_active_callback, 'Playlist update loop heartbeat')
        if self.youtube_music_client:
            create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value, self.__youtube_search_loop_active_callback, 'Youtube music search loop heartbeat')

    # Metric callback functons
    def __youtube_search_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if (self._youtube_search_task and not self._youtube_search_task.done()) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'youtube_music_search'
            })
        ]

    def __post_play_processing_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if (self._post_play_processing_task and not self._post_play_processing_task.done()) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'post_play_processing'
            })
        ]
    def __download_file_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if any(not task.done() for task in self._download_tasks) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'download_files'
            })
        ]

    def __result_task_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if (self._result_task and not self._result_task.done()) else 0
        return [
            Observation(value, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'process_download_results'
            })
        ]

    def __download_result_queue_depth_callback(self, _options):
        '''
        Total pending download results waiting to be routed to players.

        Only meaningful when broker_client owns a local result queue
        (single-process / embedded broker server); HA mode reports 0 because the
        queue lives on the broker pod and this gauge needs a synchronous read.
        '''
        result_queue = getattr(self.broker_client, 'result_queue', None)
        raw_queue = getattr(result_queue, 'raw_queue', None)
        depth = raw_queue.qsize() if raw_queue is not None else 0
        return [
            Observation(depth, attributes={
                AttributeNaming.BACKGROUND_JOB.value: 'process_download_results'
            })
        ]

    def __cleanup_player_loop_active_callback(self, _options):
        '''
        Loop active callback check
        '''
        value = 1 if (self._cleanup_task and not self._cleanup_task.done()) else 0
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
        self._cleanup_task = self.bot.loop.create_task(return_loop_runner(self.cleanup_players, self.bot, self.logger)())
        # One download loop per configured slot. Defaults to 1 so downloads stay
        # serial per egress IP (yt-dlp/YouTube rate-limits per source IP).
        self._download_tasks = [
            self.bot.loop.create_task(return_loop_runner(partial(self.download_client.run, self.bot_shutdown_event), self.bot, self.logger)())
            for _ in range(self.config.download.max_concurrent_downloads)
        ]
        self._result_task = self.bot.loop.create_task(return_loop_runner(self.process_download_results, self.bot, self.logger)())
        self._youtube_search_task = self.bot.loop.create_task(return_loop_runner(self.search_youtube_music, self.bot, self.logger)())
        if self.config.broker_server:
            broker_server = BrokerHttpServer(
                self.media_broker,
                host=self.config.broker_server.host,
                port=self.config.broker_server.port,
            )
            self.bot.loop.create_task(broker_server.serve())
        if self.db_engine:
            self._start_tasks()

    def _start_tasks(self):
        self._post_play_processing_task = self.bot.loop.create_task(
            return_loop_runner(self.post_play_processing, self.bot, self.logger)()
        )

    async def cog_unload(self):
        '''
        Run when cog stops
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cog_unload', kind=SpanKind.INTERNAL):
            self.logger.debug('Cog unload: Calling shutdown on Music')

            self.bot_shutdown_event.set()

            # Cleanup all active guilds: terminates state machines, drops queues,
            # sends shutdown message, and cancels player tasks
            for guild in [player.guild for player in self.players.values()]:
                await self.cleanup(guild, reason=CleanupReason.BOT_SHUTDOWN)

            self.logger.info('Cog unload: Cancelling main tasks')
            if self._init_task:
                self._init_task.cancel()
            if self._cleanup_task:
                self._cleanup_task.cancel()
            for download_task in self._download_tasks:
                download_task.cancel()
            if self._result_task:
                self._result_task.cancel()
            if self._post_play_processing_task:
                self._post_play_processing_task.cancel()
            if self._youtube_search_task:
                self._youtube_search_task.cancel()

            self.logger.info('Cog unload: Removing directories')
            # Remove contents of download dir by default
            if self.download_dir and self.download_dir.exists():
                rm_tree(self.download_dir)
            if self.config.player.player_dir_path is None and self.player_dir.exists():
                rm_tree(self.player_dir)

            return True


    async def post_play_processing(self):
        '''
        Update history playlists
        '''
        await sleep(.01)
        try:
            history_item = self.history_playlist_queue.get_nowait()
        except QueueEmpty:
            if self.bot_shutdown_event.is_set():
                raise ExitEarlyException('Exiting history cleanup') #pylint:disable=raise-missing-from
            return

        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.post_play_processing', kind=SpanKind.CONSUMER):
            async with self.with_db_session() as db_session:

                # Update analytics table
                await async_retry_database_commands(db_session, lambda: database_functions.update_video_guild_analytics(
                    db_session,
                    history_item.media_download.media_request.guild_id,
                    history_item.media_download.duration,
                    history_item.media_download.cache_hit))

                # Skip if added from history
                if history_item.media_download.media_request.added_from_history:
                    self.logger.info(f'Played video "{history_item.media_download.webpage_url}" was original played from history, skipping history add')
                    return
                self.logger.info(f'Attempting to add url "{history_item.media_download.webpage_url}" to history playlist {history_item.playlist_id} for server {history_item.media_download.media_request.guild_id}')
                await async_retry_database_commands(db_session, lambda: database_functions.delete_playlist_item_by_url(db_session, history_item.media_download.webpage_url, history_item.playlist_id))

                # Delete number of rows necessary to add list
                existing_items = await async_retry_database_commands(db_session, lambda: database_functions.get_playlist_size(db_session, history_item.playlist_id))
                delta = (existing_items + 1) - self.config.playlist.server_playlist_max_size
                if delta > 0:
                    self.logger.info(f'Need to delete {delta} items from history playlist {history_item.playlist_id}')
                    await async_retry_database_commands(db_session, lambda: database_functions.delete_playlist_item_limit(db_session, history_item.playlist_id, delta))
                self.logger.info(f'Adding new history item "{history_item.media_download.webpage_url}" to playlist {history_item.playlist_id}')
                await self.__playlist_insert_item(db_session, history_item.playlist_id, history_item.media_download.webpage_url, history_item.media_download.title, history_item.media_download.uploader)

    def _get_play_order_content(self, guild_id: int) -> list:
        '''
        Get queue order message content for a guild.
        '''
        player = self.players.get(guild_id)
        return player.get_queue_order_messages() if player else []

    async def create_bundle(self, guild_id: int, channel_id: int,
                            input_string: str | None = None,
                            has_search_banner: bool = False) -> str:
        '''Create a broker-owned bundle and return its uuid.'''
        return await self.broker_client.create_bundle(
            guild_id, channel_id,
            input_string=input_string, has_search_banner=has_search_banner,
        )

    async def delete_bundle(self, _guild_id: int, bundle_uuid: str) -> None:
        '''Tear down a broker-owned bundle.

        guild_id is accepted for callers that already have it on hand but the
        broker keys bundles by uuid alone.
        '''
        await self.broker_client.delete_bundle(bundle_uuid)

    async def _push_state(self, media_request: MediaRequest, event: LifecycleEvent,
                          **details) -> None:
        '''Send a lifecycle transition to the broker (which renders the bundle).

        In single-process mode this also mutates the local request, since the
        broker holds the same MediaRequest object.  The broker performs the
        mark AND re-renders the bundle — closing the render gap left by the
        retired set_on_change callback.
        '''
        await self.broker_client.update_request_status(
            str(media_request.uuid),
            LifecycleStatusUpdate(event=event, **details),
        )

    async def cleanup_players(self):
        '''
        Check for players with no members, cleanup bot in channels that do
        '''
        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Bot in shutdown, exiting early')
        await sleep(1)

        if not self.players:
            return

        guilds = []
        for _guild_id, player in self.players.items():
            if player.shutdown_called:
                reason = player.shutdown_reason or CleanupReason.QUEUE_TIMEOUT
                self.logger.debug(f'Identified guild where music player shutdown called {player.guild.id}, reason: {reason.value}, sending to cleanup')
                guilds.append((player.guild, reason))
                continue
            if player.voice_channel_inactive_timeout(timeout_seconds=self.config.player.inactive_voice_channel_timeout):
                self.dispatcher.send_message(player.guild.id, player.text_channel.id,
                    'No one active in voice channel, shutting myself down',
                    delete_after=self.config.general.message_delete_after)
                self.logger.info(f'No members connected to voice channel {player.guild.id} , sending to cleanup')
                guilds.append((player.guild, CleanupReason.VOICE_INACTIVE))
        # Run in separate loop since the cleanup function removes items form self.players
        # And you might hit issues where dict size changes during iteration
        for guild, reason in guilds:
            await self.cleanup(guild, reason=reason)

    async def add_source_to_player(self, media_download: MediaDownload, player: MusicPlayer):
        '''
        Add source to player queue

        media_request : Standard media_request for pre-download
        media_download : Standard MediaDownload for post download
        player : MusicPlayer
        skiP_update_queue_strings : Skip queue string update
        '''
        attributes = media_download_attributes(media_download)
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.add_source_to_player', kind=SpanKind.INTERNAL, attributes=attributes, links=span_links_from_context(media_download.media_request.span_context)):
            try:
                # Register with the broker BEFORE the item becomes visible to the
                # player queue. For a cache hit this is the only point the broker
                # learns of the entry, and the concurrent player_loop can pop +
                # checkout the instant it is enqueued. If that race puts the
                # checkout ahead of registration the broker has no entry, checkout
                # returns None, and the player falls back to the raw S3 cache key
                # (crashing on open()). Registering first closes the window.
                await self.broker_client.register_download(media_download)
                player.add_to_play_queue(media_download)
                self.logger.info(f'Adding "{media_download.webpage_url}" '
                                 f'to queue in guild {media_download.media_request.guild_id}')
                player.trigger_prefetch()
                await self._push_state(media_download.media_request, LifecycleEvent.COMPLETED)
                key = f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}'
                req_id = self.dispatcher.update_mutable(key, player.guild.id,
                    self._get_play_order_content(player.guild.id), player.text_channel.id)
                self.logger.debug('add_source_to_player: dispatched play order update key=%s dispatch.request_id=%s', key, req_id)

                return True
            except QueueFull:
                self.logger.info(f'Play queue full, aborting download of item "{str(media_download.media_request)}"')
                reason = (f'Cannot add item "{media_download.title}" to play queue, play queue is full'
                          if media_download.media_request.bundle_uuid else None)
                await self._push_state(media_download.media_request, LifecycleEvent.FAILED, failure_reason=reason)
                await self.broker_client.discard(str(media_download.media_request.uuid))
                return False
                # Dont return to loop, file was downloaded so we can iterate on cache at least
            except PutsBlocked:
                self.logger.info(f'Puts Blocked on queue in guild "{media_download.media_request.guild_id}", assuming shutdown')
                await self._push_state(media_download.media_request, LifecycleEvent.DISCARDED)
                await self.broker_client.discard(str(media_download.media_request.uuid))
                return False

    # Take both source dict and media download
    # Since media download might be none
    async def __ensure_video_download_result(self, media_request: MediaRequest, media_download: MediaDownload):
        if media_download is None:
            await self._push_state(media_request, LifecycleEvent.FAILED,
                                   failure_reason=f'Issue downloading video "{media_request}"')
            return False
        return True

    async def __return_bad_video(self, media_request: MediaRequest, user_message: str | None,
                                 skip_callback_functions: bool=False):
        await self._push_state(media_request, LifecycleEvent.FAILED, failure_reason=user_message)
        if not skip_callback_functions and media_request.history_playlist_item_id:
            await self.__delete_non_existing_item(media_request.history_playlist_item_id)
        return

    async def _enqueue_media_download_from_cache(self, media_request: MediaRequest, player: MusicPlayer = None):
        media_download = await self.broker_client.check_cache(media_request)
        if media_download:
            # check_cache binds the cached file to THIS media_request
            # (video_cache_client.get_webpage_url_item passes it straight into the
            # MediaDownload), so media_download.media_request is the same object —
            # marking it COMPLETED advances this request's own bundle row toward
            # teardown.  add_source_to_player and the SEARCH caller (below) re-push
            # COMPLETED on the same request; the transitions are idempotent.
            await self._push_state(media_download.media_request, LifecycleEvent.COMPLETED)
            if isinstance(media_request, PlaylistAddRequest):
                playlist_result = PlaylistAddResult(
                    webpage_url=media_download.webpage_url or '',
                    title=media_download.title,
                    uploader=media_download.uploader,
                )
                await self.__add_playlist_item(media_request, playlist_result)
                return True
            if not player:
                player = await self.get_player(media_request.guild_id, create_player=False)
            if player:
                self.logger.debug(f'Search "{str(media_request)}" found in cache, placing in player queue')
                await self.add_source_to_player(media_download, player)
            return True
        return False

    async def search_youtube_music(self):
        '''
        Runner for youtube music searches
        '''
        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Bot shutdown called, exiting early')
        await sleep(.01)
        try:
            media_request = self.youtube_music_search_queue.get_nowait()
        except QueueEmpty:
            return True

        # Default lifecycle_stage is already SEARCHING — register_request rendered
        # the bundle when the request entered the pipeline.
        await self.youtube_music_backoff_time()

        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.search_youtube_music', kind=SpanKind.CLIENT,
                                           attributes=media_request_attributes(media_request),
                                           links=span_links_from_context(media_request.span_context)) as span:
            self.logger.debug(f'Running youtube music search for input "{media_request.search_result.raw_search_string}"')
            try:
                youtube_music_result = await asyncio.get_running_loop().run_in_executor(None, partial(self.youtube_music_client.search, media_request.search_result.raw_search_string))
                self.youtube_music_failure_queue.add_item(FailureStatus())
            except YoutubeMusicRetryException as e:
                self.youtube_music_failure_queue.add_item(FailureStatus(success=False, exception_type=type(e).__name__, exception_message=str(e)))
                self.logger.info(f'Youtube music search failure queue status: {self.youtube_music_failure_queue.get_status_summary()}')
                self.update_youtube_music_timestamp(backoff_multiplier=2 ** self.youtube_music_failure_queue.size)
                backoff_seconds = None
                if self.youtube_music_wait_timestamp:
                    backoff_seconds = int(self.youtube_music_wait_timestamp - datetime.now(timezone.utc).timestamp())
                    backoff_seconds = max(0, backoff_seconds)
                    self.logger.info(f'Youtube music search rate limited, waiting {backoff_seconds} seconds')
                media_request.youtube_music_retry_information.retry_count += 1
                if media_request.youtube_music_retry_information.retry_count >= self.config.download.max_youtube_music_search_retries:
                    self.logger.warning(f'Youtube music search retry limit exceeded for "{media_request.search_result.raw_search_string}"')
                    await self._push_state(media_request, LifecycleEvent.FAILED,
                                           failure_reason='Youtube music search rate limit exceeded after max retries')
                else:
                    self.youtube_music_search_queue.put_nowait(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
                    await self._push_state(media_request, LifecycleEvent.RETRY_SEARCH,
                                           error_detail=str(e), backoff_seconds=backoff_seconds,
                                           retry_count=media_request.youtube_music_retry_information.retry_count)
                span.set_status(StatusCode.ERROR)
                return False
            if youtube_music_result:
                # This returns the raw id, make sure we add the proper prefix for caching bits
                media_request.search_result.add_youtube_music_result(f'{YOUTUBE_VIDEO_PREFIX}{youtube_music_result}')

            await self._push_state(media_request, LifecycleEvent.QUEUED)

            # Check if cache item exists already
            if await self._enqueue_media_download_from_cache(media_request):
                # Cache hit: _enqueue_media_download_from_cache already pushed COMPLETED
                # on this same media_request (check_cache binds the cached download to
                # this request object), which advances the bundle row toward teardown.
                # A second COMPLETED here is redundant and trails an extra render behind
                # the remove — the source of the stranded "Media request queued for
                # download" status message — so it is intentionally omitted.
                return True

            try:
                self.logger.debug(f'Handing off media_request "{str(media_request)}" to download queue, uuid: {media_request.uuid}')
                await self.download_client.submit(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
            except PutsBlocked:
                self.logger.info(f'Puts to queue in guild {media_request.guild_id} are currently blocked, assuming shutdown')
                await self._push_state(media_request, LifecycleEvent.DISCARDED)
                return False
            except QueueFull:
                self.logger.info(f'Queue full in guild {media_request.guild_id}, cannot add more media requests')
                await self._push_state(media_request, LifecycleEvent.DISCARDED)
        return True

    def update_youtube_music_timestamp(self, backoff_multiplier: int = 1) -> bool:
        '''
        Update the youtube music search backoff timestamp

        backoff_multiplier: Multiply backoff time by factor
        '''
        new_timestamp = int(datetime.now(timezone.utc).timestamp())
        new_timestamp = new_timestamp + (self.config.download.youtube_wait_period_minimum * backoff_multiplier)
        random.seed(time())
        # bandit B311: backoff jitter, not security-sensitive
        new_timestamp = new_timestamp + (random.randint(1000, self.config.download.youtube_wait_period_max_variance * 1000) / 1000)  # nosec B311
        self.logger.info(f'Waiting on youtube music search backoff, waiting until {new_timestamp}')
        self.youtube_music_wait_timestamp = new_timestamp
        return True

    async def youtube_music_backoff_time(self):
        '''
        Wait for next youtube music search time
        '''
        if self.youtube_music_wait_timestamp is None:
            return True

        now = datetime.now(timezone.utc).timestamp()
        sleep_duration = max(0, self.youtube_music_wait_timestamp - now)

        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Exiting bot wait loop')

        if sleep_duration == 0:
            return True

        try:
            await asyncio.wait_for(
                self.bot_shutdown_event.wait(),
                timeout=sleep_duration
            )
            raise ExitEarlyException('Exiting bot wait loop')
        except asyncio.TimeoutError:
            return True

    async def process_download_results(self):
        '''
        Result consumer: routes completed DownloadResults to players or playlist handlers.
        Retryable errors are handled inside download_client.run(); only successes and
        terminal failures reach this method.
        '''
        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Bot shutdown called, exiting early')

        result = await self.broker_client.next_result()
        if result is None:
            # Idle — the broker has no finished result for us right now.  Sleep
            # before the loop runner re-calls so we don't busy-spin the broker
            # (in HA this is a remote GET /results/next poll).
            await sleep(_BROKER_POLL_INTERVAL_SECONDS)
            return

        media_request = result.media_request
        is_playlist_add = isinstance(media_request, PlaylistAddRequest)
        attributes = media_request_attributes(media_request)

        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.process_download_results', kind=SpanKind.CONSUMER, attributes=attributes, links=span_links_from_context(media_request.span_context) + span_links_from_context(result.span_context)) as span:
            if not result.status.success:
                self.logger.info(f'Terminal error on "{str(media_request)}": {result.status.error_detail or ""}')
                span.set_status(StatusCode.ERROR)
                await self.__return_bad_video(media_request, result.status.user_message)
                return

            self.logger.info(f'Successfully fetched media request "{str(media_request)}" in guild "{media_request.guild_id}"')

            if is_playlist_add:
                data = result.ytdlp_data
                if not data:
                    await self._push_state(media_request, LifecycleEvent.FAILED,
                                           failure_reason=f'No metadata returned for "{str(media_request)}"')
                    span.set_status(StatusCode.ERROR)
                    return
                playlist_result = PlaylistAddResult(
                    webpage_url=data.get('webpage_url', ''),
                    title=data.get('title', ''),
                    uploader=data.get('uploader', ''),
                )
                span.set_status(StatusCode.OK)
                await self.__add_playlist_item(media_request, playlist_result)
                return

            player = await self.get_player(media_request.guild_id, create_player=False)
            if not player or player.shutdown_called:
                self.logger.info(f'Player gone after download for guild {media_request.guild_id}, discarding "{str(media_request)}"')
                await self.broker_client.update_request_status(
                    str(media_request.uuid), LifecycleStatusUpdate(event=LifecycleEvent.DISCARDED)
                )
                span.set_status(StatusCode.OK)
                return

            # The broker already persisted this result when the download client
            # reported it (locally for InMemoryBrokerClient, remotely for
            # HttpBrokerClient) — build the MediaDownload the player needs from
            # the result rather than from a return value the HTTP client can't
            # round-trip.
            media_download = MediaDownload(result.file_name, result.ytdlp_data, media_request)
            media_download.file_size_bytes = result.file_size_bytes
            if not await self.__ensure_video_download_result(media_request, media_download):
                span.set_status(StatusCode.ERROR)
                return
            span.set_status(StatusCode.OK)
            await self.add_source_to_player(media_download, player)
            await self.broker_client.cache_cleanup()

    async def __get_history_playlist(self, guild_id: int):
        '''
        Get history playlist for guild

        guild_id : Guild id
        '''
        if not self.db_engine:
            return None
        async with self.with_db_session() as db_session:
            history_playlist = await async_retry_database_commands(db_session, lambda: database_functions.get_history_playlist(db_session, guild_id))
            if history_playlist:
                return history_playlist.id
            history_playlist = Playlist(
                server_id=guild_id,
                name=f'{PLAYHISTORY_PREFIX}{guild_id}_{datetime.now(timezone.utc).timestamp()}',
                is_history=True,
            )
            db_session.add(history_playlist)
            await async_retry_database_commands(db_session, db_session.commit)
            return history_playlist.id

    async def cleanup(self, guild, reason: CleanupReason = CleanupReason.QUEUE_TIMEOUT):
        '''
        Cleanup guild player

        guild  : Guild object
        reason : CleanupReason describing why cleanup was triggered
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cleanup', kind=SpanKind.CONSUMER, attributes={DiscordContextNaming.GUILD.value: guild.id}):
            self.logger.info(f'Starting cleanup on guild {guild.id}, reason: {reason.value}')
            player = await self.get_player(guild.id, create_player=False)
            if reason == CleanupReason.BOT_SHUTDOWN and player and self.dispatcher:
                self.dispatcher.send_message(player.guild.id, player.text_channel.id,
                    'Bot is shutting down',
                    delete_after=self.config.general.message_delete_after)

            self.logger.info(f'Disconnecting voice clients for music player in guild {guild.id}')

            # Store reference before disconnect() clears it
            voice_client = guild.voice_client
            disconnect_task = None

            if voice_client:
                # cleanup() must be called to free native memory and remove from state cache
                voice_client.cleanup()
                self.logger.debug(f'Called cleanup() on voice client for guild {guild.id}')

                # Start disconnect in background (don't block here)
                disconnect_task = create_task(voice_client.disconnect())
                self.logger.debug(f'Started disconnect task for guild {guild.id}')

            # Block download queue for later
            # Clear queues before blocking: the in-process clear_queue restores
            # preserved items via put_nowait, which raises once the queue is blocked,
            # so block-first is not an option here (holds for the Redis worker too,
            # whose clear leaves preserved items in place). The clear/block calls are
            # now async, so this runs during guild teardown (shutdown/disconnect) when
            # the submit loops are already winding down — the narrow post-clear window
            # is acceptable for a guild being torn down.
            # We also record any bundle uuids belonging to preserved (playlist-add)
            # items so we can skip deleting those bundles below — their requests
            # are still in flight and will keep updating the broker bundle UI.
            preserved_bundle_uuids: set[str] = set()
            if reason == CleanupReason.BOT_SHUTDOWN:
                preserve_predicate = None
            else:
                def preserve_predicate(req):
                    keep = not req.download_file
                    if keep and req.bundle_uuid:
                        preserved_bundle_uuids.add(req.bundle_uuid)
                    return keep

            dropped = await self.download_client.clear_guild_queue(guild.id, preserve_predicate=preserve_predicate)
            self.logger.debug(f'Cleanup found {len(dropped)} existing download items')
            for item in dropped:
                await self._push_state(item, LifecycleEvent.DISCARDED)

            dropped = self.youtube_music_search_queue.clear_queue(guild.id, preserve_predicate=preserve_predicate)
            self.logger.debug(f'Cleanup found {len(dropped)} existing search queue items')
            for item in dropped:
                await self._push_state(item, LifecycleEvent.DISCARDED)

            await self.download_client.block_guild(guild.id)
            self.youtube_music_search_queue.block(guild.id)

            player = None
            # Clear play queue if that didnt happen
            try:
                player = self.players.pop(guild.id)
            except KeyError:
                pass

            if player:
                self.logger.info(f'Calling cleanup on player {guild.id}')
                await player.cleanup()
                if reason != CleanupReason.BOT_SHUTDOWN:
                    # Cleanup queue messages if they still exist
                    self.logger.info(f'Clearing queue message for guild {guild.id}')
                    key = f'{MultipleMutableType.PLAY_ORDER.value}-{guild.id}'
                    self.dispatcher.update_mutable(key, guild.id,
                        self._get_play_order_content(guild.id), player.text_channel.id)

            # Tear down broker-owned bundles for this guild.  Skip bundles whose
            # playlist-add items survived the queue clear — those will continue
            # to render updates on their own and the cog will release the bundle
            # when those items reach a terminal state.
            guild_bundles = await self.broker_client.list_bundles_for_guild(guild.id)
            for bundle_uuid in guild_bundles:
                if reason != CleanupReason.BOT_SHUTDOWN and bundle_uuid in preserved_bundle_uuids:
                    self.logger.debug(f'Skipping delete of bundle {bundle_uuid} — has active playlist-add requests')
                    continue
                await self.delete_bundle(guild.id, bundle_uuid)
            if reason != CleanupReason.BOT_SHUTDOWN:
                self.logger.debug(f'Deleting player dir for guild {guild.id}')
                guild_player_path = self.player_dir / f'{guild.id}'
                if guild_player_path.exists():
                    rm_tree(guild_player_path)

            # Wait for voice disconnect to complete
            # Skip on BOT_SHUTDOWN — cog_unload handles directory teardown and
            # we don't need to block on graceful disconnect when the process is ending
            if disconnect_task and reason != CleanupReason.BOT_SHUTDOWN:
                await disconnect_task
                self.logger.debug(f'Disconnected voice client for guild {guild.id}')

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
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_player', kind=SpanKind.INTERNAL, attributes={DiscordContextNaming.GUILD.value: guild_id}):
            try:
                player = self.players[guild_id]
            except KeyError:
                if check_voice_client_active:
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        'I am not currently playing anything',
                        delete_after=self.config.general.message_delete_after)
                    return None
                if not create_player:
                    return None
                # Make directory for guild specific files
                guild_path = self.player_dir / f'{ctx.guild.id}'
                guild_path.mkdir(exist_ok=True, parents=True)
                # Generate and start player
                history_playlist_id = await self.__get_history_playlist(ctx.guild.id)
                player = MusicPlayer(ctx,
                                     self.logging_config,
                                     self.config.player.queue_max_size, self.config.player.disconnect_timeout,
                                     guild_path, self.dispatcher,
                                     history_playlist_id, self.history_playlist_queue,
                                     broker=self.broker_client,
                                     prefetch_limit=self.config.download.storage.prefetch_limit if self.config.download.storage else 0)
                await player.start_tasks()
                self.players[guild_id] = player
            if check_voice_client_active:
                pending_downloads = await self.download_client.queue_size(guild_id)
                if not player.guild.voice_client or (not player.guild.voice_client.is_playing() and not pending_downloads):
                    self.dispatcher.send_message(player.guild.id, player.text_channel.id,
                        'I am not currently playing anything',
                        delete_after=self.config.general.message_delete_after)
                    return None
            # Check if we should join voice
            if not player.guild.voice_client and join_channel:
                try:
                    await player.join_voice(join_channel)
                except ClientException as error:
                    self.dispatcher.send_message(player.guild.id, player.text_channel.id,
                        str(error),
                        delete_after=self.config.general.message_delete_after)
                    return None
            return player

    async def __check_author_voice_chat(self, ctx: Context, check_voice_chats: bool = True):
        '''
        Check that command author in proper voice chat
        '''
        try:
            channel = ctx.author.voice.channel
        except AttributeError:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'{ctx.author.display_name} not in voice chat channel. Please join one and try again',
                delete_after=self.config.general.message_delete_after)
            return None

        if not check_voice_chats:
            return channel

        if channel.guild.id is not ctx.guild.id:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'User not joined to channel bot is in, ignoring command',
                delete_after=self.config.general.message_delete_after)
            return None
        return channel

    async def __ensure_player(self, ctx: Context, channel: VoiceChannel) -> MusicPlayer:
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.ensure_player', kind=SpanKind.INTERNAL, attributes={DiscordContextNaming.GUILD.value: ctx.guild.id}):
            try:
                return await self.get_player(ctx.guild.id, join_channel=channel, ctx=ctx)
            except async_timeout as e:
                self.logger.warning(f'Reached async timeout error on bot joining channel, {str(e)}')
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Bot cannot join channel {channel}',
                    delete_after=self.config.general.message_delete_after)
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

        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Connected to: {channel}',
            delete_after=self.config.general.message_delete_after)

    async def enqueue_media_requests(self, ctx: Context, entries: List[MediaRequest],
                                     bundle_uuid: str, player: MusicPlayer = None) -> bool:
        '''
        Enqueue source dicts to a player or download queue.

        ctx: Discord Context
        player: Music Player
        entries: List of MediaRequest objects.  Each one is registered with the
            broker (which auto-attaches it to bundle_uuid) and routed to either
            the search or download queue based on its search type.

        Returns true if all items added, false if some were not.
        '''
        ctx_span_context = capture_span_context()
        for media_request in entries:
            if media_request.span_context is None:
                media_request.span_context = ctx_span_context
            media_request.bundle_uuid = bundle_uuid
            await self.broker_client.register_request(media_request)
            self.logger.debug(f'Running enqueue for media request "{str(media_request)}, uuid: {media_request.uuid}, bundle: {bundle_uuid}')
            # Unless a direct or youtube url, pass into the search queue
            if media_request.search_result.search_type not in [SearchType.DIRECT, SearchType.YOUTUBE]:
                try:
                    self.youtube_music_search_queue.put_nowait(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
                except PutsBlocked:
                    self.logger.info(f'Puts to search queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                    await self.delete_bundle(ctx.guild.id, bundle_uuid)
                    return False
                except QueueFull:
                    self.logger.info(f'Search Queue full in guild {ctx.guild.id}, cannot add more media requests')
                    await self._push_state(media_request, LifecycleEvent.DISCARDED)
                    break
                continue
            # Else directly add to download queue
            if await self._enqueue_media_download_from_cache(media_request, player=player):
                # Cache hit: mark the current request completed (broker bundle counts it)
                await self._push_state(media_request, LifecycleEvent.COMPLETED)
                continue
            try:
                await self.download_client.submit(media_request.guild_id, media_request)
                await self._push_state(media_request, LifecycleEvent.QUEUED)
            except PutsBlocked:
                await self.delete_bundle(ctx.guild.id, bundle_uuid)
                self.logger.info(f'Puts to download queue in guild {ctx.guild.id} are currently blocked, assuming shutdown')
                return False
            except QueueFull:
                self.logger.info(f'Download Queue full in guild {ctx.guild.id}, cannot add more media requests')
                await self._push_state(media_request, LifecycleEvent.DISCARDED)
                break

        # Lock pagination on the bundle and trigger a final render.
        await self.broker_client.finalize_bundle(bundle_uuid)
        return True

    async def _generate_media_requests_from_search(self, ctx: Context, search: str, player: MusicPlayer = None,
                                                   add_to_playlist: int = None):
        '''
        Generate media requests and a broker-owned bundle from a search input.

        ctx: Discord Context
        search: Original Search string
        player: MusicPlayer to pass into
        add_to_playlist: If came from playlist_item_add, pass it here
        '''
        # Single-search bundle — broker renders the placeholder row immediately.
        bundle_uuid = await self.create_bundle(
            ctx.guild.id, ctx.channel.id, input_string=search,
        )

        try:
            collection = await self.search_client.check_source(search, self.bot.loop,
                                                                   self.config.player.queue_max_size)
        except SearchException as exc:
            self.logger.info(f'Received download client exception for search "{search}", {str(exc)}')
            await self.delete_bundle(ctx.guild.id, bundle_uuid)
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Error searching input "{search}", message: {str(exc.user_message)}',
                delete_after=self.config.general.message_delete_after)
            return

        # Multi-item collection — drop the single-search bundle and create a
        # multi-track bundle whose banner persists across renders.
        if collection.collection_name:
            await self.delete_bundle(ctx.guild.id, bundle_uuid)
            bundle_uuid = await self.create_bundle(
                ctx.guild.id, ctx.channel.id,
                input_string=collection.collection_name, has_search_banner=True,
            )

        media_requests = []
        for search_result in collection.search_results:
            if add_to_playlist:
                mr = PlaylistAddRequest(guild_id=ctx.guild.id, channel_id=ctx.channel.id, requester_name=ctx.author.display_name, requester_id=ctx.author.id,
                                        search_result=search_result, playlist_id=add_to_playlist)
            else:
                mr = MediaRequest(guild_id=ctx.guild.id, channel_id=ctx.channel.id, requester_name=ctx.author.display_name, requester_id=ctx.author.id,
                                  search_result=search_result)
            media_requests.append(mr)
        await self.enqueue_media_requests(ctx, media_requests, bundle_uuid, player=player)

    @command(name='play')
    @command_wrapper
    async def play_(self, ctx: Context, *, search: str):
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

        await self._generate_media_requests_from_search(ctx, search, player=player)

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
        current_title = player.current_media_download.title
        player.video_skipped = True
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Skipping video "{current_title}"',
            delete_after=self.config.general.message_delete_after)
        player.guild.voice_client.stop()

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are currently no more queued videos.',
                delete_after=self.config.general.message_delete_after)
            return
        self.logger.info(f'Player clear called in guild {ctx.guild.id}')
        await player.clear_queue()
        key = f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}'
        self.dispatcher.update_mutable(key, player.guild.id,
            self._get_play_order_content(player.guild.id), player.text_channel.id)
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            'Cleared player queue',
            delete_after=self.config.general.message_delete_after)
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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There have been no videos played.',
                delete_after=self.config.general.message_delete_after)
            return

        headers = [
            Column('Pos', 3, zero_pad=True),
            Column('Title', 40),
            Column('Uploader', 40)
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```', prefix='History\n')
        table_items = player.get_history_items()
        for (count, item) in enumerate(table_items):
            uploader = item.uploader or ''
            table.add_row([
                f'{count + 1}',
                f'{item.title}',
                f'{uploader}',
            ])
        messages = table.render()
        for mess in messages:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id, mess,
                delete_after=self.config.general.message_delete_after)

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are currently no more queued videos.',
                delete_after=self.config.general.message_delete_after)
            return
        player.shuffle_queue()
        key = f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}'
        self.dispatcher.update_mutable(key, player.guild.id,
            self._get_play_order_content(player.guild.id), player.text_channel.id)

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are currently no more queued videos.',
                delete_after=self.config.general.message_delete_after)
            return

        try:
            queue_index = int(queue_index)
        except ValueError:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Invalid queue index {queue_index}',
                delete_after=self.config.general.message_delete_after)
            return

        item = player.remove_queue_item(queue_index)
        if item is None:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to remove queue index {queue_index}',
                delete_after=self.config.general.message_delete_after)
            return
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Removed item {item.title} from queue',
            delete_after=self.config.general.message_delete_after)
        await self.broker_client.remove(str(item.media_request.uuid))
        key = f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}'
        self.dispatcher.update_mutable(key, player.guild.id,
            self._get_play_order_content(player.guild.id), player.text_channel.id)

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are currently no more queued videos.',
                delete_after=self.config.general.message_delete_after)
            return
        try:
            queue_index = int(queue_index)
        except ValueError:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Invalid queue index {queue_index}',
                delete_after=self.config.general.message_delete_after)
            return

        item = player.bump_queue_item(queue_index)
        if item is None:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to bump queue index {queue_index}',
                delete_after=self.config.general.message_delete_after)
            return
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Bumped item "{item.title}" to top of queue',
            delete_after=self.config.general.message_delete_after)

        key = f'{MultipleMutableType.PLAY_ORDER.value}-{player.guild.id}'
        self.dispatcher.update_mutable(key, player.guild.id,
            self._get_play_order_content(player.guild.id), player.text_channel.id)

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
        self.logger.info(f'Stop command called for guild {ctx.guild.id}')
        player.destroy(reason=CleanupReason.USER_STOP)

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'I am already sending messages to channel {ctx.channel.name}',
                delete_after=self.config.general.message_delete_after)
            return
        bundle_index = f'{MultipleMutableType.PLAY_ORDER.value}-{ctx.guild.id}'
        # Move the bundle to the new channel (deletes old messages, re-sends in new channel)
        self.dispatcher.update_mutable_channel(bundle_index, ctx.guild.id, ctx.channel.id)

        # Update the player's text channel reference
        player.text_channel = ctx.channel

    async def __get_playlist_public_view(self, playlist_id: int, guild_id: int):
        '''
        Get playlist by db id, and view which public index servers see it as
        '''
        async with self.with_db_session() as db_session:
            playlist = await async_retry_database_commands(db_session, lambda: database_functions.get_playlist(db_session, playlist_id))
            if not playlist:
                return None
            if playlist.server_id != guild_id:
                return None
            if playlist.is_history:
                return 0

            for (count, playlist_obj) in enumerate(await async_retry_database_commands(db_session, lambda: database_functions.list_playlist_non_history(db_session, guild_id, 0))):
                if playlist_id == playlist_obj.id:
                    return count + 1
            return None

    async def __get_playlist(self, playlist_index: int, ctx: Context):
        '''
        Get playlist by 'public' index
        public index meaning what the users in the servers see
        '''



        try:
            index = int(playlist_index)
        except ValueError:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Invalid playlist index {playlist_index}',
                delete_after=self.config.general.message_delete_after)
            return None, False

        async with self.with_db_session() as db_session:
            if index > 0:
                if not await async_retry_database_commands(db_session, lambda: database_functions.playlist_count(db_session, ctx.guild.id)):
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        'No playlists in database',
                        delete_after=self.config.general.message_delete_after)
                    return None, False

            is_history = False
            if index == 0:
                playlist = await async_retry_database_commands(db_session, lambda: database_functions.get_history_playlist(db_session, ctx.guild.id))
                is_history = True
            else:
                playlist = (await async_retry_database_commands(db_session, lambda: database_functions.list_playlist_non_history(db_session, ctx.guild.id, (index - 1))))[0]
            if not playlist:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Invalid playlist index {playlist_index}',
                    delete_after=self.config.general.message_delete_after)
                return None, False
            return playlist.id, is_history

    async def __check_database_session(self, ctx: Context):
        '''
        Check if database session is in use
        '''
        if not self.db_engine:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'Functionality not available, database is not enabled')
            return False
        return True

    @group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions. Use '!help playlist'
        '''
        if ctx.invoked_subcommand is None:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'Invalid sub command passed...',
                delete_after=self.config.general.message_delete_after)

    async def __playlist_create(self, ctx: Context, name: str):


        if not await self.__check_database_session(ctx):
            return
        # Check name doesn't conflict with history
        playlist_name = shorten_string(name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to create playlist "{name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return None
        async with self.with_db_session() as db_session:
            existing_playlist = await async_retry_database_commands(db_session, lambda: database_functions.get_playlist_by_name_and_guild(db_session, playlist_name, ctx.guild.id))
            if existing_playlist:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Unable to create playlist "{name}", a playlist with that name already exists')
                return None

            playlist = Playlist(
                name=name,
                server_id=ctx.guild.id,
                is_history=False,
            )
            db_session.add(playlist)
            await async_retry_database_commands(db_session, db_session.commit)
            self.logger.info(f'Playlist created "{playlist_name}" with id {playlist.id} in guild {ctx.guild.id}')
            public_playlist_id = await self.__get_playlist_public_view(playlist.id, ctx.guild.id)
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Created playlist "{playlist_name}" with ID {public_playlist_id}',
                delete_after=self.config.general.message_delete_after)
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


        if not await self.__check_database_session(ctx):
            return
        async with self.with_db_session() as db_session:
            history_playlist = await async_retry_database_commands(db_session, lambda: database_functions.get_history_playlist(db_session, ctx.guild.id))
            playlist_items = await async_retry_database_commands(db_session, lambda: database_functions.list_playlist_non_history(db_session, ctx.guild.id, 0))

            if not playlist_items and not history_playlist:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    'No playlists in database',
                    delete_after=self.config.general.message_delete_after)
                return

            if history_playlist:
                playlist_items = [history_playlist] + [i for i in playlist_items]

            headers = [
                Column('ID', 3),
                Column('Playlist Name', 64),
                Column('Last Queued', 20),
            ]
            table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                                enclosure_start='```', enclosure_end='```', prefix='Playlist List\n')
            for (count, item) in enumerate(playlist_items):
                last_queued = 'N/A'
                if item.last_queued:
                    last_queued = item.last_queued.strftime('%Y-%m-%d %H:%M:%S')
                name = item.name
                if item.is_history:
                    name = PLAYHISTORY_NAME
                table.add_row([
                    f'{count}',
                    name,
                    last_queued,
                ])
            messages = table.render()
            for mess in messages:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id, mess,
                    delete_after=self.config.general.message_delete_after)

    async def __playlist_insert_item(self, db_session, playlist_id: int, video_url: str, video_title: str, video_uploader: str):
        self.logger.info(f'Adding video "{video_url}" to playlist {playlist_id}')
        item_count = await async_retry_database_commands(db_session, lambda: database_functions.get_playlist_size(db_session, playlist_id))
        if item_count >= self.config.playlist.server_playlist_max_size:
            raise PlaylistMaxLength(f'Playlist {playlist_id} greater to or equal to max length {self.config.playlist.server_playlist_max_size}')

        existing_item = await async_retry_database_commands(db_session, lambda: database_functions.get_playlist_item_by_url(db_session, playlist_id, video_url))
        if existing_item:
            return None

        # Truncate strings to fit database varchar(256) constraints
        playlist_item = PlaylistItem(
            title=shorten_string(video_title, 256) if video_title else None,
            video_url=shorten_string(video_url, 256) if video_url else None,
            uploader=shorten_string(video_uploader, 256) if video_uploader else None,
            playlist_id=playlist_id,
        )
        db_session.add(playlist_item)
        await async_retry_database_commands(db_session, db_session.commit)
        return playlist_item.id

    async def __add_playlist_item(self, request: PlaylistAddRequest, result: PlaylistAddResult):
        '''
        Insert a playlist item using the lightweight PlaylistAddResult metadata.

        request : PlaylistAddRequest carrying playlist_id
        result : PlaylistAddResult with webpage_url, title, uploader
        '''
        self.logger.info(f'Adding video_url "{result.webpage_url}" to playlist "{request.playlist_id}"'
                         f' in guild {request.guild_id}')
        try:
            async with self.with_db_session() as db_session:
                playlist_item_id = await self.__playlist_insert_item(db_session, request.playlist_id, result.webpage_url, result.title, result.uploader)
        except PlaylistMaxLength:
            await self._push_state(request, LifecycleEvent.FAILED,
                                   failure_reason='Unable to add item to playlist, playlist too long')
            return
        playlist_public_view_id = await self.__get_playlist_public_view(request.playlist_id, request.guild_id)
        if playlist_item_id:
            await self._push_state(request, LifecycleEvent.COMPLETED)
            return
        await self._push_state(request, LifecycleEvent.FAILED,
                               failure_reason=f'Item "{result.title}" already exists in playlist {playlist_public_view_id}')

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to add "{search}" to history playlist, is reserved and cannot be added to manually',
                delete_after=self.config.general.message_delete_after)
            return

        await self._generate_media_requests_from_search(ctx, search, add_to_playlist=playlist_id)

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

        if not await self.__check_database_session(ctx):
            return

        playlist_id, _is_history  = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None
        try:
            video_index = int(video_index)
        except ValueError:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Invalid item index {video_index}',
                delete_after=self.config.general.message_delete_after)
            return
        if video_index < 1:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Invalid item index {video_index}',
                delete_after=self.config.general.message_delete_after)
            return

        async with self.with_db_session() as db_session:
            item = await async_retry_database_commands(db_session, lambda: database_functions.delete_playlist_item_by_index(db_session, playlist_id, (video_index - 1)))
            public_playlist_id = await self.__get_playlist_public_view(playlist_id, ctx.guild.id)
            if item:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Removed item "{item.title}" from playlist {public_playlist_id}',
                    delete_after=self.config.general.message_delete_after)
                return
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to find item {video_index}',
                delete_after=self.config.general.message_delete_after)
            return

    @playlist.command(name='show')
    @command_wrapper
    async def playlist_show(self, ctx: Context, playlist_index: int):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''

        if not await self.__check_database_session(ctx):
            return

        playlist_id, _is_history = await self.__get_playlist(playlist_index, ctx)
        if not playlist_id:
            return None

        async with self.with_db_session() as db_session:
            headers = [
                Column('Pos', 3, zero_pad=True),
                Column('Title', 32),
                Column('Uploader', 32),
            ]
            table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                                enclosure_start='```', enclosure_end='```', prefix=f'Playlist {playlist_index} Items\n')
            total = 0
            for (count, item) in enumerate(await async_retry_database_commands(db_session, lambda: database_functions.list_playlist_items(db_session, playlist_id))):
                uploader = item.uploader or ''
                table.add_row([
                    f'{count + 1}',
                    f'{item.title}',
                    f'{uploader}',
                ])
                total += 1
            if not total:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'No items in playlist {playlist_id}',
                    delete_after=self.config.general.message_delete_after)
                return
            messages = table.render()
            for mess in messages:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id, mess,
                    delete_after=self.config.general.message_delete_after)

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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Cannot delete playlist, unable to find id {playlist_index}',
                delete_after=self.config.general.message_delete_after)
            return
        if is_history:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'Cannot delete history playlist, is reserved',
                delete_after=self.config.general.message_delete_after)
            return
        await self.__playlist_delete(playlist_id)
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Deleted playlist {playlist_index}',
            delete_after=self.config.general.message_delete_after)
        return

    async def __playlist_delete(self, playlist_id: int):
        self.logger.info(f'Deleting playlist items "{playlist_id}"')
        async with self.with_db_session() as db_session:
            await async_retry_database_commands(db_session, lambda: database_functions.delete_playlist(db_session, playlist_id))
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

        if not await self.__check_database_session(ctx):
            return

        playlist_id, is_history = await self.__get_playlist(playlist_index, ctx)
        if is_history:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'Cannot rename history playlist, is reserved',
                delete_after=self.config.general.message_delete_after)
            return
        if not playlist_id:
            return None

        playlist_name = shorten_string(playlist_name, 256)
        if PLAYHISTORY_PREFIX in playlist_name.lower():
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to create playlist "{playlist_name}", name cannot contain {PLAYHISTORY_PREFIX}')
            return None

        self.logger.info(f'Renaming playlist {playlist_id} to name "{playlist_name}"')
        async with self.with_db_session() as db_session:
            await async_retry_database_commands(db_session, lambda: database_functions.rename_playlist(db_session, playlist_id, playlist_name))
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Renamed playlist {playlist_index} to name "{playlist_name}"',
                delete_after=self.config.general.message_delete_after)
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
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'No player connected, no queue to save',
                delete_after=self.config.general.message_delete_after)
            return

        # Do a deepcopy here so list doesn't mutate as we iterate
        if is_history:
            queue_copy = player.get_history_items()
        else:
            queue_copy = player.get_queue_items()

        self.logger.info(f'Saving queue contents to playlist "{name}", is_history? {is_history}')

        if len(queue_copy) == 0:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are no videos to add to playlist',
                delete_after=self.config.general.message_delete_after)
            return

        async with self.with_db_session() as db_session:
            for data in queue_copy:
                try:
                    playlist_item_id = await self.__playlist_insert_item(db_session, playlist_id, data.webpage_url, data.title, data.uploader)
                except PlaylistMaxLength:
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        'Cannot add more items to playlist, already max size',
                        delete_after=self.config.general.message_delete_after)
                    break
                if playlist_item_id:
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        f'Added item "{data.title}" to playlist',
                        delete_after=self.config.general.message_delete_after)
                    continue
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Unable to add playlist item "{data.title}", likely already exists',
                    delete_after=self.config.general.message_delete_after)
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Finished adding items to playlist "{name}"',
            delete_after=self.config.general.message_delete_after)
        return

    async def __delete_non_existing_item(self, item_id: int):
        self.logger.info(f'Unable to find playlist item {item_id} from history playlist, deleting')
        async with self.with_db_session() as db_session:
            item = await db_session.get(PlaylistItem, item_id)
            await db_session.delete(item)
            await db_session.commit()

    async def __playlist_queue(self, ctx: Context, player: MusicPlayer, playlist_id: int, shuffle: bool, max_num: int, is_history: bool = False):



        self.logger.info(f'Playlist queue called for playlist {playlist_id} in server "{ctx.guild.id}"')

        async with self.with_db_session() as db_session:
            playlist_name = await async_retry_database_commands(db_session, lambda: database_functions.get_playlist_name(db_session, playlist_id))
            if is_history:
                playlist_name = PLAYHISTORY_NAME
            playlist_items = []
            for item in await async_retry_database_commands(db_session, lambda: database_functions.list_playlist_items(db_session, playlist_id)):
                search_result = SearchResult(search_type=SearchType.YOUTUBE if check_youtube_video(item.video_url) else SearchType.DIRECT,
                                             raw_search_string=item.video_url, proper_name=item.title)
                media_request = MediaRequest(guild_id=ctx.guild.id,
                                             channel_id=ctx.channel.id,
                                             requester_name=ctx.author.display_name,
                                             requester_id=ctx.author.id,
                                             search_result=search_result,
                                             added_from_history=is_history,
                                             history_playlist_item_id=item.id)
                playlist_items.append(media_request)

            # Check if playlist is empty and provide user feedback
            if not playlist_items:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Playlist "{playlist_name}" contains no items to queue',
                    delete_after=self.config.general.message_delete_after)
                return

            if shuffle:
                # https://stackoverflow.com/a/51295230
                random.seed(time())
                random.shuffle(playlist_items)

            if max_num:
                if max_num < 0:
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        f'Invalid number of videos {max_num}',
                        delete_after=self.config.general.message_delete_after)
                    return
                if max_num < len(playlist_items):
                    playlist_items = playlist_items[:max_num]
                else:
                    max_num = 0


            bundle_uuid = await self.create_bundle(
                ctx.guild.id, ctx.channel.id,
                input_string=playlist_name, has_search_banner=True,
            )
            finished_all = await self.enqueue_media_requests(ctx, playlist_items, bundle_uuid, player=player)

            if not finished_all:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Added as many videos in playlist "{playlist_name}" to queue as possible, but hit limit',
                    delete_after=self.config.general.message_delete_after)

            await async_retry_database_commands(db_session, lambda: database_functions.update_playlist_queued_at(db_session, playlist_id))

    @playlist.command(name='queue')
    @command_wrapper
    async def playlist_queue(self, ctx: Context, playlist_index: int, *args):
        '''
        Add playlist to queue

        playlist_index: integer [Required]
            ID of playlist
        Additional arguments (can be in any order):
            [shuffle] - Shuffle playlist when entering it into queue
            [number] - Only add this number of videos to the queue (max_num)
        
        Examples:
            !playlist queue 0 shuffle 16 # Shuffle Playlist 0 but only play 16 items
            !playlist queue 0 16 shuffle # Shuffle Playlist 0 but only play 16 items
            !playlist queue 0 shuffle
            !playlist queue 0 16
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

        # Parse arguments - can be in any order
        shuffle = False
        max_num = None

        for arg in args:
            arg_str = str(arg).lower()
            if arg_str == 'shuffle':
                shuffle = True
            elif arg_str.isdigit() and max_num is None:  # Use first number found
                max_num = int(arg_str)
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

        if not await self.__check_database_session(ctx):
            return

        self.logger.info(f'Calling playlist merge of "{playlist_index_one}" and "{playlist_index_two}" in server "{ctx.guild.id}"')
        playlist_one_id, is_history1 = await self.__get_playlist(playlist_index_one, ctx)
        playlist_two_id, is_history2  = await self.__get_playlist(playlist_index_two, ctx)
        if is_history1 or is_history2:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'Cannot merge history playlist, is reserved',
                delete_after=self.config.general.message_delete_after)
            return
        if not playlist_one_id:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Cannot find playlist {playlist_index_one}',
                delete_after=self.config.general.message_delete_after)
            return
        if not playlist_two_id:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Cannot find playlist {playlist_index_two}',
                delete_after=self.config.general.message_delete_after)
            return
        async with self.with_db_session() as db_session:
            for item in await async_retry_database_commands(db_session, lambda: database_functions.list_playlist_items(db_session, playlist_two_id)):
                try:
                    playlist_item_id = await self.__playlist_insert_item(db_session, playlist_one_id, item.video_url, item.title, item.uploader)
                except PlaylistMaxLength:
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        f'Cannot add more items to playlist "{playlist_one_id}", already max size',
                        delete_after=self.config.general.message_delete_after)
                    return
                if playlist_item_id:
                    self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                        f'Added item "{item.title}" to playlist {playlist_index_one}',
                        delete_after=self.config.general.message_delete_after)
                    continue
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Unable to add playlist item "{item.title}", likely already exists',
                    delete_after=self.config.general.message_delete_after)
        await self.__playlist_delete(playlist_two_id)

    @command(name='random-play')
    @command_wrapper
    async def playlist_random_play(self, ctx: Context):
        '''
        Play 32 random items from history playlist with shuffle enabled

        Equivalent to: !playlist queue 0 shuffle 32
        '''
        channel = await self.__check_author_voice_chat(ctx)
        if not channel:
            return
        if not await self.__check_database_session(ctx):
            return

        player = await self.__ensure_player(ctx, channel)
        if not player:
            return

        # Get history playlist (id 0)
        playlist_id, is_history = await self.__get_playlist(0, ctx)
        if not playlist_id:
            return None

        # Play 32 items with shuffle enabled
        return await self.__playlist_queue(ctx, player, playlist_id, shuffle=True, max_num=32, is_history=is_history)

    @command(name='music-stats')
    @command_wrapper
    async def music_stats(self, ctx: Context):
        '''
        Show music player stats
        '''
        if not await self.__check_database_session(ctx):
            return

        async with self.with_db_session() as db_session:
            guild_analytics = await async_retry_database_commands(db_session, lambda: database_functions.ensure_guild_video_analytics(db_session, ctx.guild.id))
            hours = guild_analytics.total_duration_seconds // 3600
            minutes = (guild_analytics.total_duration_seconds % 3600) // 60
            seconds = guild_analytics.total_duration_seconds % 60
            message = f'```Music Stats for Server\nTotal Plays: {guild_analytics.total_plays}\nCached Plays: {guild_analytics.cached_plays}\n' \
                    f'Total Time Played: {guild_analytics.total_duration_days} days, {hours} hours, {minutes} minutes, and {seconds} seconds\n' \
                    f'Tracked Since: {guild_analytics.created_at.strftime("%Y-%m-%d %H:%M:%S")} UTC\n```'
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                message, delete_after=self.config.general.message_delete_after)
