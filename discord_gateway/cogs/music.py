# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

import asyncio
from asyncio import sleep
from asyncio import QueueFull, TimeoutError as async_timeout
from functools import partial
from pathlib import Path
import random
from shutil import disk_usage
from tempfile import TemporaryDirectory
from time import time
from typing import List, Optional
from uuid import uuid4

from dappertable import shorten_string, DapperTable, Columns, Column, PaginationLength
from discord.ext.commands import Bot, Context, group, command
from discord import VoiceChannel
from discord.errors import ClientException
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode
from opentelemetry.metrics import Observation
from pydantic import BaseModel, Field, model_validator

from discord_core.clients.http_client_base import start_seam_checks
from discord_core.cogs.music_helpers.common import SearchType, PLAYHISTORY_PREFIX
from discord_core.common import DISCORD_MAX_MESSAGE_LENGTH
from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException, ExitEarlyException
from discord_core.types.download import LifecycleEvent, LifecycleStatusUpdate, is_rejection
from discord_core.types.media_download import MediaDownload, media_download_attributes
from discord_core.types.media_request import MediaRequest, media_request_attributes
from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.search import SearchResult
from discord_core.types.search_resolution import SearchResolution
from discord_core.utils.common import (rm_tree, return_loop_runner,
                                      seam_contract_from_settings,
                                      tracing_config_from_settings)
from discord_core.utils.discord_context import DiscordContextNaming
from discord_core.utils.loop_health import LOOP_HEALTH
from discord_core.utils.otel import async_otel_span_wrapper, AttributeNaming, capture_span_context, MetricNaming, METER_PROVIDER, create_observable_gauge, loop_heartbeat_observations, span_links_from_context
from discord_core.interfaces.database_protocols import GuildAnalyticsStore, PlaylistStore
from discord_core.types.playlist import PlaylistItemAddStatus, PlaylistItemWrite
from discord_core.types.video_cache import MusicCacheConfig
from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.interfaces.download_client_protocol import (
    DownloadClient, RETRY_BACKOFF_SECONDS_MINIMUM,
)
from discord_core.types.queue import PutsBlocked
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.types.player_session import PlayerSession
from discord_core.clients.dispatch_client_base import DispatchClientBase

from discord_gateway.clients.http_download_client import HttpDownloadClient
from discord_gateway.clients.http_media_search_client import HttpMediaSearchClient
from discord_gateway.clients.youtube_music_search_client import (
    HttpYoutubeMusicSearchClient, YoutubeMusicSearchClient,
)
from discord_gateway.cogs.common import CogHelperBase
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer
from discord_gateway.cogs.music_helpers.search_client import SearchClient, SearchException, check_youtube_video
from discord_gateway.types.cleanup_reason import CleanupReason
from discord_gateway.types.playlist_add_result import PlaylistAddResult
from discord_gateway.utils.bot_metrics import BotMetricNaming
from discord_gateway.utils.otel_command import command_wrapper

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
    # Per-egress bucket for the shared YouTube backoff/failure keys. Pods behind
    # distinct egress IPs should use distinct keys so their rate-limits don't couple.
    youtube_egress_key: str = 'default'
    # No spotify_credentials / youtube_api_key. The cog does not hold provider
    # credentials any more -- source expansion is a round trip to the search pod,
    # which reads both out of music.download in ITS config (cli/search.py). Left
    # here they would be a config surface nothing reads: pydantic's default
    # extra='ignore' means discord.bot.conf can keep carrying them through the
    # cutover either way, so the only thing declaring them would buy is the
    # implication that the bot still uses them.
    server_queue_priority: list[ServerQueuePriorityConfig] = Field(default_factory=list)
    cache: MusicCacheConfig = Field(default_factory=MusicCacheConfig)
    storage: Optional[MusicStorageConfig] = None
    normalize_audio: bool = False
    max_download_retries: int = Field(default=3, ge=1)
    # Hold-off before a failed YouTube download is retried, doubling per attempt.
    # 0 restores the immediate requeue (see RETRY_BACKOFF_SECONDS_MINIMUM).
    retry_backoff_seconds_minimum: int = Field(default=RETRY_BACKOFF_SECONDS_MINIMUM, ge=0)
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

class BrokerClientConfig(BaseModel):
    '''Config for connecting to the broker pod's HTTP server.

    Required: the broker runs as a standalone pod and the cog is a client only.
    It used to be optional, selecting an in-process AsyncioBroker when absent —
    that fallback is gone, so a missing url is a startup error rather than a quiet
    mode switch (projects/discord-bot-ha-only).

    There is no matching server config here any more.  music.broker_server used to
    run a BrokerHttpServer inside the bot process so the download and search pods
    could reach an otherwise in-process broker; the broker pod hosts its own server
    via cli/broker.py (general.broker_server), and that key is the one that has
    been in use since the broker cutover on 2026-07-01.'''
    url: str

class DownloadClientConfig(BaseModel):
    '''Config for connecting to the downloader pod's HTTP server.

    Required: downloads run in the standalone downloader pod and the cog submits,
    clears and blocks over HTTP.  It used to be optional, selecting an in-process
    download worker when absent — that fallback is gone, so a missing url is a
    startup error rather than a quiet mode switch (projects/discord-bot-ha-only).

    Unlike the broker there is no matching server config here: the downloader pod
    hosts its own server via its CLI entrypoint, the cog is a client only.'''
    url: str

class YoutubeMusicSearchClientConfig(BaseModel):
    '''Config for connecting to the search pod's HTTP server.

    Required: the search loop runs in the standalone search pod, and the cog is a
    client only.  It used to be optional, selecting an in-process search worker
    when absent — that fallback is gone, so a missing url is a startup error
    rather than a quiet mode switch (projects/discord-bot-ha-only).

    The pod hosts its own server via its CLI entrypoint, so there is no matching
    server config here.

    Named youtube_music_search_client, NOT search_client: the cog already has a
    self.search_client (the SearchClient source-expansion member), and the two
    are unrelated.'''
    url: str

class MediaSearchClientConfig(BaseModel):
    '''Config for connecting to the search pod's source-expansion routes.

    Required, and required from the first release that reads it — unlike the three
    seams above, this one never had an optional phase.  Those each spent a release
    selecting an in-process worker when the url was absent, and each had to be
    un-defaulted later (projects/discord-bot-ha-only); there is no reason to
    repeat that here when the pod has been serving these routes since !264.

    A missing url is therefore a startup error.  That is deliberate: the
    alternative is an in-process fallback, which would mean importing
    clients/media_search_client — and with it spotipy and googleapiclient, the two
    packages this whole extraction exists to get off the bot image.  A fallback
    nothing can reach without undoing the point of the change is not a fallback.

    The same pod and port as youtube_music_search_client: one bind fronts both
    route families (servers/composite_server.py).  They stay separate keys anyway,
    because sharing a pod today is not a promise to share one forever, and a
    single key would make splitting them a config break rather than a config
    edit.'''
    url: str

class MusicConfig(BaseModel):
    '''Top-level music cog configuration'''
    general: MusicGeneralConfig = Field(default_factory=MusicGeneralConfig)
    player: MusicPlayerConfig = Field(default_factory=MusicPlayerConfig)
    playlist: MusicPlaylistConfig = Field(default_factory=MusicPlaylistConfig)
    download: MusicDownloadConfig = Field(default_factory=MusicDownloadConfig)
    broker_client: BrokerClientConfig
    download_client: DownloadClientConfig
    youtube_music_search_client: YoutubeMusicSearchClientConfig
    media_search_client: MediaSearchClientConfig

#
# Exceptions
#

OTEL_SPAN_PREFIX = 'music'
# Idle backoff for process_download_results when the broker has no finished
# result ready — this paces the remote GET /results/next poll.
_BROKER_POLL_INTERVAL_SECONDS = 1.0
# Idle backoff for the process_search_results loop when its queue is empty. Sleeping ONLY on
# the empty path (not every iteration) keeps busy work back-to-back while cutting idle
# allocation churn (OOM fix).
_IDLE_POLL_BACKOFF_SECONDS = 0.25
# How long a restarted gateway waits for the previous one's now-playing record to lapse before
# taking over a guild anyway. The broker expires the record 15s after the last heartbeat.
FOREIGN_PLAYER_WAIT_SECONDS = 20.0
# What enqueue_track answers when it does not accept a track (the broker's own spellings).
ENQUEUE_FULL = 'full'
ENQUEUE_CLOSED = 'closed'
ENQUEUE_DUPLICATE = 'duplicate'

# Background-loop names. Used as both the LoopHealth registry key and the
# heartbeat gauge's background_job attribute, so the metric series and the health
# server's `loops` payload name the same loop the same way.
LOOP_CLEANUP_PLAYERS = 'cleanup_players'
LOOP_PROCESS_DOWNLOAD_RESULTS = 'process_download_results'
LOOP_PROCESS_SEARCH_RESULTS = 'process_search_results'

#
def _tagged(observation: Observation, tracked_by: str) -> Observation:
    '''
    Re-stamp an observation with the side of the voice stack that produced it.

    The two callbacks already emit their own attributes -- a guild id, or nothing
    at all for the explicit zero -- and both shapes have to survive. Merging into
    a copy rather than mutating keeps the zero series distinct from the per-guild
    ones, which is what makes `sum()` over this metric mean what it looks like.
    '''
    attributes = dict(observation.attributes or {})
    attributes[AttributeNaming.TRACKED_BY.value] = tracked_by
    return Observation(observation.value, attributes=attributes)


class Music(CogHelperBase): #pylint:disable=too-many-public-methods
    '''
    Music related commands
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 stores: object = None, redis_manager=None): #pylint:disable=too-many-statements
        # Enabled-check BEFORE the config validation in the base class: with music
        # switched off there is no music section to validate, and its absence has to
        # read as "not enabled" rather than as a config error.
        if not settings.get('general', {}).get('include', {}).get('music', False):
            raise CogMissingRequiredArg('Music not enabled')
        try:
            super().__init__(bot, settings, dispatcher, stores,
                             settings_prefix='music', config_model=MusicConfig,
                             redis_manager=redis_manager)
        except CogMissingRequiredArg as exc:
            # load_cogs reads CogMissingRequiredArg as "this cog opts out" and skips
            # it with a debug line. Right for "not enabled"; wrong for a music
            # section that IS present and fails to validate. The client urls are the
            # HA wiring, and a bot that comes up silently music-less because one was
            # fat-fingered is exactly the "looks applied, does nothing" failure this
            # project set out to remove — so a bad music config is fatal, not a skip.
            raise DiscordBotException(f'Invalid music config: {exc}') from exc

        self.players = {}
        self._cleanup_task = None
        self._result_task = None
        self._search_result_task = None
        self._init_task = None
        # Names this process in the broker's now-playing record, so a restarted gateway can tell
        # a heartbeat it did not make from its own.
        self.gateway_id = uuid4().hex

        # Keep track of when bot is in shutdown mode
        self.bot_shutdown_event = asyncio.Event()
        self._message_delete_after = self.config.general.message_delete_after

        # Every playlist read and write goes through this. It was annotated against
        # the Protocol rather than PlaylistClient so the HTTP store could drop in
        # without touching a call site -- which is what MR 4b did, and no call site
        # below changed.
        self.playlist_store: PlaylistStore | None = None
        # Same rule for the analytics tables, and with this one the cog holds no
        # session of its own at all.
        self.guild_analytics_store: GuildAnalyticsStore | None = None
        if self.stores:
            self.playlist_store = self.stores.playlist
            self.guild_analytics_store = self.stores.guild_analytics

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

        # The broker runs in the standalone broker pod: it owns the registry, the
        # bundle state, the video cache and the S3 checkout, and the cog reaches
        # all of it over HTTP.  No in-process AsyncioBroker and no VideoCacheClient
        # are built here — the broker pod builds both from its own config
        # (cli/broker.py reads music.download.cache), so music.download.cache in a
        # BOT config is inert.
        #
        # bucket_name is load-bearing: the broker's checkout returns
        # CheckoutResult(s3_key=...), and the client stamps bucket_name onto it so
        # MusicPlayer knows where to fetch the file from S3.  Without it the player
        # falls through to open() the raw s3_key and 404s.
        # Read once and shared by all four of this cog's HTTP clients: they speak
        # four different seams but the grace window is a property of the roll, not
        # of the seam, so one config drives them all.
        seam_contract_config = seam_contract_from_settings(settings)
        self.broker_client: BrokerClient = HttpBrokerClient(
            self.config.broker_client.url, bucket_name=storage_bucket_name,
            seam_contract=seam_contract_config)

        # Source expansion runs in the search pod: the cog posts a Spotify or
        # YouTube-playlist id over HTTP and gets a CatalogResponse back, and never
        # builds a provider client of its own.  SearchClient is unchanged either
        # side of this line — it takes a MediaSearchClient, and both
        # implementations satisfy that Protocol, which is the entire reason MR 1
        # put the Protocol in first.
        #
        # A provider failure still arrives as MediaSearchError, raised by the
        # remote client out of the pod's typed error body, so the rendering below
        # it does not know the difference.
        # Kept as an attribute as well as wrapped: SearchClient is a Protocol
        # consumer and does not expose the transport, but the seam check is a
        # property of the HTTP client underneath it.
        self.media_search_client = HttpMediaSearchClient(
            self.config.media_search_client.url, seam_contract=seam_contract_config)
        self.search_client = SearchClient(self.media_search_client)
        # Downloads run in the standalone downloader pod: the cog submits, clears
        # and blocks over HTTP and never builds an in-process worker or queue.
        # Results still return through the broker's download-result queue, which
        # process_download_results consumes.
        # Span toggles live in general.monitoring.tracing, alongside the logging
        # block the base class reads the same way. Read here rather than threaded
        # from the cli because the cog is where these two clients are built, and a
        # new constructor argument through load_cogs would be one more hop at
        # which the value could be dropped and silently replaced by the default.
        tracing_config = tracing_config_from_settings(self.settings)
        self.download_client: DownloadClient = HttpDownloadClient(
            self.config.download_client.url,
            trace_status_poll=tracing_config.trace_queue_worker_status_poll,
            seam_contract=seam_contract_config)
        # The search loop runs in the standalone search pod; the cog submits,
        # clears and blocks over HTTP and never builds a worker, queue or driver.
        # Resolutions come back through the broker's search-result queue, which
        # process_search_results already consumes.
        self.youtube_music_search_client: YoutubeMusicSearchClient = HttpYoutubeMusicSearchClient(
            self.config.youtube_music_search_client.url,
            trace_status_poll=tracing_config.trace_queue_worker_status_poll,
            seam_contract=seam_contract_config)

        # Callback functions
        create_observable_gauge(METER_PROVIDER, BotMetricNaming.VOICE_SESSIONS.value, self.__voice_sessions_callback, 'Voice sessions, by what counted them')
        # Cache filesystem stats — only meaningful in local mode with a dedicated mount
        if not storage_bucket_name and self.download_dir and self.download_dir.is_mount():
            # Cache stats
            create_observable_gauge(METER_PROVIDER, BotMetricNaming.CACHE_FILESYSTEM_MAX_BYTES.value, self.__cache_filestats_callback_total, 'Max size of cache filesystem', unit='bytes')
            create_observable_gauge(METER_PROVIDER, BotMetricNaming.CACHE_FILESYSTEM_USED_BYTES.value, self.__cache_filestats_callback_used, 'Used size of cache filesystem', unit='bytes')
        # Heartbeat gauges. Every one is driven by LoopHealth (successful
        # iterations), not by task liveness — see utils/loop_health. A loop only
        # emits a series once it registers in cog_load, so the ones that don't
        # run in this deployment mode (e.g. the bot-side download loop under HA,
        # where the loop lives in the downloader pod) report nothing at all
        # rather than a permanent 0 that would trip the stalled-loop alert.
        for job_name, description in (
                (LOOP_CLEANUP_PLAYERS, 'Cleanup player loop heartbeat'),
                (LOOP_PROCESS_DOWNLOAD_RESULTS, 'Download result processing loop heartbeat'),
                (LOOP_PROCESS_SEARCH_RESULTS, 'Search result processing loop heartbeat'),
        ):
            create_observable_gauge(METER_PROVIDER, MetricNaming.HEARTBEAT.value,
                                    partial(loop_heartbeat_observations, job_name), description)

    # Metric callback functons
    def __voice_sessions_callback(self, _options):
        '''
        Both counts of the bot's voice sessions, under one metric.

        `tracked_by=player` is MusicPlayer objects the cog holds; `voice_client`
        is discord.py's raw sockets. They agree almost always -- 1 disagreeing
        minute in 10,008 over a week -- and the disagreement is the signal: a
        socket with no player behind it is a stranded bot, and that condition
        cannot be written without both series.
        '''
        return ([_tagged(o, 'player') for o in self.__active_players_callback(_options)]
                + [_tagged(o, 'voice_client')
                   for o in self.__voice_clients_connected_callback(_options)])

    def __active_players_callback(self, _options):
        '''
        Get active players, or an explicit zero when there are none.

        The zero matters more than it looks.  Emitting only per-guild
        observations meant the whole series vanished whenever no guild had a
        player, so "bot up and idle" and "bot down" were indistinguishable in
        Mimir — and the condition worth alerting on, a broker bundle still alive
        with no player behind it, could not be written at all.  The zero exists
        only while this pod is running, which is exactly what separates the two.

        It carries no guild attribute because there is no guild to name; that
        makes it a distinct series from the per-guild ones, so aggregate with
        sum() rather than reading a single series.  Note the per-guild series go
        stale rather than vanishing instantly, so sum() can briefly count both a
        departing guild and this zero — any alert on it wants a `for:` longer
        than the staleness window.
        '''
        if not self.players:
            return [Observation(0)]
        return [
            Observation(1, attributes={DiscordContextNaming.GUILD.value: key})
            for key in self.players
        ]

    def __voice_clients_connected_callback(self, _options):
        '''
        Active voice connections, one observation per connected guild.

        Unlike active_players (which counts MusicPlayer objects in self.players),
        this reflects the raw voice socket, so an orphaned connection whose player
        was already reaped still shows here — the signal a stranded bot leaves.

        Falls back to an explicit zero when nothing is connected, for the same
        reason active_players does: without it the series disappears entirely and
        an idle bot reads identically to a dead one.
        '''
        items = []
        for voice_client in self.bot.voice_clients:
            guild = getattr(voice_client, 'guild', None)
            if guild is None:
                continue
            items.append(Observation(1, attributes={
                DiscordContextNaming.GUILD.value: guild.id,
            }))
        return items or [Observation(0)]

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
        # The bot is the client that broke twice on the broker seam: it depended on
        # the broker's GET /search-results/next while a pre-MR2 broker was still the
        # Service's only ready endpoint. Started here rather than in __init__
        # because there is no running loop there, and it must never be able to
        # stop the cog from loading.
        #
        # All four of this cog's seams, not just the broker: the download client
        # speaks queue_worker to the downloader pod, the ytmusic client speaks the
        # same seam to the search pod at a different prefix, and the media-search
        # client speaks media_search to that same pod. Each claims only its own
        # peer's routes, so the two queue_worker clients do not demand each
        # other's prefix.
        start_seam_checks()
        self._cleanup_task = self.bot.loop.create_task(
            return_loop_runner(self.cleanup_players, self.bot, self.logger,
                               health=LOOP_HEALTH.register(LOOP_CLEANUP_PLAYERS))()
        )
        # One-shot, not a loop: sessions are consumed once at startup.  It waits
        # on the gateway itself rather than going through return_loop_runner,
        # which would re-run it forever.
        self._init_task = self.bot.loop.create_task(self.resume_player_sessions())
        # The download loop lives in the downloader pod. The cog starts only the
        # client's status poller. The bot registers no download loop and emits no
        # download_files heartbeat; the downloader pod publishes that series.
        await self.download_client.start(self.bot, self.bot_shutdown_event)
        self._result_task = self.bot.loop.create_task(
            return_loop_runner(self.process_download_results, self.bot, self.logger,
                               health=LOOP_HEALTH.register(LOOP_PROCESS_DOWNLOAD_RESULTS))()
        )
        self._search_result_task = self.bot.loop.create_task(
            return_loop_runner(self.process_search_results, self.bot, self.logger,
                               health=LOOP_HEALTH.register(LOOP_PROCESS_SEARCH_RESULTS))()
        )
        # The search loop lives in the search pod, so the cog starts only the
        # client's status poller. The bot registers no youtube_music_search loop
        # and emits no heartbeat for one — the search pod publishes that series
        # instead (cli/search.py's LOOP_SEARCH_WORKER, the same loop name).
        await self.youtube_music_search_client.start(self.bot, self.bot_shutdown_event)
        # No embedded BrokerHttpServer: the broker pod serves that surface. Recording plays
        # (analytics and the history playlist) is the broker's history worker now.

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
            # Cancelling a task never runs the loop runner's exit path, so mark
            # these stopped here: a cancelled loop is a deliberate shutdown, not
            # a wedge, and must not fail the liveness probe while the pod drains.
            LOOP_HEALTH.mark_stopped(LOOP_CLEANUP_PLAYERS, LOOP_PROCESS_DOWNLOAD_RESULTS,
                                     LOOP_PROCESS_SEARCH_RESULTS)
            if self._init_task:
                self._init_task.cancel()
            if self._cleanup_task:
                self._cleanup_task.cancel()
            # No bot-side download loops to cancel — stop the status poller and
            # close the HTTP session instead.
            await self.download_client.stop()
            # No bot-side search loop to cancel — just the poller + session.
            await self.youtube_music_search_client.stop()
            if self._result_task:
                self._result_task.cancel()
            if self._search_result_task:
                self._search_result_task.cancel()

            self.logger.info('Cog unload: Removing directories')
            # Remove contents of download dir by default
            if self.download_dir and self.download_dir.exists():
                rm_tree(self.download_dir)
            if self.config.player.player_dir_path is None and self.player_dir.exists():
                rm_tree(self.player_dir)

            return True


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

        The broker performs the mark AND re-renders the bundle — closing the render gap left by the
        retired set_on_change callback.
        '''
        await self.broker_client.update_request_status(
            str(media_request.uuid),
            LifecycleStatusUpdate(event=event, **details),
        )

    async def _cleanup_orphaned_voice_clients(self):
        '''
        Disconnect any voice client that has no backing MusicPlayer in
        self.players. Such an orphan appears when a player is removed but its
        voice connection is not (e.g. a disconnect that failed part-way through
        cleanup). The per-player logic in cleanup_players only walks
        self.players, so without this sweep an orphan sits in the channel until
        the pod restarts.
        '''
        for voice_client in list(self.bot.voice_clients):
            guild = getattr(voice_client, 'guild', None)
            if guild is None or guild.id in self.players:
                continue
            self.logger.warning(f'Found orphaned voice client in guild {guild.id} with no active player, disconnecting')
            try:
                await voice_client.disconnect()
            except Exception as e:
                self.logger.warning(f'Error disconnecting orphaned voice client in guild {guild.id}: {e}')

    async def cleanup_players(self):
        '''
        Check for players with no members, cleanup bot in channels that do
        '''
        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Bot in shutdown, exiting early')
        await sleep(1)

        # Reap orphaned voice clients first — runs even when self.players is
        # empty, which is exactly the state a stranded connection leaves behind.
        await self._cleanup_orphaned_voice_clients()

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
        Queue a downloaded track for the guild's player.

        media_download : Standard MediaDownload for post download
        player : MusicPlayer
        '''
        attributes = media_download_attributes(media_download)
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.add_source_to_player', kind=SpanKind.INTERNAL, attributes=attributes, links=span_links_from_context(media_download.media_request.span_context)):
            media_uuid = str(media_download.media_request.uuid)
            # Register with the broker BEFORE the track becomes visible in the queue. For a
            # cache hit this is the only point the broker learns of the entry, and the
            # player can claim the track the instant it is queued. If that race puts the
            # claim ahead of registration the broker has no entry to check out, and the
            # player would fall back to the raw S3 cache key (crashing on open()).
            # Registering first closes the window.
            await self.broker_client.register_download(media_download)
            result = await self.broker_client.enqueue_track(
                media_download.media_request.guild_id, media_uuid, self.config.player.queue_max_size)
            if result == ENQUEUE_FULL:
                self.logger.info(f'Play queue full, aborting download of item "{str(media_download.media_request)}"')
                reason = (f'Cannot add item "{media_download.title}" to play queue, play queue is full'
                          if media_download.media_request.bundle_uuid else None)
                await self._push_state(media_download.media_request, LifecycleEvent.FAILED, failure_reason=reason)
                await self.broker_client.discard(media_uuid)
                return False
                # Dont return to loop, file was downloaded so we can iterate on cache at least
            if result == ENQUEUE_CLOSED:
                self.logger.info(f'Queue closed in guild "{media_download.media_request.guild_id}", assuming shutdown')
                await self._push_state(media_download.media_request, LifecycleEvent.DISCARDED)
                await self.broker_client.discard(media_uuid)
                return False
            if result == ENQUEUE_DUPLICATE:
                # Already queued, so the track will play; nothing to undo.
                self.logger.warning(f'"{media_download.webpage_url}" was already queued in guild '
                                    f'{media_download.media_request.guild_id}')
            self.logger.info(f'Adding "{media_download.webpage_url}" '
                             f'to queue in guild {media_download.media_request.guild_id}')
            player.notify_enqueued()
            player.trigger_prefetch()
            await self._push_state(media_download.media_request, LifecycleEvent.COMPLETED)
            return True

    # Take both source dict and media download
    # Since media download might be none
    async def __ensure_video_download_result(self, media_request: MediaRequest, media_download: MediaDownload):
        if media_download is None:
            await self._push_state(media_request, LifecycleEvent.FAILED,
                                   failure_reason=f'Issue downloading video "{media_request}"')
            return False
        return True

    async def __return_bad_video(self, media_request: MediaRequest, user_message: str | None,
                                 skip_callback_functions: bool=False, rejected: bool=False):
        await self._push_state(media_request, LifecycleEvent.FAILED, failure_reason=user_message,
                               rejected=rejected)
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

    async def _requeue_search_result(self, resolution: SearchResolution) -> None:
        '''
        Put a resolution back on the broker's search-result queue.

        next_search_result is a destructive pop: the resolution exists nowhere
        else once this loop holds it, so any failure between that pop and a
        successful submit destroys the request outright -- the media request is
        never downloaded and its bundle row strands on QUEUED forever, with no
        error the user can see.  The downloader deploys under Recreate (one
        Mullvad key, so no rolling overlap is possible), which makes a hard gap
        on its Service a routine event rather than an exotic one.

        A failure to requeue is the one case where the request really is lost,
        so it is logged as such rather than folded into the caller's traceback.
        '''
        try:
            await self.broker_client.register_search_result(resolution)
        except Exception:
            self.logger.exception(
                f'Failed to requeue search result for media_request '
                f'{resolution.media_request.uuid}; the request is lost')

    async def process_search_results(self):
        '''
        Search-result consumer: routes resolved searches into the download
        pipeline.  Resolution happens on the search loop (on the
        standalone search pod); this loop runs the bot-side tail —
        cache-check then download submit — which can only run where the download
        client and cache live.  Mirrors process_download_results.
        '''
        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Bot shutdown called, exiting early')

        resolution = await self.broker_client.next_search_result()
        if resolution is None:
            # Idle — no resolved search ready right now.  Sleep before the loop
            # runner re-calls so we don't busy-spin the broker (a remote
            # GET /search-results/next poll).
            await sleep(_BROKER_POLL_INTERVAL_SECONDS)
            return

        media_request = resolution.media_request
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.process_search_results', kind=SpanKind.CONSUMER,
                                           attributes=media_request_attributes(media_request),
                                           links=span_links_from_context(media_request.span_context) + span_links_from_context(resolution.span_context)) as span:
            await self._push_state(media_request, LifecycleEvent.QUEUED)

            # Check if cache item exists already
            if await self._enqueue_media_download_from_cache(media_request):
                # Cache hit: _enqueue_media_download_from_cache already pushed COMPLETED
                # on this same media_request (check_cache binds the cached download to
                # this request object), which advances the bundle row toward teardown.
                # A second COMPLETED here is redundant and trails an extra render behind
                # the remove — the source of the stranded "Media request queued for
                # download" status message — so it is intentionally omitted.
                span.set_status(StatusCode.OK)
                return

            try:
                self.logger.debug(f'Handing off media_request "{str(media_request)}" to download queue, uuid: {media_request.uuid}')
                await self.download_client.submit(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
            except PutsBlocked:
                self.logger.info(f'Puts to queue in guild {media_request.guild_id} are currently blocked, assuming shutdown')
                await self._push_state(media_request, LifecycleEvent.DISCARDED)
                return
            except QueueFull:
                self.logger.info(f'Queue full in guild {media_request.guild_id}, cannot add more media requests')
                await self._push_state(media_request, LifecycleEvent.DISCARDED)
            except Exception:
                # PutsBlocked and QueueFull are the downloader *answering*; anything
                # else means it never got the request. Most often that is a
                # ClientConnectorError against a downloader pod that is mid-Recreate,
                # which downloader-app.yaml calls harmless on the grounds that
                # "downloads queue in Redis and resume when the new pod's tunnel is
                # up". That holds for work already on the downloader's queue and not
                # for work in this handoff, which is why the resolution goes back
                # before the exception propagates.
                #
                # Re-raised rather than swallowed so the loop runner's capped backoff
                # applies. Returning normally here would re-pop the same resolution
                # immediately and spin against a Service that is still down.
                await self._requeue_search_result(resolution)
                raise
            span.set_status(StatusCode.OK)

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
            # (a remote GET /results/next poll).
            await sleep(_BROKER_POLL_INTERVAL_SECONDS)
            return

        media_request = result.media_request
        is_playlist_add = isinstance(media_request, PlaylistAddRequest)
        attributes = media_request_attributes(media_request)

        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.process_download_results', kind=SpanKind.CONSUMER, attributes=attributes, links=span_links_from_context(media_request.span_context) + span_links_from_context(result.span_context)) as span:
            if not result.status.success:
                rejected = is_rejection(result.status.error_type)
                self.logger.info(f'Terminal error on "{str(media_request)}": {result.status.error_detail or ""}')
                # A rejection is a decision, not a fault — the pipeline looked at the
                # video and declined it (too long, banned, private, age restricted).
                # Marking those spans ERROR made the Consumer Span Error Rate alert
                # page on ordinary user input, so only genuine failures stay ERROR.
                span.set_status(StatusCode.OK if rejected else StatusCode.ERROR)
                await self.__return_bad_video(media_request, result.status.user_message,
                                              rejected=rejected)
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

            # The broker pod already persisted this result when the download
            # client reported it — build the MediaDownload the player needs from
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
        Make sure the guild has a history playlist, and return it.

        The broker's history worker records plays into it, but it only gets there after a
        track has played. Creating it as the guild's player starts keeps the playlist list
        (where history is index 0) stable from the first command, as it was when the gateway
        recorded plays itself.

        guild_id : Guild id
        '''
        if not self.stores:
            return None
        # Get-or-create in one call. Split across a read and a conditional write
        # it is a race between any two players starting at once, and the table's
        # unique constraint on (name, server_id) would surface that as an error
        # on a path with nowhere to report one.
        return await self.playlist_store.ensure_history_playlist(guild_id)

    async def _save_player_session(self, guild, player: MusicPlayer) -> None:
        '''
        Remember where this guild's player is, so a restart can rejoin it.

        Only the voice and text channels are kept: the queue itself lives in the broker and
        survives the restart on its own. Saved when the player joins voice (and on shutdown), so
        a crash leaves a session behind too.

        No session is written when the bot is not in a voice channel: there is nothing to
        rejoin, and a session with no channel would only be discarded on the way back up.
        '''
        voice_client = guild.voice_client
        channel = getattr(voice_client, 'channel', None)
        if channel is None:
            self.logger.debug(f'No voice channel for guild {guild.id}, skipping session save')
            return
        session = PlayerSession(
            guild_id=guild.id,
            voice_channel_id=channel.id,
            text_channel_id=player.text_channel.id,
            was_playing=player.current_media_download is not None,
        )
        self.logger.debug(f'Saving player session for guild {guild.id}, was_playing={session.was_playing}')
        await self.broker_client.save_player_session(session)

    async def resume_player_sessions(self) -> None:
        '''
        Rejoin and resume any guild that was playing when the bot went down.

        One-shot, run after the gateway is ready so guild/channel lookups resolve.
        '''
        await self.bot.wait_until_ready()
        sessions = await self.broker_client.list_player_sessions()
        self.logger.info(f'Found {len(sessions)} player session(s) to consider resuming')
        for session in sessions:
            try:
                await self._resume_player_session(session)
            except Exception as e:  #pylint:disable=broad-except
                # One guild's bad session must not stop the others from resuming.
                self.logger.exception(f'Error resuming player session for guild {session.guild_id}: {e}')

    async def _abandon_guild_state(self, guild_id: int) -> None:
        '''
        Drop whatever the broker holds for a guild nobody is going to resume.

        Without this a queue left behind by a session that cannot be resumed would come back to
        life on someone's next !play.
        '''
        released = await self.broker_client.close_guild(guild_id)
        self.logger.info(f'Closed broker state for guild {guild_id}, released {released} entr(ies)')

    async def _wait_out_foreign_player(self, guild_id: int) -> None:
        '''
        If another gateway still looks like it is playing in this guild, wait for it to stop.

        A replacement that starts within the broker's heartbeat window (15s) would otherwise find
        the old gateway's now-playing record still alive and be refused the interrupted track.
        The record lapses by itself once the old gateway stops beating, so this waits for that,
        for a bounded time, and then goes ahead regardless.
        '''
        deadline = time() + FOREIGN_PLAYER_WAIT_SECONDS
        while True:
            queue = await self.broker_client.get_guild_queue(guild_id)
            playing = queue.playing
            if playing is None or playing.gateway_id == self.gateway_id:
                return
            if time() >= deadline:
                self.logger.warning(f'Gateway {playing.gateway_id} still appears to be playing in guild '
                                    f'{guild_id} after {FOREIGN_PLAYER_WAIT_SECONDS:.0f}s, continuing anyway')
                return
            self.logger.info(f'Waiting for gateway {playing.gateway_id} to stop playing in guild {guild_id}')
            await sleep(1)

    async def _resume_player_session(self, session: PlayerSession) -> None:
        '''
        Resume a single stored session, if it still makes sense to.

        The session is dropped first, unconditionally: it describes a moment that has already
        passed, so a resume that fails part-way through must not be retried on the next restart
        against even staler state. A successful resume saves a fresh one when it rejoins voice.
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.resume_player_session', kind=SpanKind.INTERNAL,
                                           attributes={DiscordContextNaming.GUILD.value: session.guild_id}):
            await self.broker_client.delete_player_session(session.guild_id)

            guild = self.bot.get_guild(session.guild_id)
            if guild is None:
                self.logger.warning(f'Guild {session.guild_id} not found, cannot resume session')
                await self._abandon_guild_state(session.guild_id)
                return
            voice_channel = guild.get_channel(session.voice_channel_id)
            text_channel = guild.get_channel(session.text_channel_id)
            if voice_channel is None or text_channel is None:
                self.logger.warning(f'Voice or text channel gone for guild {session.guild_id}, cannot resume session')
                await self._abandon_guild_state(session.guild_id)
                return
            # Don't play to an empty room.  Everyone leaving while the bot was
            # down is the clearest signal nobody is waiting on the queue.
            if not [member for member in voice_channel.members if not member.bot]:
                self.logger.info(f'No listeners left in voice channel {voice_channel.id} for guild '
                                 f'{session.guild_id}, not resuming')
                await self._abandon_guild_state(session.guild_id)
                return

            await self._wait_out_foreign_player(session.guild_id)
            # Taking ownership of the guild is what hands back a track the old gateway had
            # started and never finished.
            await self.broker_client.open_guild(session.guild_id, text_channel.id)
            # A track recovered from the old gateway is back at the head of the queue by now,
            # so the queue alone is the count of what is waiting.
            queue = await self.broker_client.get_guild_queue(session.guild_id)
            waiting = len(queue.items)
            if not waiting and queue.playing is None:
                self.logger.info(f'Nothing queued for guild {session.guild_id}, not resuming')
                await self._abandon_guild_state(session.guild_id)
                return

            self.logger.info(f'Resuming playback in guild {session.guild_id} with {waiting} item(s) queued')
            player = await self.get_player(session.guild_id, join_channel=voice_channel,
                                           guild=guild, text_channel=text_channel)
            if player is None:
                self.logger.warning(f'Could not build player for guild {session.guild_id}, cannot resume session')
                await self._abandon_guild_state(session.guild_id)
                return
            self.dispatcher.send_message(session.guild_id, text_channel.id,
                f'Resumed after a restart, {waiting} item(s) still queued',
                delete_after=self.config.general.message_delete_after)

    async def cleanup(self, guild, reason: CleanupReason = CleanupReason.QUEUE_TIMEOUT):
        '''
        Cleanup guild player

        guild  : Guild object
        reason : CleanupReason describing why cleanup was triggered
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cleanup', kind=SpanKind.CONSUMER, attributes={DiscordContextNaming.GUILD.value: guild.id}):
            self.logger.info(f'Starting cleanup on guild {guild.id}, reason: {reason.value}')
            player = await self.get_player(guild.id, create_player=False)
            if player:
                # Before the voice disconnect below: that stops playback, which looks to the
                # player like its track finishing. Not by marking the player shut down: the
                # cleanup_players loop would see that and start a second cleanup, with the wrong
                # reason, on top of this one.
                await player.stop_loop()
            if reason == CleanupReason.BOT_SHUTDOWN and player:
                # Capture the session before anything below tears state down: the
                # voice disconnect drops the channel we need to rejoin. The queue needs no
                # capturing; it stays in the broker.
                await self._save_player_session(guild, player)
            if reason == CleanupReason.BOT_SHUTDOWN and player and self.dispatcher:
                self.dispatcher.send_message(player.guild.id, player.text_channel.id,
                    'Bot is restarting. The play queue is saved and playback will resume when '
                    'I am back. Queued downloads keep running in the background.',
                    delete_after=self.config.general.message_delete_after)

            self.logger.info(f'Disconnecting voice clients for music player in guild {guild.id}')

            # Disconnect the voice client BEFORE the broker/queue teardown below.
            # That teardown can raise (e.g. a broker HTTP 500 on a Redis blip), and
            # doing the disconnect last as a fire-and-forget task — the previous
            # behaviour — meant such a raise skipped it while the player was already
            # popped from self.players. The bot was then stranded in the channel with
            # nothing left to reap it (cleanup_players only walks self.players).
            # disconnect() also frees native memory / drops the client from the state
            # cache, so we must NOT call voice_client.cleanup() first: that detaches
            # the socket and can suppress the gateway leave.
            voice_client = guild.voice_client
            if voice_client:
                try:
                    await voice_client.disconnect()
                    self.logger.debug(f'Disconnected voice client for guild {guild.id}')
                except Exception as e:
                    self.logger.warning(f'Error disconnecting voice client for guild {guild.id}: {e}')

            # Block download queue for later
            # Clear queues before blocking: the worker's clear_queue restores
            # preserved items via put_nowait, which raises once the queue is blocked,
            # so block-first is not an option here (holds for the Redis worker too,
            # whose clear leaves preserved items in place). The clear/block calls are
            # now async, so this runs during guild teardown (shutdown/disconnect) when
            # the submit loops are already winding down — the narrow post-clear window
            # is acceptable for a guild being torn down.
            # We also record any bundle uuids belonging to preserved (playlist-add)
            # items so we can skip deleting those bundles below — their requests
            # are still in flight and will keep updating the broker bundle UI.
            #
            # None of this runs on BOT_SHUTDOWN: the queues are parked instead of
            # drained. Clearing a queue drops the request from Redis, but its broker
            # registry entry only leaves the in_flight zone when the follow-up
            # DISCARDED push below deletes it — and that loop is one round-trip per
            # item, so a SIGKILL part-way through (the pod runs a short grace period)
            # strands every remaining entry, plus the bundle holding them, until the
            # 24h TTL. Parking costs nothing: the downloader and search tiers outlive
            # this pod, work the backlog, and every request reaches a terminal state
            # that reaps its own entry, while the media lands in the shared cache so
            # a re-request after the restart is a cache hit.
            preserved_bundle_uuids: set[str] = set()
            if reason != CleanupReason.BOT_SHUTDOWN:
                def preserve_predicate(req):
                    keep = not req.download_file
                    if keep and req.bundle_uuid:
                        preserved_bundle_uuids.add(req.bundle_uuid)
                    return keep

                clear_result = await self.download_client.clear_guild_queue(guild.id, preserve_predicate=preserve_predicate)
                # The predicate runs on the downloader pod, so the closure above
                # never sees the preserved items — union the pod-reported bundle_uuids
                # so their bundles are skipped below.
                preserved_bundle_uuids |= clear_result.preserved_bundle_uuids
                self.logger.debug(f'Cleanup found {len(clear_result.dropped)} existing download items')
                for item in clear_result.dropped:
                    await self._push_state(item, LifecycleEvent.DISCARDED)

                search_clear_result = await self.youtube_music_search_client.clear_guild_queue(guild.id, preserve_predicate=preserve_predicate)
                # Playlist-adds queue for search before they queue for download, so the
                # search side preserves bundles too — and its predicate also runs
                # on the search pod, out of reach of the closure above.
                preserved_bundle_uuids |= search_clear_result.preserved_bundle_uuids
                self.logger.debug(f'Cleanup found {len(search_clear_result.dropped)} existing search queue items')
                for item in search_clear_result.dropped:
                    await self._push_state(item, LifecycleEvent.DISCARDED)

                await self.download_client.block_guild(guild.id)
                await self.youtube_music_search_client.block_guild(guild.id)

            player = None
            # Clear play queue if that didnt happen
            try:
                player = self.players.pop(guild.id)
            except KeyError:
                pass

            if player:
                self.logger.info(f'Calling cleanup on player {guild.id}')
                await player.cleanup()

            # Release the guild's queue, the track it was playing and any claim, and take the
            # play-order message down (the broker does both). Not on BOT_SHUTDOWN: the queue
            # is what the next gateway picks up, and its message stays up until then.
            if reason != CleanupReason.BOT_SHUTDOWN:
                released = await self.broker_client.close_guild(guild.id)
                self.logger.info(f'Closed broker state for guild {guild.id}, released {released} entr(ies)')

            # Tear down broker-owned bundles for this guild.  Skip bundles whose
            # playlist-add items survived the queue clear — those will continue
            # to render updates on their own and the cog will release the bundle
            # when those items reach a terminal state.
            #
            # Skipped entirely on BOT_SHUTDOWN, for the same reason the queues are
            # parked above: the backlog those bundles track is still being worked by
            # the downloader and search tiers, so each bundle keeps rendering real
            # progress and releases itself once its requests go terminal.  Deleting
            # them here would drop a live progress UI on the floor, and doing it
            # after the O(queue) work above is what made bundle teardown the first
            # casualty of the grace period.
            if reason != CleanupReason.BOT_SHUTDOWN:
                guild_bundles = await self.broker_client.list_bundles_for_guild(guild.id)
                for bundle_uuid in guild_bundles:
                    if bundle_uuid in preserved_bundle_uuids:
                        self.logger.debug(f'Skipping delete of bundle {bundle_uuid} — has active playlist-add requests')
                        continue
                    await self.delete_bundle(guild.id, bundle_uuid)

            if reason != CleanupReason.BOT_SHUTDOWN:
                self.logger.debug(f'Deleting player dir for guild {guild.id}')
                guild_player_path = self.player_dir / f'{guild.id}'
                if guild_player_path.exists():
                    rm_tree(guild_player_path)

    async def get_player(self, guild_id: int,
                         join_channel = None,
                         create_player: bool=True,
                         ctx: Context = None,
                         check_voice_client_active: bool=False,
                         guild = None,
                         text_channel = None):
        '''
        Retrieve the guild player, or generate one.

        guild_id : Guild id for player
        join_channel: Turn on voice client while we're here
        create_player : Create player if doesn't exist yet
        ctx: Original context call
        check_voice_client_active: Check if we're currently playing anything
        guild / text_channel : Explicit stand-ins for ctx.guild / ctx.channel, for
            callers with no command behind them (a session resumed at startup).
            Ignored when ctx is given.
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.get_player', kind=SpanKind.INTERNAL, attributes={DiscordContextNaming.GUILD.value: guild_id}):
            target_guild = ctx.guild if ctx else guild
            target_text_channel = ctx.channel if ctx else text_channel
            try:
                player = self.players[guild_id]
            except KeyError:
                if check_voice_client_active:
                    self.dispatcher.send_message(target_guild.id, target_text_channel.id,
                        'I am not currently playing anything',
                        delete_after=self.config.general.message_delete_after)
                    return None
                if not create_player:
                    return None
                # Make directory for guild specific files
                guild_path = self.player_dir / f'{target_guild.id}'
                guild_path.mkdir(exist_ok=True, parents=True)
                # Generate and start player
                # Take ownership of the guild's queue before the player exists: this reopens it,
                # tells the broker which channel the play-order message belongs in, and hands back
                # a track a previous gateway had started and never finished.
                await self.broker_client.open_guild(target_guild.id, target_text_channel.id)
                await self.__get_history_playlist(target_guild.id)
                player = MusicPlayer(self.bot, target_guild, target_text_channel,
                                     self.logging_config,
                                     self.config.player.queue_max_size, self.config.player.disconnect_timeout,
                                     guild_path, self.broker_client, self.gateway_id,
                                     prefetch_limit=self.config.download.storage.prefetch_limit if self.config.download.storage else 0,
                                     bucket_name=self.config.download.storage.bucket_name if self.config.download.storage else None)
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
                # Remember where we are, so a restart (or a crash) can come back to this channel.
                await self._save_player_session(player.guild, player)
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
                    await self.youtube_music_search_client.submit(media_request.guild_id, media_request, priority=self.server_queue_priority.get(media_request.guild_id, None))
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
            collection = await self.search_client.check_source(
                search, self.config.player.queue_max_size)
        except SearchException as exc:
            self.logger.info(f'Received download client exception for search "{search}", {str(exc)}')
            await self.delete_bundle(ctx.guild.id, bundle_uuid)
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Error searching input "{search}", message: {str(exc.user_message)}',
                delete_after=self.config.general.message_delete_after)
            return
        except Exception:
            # check_source stopped being an in-process call at the media_search
            # cutover: it is a round trip to the search pod now, so it can fail with
            # a transport error that is not a SearchException and that the clause
            # above was never written to see. The bundle above is already created
            # and rendered, so letting one through leaves a placeholder row that
            # never resolves -- the same stranded-row failure as a dropped search
            # resolution, reached from the other end of the pipeline.
            await self.delete_bundle(ctx.guild.id, bundle_uuid)
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Error searching input "{search}", the search backend is unavailable',
                delete_after=self.config.general.message_delete_after)
            raise

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

        queue = await self.broker_client.get_guild_queue(ctx.guild.id)
        if not queue.items:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are currently no more queued videos.',
                delete_after=self.config.general.message_delete_after)
            return
        self.logger.info(f'Player clear called in guild {ctx.guild.id}')
        await self.broker_client.clear_queue(ctx.guild.id)
        for item in queue.items:
            player.discard_staged(item)
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
        history = await self.broker_client.get_guild_history(ctx.guild.id)

        if not history:
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
        for (count, item) in enumerate(history):
            uploader = item.get('uploader') or ''
            table.add_row([
                f'{count + 1}',
                f"{item.get('title')}",
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

        queue = await self.broker_client.get_guild_queue(ctx.guild.id)
        if not queue.items:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are currently no more queued videos.',
                delete_after=self.config.general.message_delete_after)
            return
        await self.broker_client.shuffle_queue(ctx.guild.id)
        # The head of the queue changed, so the files worth having on disk did too.
        player.trigger_prefetch()

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

        queue = await self.broker_client.get_guild_queue(ctx.guild.id)
        if not queue.items:
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

        # The broker removes by track, not position: the user's index names the track they saw,
        # and if it has moved or started playing since, nothing is removed.
        item = None
        if 1 <= queue_index <= len(queue.items):
            item = await self.broker_client.remove_queued_track(
                ctx.guild.id, str(queue.items[queue_index - 1].media_request.uuid))
        if item is None:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to remove queue index {queue_index}',
                delete_after=self.config.general.message_delete_after)
            return
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Removed item {item.title} from queue',
            delete_after=self.config.general.message_delete_after)
        player.discard_staged(item)
        player.trigger_prefetch()

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

        queue = await self.broker_client.get_guild_queue(ctx.guild.id)
        if not queue.items:
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

        item = None
        if 1 <= queue_index <= len(queue.items):
            item = await self.broker_client.bump_queued_track(
                ctx.guild.id, str(queue.items[queue_index - 1].media_request.uuid))
        if item is None:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to bump queue index {queue_index}',
                delete_after=self.config.general.message_delete_after)
            return
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Bumped item "{item.title}" to top of queue',
            delete_after=self.config.general.message_delete_after)
        player.trigger_prefetch()

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
        # The broker owns the play-order message, so telling it the new channel moves it
        # (deletes the old message, sends it again in the new channel).
        await self.broker_client.open_guild(ctx.guild.id, ctx.channel.id)

        # Update the player's text channel reference, and the session a restart would rejoin from
        player.text_channel = ctx.channel
        await self._save_player_session(player.guild, player)

    async def __get_playlist_public_view(self, playlist_id: int, guild_id: int):
        '''
        Get playlist by db id, and view which public index servers see it as
        '''
        playlist = await self.playlist_store.get_playlist(playlist_id)
        if not playlist:
            return None
        if playlist.server_id != guild_id:
            return None
        if playlist.is_history:
            return 0

        # The index IS the position in list order, which is why that order is
        # part of the store's contract rather than a detail of this loop.
        for (count, playlist_obj) in enumerate(await self.playlist_store.list_playlists(guild_id)):
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

        if index > 0:
            if not await self.playlist_store.count_playlists(ctx.guild.id):
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    'No playlists in database',
                    delete_after=self.config.general.message_delete_after)
                return None, False

        is_history = False
        if index == 0:
            playlist = await self.playlist_store.get_history_playlist(ctx.guild.id)
            is_history = True
        else:
            # The offset-and-take-first was an IndexError waiting on an index
            # past the end; the count check above only rules out zero playlists,
            # not `!playlist show 9` against three of them.
            playlists = await self.playlist_store.list_playlists(ctx.guild.id)
            playlist = playlists[index - 1] if index <= len(playlists) else None
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
        if not self.stores:
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
        existing_playlist = await self.playlist_store.get_playlist_by_name(ctx.guild.id, playlist_name)
        if existing_playlist:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to create playlist "{name}", a playlist with that name already exists')
            return None

        playlist = await self.playlist_store.create_playlist(ctx.guild.id, name)
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
        history_playlist = await self.playlist_store.get_history_playlist(ctx.guild.id)
        playlist_items = await self.playlist_store.list_playlists(ctx.guild.id)

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

    async def __add_playlist_item(self, request: PlaylistAddRequest, result: PlaylistAddResult):
        '''
        Insert a playlist item using the lightweight PlaylistAddResult metadata.

        request : PlaylistAddRequest carrying playlist_id
        result : PlaylistAddResult with webpage_url, title, uploader
        '''
        self.logger.info(f'Adding video_url "{result.webpage_url}" to playlist "{request.playlist_id}"'
                         f' in guild {request.guild_id}')
        # A batch of one. The store reports why rather than raising, so the
        # full-playlist case is an outcome to branch on instead of an exception
        # whose type cannot survive a network hop.
        outcomes = await self.playlist_store.add_items(
            request.playlist_id,
            [PlaylistItemWrite(video_url=result.webpage_url, title=result.title,
                               uploader=result.uploader)],
            self.config.playlist.server_playlist_max_size)
        outcome = outcomes[0]
        if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
            await self._push_state(request, LifecycleEvent.FAILED,
                                   failure_reason='Unable to add item to playlist, playlist too long',
                                   rejected=True)
            return
        playlist_public_view_id = await self.__get_playlist_public_view(request.playlist_id, request.guild_id)
        if outcome.status == PlaylistItemAddStatus.ADDED:
            await self._push_state(request, LifecycleEvent.COMPLETED)
            return
        await self._push_state(request, LifecycleEvent.FAILED,
                               failure_reason=f'Item "{result.title}" already exists in playlist {playlist_public_view_id}',
                               rejected=True)

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

        # The entry comes back built, because the caller wants the deleted
        # item's title for its message and the row is gone by then.
        item = await self.playlist_store.delete_item_by_index(playlist_id, video_index - 1)
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

        headers = [
            Column('Pos', 3, zero_pad=True),
            Column('Title', 32),
            Column('Uploader', 32),
        ]
        table = DapperTable(columns=Columns(headers), pagination_options=PaginationLength(DISCORD_MAX_MESSAGE_LENGTH),
                            enclosure_start='```', enclosure_end='```', prefix=f'Playlist {playlist_index} Items\n')
        total = 0
        # The position shown here is the index `!playlist item-remove` takes, so
        # it has to be the same order the store deletes by.
        for (count, item) in enumerate(await self.playlist_store.list_items(playlist_id)):
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
        await self.playlist_store.delete_playlist(playlist_id)
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
        await self.playlist_store.rename_playlist(playlist_id, playlist_name)
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

        # Snapshot first, so the queue changing under us cannot change what is saved.
        if is_history:
            queue_copy = [PlaylistItemWrite(video_url=item['webpage_url'], title=item.get('title'),
                                            uploader=item.get('uploader'))
                          for item in await self.broker_client.get_guild_history(ctx.guild.id)]
        else:
            queue_copy = [PlaylistItemWrite(video_url=item.webpage_url, title=item.title, uploader=item.uploader)
                          for item in (await self.broker_client.get_guild_queue(ctx.guild.id)).items]

        self.logger.info(f'Saving queue contents to playlist "{name}", is_history? {is_history}')

        if len(queue_copy) == 0:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'There are no videos to add to playlist',
                delete_after=self.config.general.message_delete_after)
            return

        # One call for the batch, then a message per outcome. The loop used to
        # hold a session open while awaiting a Discord send per item.
        outcomes = await self.playlist_store.add_items(
            playlist_id,
            queue_copy,
            self.config.playlist.server_playlist_max_size)
        for outcome in outcomes:
            if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    'Cannot add more items to playlist, already max size',
                    delete_after=self.config.general.message_delete_after)
                break
            if outcome.status == PlaylistItemAddStatus.ADDED:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Added item "{outcome.title}" to playlist',
                    delete_after=self.config.general.message_delete_after)
                continue
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to add playlist item "{outcome.title}", likely already exists',
                delete_after=self.config.general.message_delete_after)
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            f'Finished adding items to playlist "{name}"',
            delete_after=self.config.general.message_delete_after)
        return

    async def __delete_non_existing_item(self, item_id: int):
        self.logger.info(f'Unable to find playlist item {item_id} from history playlist, deleting')
        await self.playlist_store.delete_item(item_id)

    async def __playlist_queue(self, ctx: Context, player: MusicPlayer, playlist_id: int, shuffle: bool, max_num: int, is_history: bool = False):



        self.logger.info(f'Playlist queue called for playlist {playlist_id} in server "{ctx.guild.id}"')

        # Both reads happen up front and the connection is released before the
        # enqueue below, which dispatches searches and downloads. The session
        # used to stay open for that entire stretch because these rows were live.
        playlist = await self.playlist_store.get_playlist(playlist_id)
        playlist_name = playlist.name if playlist else None
        items = await self.playlist_store.list_items(playlist_id)
        if is_history:
            playlist_name = PLAYHISTORY_NAME
        playlist_items = []
        for item in items:
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

        await self.playlist_store.mark_queued(playlist_id)

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
        source_items = await self.playlist_store.list_items(playlist_two_id)
        outcomes = await self.playlist_store.add_items(
            playlist_one_id,
            [PlaylistItemWrite(video_url=item.video_url, title=item.title, uploader=item.uploader)
             for item in source_items],
            self.config.playlist.server_playlist_max_size)
        for outcome in outcomes:
            if outcome.status == PlaylistItemAddStatus.PLAYLIST_FULL:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Cannot add more items to playlist "{playlist_one_id}", already max size',
                    delete_after=self.config.general.message_delete_after)
                # Returning rather than breaking, same as before: a merge that
                # could not take every item must not then delete the source.
                return
            if outcome.status == PlaylistItemAddStatus.ADDED:
                self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                    f'Added item "{outcome.title}" to playlist {playlist_index_one}',
                    delete_after=self.config.general.message_delete_after)
                continue
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                f'Unable to add playlist item "{outcome.title}", likely already exists',
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

        # An entry, not a row: every read below happened inside the session
        # block that loaded it, which is exactly what could not survive the seam.
        guild_analytics = await self.guild_analytics_store.get_analytics(ctx.guild.id)
        hours = guild_analytics.total_duration_seconds // 3600
        minutes = (guild_analytics.total_duration_seconds % 3600) // 60
        seconds = guild_analytics.total_duration_seconds % 60
        message = f'```Music Stats for Server\nTotal Plays: {guild_analytics.total_plays}\nCached Plays: {guild_analytics.cached_plays}\n' \
                f'Total Time Played: {guild_analytics.total_duration_days} days, {hours} hours, {minutes} minutes, and {seconds} seconds\n' \
                f'Tracked Since: {guild_analytics.created_at.strftime("%Y-%m-%d %H:%M:%S")} UTC\n```'
        self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
            message, delete_after=self.config.general.message_delete_after)
