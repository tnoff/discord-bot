# Music bot setup
# Music taken from https://gist.github.com/EvieePy/ab667b74e9758433b3eb806c53a19f34

import asyncio
from asyncio import sleep
from functools import partial
from pathlib import Path
from shutil import disk_usage
from tempfile import TemporaryDirectory
from time import time
from typing import Optional
from uuid import uuid4

from discord.ext.commands import Bot, Context, group, command
from discord.errors import ClientException
from opentelemetry.trace import SpanKind
from opentelemetry.metrics import Observation
from pydantic import BaseModel, Field, model_validator

from discord_core.clients.http_client_base import start_seam_checks
from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException, ExitEarlyException
from discord_core.types.media_download import MediaDownload
from discord_core.utils.common import (is_peer_unavailable, rm_tree,
                                      return_loop_runner,
                                      seam_contract_from_settings,
                                      tracing_config_from_settings)
from discord_core.utils.discord_context import DiscordContextNaming
from discord_core.utils.loop_health import LOOP_HEALTH
from discord_core.utils.otel import async_otel_span_wrapper, AttributeNaming, MetricNaming, METER_PROVIDER, create_observable_gauge, loop_heartbeat_observations
from discord_core.interfaces.database_protocols import PlaylistStore
from discord_core.types.video_cache import MusicCacheConfig
from discord_core.clients.http_broker_client import HttpBrokerClient
from discord_core.interfaces.download_client_protocol import (
    DownloadClient, RETRY_BACKOFF_SECONDS_MINIMUM,
)
from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.commands_client import CommandsUnavailable
from discord_core.types.player_session import PlayerSession
from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.utils.media_enqueuer import MediaEnqueuer
from discord_core.clients.search_client import SearchClient
from discord_core.clients.http_download_client import HttpDownloadClient
from discord_core.clients.http_media_search_client import HttpMediaSearchClient
from discord_core.clients.http_youtube_music_search_client import (
    HttpYoutubeMusicSearchClient, YoutubeMusicSearchClient,
)

from discord_gateway.cogs.common import CogHelperBase, build_commands_client
from discord_gateway.loop_logic.gateway_intents import GatewayIntentsLoop
from discord_gateway.cogs.music_helpers.music_player import MusicPlayer
from discord_gateway.cogs.music_helpers.player_gateway import CogPlayerGateway
from discord_gateway.types.cleanup_reason import CleanupReason
from discord_gateway.utils.bot_metrics import BotMetricNaming
from discord_gateway.utils.otel_command import command_wrapper
from discord_gateway.utils.peer_probe import wait_for_peers

# GLOBALS


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
# Idle backoff for the process_search_results loop when its queue is empty. Sleeping ONLY on
# the empty path (not every iteration) keeps busy work back-to-back while cutting idle
# allocation churn (OOM fix).
_IDLE_POLL_BACKOFF_SECONDS = 0.25
# How often the gateway asks the broker for work other pods want done (join, stop, wake). Short, because
# a join waits on it and a wake is only as prompt as this; an empty answer is a cheap 204.
_INTENT_POLL_INTERVAL_SECONDS = 0.25
# One retry, after a short wait: a pod mid-restart is back in a second or two, and the voice
# disconnect and the player's cleanup are waiting behind this.
_TEARDOWN_ATTEMPTS = 2
_TEARDOWN_RETRY_DELAY_SECONDS = 1.0
# How long a restarted gateway waits for the previous one's now-playing record to lapse before
# taking over a guild anyway. The broker expires the record 15s after the last heartbeat.
FOREIGN_PLAYER_WAIT_SECONDS = 20.0
# How long the startup resume keeps asking a broker that is away for the stored player sessions
# before giving up on them. The broker client already rides out a refused connection for a minute
# and the startup peer wait holds for two more, so this only matters when the broker is down
# longer than both: a node roll where its pod is still pulling an image (3.5 minutes on
# 2026-10-28, answering about 7 minutes after the gateway came up). Bounded so a broker that is
# gone for good ends in an error instead of a task that waits forever.
RESUME_BROKER_WAIT_SECONDS = 900.0
RESUME_BROKER_POLL_SECONDS = 10.0

# Background-loop names. Used as both the LoopHealth registry key and the
# heartbeat gauge's background_job attribute, so the metric series and the health
# server's `loops` payload name the same loop the same way.
LOOP_CLEANUP_PLAYERS = 'cleanup_players'
LOOP_PROCESS_GATEWAY_INTENTS = 'process_gateway_intents'

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

        # The playlist commands that only touch the store run in the commands pod; see forward_command.
        self.commands_client = build_commands_client(settings)

        self.players = {}
        self._cleanup_task = None
        self._intent_task = None
        self._init_task = None
        self._peers_ready_task = None
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
        if self.stores:
            self.playlist_store = self.stores.playlist

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
        # Unlike the other logic classes this one holds state (a task per guild), so it is built once.
        self.intents_logic = GatewayIntentsLoop(
            self.logger, self.broker_client, CogPlayerGateway(self), self.gateway_id)

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
                (LOOP_PROCESS_GATEWAY_INTENTS, 'Gateway intent processing loop heartbeat'),
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
        # Every loop below calls the broker, downloader or search pod. A gateway that
        # comes up first (a whole-namespace rollout) would otherwise open with a burst
        # of connection errors on each of them, so they all wait on this one probe
        # task; it gives up after a bounded wait and lets them start regardless.
        self._peers_ready_task = self.bot.loop.create_task(wait_for_peers({
            'broker': self.config.broker_client.url,
            'download': self.config.download_client.url,
            'search': self.config.youtube_music_search_client.url,
        }, self.logger))
        self._cleanup_task = self.bot.loop.create_task(
            self._after_peers_ready(return_loop_runner(
                self.cleanup_players, self.bot, self.logger,
                health=LOOP_HEALTH.register(LOOP_CLEANUP_PLAYERS),
                peer_unavailable=is_peer_unavailable))
        )
        # One-shot, not a loop: sessions are consumed once at startup.  It waits
        # on the gateway itself rather than going through return_loop_runner,
        # which would re-run it forever.
        self._init_task = self.bot.loop.create_task(
            self._after_peers_ready(self.resume_player_sessions))
        # The download loop lives in the downloader pod. The cog starts only the
        # client's status poller. The bot registers no download loop and emits no
        # download_files heartbeat; the downloader pod publishes that series.
        await self.download_client.start(self.bot, self.bot_shutdown_event)
        self._intent_task = self.bot.loop.create_task(
            self._after_peers_ready(return_loop_runner(
                self.process_gateway_intents, self.bot, self.logger,
                health=LOOP_HEALTH.register(LOOP_PROCESS_GATEWAY_INTENTS),
                peer_unavailable=is_peer_unavailable))
        )
        # The search loop lives in the search pod, so the cog starts only the
        # client's status poller. The bot registers no youtube_music_search loop
        # and emits no heartbeat for one — the search pod publishes that series
        # instead (cli/search.py's LOOP_SEARCH_WORKER, the same loop name).
        await self.youtube_music_search_client.start(self.bot, self.bot_shutdown_event)
        # No embedded BrokerHttpServer: the broker pod serves that surface. Recording plays
        # (analytics and the history playlist) is the broker's history worker now.

    async def _after_peers_ready(self, func):
        '''Await func() once the startup peer wait has finished (reachable or timed out).

        Takes the function, not a coroutine, so a task cancelled during the wait never
        leaves a created-but-unawaited coroutine behind.'''
        await self._peers_ready_task
        return await func()

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
            LOOP_HEALTH.mark_stopped(LOOP_CLEANUP_PLAYERS, LOOP_PROCESS_GATEWAY_INTENTS)
            if self._peers_ready_task:
                self._peers_ready_task.cancel()
            if self._init_task:
                self._init_task.cancel()
            if self._cleanup_task:
                self._cleanup_task.cancel()
            # No bot-side download loops to cancel — stop the status poller and
            # close the HTTP session instead.
            await self.download_client.stop()
            # No bot-side search loop to cancel — just the poller + session.
            await self.youtube_music_search_client.stop()
            if self._intent_task:
                self._intent_task.cancel()
            await self.intents_logic.close()

            self.logger.info('Cog unload: Removing directories')
            # Remove contents of download dir by default
            if self.download_dir and self.download_dir.exists():
                rm_tree(self.download_dir)
            if self.config.player.player_dir_path is None and self.player_dir.exists():
                rm_tree(self.player_dir)

            return True


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

        The broker half is enqueue_logic.enqueue_download; what stays here is waking the player
        once the broker has the track.

        media_download : Standard MediaDownload for post download
        player : MusicPlayer
        '''
        async def wake_player():
            player.notify_enqueued()
            player.trigger_prefetch()
        return await self.enqueue_logic.enqueue_download(media_download, wake_player)

    async def process_gateway_intents(self):
        '''
        Intent consumer: claims what other pods asked this gateway to do and hands it to
        intents_logic. One poll covers every guild.
        '''
        if self.bot_shutdown_event.is_set():
            raise ExitEarlyException('Bot shutdown called, exiting early')

        if await self.intents_logic.poll() == 0:
            await sleep(_INTENT_POLL_INTERVAL_SECONDS)

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

    async def _save_player_session(self, guild, player: MusicPlayer) -> bool:
        '''
        Remember where this guild's player is, so a restart can rejoin it.

        Only the voice and text channels are kept: the queue itself lives in the broker and
        survives the restart on its own. Saved when the player joins voice (and on shutdown), so
        a crash leaves a session behind too.

        No session is written when the bot is not in a voice channel: there is nothing to
        rejoin, and a session with no channel would only be discarded on the way back up.
        Returns whether one was written.
        '''
        voice_client = guild.voice_client
        channel = getattr(voice_client, 'channel', None)
        if channel is None:
            self.logger.debug(f'No voice channel for guild {guild.id}, skipping session save')
            return False
        session = PlayerSession(
            guild_id=guild.id,
            voice_channel_id=channel.id,
            text_channel_id=player.text_channel.id,
            was_playing=player.current_media_download is not None,
        )
        self.logger.debug(f'Saving player session for guild {guild.id}, was_playing={session.was_playing}')
        await self.broker_client.save_player_session(session)
        return True

    async def resume_player_sessions(self) -> None:
        '''
        Rejoin and resume any guild that was playing when the bot went down.

        One-shot, run after the gateway is ready so guild/channel lookups resolve.
        '''
        await self.bot.wait_until_ready()
        sessions = await self._list_player_sessions_when_broker_up()
        self.logger.info(f'Found {len(sessions)} player session(s) to consider resuming')
        for session in sessions:
            try:
                await self._resume_player_session(session)
            except Exception as e:  #pylint:disable=broad-except
                # One guild's bad session must not stop the others from resuming.
                self.logger.exception(f'Error resuming player session for guild {session.guild_id}: {e}')

    async def _list_player_sessions_when_broker_up(self) -> list[PlayerSession]:
        '''
        Fetch the stored player sessions, waiting out a broker that is away.

        Resume runs once at startup, so a broker that is unreachable at that moment would
        otherwise end the task for the life of the pod: nothing is resumed, and the sessions
        sit in the broker until the next restart. Only an unreachable peer is retried (see
        is_peer_unavailable); any other error is a real fault and propagates at once. Gives up,
        loudly, after RESUME_BROKER_WAIT_SECONDS.
        '''
        deadline = time() + RESUME_BROKER_WAIT_SECONDS
        attempts = 0
        while True:
            attempts += 1
            try:
                return await self.broker_client.list_player_sessions()
            except Exception as error:
                if is_peer_unavailable(error) and time() < deadline:
                    self.logger.warning(f'Broker unavailable (attempt {attempts}), will retry resuming player '
                                        f'sessions: {type(error).__name__}: {error}')
                    await sleep(RESUME_BROKER_POLL_SECONDS)
                    continue
                if is_peer_unavailable(error):
                    self.logger.error(f'Broker still unavailable after {RESUME_BROKER_WAIT_SECONDS:.0f}s, '
                                      'not resuming player sessions; they stay stored for the next start')
                raise

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

        The stored session stays put until the attempt has an outcome, so a gateway that is
        stopped part-way through (a rolling restart, a node going away) leaves it for the next
        start instead of taking the guild's way back with it. An outcome is: resumed, with
        nothing to resume, or failed. The last two drop it: it describes a moment that has
        already passed, and a resume that fails must not be retried against even staler state.
        A resume that works leaves the fresh session the voice join wrote.

        A cancelled resume is deliberately not an outcome. CancelledError is a BaseException,
        so it passes the handler below, which is why this is not a try/finally.
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.resume_player_session', kind=SpanKind.INTERNAL,
                                           attributes={DiscordContextNaming.GUILD.value: session.guild_id}):
            try:
                resumed = await self._rejoin_from_session(session)
            except Exception:
                await self.broker_client.delete_player_session(session.guild_id)
                raise
            if not resumed:
                await self.broker_client.delete_player_session(session.guild_id)

    async def _rejoin_from_session(self, session: PlayerSession) -> bool:
        '''
        Rebuild the guild's player from a stored session. True when the guild is playing again
        with a session saved for the next restart, False when there was nothing to resume.
        '''
        guild = self.bot.get_guild(session.guild_id)
        if guild is None:
            self.logger.warning(f'Guild {session.guild_id} not found, cannot resume session')
            await self._abandon_guild_state(session.guild_id)
            return False
        voice_channel = guild.get_channel(session.voice_channel_id)
        text_channel = guild.get_channel(session.text_channel_id)
        if voice_channel is None or text_channel is None:
            self.logger.warning(f'Voice or text channel gone for guild {session.guild_id}, cannot resume session')
            await self._abandon_guild_state(session.guild_id)
            return False
        # Don't play to an empty room.  Everyone leaving while the bot was
        # down is the clearest signal nobody is waiting on the queue.
        if not [member for member in voice_channel.members if not member.bot]:
            self.logger.info(f'No listeners left in voice channel {voice_channel.id} for guild '
                             f'{session.guild_id}, not resuming')
            await self._abandon_guild_state(session.guild_id)
            return False

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
            return False

        self.logger.info(f'Resuming playback in guild {session.guild_id} with {waiting} item(s) queued')
        player = await self.get_player(session.guild_id, join_channel=voice_channel,
                                       guild=guild, text_channel=text_channel)
        if player is None:
            self.logger.warning(f'Could not build player for guild {session.guild_id}, cannot resume session')
            await self._abandon_guild_state(session.guild_id)
            return False
        self.dispatcher.send_message(session.guild_id, text_channel.id,
            f'Resumed after a restart, {waiting} item(s) still queued',
            delete_after=self.config.general.message_delete_after)
        # The join above saved a session. Saving again is what makes "the stored session is
        # current, or gone" true even when no join happened here; False means we are not in
        # voice, so what is stored is stale and the caller drops it.
        return await self._save_player_session(guild, player)

    async def cleanup(self, guild, reason: CleanupReason = CleanupReason.QUEUE_TIMEOUT):
        '''
        Cleanup guild player

        guild  : Guild object
        reason : CleanupReason describing why cleanup was triggered
        '''
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.cleanup', kind=SpanKind.CONSUMER, attributes={DiscordContextNaming.GUILD.value: guild.id}):
            self.logger.info(f'Starting cleanup on guild {guild.id}, reason: {reason.value}')
            player = await self.get_player(guild.id, create_player=False)
            if reason == CleanupReason.BOT_SHUTDOWN and player:
                # Capture the session before anything below tears state down: the
                # voice disconnect drops the channel we need to rejoin, and stopping the
                # loop clears the track it was playing (was_playing). The queue needs no
                # capturing; it stays in the broker.
                await self._save_player_session(guild, player)
            if player:
                # Before the voice disconnect below: that stops playback, which looks to the
                # player like its track finishing. Not by marking the player shut down: the
                # cleanup_players loop would see that and start a second cleanup, with the wrong
                # reason, on top of this one.
                await player.stop_loop()
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

            # The guild's queued downloads and searches are the commands pod's to clear and block,
            # and it deletes the guild's bundles with them; see _request_guild_teardown.
            #
            # None of this runs on BOT_SHUTDOWN: the queues are parked instead of
            # drained. Clearing a queue drops the request from Redis, but its broker
            # registry entry only leaves the in_flight zone when the follow-up
            # DISCARDED push deletes it — and that loop is one round-trip per
            # item, so a SIGKILL part-way through (the pod runs a short grace period)
            # strands every remaining entry, plus the bundle holding them, until the
            # 24h TTL. Parking costs nothing: the downloader and search tiers outlive
            # this pod, work the backlog, and every request reaches a terminal state
            # that reaps its own entry, while the media lands in the shared cache so
            # a re-request after the restart is a cache hit. The bundles stay for the
            # same reason: each keeps rendering real progress and releases itself once
            # its requests go terminal.
            if reason != CleanupReason.BOT_SHUTDOWN:
                await self._request_guild_teardown(guild.id)

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

            if reason != CleanupReason.BOT_SHUTDOWN:
                self.logger.debug(f'Deleting player dir for guild {guild.id}')
                guild_player_path = self.player_dir / f'{guild.id}'
                if guild_player_path.exists():
                    rm_tree(guild_player_path)

    async def _request_guild_teardown(self, guild_id: int) -> None:
        '''
        Ask the commands pod to clear the guild's queued downloads and searches, block new ones for
        a moment, and take down the guild's bundles.

        Tried twice. If the pod cannot be reached the guild is left as it is and cleanup goes on:
        its queued work runs until the result loop finds no player and discards it, its bundles
        last until their 24h TTL, and nothing is blocked. That is better than holding up the
        voice disconnect and the player's own cleanup behind a pod that is down.
        '''
        for attempt in range(1, _TEARDOWN_ATTEMPTS + 1):
            try:
                await self.commands_client.teardown_guild(guild_id)
                return
            except CommandsUnavailable as error:
                self.logger.warning(f'Teardown request for guild {guild_id} failed '
                                    f'(attempt {attempt}/{_TEARDOWN_ATTEMPTS}): {error}')
                if attempt < _TEARDOWN_ATTEMPTS:
                    await sleep(_TEARDOWN_RETRY_DELAY_SECONDS)
        self.logger.warning(f'Commands pod unreachable, queued work for guild {guild_id} is not cleared')

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

    @command(name='join', aliases=['awaken'])
    @command_wrapper
    async def connect_(self, ctx: Context):
        '''
        Connect to voice channel.
        '''
        await self.forward_command(ctx, 'join')

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
        await self.forward_command(ctx, 'play', search=search)


    @command(name='skip')
    @command_wrapper
    async def skip_(self, ctx):
        '''
        Skip the video.
        '''
        await self.forward_command(ctx, 'skip')

    @command(name='clear')
    @command_wrapper
    async def clear(self, ctx):
        '''
        Clear all items from queue
        '''
        await self.forward_command(ctx, 'clear')

    @command(name='history')
    @command_wrapper
    async def history_(self, ctx: Context):
        '''
        Show recently played videos
        '''
        await self.forward_command(ctx, 'history')

    @command(name='shuffle')
    @command_wrapper
    async def shuffle_(self, ctx):
        '''
        Shuffle video queue.
        '''
        await self.forward_command(ctx, 'shuffle')

    @command(name='remove')
    @command_wrapper
    async def remove_item(self, ctx, queue_index):
        '''
        Remove item from queue.

        queue_index: integer [Required]
            Position in queue of video that will be removed.
        '''
        await self.forward_command(ctx, 'remove', queue_index=queue_index)

    @command(name='bump')
    @command_wrapper
    async def bump_item(self, ctx, queue_index):
        '''
        Bump item to top of queue

        queue_index: integer [Required]
            Position in queue of video that will be removed.
        '''
        await self.forward_command(ctx, 'bump', queue_index=queue_index)

    @command(name='stop')
    @command_wrapper
    async def stop_(self, ctx):
        '''
        Stop the currently playing video and disconnect bot from voice chat.
        '''
        await self.forward_command(ctx, 'stop')

    @command(name='move-messages')
    @command_wrapper
    async def move_messages_here(self, ctx):
        '''
        Move queue messages to this text chanel
        '''
        await self.forward_command(ctx, 'move-messages')

    @property
    def enqueue_logic(self) -> MediaEnqueuer:
        '''
        Search, cache-check and routing of media requests, built from this cog's current clients.
        Stateless and rebuilt per use, like the other logic properties. The result loops use it too.
        '''
        return MediaEnqueuer(
            self.broker_client, self.search_client, self.youtube_music_search_client,
            self.download_client, self.playlist_store, CogPlayerGateway(self), self.dispatcher,
            self.logger, self.config.player.queue_max_size, self.config.playlist.server_playlist_max_size,
            self.config.general.message_delete_after, self.server_queue_priority)

    @group(name='playlist', invoke_without_command=False)
    async def playlist(self, ctx):
        '''
        Playlist functions. Use '!help playlist'
        '''
        if ctx.invoked_subcommand is None:
            self.dispatcher.send_message(ctx.guild.id, ctx.channel.id,
                'Invalid sub command passed...',
                delete_after=self.config.general.message_delete_after)

    @playlist.command(name='create')
    @command_wrapper
    async def playlist_create(self, ctx: Context, *, name: str):
        '''
        Create new playlist.

        name: str [Required]
            Name of new playlist to create
        '''
        await self.forward_command(ctx, 'playlist.create', name=name)

    @playlist.command(name='list')
    @command_wrapper
    async def playlist_list(self, ctx: Context):
        '''
        List playlists.
        '''
        await self.forward_command(ctx, 'playlist.list')

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
        await self.forward_command(ctx, 'playlist.item-add', playlist_index=playlist_index, search=search)


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
        await self.forward_command(ctx, 'playlist.item-remove', playlist_index=playlist_index, video_index=video_index)

    @playlist.command(name='show')
    @command_wrapper
    async def playlist_show(self, ctx: Context, playlist_index: int):
        '''
        Show Items in playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        await self.forward_command(ctx, 'playlist.show', playlist_index=playlist_index)

    @playlist.command(name='delete')
    @command_wrapper
    async def playlist_delete(self, ctx: Context, playlist_index: int):
        '''
        Delete playlist

        playlist_index: integer [Required]
            ID of playlist
        '''
        await self.forward_command(ctx, 'playlist.delete', playlist_index=playlist_index)

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
        await self.forward_command(ctx, 'playlist.rename', playlist_index=playlist_index, playlist_name=playlist_name)

    @playlist.command(name='save-queue')
    @command_wrapper
    async def playlist_queue_save(self, ctx: Context, *, name: str):
        '''
        Save contents of queue to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        await self.forward_command(ctx, 'playlist.save-queue', name=name)

    @playlist.command(name='save-history')
    @command_wrapper
    async def playlist_history_save(self, ctx: Context, *, name: str):
        '''
        Save contents of history to a new playlist

        name: str [Required]
            Name of new playlist to create
        '''
        await self.forward_command(ctx, 'playlist.save-history', name=name)

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
        await self.forward_command(ctx, 'playlist.queue', playlist_index=playlist_index, args=[str(arg) for arg in args])


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
        await self.forward_command(ctx, 'playlist.merge', playlist_index_one=playlist_index_one, playlist_index_two=playlist_index_two)

    @command(name='random-play')
    @command_wrapper
    async def playlist_random_play(self, ctx: Context):
        '''
        Play 32 random items from history playlist with shuffle enabled

        Equivalent to: !playlist queue 0 shuffle 32
        '''
        await self.forward_command(ctx, 'random-play')


    @command(name='music-stats')
    @command_wrapper
    async def music_stats(self, ctx: Context):
        '''
        Show music player stats
        '''
        await self.forward_command(ctx, 'music-stats')
