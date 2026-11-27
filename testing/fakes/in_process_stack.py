'''
The in-process broker / download / search stacks a cog test wires up.

Split out of testing/helpers.py: these are the helpers that need the broker, downloader, search
and db pods' test doubles, so only the tests that attach those stacks should have to load them.
'''
from functools import partial
from typing import Any, Optional

from unittest.mock import patch

from sqlalchemy.ext.asyncio import AsyncEngine

from discord_core.utils.failure_queue import FailureQueue

from discord_broker.workers.guild_queue_registry import GPLAYING_KEY_PREFIX
from discord_db.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_search.workers.youtube_music_search_driver import YoutubeMusicSearchDriver

from testing.db_helpers import async_mock_session
from testing.fakes.asyncio_broker import AsyncioBroker
from testing.fakes.asyncio_broker_client import AsyncioBrokerClient
from testing.fakes.asyncio_download_client import AsyncioDownloadClient
from testing.fakes.asyncio_download_worker import AsyncioDownloadWorker
from testing.fakes.asyncio_queues import make_guild_queue_for
from testing.fakes.asyncio_youtube_music_search_client import AsyncioYoutubeMusicSearchClient
from testing.fakes.asyncio_youtube_music_search_worker import AsyncioYoutubeMusicSearchWorker
from testing.fakes.pod_music_config import pod_music_config
from testing.fakes.stub_youtube_music_client import StubYoutubeMusicClient


def attach_in_process_broker(cog: Any, video_cache: Optional[Any] = None,
                            db_engine: Optional[AsyncEngine] = None) -> AsyncioBroker:
    '''
    Rebuild the in-process broker stack the cog no longer builds itself.

    The cog is an HTTP client only since the broker dual path was collapsed — the
    registry, the bundle state, the video cache and the S3 checkout all live in
    the broker pod. AsyncioBroker, VideoCacheClient and AsyncioBrokerClient
    survive as test doubles (projects/discord-bot-ha-only), and this wires them
    exactly as the cog used to, so tests keep driving real broker behaviour
    through `cog.broker_client` instead of standing up a broker pod behind an
    aiohttp server.

    The client also gets a real GuildQueueBroker over the same engine (on fakeredis), so the
    cog's queue calls run the broker's actual queue code; see testing/fakes/asyncio_broker_client.

    Returns the engine, which is what tests reach for when they need to assert on
    registry state directly (the cog's old `cog.media_broker`).

    Call this BEFORE attach_in_process_download / attach_in_process_search: both
    read `cog.broker_client` to wire their worker, so attaching them first leaves
    them holding the HttpBrokerClient this replaces.

    video_cache overrides the client built from cog config — pass one to exercise
    the cache path without an enable_cache_files config, or a fake to assert on
    calls.

    db_engine is what the cache gets built over. It has to be passed in now: the
    cog stopped carrying a database handle when persistence moved behind HTTP,
    and the video cache was never the bot's to begin with — it is broker state,
    and this function is standing up the broker. Without an engine the config's
    cache settings are inert, which is exactly what a bot process sees.
    '''
    config = pod_music_config(cog)
    bucket_name = config.download.storage.bucket_name if config.download.storage else None
    if video_cache is None and config.download.cache.enable_cache_files and db_engine and bucket_name:
        max_mb = config.download.cache.max_cache_size_mb
        video_cache = VideoCacheClient(
            config.download.cache.max_cache_files,
            partial(async_mock_session, db_engine),
            max_cache_size_bytes=(max_mb * 1024 * 1024 if max_mb else None),
            storage_type='s3',
        )
    broker = AsyncioBroker(
        video_cache=video_cache,
        bucket_name=bucket_name,
        dispatcher=cog.dispatcher,
        download_max_retries=config.download.max_download_retries,
        search_max_retries=config.download.max_youtube_music_search_retries,
        message_delete_after=config.general.message_delete_after,
    )
    # Only broker_client is set on the cog: it is real production state, just a
    # different implementation. The engine and the cache are NOT re-attached as
    # cog.media_broker / cog.video_cache — the cog has no such attributes any
    # more, and re-adding them would let a test assert against a shape production
    # cannot have. Tests that need the engine use the returned handle.
    cog.broker_client = AsyncioBrokerClient(broker, guild_queue=make_guild_queue_for(broker))
    return broker


async def queue_in_broker(cog: Any, *media_downloads: Any, max_size: int = 10) -> None:
    '''
    Put downloaded tracks in the broker's queue for their guild, in order, the way
    add_source_to_player does: registered with the broker first, then enqueued.

    Needs a cog wired with attach_in_process_broker.
    '''
    for media_download in media_downloads:
        await cog.broker_client.register_download(media_download)
        result = await cog.broker_client.enqueue_track(
            media_download.media_request.guild_id, str(media_download.media_request.uuid), max_size)
        assert result == 'ok', f'queueing {media_download.title} returned {result}'


async def play_through_in_broker(cog: Any, media_download: Any, max_size: int = 10) -> None:
    '''
    Play a track to the end in the broker, so it lands in its guild's history.

    Queues it, claims it as the cog's gateway, and finishes it unskipped.
    '''
    await queue_in_broker(cog, media_download, max_size=max_size)
    guild_id = media_download.media_request.guild_id
    claimed = await cog.broker_client.claim_next_track(guild_id, cog.gateway_id)
    assert str(claimed.download.media_request.uuid) == str(media_download.media_request.uuid)
    await cog.broker_client.finish_track(guild_id, str(media_download.media_request.uuid), False, max_size)


def attach_in_process_download(cog: Any, worker_cls: Optional[type] = None) -> AsyncioDownloadClient:
    '''
    Rebuild the in-process download stack the cog no longer builds itself.

    The cog is an HTTP client only since the download dual path was collapsed —
    the consumer loop runs in the downloader pod. AsyncioDownloadWorker and
    AsyncioDownloadClient survive as test doubles (projects/discord-bot-ha-only),
    and this wires them exactly as the cog used to, so tests keep driving the real
    worker with `await cog.download_client.run(...)` instead of standing up a
    downloader pod.

    worker_cls swaps in a fake worker subclass — what the tests used to get by
    patching discord_gateway.cogs.music.AsyncioDownloadWorker, which no longer exists
    to patch.
    '''
    config = pod_music_config(cog)
    bucket_name = config.download.storage.bucket_name if config.download.storage else None
    worker = (worker_cls or AsyncioDownloadWorker)(
        cog.logging_config,
        cog.download_dir,
        queue_max_size=config.player.queue_max_size,
        extra_ytdlp_options=config.download.extra_ytdlp_options,
        max_video_length=config.download.max_video_length,
        banned_video_list=config.download.banned_videos_list,
        failure_queue=FailureQueue(
            max_size=config.download.failure_tracking_max_size,
            max_age_seconds=config.download.failure_tracking_max_age_seconds,
        ),
        wait_period_minimum=config.download.youtube_wait_period_minimum,
        wait_period_max_variance=config.download.youtube_wait_period_max_variance,
        bucket_name=bucket_name,
        normalize_audio=config.download.normalize_audio,
        broker=cog.broker_client,
        max_retries=config.download.max_download_retries,
        retry_backoff_seconds_minimum=config.download.retry_backoff_seconds_minimum,
    )
    cog.download_client = AsyncioDownloadClient(worker)
    return cog.download_client


def attach_in_process_search(cog: Any, client: Optional[Any] = None) -> YoutubeMusicSearchDriver:
    '''
    Rebuild the in-process search stack the cog no longer builds itself.

    The cog is an HTTP client only since the dual-path collapse — the search loop
    runs in the standalone search pod. These in-memory implementations survive as
    test doubles (projects/discord-bot-ha-only): driving the driver against them
    keeps its behaviour under test without standing up an aiohttp server, which is
    what the alternative would cost.

    Swaps the cog's HttpYoutubeMusicSearchClient for an InMemory one over a real
    AsyncioYoutubeMusicSearchWorker, wired exactly as the cog used to wire it, and
    returns the driver that used to be cog.youtube_music_search_driver. Callers
    drive one iteration with `await driver.run_once(cog.bot_shutdown_event)`.
    '''
    config = pod_music_config(cog)
    worker = AsyncioYoutubeMusicSearchWorker(
        cog.logging_config,
        # Defaults to a STUB, not a live client. This line read
        # `client or youtube_music.YoutubeMusicClient()`, which gave all 65
        # callers a real YouTube Music client -- see
        # testing/fakes/stub_youtube_music_client.py for what that cost.
        client or StubYoutubeMusicClient(),
        FailureQueue(
            max_size=config.download.failure_tracking_max_size,
            max_age_seconds=config.download.failure_tracking_max_age_seconds,
        ),
        config.download.youtube_wait_period_minimum,
        config.download.youtube_wait_period_max_variance,
        queue_max_size=config.player.queue_max_size * 2,
    )
    cog.youtube_music_search_client = AsyncioYoutubeMusicSearchClient(worker)
    return YoutubeMusicSearchDriver(
        cog.youtube_music_search_client,
        cog.broker_client,
        cog.logger,
        max_retries=config.download.max_youtube_music_search_retries,
        queue_priority={int(item.server_id): item.priority
                        for item in config.download.server_queue_priority},
    )


# What the in-process stacks above are built from lives in the downloader and the broker, so the tests that need
# to reach into it do it from here and not by naming those pods' modules themselves.
_DOWNLOAD_PROTOCOLS = 'discord_downloader.interfaces.download_protocols'
_BROKER_PROTOCOLS = 'discord_broker.interfaces.broker_protocols'


def patch_download_waits(mocker) -> None:
    '''Make the in-process download worker's backoff instant: its jitter is fixed and its sleeps are skipped.'''
    mocker.patch(f'{_DOWNLOAD_PROTOCOLS}.random.randint', return_value=5000)
    mocker.patch(f'{_DOWNLOAD_PROTOCOLS}.sleep', return_value=None)


def patch_download_ytdl():
    '''Patch the yt-dlp class the in-process download worker constructs; a context manager yielding the mock.'''
    return patch(f'{_DOWNLOAD_PROTOCOLS}.YoutubeDL')


def patch_in_process_upload(mocker, return_value=True):
    '''Patch the S3 upload the in-process download worker calls; returns the mock.'''
    return mocker.patch(f'{_DOWNLOAD_PROTOCOLS}.upload_file', return_value=return_value)


def patch_in_process_delete(mocker, return_value=True):
    '''Patch the S3 delete the in-process broker calls; returns the mock.'''
    return mocker.patch(f'{_BROKER_PROTOCOLS}.delete_file_async', return_value=return_value)


__all__ = ['GPLAYING_KEY_PREFIX', 'attach_in_process_broker', 'attach_in_process_download',
           'attach_in_process_search', 'patch_download_waits', 'patch_download_ytdl', 'patch_in_process_delete',
           'patch_in_process_upload', 'play_through_in_broker', 'queue_in_broker']
