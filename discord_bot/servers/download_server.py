'''
HTTP server for the download worker pod.

Fronts a RedisDownloadWorker so bot pods (via HttpDownloadClient) can submit /
clear / block downloads and poll queue/backoff status over HTTP.  The worker pod
also drives its own consumer loop in-process to drain the Redis queue.

All four handlers, the AiohttpServerBase serve()/drain lifecycle, and the
heartbeat gauge come from QueueWorkerHttpServer — the YouTube-Music search pod
fronts the same shape, so this module only supplies the routes/labels that differ.
'''
import logging
from typing import ClassVar

from discord_bot.servers.queue_worker_server import QueueWorkerHttpServer
from discord_bot.workers.redis_download_worker import RedisDownloadWorker

logger = logging.getLogger(__name__)


class DownloadHttpServer(QueueWorkerHttpServer):
    '''
    aiohttp HTTP server fronting a RedisDownloadWorker.

    Routes:
        POST /downloads          submit
        POST /downloads/clear    clear_guild_queue (preserve_playlist_adds flag)
        POST /downloads/block    block_guild
        GET  /downloads/status   queue_size + failure_summary + backoff snapshot
    '''

    ROUTE_PREFIX: ClassVar[str] = '/downloads'
    SPAN_PREFIX: ClassVar[str] = 'downloader'
    # RENAMED from 'downloader' (this MR).  This gauge reports is_serving — the
    # TCP site is up — not that the download loop is turning, and the bare name
    # read like the latter: heartbeat{background_job="downloader"} sat at 1 on a
    # pod whose consumer loop was wedged, right up until the liveness probe
    # restarted it.  The pod now publishes its consumer loop's LoopHealth under
    # the loop's own name (cli/_lib/worker_pod.py), so the two would otherwise
    # collide on this label.  Matches the search pod's existing
    # 'youtube_music_search_server' convention.
    HEARTBEAT_JOB: ClassVar[str] = 'downloader_server'
    HEARTBEAT_DESCRIPTION: ClassVar[str] = 'Download HTTP server heartbeat'
    DEFAULT_PORT: ClassVar[int] = 8083

    def __init__(self, worker: RedisDownloadWorker, host: str = '0.0.0.0',  # nosec B104
                 port: int | None = None):
        '''Typed constructor — the base accepts any worker with the queue surface.'''
        super().__init__(worker, host=host, port=port)
