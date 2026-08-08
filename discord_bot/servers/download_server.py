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
    HEARTBEAT_JOB: ClassVar[str] = 'downloader'
    HEARTBEAT_DESCRIPTION: ClassVar[str] = 'Download HTTP server heartbeat'
    DEFAULT_PORT: ClassVar[int] = 8083

    def __init__(self, worker: RedisDownloadWorker, host: str = '0.0.0.0',  # nosec B104
                 port: int | None = None):
        '''Typed constructor — the base accepts any worker with the queue surface.'''
        super().__init__(worker, host=host, port=port)
