'''
Cog-facing download client.

InMemoryDownloadClient is a thin wrapper around a DownloadWorkerBase engine
(AsyncioDownloadWorker in single-process) — it forwards the cog-facing
DownloadClient Protocol surface to the worker, mirroring how InMemoryBrokerClient
wraps a MediaBrokerBase.  HttpDownloadClient forwards the same surface to a
remote downloader pod for HA, over the shared HttpQueueWorkerClient base.

The download exceptions, yt-dlp helpers, and the DownloadWorkerBase engine live
in interfaces/download_protocols.py; they are re-exported here so existing
`from discord_bot.clients.download_client import ...` call sites keep working.
'''
import asyncio
import logging
from typing import ClassVar

from discord_bot.clients.http_queue_worker_client import HttpQueueWorkerClient
from discord_bot.clients.in_memory_queue_worker_client import InMemoryQueueWorkerClient
from discord_bot.interfaces.download_protocols import (
    DownloadClient,
    DownloadWorkerBase,
    DirectItemAvailableException,
    DownloadClientException,
    DownloadTerminalException,
    RetryableException,
    RetryLimitExceeded,
    InvalidFormatException,
    VideoNotFoundException,
    MetadataCheckFailedException,
    VideoAgeRestrictedException,
    VideoUnavailableException,
    VideoViolatedTermsException,
    PrivateVideoException,
    VideoTooLong,
    VideoBanned,
    BotDownloadFlagged,
    match_generator,
    OTEL_SPAN_PREFIX,
    YTDLP_OUTPUT_TEMPLATE,
    YTDLP_SOURCE_ADDRESS,
)

logger = logging.getLogger(__name__)

__all__ = [
    'DownloadClient',
    'DownloadWorkerBase',
    'InMemoryDownloadClient',
    'HttpDownloadClient',
    'DirectItemAvailableException',
    'DownloadClientException',
    'DownloadTerminalException',
    'RetryableException',
    'RetryLimitExceeded',
    'InvalidFormatException',
    'VideoNotFoundException',
    'MetadataCheckFailedException',
    'VideoAgeRestrictedException',
    'VideoUnavailableException',
    'VideoViolatedTermsException',
    'PrivateVideoException',
    'VideoTooLong',
    'VideoBanned',
    'BotDownloadFlagged',
    'match_generator',
    'OTEL_SPAN_PREFIX',
    'YTDLP_OUTPUT_TEMPLATE',
    'YTDLP_SOURCE_ADDRESS',
]


class InMemoryDownloadClient(InMemoryQueueWorkerClient):
    '''
    Single-process DownloadClient: a thin wrapper around a DownloadWorkerBase.

    The shared forwarding surface (submit / block_guild / clear_guild_queue /
    queue_size / failure_summary / backoff_seconds_remaining) comes
    from InMemoryQueueWorkerClient; the worker owns the yt-dlp pipeline and input
    queues.  Only run() — the download consumer loop, which has no search-side
    equivalent — is specific to this client.
    '''

    @property
    def local_worker(self) -> DownloadWorkerBase:
        '''The wrapped engine — only meaningful in single-process mode.'''
        return self._worker

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''Consume one queued request and download it; driven as a background loop.'''
        await self._worker.run(shutdown_event)


class HttpDownloadClient(HttpQueueWorkerClient):
    '''
    DownloadClient that forwards the cog-facing surface to a remote downloader pod.

    The whole surface — producer calls (submit / block_guild / clear_guild_queue)
    against the downloader's DownloadHttpServer, plus the cached read surface
    (failure_summary / backoff_seconds_remaining / queue_size) a background poller
    refreshes from GET /downloads/status — lives on HttpQueueWorkerClient, which the
    search pod's client shares.  Only the route and span prefixes differ.

    There is no `run` or `local_worker`: the download consumer loop runs in the
    downloader pod (owned by its CLI entrypoint), not on the bot side.  start() /
    stop() drive the status poller, mirroring the local client's run() lifecycle.
    '''

    ROUTE_PREFIX: ClassVar[str] = '/downloads'
    SPAN_PREFIX: ClassVar[str] = 'downloader'
