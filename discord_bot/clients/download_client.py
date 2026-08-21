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

from discord_bot.clients.http_download_client import HttpDownloadClient
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
