'''
Cog-facing download client.

InMemoryDownloadClient is a thin wrapper around a DownloadWorkerBase engine
(AsyncioDownloadWorker in single-process) — it forwards the cog-facing
DownloadClient Protocol surface to the worker, mirroring how InMemoryBrokerClient
wraps a MediaBrokerBase.  A future HttpDownloadClient will forward the same
surface to a remote downloader pod for HA.

The download exceptions, yt-dlp helpers, and the DownloadWorkerBase engine live
in interfaces/download_protocols.py; they are re-exported here so existing
`from discord_bot.clients.download_client import ...` call sites keep working.
'''
import asyncio
from typing import Callable

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
from discord_bot.types.media_request import MediaRequest

__all__ = [
    'DownloadClient',
    'DownloadWorkerBase',
    'InMemoryDownloadClient',
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


class InMemoryDownloadClient:
    '''
    Single-process DownloadClient: a thin wrapper around a DownloadWorkerBase.

    Forwards the cog-facing Protocol surface (submit / block_guild /
    clear_guild_queue / queue_size / failure_summary / backoff_seconds_remaining
    / run) straight to the worker, which owns the yt-dlp pipeline and input
    queues.  local_worker exposes the wrapped engine for single-process callers
    that need it (there is no equivalent on a future HttpDownloadClient).
    '''
    def __init__(self, worker: DownloadWorkerBase):
        self._worker = worker

    @property
    def local_worker(self) -> DownloadWorkerBase:
        '''The wrapped engine — only meaningful in single-process mode.'''
        return self._worker

    def submit(self, guild_id: int, media_request: MediaRequest,
               priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download; results are reported to the broker.'''
        self._worker.submit(guild_id, media_request, priority=priority)

    def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''
        return self._worker.block_guild(guild_id)

    def clear_guild_queue(self, guild_id: int,
                          preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                          ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''
        return self._worker.clear_guild_queue(guild_id, preserve_predicate=preserve_predicate)

    def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''
        return self._worker.queue_size(guild_id)

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the download failure queue.'''
        return self._worker.failure_summary

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff period, or None if not set.'''
        return self._worker.backoff_seconds_remaining

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''Consume one queued request and download it; driven as a background loop.'''
        await self._worker.run(shutdown_event)
