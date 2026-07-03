'''
Cog-facing DownloadClient Protocol.

The download client is the cog's handle onto the yt-dlp download pipeline.

  InMemoryDownloadClient (clients/download_client.py) — runs the pipeline
    in-process for single-process deployments.

  A future HttpDownloadClient will forward calls to a remote downloader pod
    for HA deployments where the downloader runs separately.

Both shapes satisfy this Protocol; the cog only depends on the Protocol and
lets config decide which impl is constructed — mirroring the BrokerClient
seam in interfaces/broker_protocols.py.
'''
import asyncio
from typing import Callable, Protocol

from discord_bot.types.media_request import MediaRequest


class DownloadClient(Protocol):
    '''
    Cog-facing handle for the download pipeline.

    The producer surface (submit / block_guild / clear_guild_queue /
    queue_size) is synchronous; the cog drives the consumer via run() as a
    background loop.  failure_summary / backoff_seconds_remaining expose the
    worker's backoff state for logging and metrics.
    '''
    def submit(self, guild_id: int, media_request: MediaRequest,
               priority: int | None = None) -> None:
        '''Enqueue a MediaRequest for download; results are reported to the broker.'''

    def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''

    def clear_guild_queue(self, guild_id: int,
                          preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                          ) -> list[MediaRequest]:
        '''Clear the input queue for a guild, returning the dropped requests.'''

    def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the download failure queue.'''

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff period, or None if not set.'''

    async def run(self, shutdown_event: asyncio.Event) -> None:
        '''Consume one queued request and download it; driven as a background loop.'''
