'''
Single-process DownloadClient: a thin wrapper around a DownloadWorkerBase.

Retired from production by the HA rollout and kept only as a test double.
The pod-facing implementation is HttpDownloadClient
(clients/http_download_client.py), which forwards the same surface to the
downloader pod. The shared forwarding half lives in InMemoryQueueWorkerClient;
only run() -- the download consumer loop, which has no search-side equivalent
-- is specific to this client.
'''
import asyncio

from discord_bot.services.downloader.interfaces.download_protocols import DownloadWorkerBase

from tests.fakes.in_memory_queue_worker_client import InMemoryQueueWorkerClient


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
