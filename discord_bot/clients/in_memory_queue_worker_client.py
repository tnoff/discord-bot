'''
Shared single-process client for the queue workers.

The in-process counterpart to http_queue_worker_client.py: InMemoryDownloadClient
and InMemoryYoutubeMusicSearchClient both wrap a worker engine and forward the
same submit / block / clear / queue_size / failure_summary / backoff surface to
it, so that forwarding lives here once and each subclass adds only the half that
is specific to its engine (the download consumer loop; the search pop/resolve
calls).  Two copies would trip pylint's duplicate-code check.

Both wrap the caller's preserve predicate identically so a clear reports the
preserved bundle_uuids whether the predicate ran here or on a worker pod — see
ClearGuildResult.
'''
from typing import Callable

from discord_bot.types.clear_guild_result import ClearGuildResult
from discord_bot.types.media_request import MediaRequest


class InMemoryQueueWorkerClient:
    '''Forwards the cog-facing queue-client surface to a wrapped worker engine.'''

    def __init__(self, worker):
        self._worker = worker

    # local_worker — the single-process escape hatch onto the wrapped engine — is
    # declared by each subclass instead of here, so it carries that subclass's
    # engine type (DownloadWorkerBase / YoutubeMusicSearchWorkerBase).

    async def submit(self, guild_id: int, media_request: MediaRequest,
                     priority: int | None = None) -> None:
        '''Enqueue a MediaRequest on the worker's input queue.'''
        await self._worker.submit(guild_id, media_request, priority=priority)

    async def block_guild(self, guild_id: int) -> bool:
        '''Block new submissions for a guild (used during shutdown/cleanup).'''
        return await self._worker.block_guild(guild_id)

    async def clear_guild_queue(self, guild_id: int,
                                preserve_predicate: Callable[[MediaRequest], bool] | None = None,
                                ) -> ClearGuildResult:
        '''Clear the input queue for a guild.

        Wraps the predicate to record the bundle_uuids of preserved items so the
        result carries them uniformly with the HTTP client (whose worker pod runs
        the predicate remotely).
        '''
        preserved_bundle_uuids: set[str] = set()
        if preserve_predicate is not None:
            def wrapped(media_request: MediaRequest) -> bool:
                keep = preserve_predicate(media_request)
                if keep and media_request.bundle_uuid:
                    preserved_bundle_uuids.add(media_request.bundle_uuid)
                return keep
        else:
            wrapped = None
        dropped = await self._worker.clear_guild_queue(guild_id, preserve_predicate=wrapped)
        return ClearGuildResult(dropped=dropped, preserved_bundle_uuids=preserved_bundle_uuids)

    async def queue_size(self, guild_id: int) -> int:
        '''Return the number of pending requests for a guild, or 0 if none.'''
        return await self._worker.queue_size(guild_id)

    @property
    def failure_summary(self) -> str:
        '''Human-readable summary of the worker's failure queue.'''
        return self._worker.failure_summary

    @property
    def backoff_seconds_remaining(self) -> int | None:
        '''Seconds remaining in the current backoff window, or None if not set.'''
        return self._worker.backoff_seconds_remaining
