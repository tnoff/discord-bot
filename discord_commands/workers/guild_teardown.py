'''
Clears a guild's queued music work when its player goes away (#1030).

The gateway used to do this itself in ``cleanup()``, which tied it to the downloader and the search
pod. It now asks this pod (``POST /guilds/{guild_id}/teardown``, routes/guild_teardown.py), which
already holds those clients, and the route answers once the job is taken, not once it is done: it
is a round trip per queued request.

The job runs here, off the per-guild command FIFO (workers/command_executor.py). A guild's
teardown is asked for while a ``play`` may still be running, and queueing it behind that command
would let the work it is meant to cancel finish first.

Order matters. Each queue is cleared before it is blocked: the worker's clear restores preserved
items with ``put_nowait``, which raises once the queue is blocked. Playlist-add requests are
preserved, because they carry no download and their bundles keep rendering their own progress, so
the bundles holding them are not deleted below.

Repeating a teardown is harmless, and a repeat for a guild already being torn down is dropped.
'''
from asyncio import Task, create_task, wait
from logging import Logger

from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.interfaces.download_client_protocol import DownloadClient
from discord_core.interfaces.youtube_music_search_protocols import YoutubeMusicSearchClient
from discord_core.types.download import LifecycleEvent
from discord_core.types.media_request import MediaRequest
from discord_core.utils.media_enqueuer import MediaEnqueuer

SHUTDOWN_GRACE_SECONDS = 10.0


class GuildTeardown:
    '''Runs guild teardowns in the background, one at a time per guild.'''

    def __init__(self, download_client: DownloadClient, search_client: YoutubeMusicSearchClient,
                 broker_client: BrokerClient, enqueuer: MediaEnqueuer, logger: Logger,
                 grace: float = SHUTDOWN_GRACE_SECONDS):
        self.download_client = download_client
        self.search_client = search_client
        self.broker_client = broker_client
        self.enqueuer = enqueuer
        self.logger = logger
        self.grace = grace
        self._tasks: dict[int, Task] = {}

    def submit(self, guild_id: int) -> bool:
        '''
        Start tearing the guild down. Returns False, starting nothing, when one is already running
        for it: that one clears everything this one would.
        '''
        if guild_id in self._tasks:
            return False
        task = create_task(self._run(guild_id))
        self._tasks[guild_id] = task
        task.add_done_callback(lambda _: self._tasks.pop(guild_id, None))
        return True

    async def close(self) -> None:
        '''Let teardowns in flight finish for ``grace`` seconds, then cancel what is left.'''
        tasks = list(self._tasks.values())
        if not tasks:
            return
        _, pending = await wait(tasks, timeout=self.grace)
        for task in pending:
            task.cancel()
        if pending:
            await wait(pending)

    async def _run(self, guild_id: int) -> None:
        '''One teardown. It is a background task: a failure is logged, never raised to anyone.'''
        try:
            preserved_bundle_uuids = set()
            for client in (self.download_client, self.search_client):
                preserved_bundle_uuids |= await self._clear(client, guild_id)
            await self.download_client.block_guild(guild_id)
            await self.search_client.block_guild(guild_id)
            await self._delete_bundles(guild_id, preserved_bundle_uuids)
        except Exception:  # pylint: disable=broad-exception-caught
            self.logger.exception(f'Teardown of guild {guild_id} failed')

    async def _clear(self, client: DownloadClient | YoutubeMusicSearchClient, guild_id: int) -> set[str]:
        '''Drop the guild's queued requests, discard them with the broker, return the bundles kept.'''
        result = await client.clear_guild_queue(guild_id, preserve_predicate=_keep_playlist_adds)
        self.logger.debug(f'Teardown of guild {guild_id} dropped {len(result.dropped)} queued requests')
        for item in result.dropped:
            try:
                await self.enqueuer.push_state(item, LifecycleEvent.DISCARDED)
            except Exception:  # pylint: disable=broad-exception-caught
                # One request the broker no longer knows must not leave the rest undiscarded and
                # the guild unblocked.
                self.logger.exception(f'Teardown of guild {guild_id} could not discard request {item.uuid}')
        return result.preserved_bundle_uuids

    async def _delete_bundles(self, guild_id: int, preserved_bundle_uuids: set[str]) -> None:
        '''Take down the guild's progress messages, except those of requests still in flight.'''
        for bundle_uuid in await self.broker_client.list_bundles_for_guild(guild_id):
            if bundle_uuid in preserved_bundle_uuids:
                self.logger.debug(f'Skipping delete of bundle {bundle_uuid}, it has active playlist-add requests')
                continue
            await self.broker_client.delete_bundle(bundle_uuid)


def _keep_playlist_adds(request: MediaRequest) -> bool:
    '''A request with no download is a playlist add: it only needs metadata, so it is left to finish.'''
    return not request.download_file
