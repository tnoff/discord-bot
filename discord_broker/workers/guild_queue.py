'''
The player queue, as the broker serves it: queue state joined to the entries behind it.

RedisBroker owns media lifecycle (IN_FLIGHT -> AVAILABLE -> CHECKED_OUT); GuildQueueRegistry owns
the ordered per-guild lists.  This class is the join: a queued track is an AVAILABLE entry whose
uuid is on the guild's list, and the playing track is the entry CHECKED_OUT by the guild, named
by the now-playing record.  Keeping it a separate class keeps both of those small, and means the
player queue can change without touching how media moves through the broker.
'''
import logging

from discord_core.types.checkout_result import CheckoutResult

from discord_broker.interfaces.broker_protocols import (
    BrokerEntry, ClaimedTrack, GuildQueue, PlayingTrack, Zone,
)
from discord_broker.workers.guild_queue_registry import GuildQueueRegistry
# Not public API, but it is the one definition of how a download is stored, and a history item
# has to match it so the history worker can rebuild a MediaDownload from either.
from discord_broker.workers.redis_broker import RedisBroker, _download_to_dict

logger = logging.getLogger(__name__)


class GuildQueueBroker:
    '''
    Per-guild player queue operations over a RedisBroker's entries.

    Results that can fail for ordinary reasons (queue full, track already gone) are returned, not
    raised: callers turn them into user-facing messages.
    '''

    def __init__(self, broker: RedisBroker, queues: GuildQueueRegistry):
        self._broker = broker
        self._queues = queues

    async def enqueue(self, guild_id: int, media_request_uuid: str, max_size: int = 0) -> str:
        '''
        Queue an AVAILABLE entry for the guild's player.

        Returns one of the registry's ENQUEUE_* results: ok, closed, full or duplicate.  The
        entry must already be registered with register_download.
        '''
        return await self._queues.queue_enqueue(guild_id, media_request_uuid, max_size)

    async def get_queue(self, guild_id: int) -> GuildQueue:
        '''The guild's queue, now-playing track and pending skip, with entries loaded.'''
        state = await self._queues.queue_state(guild_id)
        uuids = list(state.queue)
        playing_uuid = state.playing['uuid'] if state.playing else None
        if playing_uuid:
            uuids.append(playing_uuid)
        by_uuid = dict(zip(uuids, await self._broker.get_entries(uuids)))

        items = []
        for uuid in state.queue:
            entry = by_uuid[uuid]
            if entry is None:
                logger.warning('Guild %s queue holds %s but its entry is gone', guild_id, uuid)
                continue
            items.append(entry)
        playing = None
        if state.playing:
            playing = PlayingTrack(
                uuid=playing_uuid,
                started_at=float(state.playing['started_at']),
                gateway_id=state.playing['gateway_id'],
                entry=by_uuid[playing_uuid],
            )
        return GuildQueue(version=state.version, items=items, playing=playing,
                          skip_for=state.skip_for, closed=state.closed)

    async def poll(self, guild_id: int) -> tuple[int, str | None]:
        '''Cheap change check for the gateway: (queue version, uuid a skip is pending for).'''
        return await self._queues.queue_poll(guild_id)

    async def remove(self, guild_id: int, media_request_uuid: str) -> BrokerEntry | None:
        '''
        Take a track out of the queue and drop its entry.

        Returns the entry that was removed, or None if it was not queued (it started playing, or
        was removed, between the caller looking at the queue and getting here).
        '''
        entry = await self._broker.get_entry(media_request_uuid)
        if not await self._queues.queue_remove(guild_id, media_request_uuid):
            return None
        await self._broker.remove(media_request_uuid)
        return entry

    async def bump(self, guild_id: int, media_request_uuid: str) -> BrokerEntry | None:
        '''Move a queued track to the head of the queue. None if it was not queued.'''
        entry = await self._broker.get_entry(media_request_uuid)
        if not await self._queues.queue_bump(guild_id, media_request_uuid):
            return None
        return entry

    async def shuffle(self, guild_id: int) -> bool:
        '''Shuffle the guild's queue.'''
        return await self._queues.queue_shuffle(guild_id)

    async def clear(self, guild_id: int) -> int:
        '''Empty the guild's queue, dropping every entry that was in it. Returns how many.'''
        uuids = await self._queues.queue_clear(guild_id)
        for uuid in uuids:
            await self._broker.remove(uuid)
        return len(uuids)

    async def claim_next(self, guild_id: int, gateway_id: str) -> ClaimedTrack | None:
        '''
        Take the guild's next track and mark it as playing, or None if the queue is empty.

        Pop, checkout and confirm are separate steps, so a gateway that dies partway through
        is picked up by the next call: the registry hands the same uuid back, and an entry
        already checked out by this guild is accepted rather than refused.  Entries that have
        vanished or cannot be checked out are dropped and the next one tried.
        '''
        while True:
            uuid = await self._queues.queue_claim_next(guild_id)
            if uuid is None:
                return None
            entry = await self._broker.get_entry(uuid)
            checkout = None
            if entry is not None:
                if entry.zone is Zone.CHECKED_OUT and entry.checked_out_by == guild_id:
                    checkout = self._checkout_result(entry)
                else:
                    checkout = await self._broker.checkout(uuid, guild_id)
            if checkout is None:
                logger.warning('Dropping unplayable queued track %s in guild %s', uuid, guild_id)
                await self._queues.drop_claim(guild_id)
                if entry is not None:
                    await self._broker.release(uuid)
                continue
            if not await self._queues.confirm_claim(guild_id, uuid, gateway_id):
                # The guild was closed (or the claim dropped) while we were checking out.
                await self._broker.release(uuid)
                return None
            return ClaimedTrack(entry=await self._broker.get_entry(uuid), checkout=checkout)

    def _checkout_result(self, entry: BrokerEntry) -> CheckoutResult | None:
        '''CheckoutResult for an entry already checked out, None when it has no file to hand over.'''
        if entry.download is None or not entry.download.file_path:
            return None
        return CheckoutResult(s3_key=str(entry.download.file_path), bucket_name=self._broker.bucket_name)

    async def heartbeat(self, guild_id: int, media_request_uuid: str) -> bool:
        '''Keep the guild's now-playing record alive. False if that track is no longer playing.'''
        return await self._queues.playing_heartbeat(guild_id, media_request_uuid)

    async def skip(self, guild_id: int, media_request_uuid: str) -> str:
        '''
        Request a skip of the guild's playing track, if it is still media_request_uuid.

        Returns one of the registry's SKIP_* results.  The gateway acts on it; nothing here
        stops audio.
        '''
        return await self._queues.request_skip(guild_id, media_request_uuid)

    async def finish(self, guild_id: int, media_request_uuid: str, skipped: bool,
                     history_cap: int) -> None:
        '''
        The gateway is done with a track: play ended or it was skipped.

        Clears the now-playing and skip markers, adds the track to the guild's history unless
        it was skipped, then releases its entry.  History is written first because the release
        deletes the entry it is built from.
        '''
        history_item = None
        if not skipped:
            entry = await self._broker.get_entry(media_request_uuid)
            if entry is not None:
                history_item = {
                    'uuid': media_request_uuid,
                    'guild_id': guild_id,
                    'requester_name': entry.request.requester_name,
                    'requester_id': entry.request.requester_id,
                    'added_from_history': entry.request.added_from_history,
                    **(_download_to_dict(entry.download) if entry.download else {}),
                }
        await self._queues.finish_track(guild_id, media_request_uuid, skipped, history_item, history_cap)
        await self._broker.release(media_request_uuid)

    async def get_history(self, guild_id: int) -> list[dict]:
        '''Tracks that played to the end in this guild, oldest first.'''
        return await self._queues.history_items(guild_id)

    async def close(self, guild_id: int) -> int:
        '''
        Shut the guild's player state down and release everything it held.

        Further enqueues are refused until open.  Returns how many entries were released.
        '''
        queued, claimed, playing = await self._queues.queue_close(guild_id)
        uuids = [uuid for uuid in (playing, claimed, *queued) if uuid]
        for uuid in uuids:
            await self._broker.release(uuid)
        return len(uuids)

    async def open(self, guild_id: int) -> None:
        '''Reopen a closed guild so a new player can enqueue.'''
        await self._queues.queue_open(guild_id)
