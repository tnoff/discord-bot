'''
The guild-queue half of the cog-facing broker surface, on its own.

Split from BrokerClient the way PlayerSessionClient is: it is player state the broker pod
happens to host, not part of the media-broker contract, and a caller that only needs one half
should be able to annotate for just that half.  BrokerClient includes it, so the cog still
depends on one Protocol.

Rejections and misses are return values, not exceptions (see routes/broker.py): a full queue or
a track that already left is an ordinary answer the caller turns into a user message.
'''
from typing import Protocol

from discord_core.types.guild_queue import ClaimedDownload, GuildQueueSnapshot
from discord_core.types.media_download import MediaDownload

__all__ = ['GuildQueueClient']


class GuildQueueClient(Protocol):
    '''Cog-facing handle for a guild's player queue, served by the broker pod.'''
    async def enqueue_track(self, guild_id: int, uuid: str, max_size: int = 0) -> str:
        '''Queue an AVAILABLE entry for the guild's player.  Returns ok, closed, full or
        duplicate.  max_size 0 means unbounded.'''
    async def get_guild_queue(self, guild_id: int) -> GuildQueueSnapshot:
        '''The guild's queue in play order, its playing track and its pending skip.'''
    async def remove_queued_track(self, guild_id: int, uuid: str) -> MediaDownload | None:
        '''Take a track out of the queue and drop its entry.  None if it was not queued.'''
    async def bump_queued_track(self, guild_id: int, uuid: str) -> MediaDownload | None:
        '''Move a queued track to the head of the queue.  None if it was not queued.'''
    async def shuffle_queue(self, guild_id: int) -> bool:
        '''Shuffle the guild's queue.'''
    async def clear_queue(self, guild_id: int) -> int:
        '''Empty the guild's queue, dropping every entry in it.  Returns how many.'''
    async def poll_guild_queue(self, guild_id: int, since: int | None = None) -> tuple[int, str | None] | None:
        '''Cheap change check: (queue version, uuid a skip is pending for), or None when the
        version still equals since and no skip is pending.  Non-blocking.'''
    async def claim_next_track(self, guild_id: int, gateway_id: str) -> ClaimedDownload | None:
        '''Take the guild's next track and mark it as playing, or None if the queue is empty.
        A claim that was never confirmed (a gateway died mid-start) is handed out again.'''
    async def playing_heartbeat(self, guild_id: int, uuid: str) -> bool:
        '''Keep the now-playing record alive.  False if that track is no longer playing.'''
    async def skip_track(self, guild_id: int, uuid: str) -> str:
        '''Request a skip of the playing track, if it is still uuid.  Returns ok, no_player or
        not_current.  The gateway acts on it; nothing here stops audio.'''
    async def finish_track(self, guild_id: int, uuid: str, skipped: bool, history_cap: int) -> None:
        '''The gateway is done with a track.  Clears its markers, records it in history unless
        skipped, and releases its entry.'''
    async def get_guild_history(self, guild_id: int) -> list[dict]:
        '''Tracks that played to the end, oldest first.'''
    async def close_guild(self, guild_id: int) -> int:
        '''Shut the guild's player state down, refusing enqueues, and release everything it held.
        Returns how many entries were released.'''
    async def open_guild(self, guild_id: int) -> None:
        '''Reopen a closed guild so a new player can enqueue.'''
