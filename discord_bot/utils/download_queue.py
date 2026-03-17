"""
Download queue abstractions for the music cog.

Provides a Protocol and two concrete implementations:
- InProcessDownloadQueue: thin async wrapper around DistributedQueue (non-HA mode)
- RedisDownloadQueue: Redis Streams-backed queue for HA mode
"""
import json
from asyncio import QueueEmpty
from typing import Callable, Generic, TypeVar

from discord_bot.utils.distributed_queue import DistributedQueue
from discord_bot.utils.queue import PutsBlocked

T = TypeVar('T')

REDIS_BLOCKED_SET = 'music:blocked_guilds'
REDIS_STREAM_PREFIX = 'music:download_queue'


class InProcessDownloadQueue(Generic[T]):
    '''
    Async wrapper around DistributedQueue for non-HA mode.

    All methods are trivially awaitable (no I/O yield points), which preserves
    the clear_queue → block atomicity guarantee in asyncio: neither call ever
    yields to the event loop, so no other coroutine can interleave between them.
    '''

    def __init__(self, max_size: int):
        self._queue: DistributedQueue[T] = DistributedQueue(max_size)

    async def put_nowait(self, guild_id: int, entry: T, priority: int | None = None) -> None:
        '''Put an item into the queue for the given guild (raises PutsBlocked or QueueFull).'''
        self._queue.put_nowait(guild_id, entry, priority=priority)

    async def get_nowait(self) -> T:
        '''Get the next item (raises QueueEmpty if none).'''
        return self._queue.get_nowait()

    async def block(self, guild_id: int) -> bool:
        '''Block further puts for the given guild.'''
        return self._queue.block(guild_id)

    async def clear_queue(self, guild_id: int, preserve_predicate: Callable | None = None) -> list[T]:
        '''Clear the queue for the given guild, returning removed items.'''
        return self._queue.clear_queue(guild_id, preserve_predicate=preserve_predicate)

    def size(self, guild_id: int) -> int:
        '''Return the number of items queued for the given guild.'''
        return self._queue.size(guild_id)

    @property
    def queues(self):
        '''Expose underlying DistributedQueue.queues for test inspection.'''
        return self._queue.queues


class RedisDownloadQueue:
    '''
    Redis Streams-backed download queue for HA mode.

    Each guild gets its own stream: music:download_queue:{guild_id}
    Blocked guilds are tracked in a Redis set: music:blocked_guilds
    Full MediaRequest objects are stored in _pending (keyed by uuid) so the
    bot's original objects (with their state machines) can be retrieved when
    the worker completes a download.

    get_nowait() always raises QueueEmpty — the external worker consumes
    directly via XREADGROUP and does not use this path.
    '''

    def __init__(self, redis_url: str, max_size: int, consumer_group: str):
        # Lazy import to avoid hard dep when redis not installed in non-HA mode
        import redis.asyncio as aioredis  # pylint: disable=import-outside-toplevel
        self._redis = aioredis.from_url(redis_url, decode_responses=True)
        self._max_size = max_size
        self._consumer_group = consumer_group
        # Original MediaRequest objects keyed by their uuid string
        # Only the bot's copy of the state machine drives lifecycle state;
        # the worker gets a fresh copy via deserialization.
        self._pending: dict[str, object] = {}
        # Per-guild size counters (avoids network call for size())
        self._size: dict[int, int] = {}

    def _stream_key(self, guild_id: int) -> str:
        return f'{REDIS_STREAM_PREFIX}:{guild_id}'

    async def put_nowait(self, guild_id: int, entry, priority: int | None = None) -> None:  # pylint: disable=unused-argument
        '''
        Enqueue a MediaRequest for the given guild.

        Raises PutsBlocked if the guild is in the blocked set.
        Raises QueueFull if the per-guild counter is at max_size.
        Priority is accepted for interface compatibility but not used by Redis streams.
        '''
        # Check blocked set
        if await self._redis.sismember(REDIS_BLOCKED_SET, str(guild_id)):
            raise PutsBlocked(f'Puts blocked for guild {guild_id}')

        current = self._size.get(guild_id, 0)
        if current >= self._max_size:
            from asyncio import QueueFull  # pylint: disable=import-outside-toplevel
            raise QueueFull(f'Download queue full for guild {guild_id}')

        # Store original object so we can return it after the worker finishes
        self._pending[str(entry.uuid)] = entry

        # Serialize to JSON for Redis (state_machine is a PrivateAttr, excluded automatically)
        payload = entry.serialize()
        await self._redis.xadd(self._stream_key(guild_id), {'payload': payload, 'guild_id': str(guild_id)})

        self._size[guild_id] = current + 1

    async def get_nowait(self):
        '''
        Not used in HA mode — the worker consumes via XREADGROUP.
        Always raises QueueEmpty.
        '''
        raise QueueEmpty('RedisDownloadQueue: get_nowait not used in HA mode')

    async def block(self, guild_id: int) -> bool:
        '''Mark the guild as blocked (idempotent SADD).'''
        await self._redis.sadd(REDIS_BLOCKED_SET, str(guild_id))
        return True

    async def clear_queue(self, guild_id: int, preserve_predicate: Callable | None = None) -> list:
        '''
        Block the guild, then drain all unprocessed stream messages.

        Returns the list of original MediaRequest objects that were removed
        (so the caller can mark them as discarded).  If preserve_predicate is
        given, items for which it returns True are kept in the stream and
        excluded from the returned list.
        '''
        # Block first so no new items arrive while we drain
        await self._redis.sadd(REDIS_BLOCKED_SET, str(guild_id))

        stream_key = self._stream_key(guild_id)
        # Read all pending (unacknowledged by worker) messages
        messages = await self._redis.xrange(stream_key)

        dropped = []
        kept_ids = []

        for msg_id, data in messages:
            raw = data.get('payload', '{}')
            try:
                item_data = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                # Corrupt message — delete it
                await self._redis.xdel(stream_key, msg_id)
                continue

            uuid_str = item_data.get('uuid')
            original = self._pending.pop(uuid_str, None) if uuid_str else None

            if original is not None and preserve_predicate is not None and preserve_predicate(original):
                kept_ids.append(msg_id)
                # Re-add to pending since we're keeping it
                self._pending[uuid_str] = original
                continue

            # Delete message from stream
            await self._redis.xdel(stream_key, msg_id)
            if original is not None:
                dropped.append(original)

        # Update size counter
        kept_count = len(kept_ids)
        self._size[guild_id] = kept_count

        return dropped

    def size(self, guild_id: int) -> int:
        '''Return cached queue size for the guild (no network call).'''
        return self._size.get(guild_id, 0)

    def pop_pending(self, uuid_str: str):
        '''
        Remove and return the original MediaRequest for the given UUID.
        Returns None if the request was already discarded (guild cleanup).
        '''
        return self._pending.pop(uuid_str, None)

    async def ensure_consumer_groups(self, guild_ids: list[int]) -> None:
        '''
        Create consumer groups for known guild streams (idempotent).
        Call on startup if desired; not strictly required.
        '''
        for guild_id in guild_ids:
            stream_key = self._stream_key(guild_id)
            try:
                await self._redis.xgroup_create(stream_key, self._consumer_group, id='0', mkstream=True)
            except Exception:  # pylint: disable=broad-except
                # Group already exists — ignore
                pass

    async def close(self) -> None:
        '''Close the Redis connection.'''
        await self._redis.aclose()
