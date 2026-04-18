import asyncio
from asyncio import QueueEmpty
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import IntEnum
from functools import cached_property, partial
import itertools
import uuid
from typing import Any, Callable, List

from discord import Message
from discord.errors import NotFound
from discord.ext.commands import Bot

from opentelemetry import trace
from opentelemetry.metrics import Observation
from opentelemetry.trace.status import StatusCode

from discord_bot.cogs.common import CogHelperBase
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.types.fetched_message import FetchedMessage
from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
)
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.utils.discord_retry import async_retry_discord_message_command
from discord_bot.utils.dispatch_queue import RedisDispatchQueue
from discord_bot.clients.redis_client import (
    delete_bundle as redis_delete_bundle,
    get_redis_client,
    load_all_bundles,
    load_bundle as redis_load_bundle,
    save_bundle as redis_save_bundle,
)
from discord_bot.utils.otel import async_otel_span_wrapper, otel_span_wrapper, AttributeNaming, DispatchNaming, METER_PROVIDER, MetricNaming, create_observable_gauge, span_links_from_context


_DRAIN_TIMEOUT_SECONDS = 30

# Redis queue member prefixes — used when building and routing work-queue members.
_MEMBER_MUTABLE = 'mutable:'
_MEMBER_REMOVE = 'remove:'
_MEMBER_SEND = 'send:'
_MEMBER_DELETE = 'delete:'
_MEMBER_UPDATE_CHANNEL = 'update_channel:'
_MEMBER_FETCH_HISTORY = 'fetch_history:'
_MEMBER_FETCH_EMOJIS = 'fetch_emojis:'


class DispatchPriority(IntEnum):
    '''Queue priority levels: lower value = higher priority.'''
    HIGH = 0    # mutable bundle sentinel
    NORMAL = 1  # one-off sends
    LOW = 2     # background reads (channel history, fetch_message)


# ---------------------------------------------------------------------------
# Mutable message tracking
# ---------------------------------------------------------------------------

@dataclass
class MessageContext:
    '''Track metadata for a single Discord message managed by a mutable bundle.'''
    guild_id: int
    channel_id: int
    message_id: int | None = None
    message_content: str | None = None
    delete_after: int | None = None

    def set_message(self, message: Message):
        '''Set the message ID after a message has been sent.'''
        self.message_id = message.id if message else None

    async def delete_message(self, get_channel: Callable) -> bool:
        '''Delete the message via a PartialMessage looked up from get_channel.'''
        if self.message_id is None:
            return False
        channel = get_channel(self.channel_id)
        if channel is None:
            return False
        try:
            await channel.get_partial_message(self.message_id).delete()
        except NotFound:
            return True
        return True

    async def edit_message(self, get_channel: Callable, **kwargs) -> bool:
        '''Edit the message via a PartialMessage looked up from get_channel.'''
        if self.message_id is None:
            return False
        channel = get_channel(self.channel_id)
        if channel is None:
            return False
        await channel.get_partial_message(self.message_id).edit(**kwargs)
        return True


class MessageMutableBundle:
    '''Collection of multiple mutable Discord messages.'''

    def __init__(self, guild_id: int, channel_id: int, sticky_messages: bool = True):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.sticky_messages = sticky_messages
        self.message_contexts: List[MessageContext] = []

    async def should_clear_messages(self, check_last_message_func: Callable) -> bool:
        '''Check if messages should be cleared (sticky check).'''
        if not self.message_contexts:
            return False
        if not self.sticky_messages:
            return False
        history_messages = await async_retry_discord_message_command(
            partial(check_last_message_func, len(self.message_contexts))
        )
        for count, hist_message in enumerate(history_messages):
            index = len(self.message_contexts) - 1 - count
            if index < 0:
                break
            context = self.message_contexts[index]
            if context.message_id is None or context.message_id != hist_message.id:
                return True
        return False

    def _match_existing_message_content(self, message_content: List[str], delete_after: int | None) -> dict:
        '''Find matching existing message contexts for new content.'''
        mapping = {}
        for (new_index, message) in enumerate(message_content):
            for (existing_index, context) in enumerate(self.message_contexts):
                if context.message_content == message and delete_after == context.delete_after:
                    mapping[existing_index] = new_index
                    break
        return mapping

    def get_message_dispatch(self, message_content: List[str], send_function: Callable,
                             get_channel: Callable,
                             clear_existing: bool = False,
                             delete_after: int = None) -> List[Callable]:
        '''Return list of callables to sync Discord messages with new content.'''
        dispatch_functions = []

        if clear_existing and self.message_contexts:
            for context in self.message_contexts:
                if context.message_id is not None:
                    dispatch_functions.append(partial(context.delete_message, get_channel))
            self.message_contexts = []

        if not self.message_contexts:
            for content in message_content:
                mc = MessageContext(self.guild_id, self.channel_id, message_content=content, delete_after=delete_after)
                send_func = partial(send_function, content=content, delete_after=delete_after)
                self.message_contexts.append(mc)
                dispatch_functions.append(send_func)
            return dispatch_functions

        existing_count = len(self.message_contexts)
        new_count = len(message_content)

        if existing_count > new_count:
            expected_delete_count = existing_count - new_count
            existing_mapping = self._match_existing_message_content(message_content, delete_after)
            delete_count = 0
            new_contexts = []
            for index, item in reversed(list(enumerate(self.message_contexts))):
                if existing_mapping.get(index, None) is not None:
                    new_contexts.insert(0, item)
                    continue
                if delete_count < expected_delete_count:
                    dispatch_functions.append(partial(item.delete_message, get_channel))
                    delete_count += 1
                    continue
                edit_func = partial(item.edit_message, get_channel, content=message_content[index], delete_after=delete_after)
                item.delete_after = delete_after
                item.message_content = message_content[index]
                dispatch_functions.append(edit_func)
                new_contexts.insert(0, item)
            self.message_contexts = new_contexts
            return dispatch_functions

        existing_mapping = self._match_existing_message_content(message_content, delete_after)
        new_contexts = []
        for index, item in enumerate(self.message_contexts):
            if existing_mapping.get(index, None) == index:
                new_contexts.append(item)
                continue
            edit_func = partial(item.edit_message, get_channel, content=message_content[index], delete_after=delete_after)
            item.delete_after = delete_after
            item.message_content = message_content[index]
            dispatch_functions.append(edit_func)
            new_contexts.append(item)
        self.message_contexts = new_contexts

        if new_count > existing_count:
            for i in range(existing_count, new_count):
                content = message_content[i]
                mc = MessageContext(self.guild_id, self.channel_id, message_content=content, delete_after=delete_after)
                send_func = partial(send_function, content=content, delete_after=delete_after)
                self.message_contexts.append(mc)
                dispatch_functions.append(send_func)

        return dispatch_functions

    def clear_all_messages(self, get_channel: Callable) -> List[Callable]:
        '''Return callables to delete all managed messages and clear contexts.'''
        delete_functions = []
        for context in self.message_contexts:
            if context.message_id is not None:
                delete_functions.append(partial(context.delete_message, get_channel))
        self.message_contexts = []
        return delete_functions

    def update_text_channel(self, new_guild_id: int, new_channel_id: int, get_channel: Callable) -> List[Callable]:
        '''Move this bundle to a different channel; returns delete funcs for old messages.'''
        dispatch_functions = list(self.clear_all_messages(get_channel))
        self.guild_id = new_guild_id
        self.channel_id = new_channel_id
        return dispatch_functions

    def to_dict(self) -> dict:
        '''Serialize this bundle to a JSON-safe dict for Redis persistence.'''
        return {
            'guild_id': self.guild_id,
            'channel_id': self.channel_id,
            'sticky_messages': self.sticky_messages,
            'message_contexts': [asdict(ctx) for ctx in self.message_contexts],
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'MessageMutableBundle':
        '''Restore a bundle from the dict produced by :meth:`to_dict`.'''
        bundle = cls(data['guild_id'], data['channel_id'], data.get('sticky_messages', True))
        bundle.message_contexts = [MessageContext(**ctx) for ctx in data.get('message_contexts', [])]
        return bundle


# ---------------------------------------------------------------------------
# Internal work-item types.
# Items are stored in the PriorityQueue wrapped as (priority, seq, item)
# tuples so that Python can compare them without ever touching the payload
# (which may contain non-comparable objects like Callables or Futures).
# seq is a per-dispatcher monotonic counter that keeps equal-priority items
# FIFO.
# ---------------------------------------------------------------------------

@dataclass
class _MutableSentinel:
    '''High-priority sentinel that triggers a mutable bundle flush.'''
    priority: int = field(default=DispatchPriority.HIGH, init=False)
    seq: int = field(default=0)
    key: str = ''


@dataclass
class _DeleteItem:
    '''Work item for a single message deletion.'''
    priority: int = field(default=DispatchPriority.NORMAL, init=False)
    seq: int = field(default=0)
    channel_id: int = 0
    message_id: int = 0
    span_context: dict | None = None


@dataclass
class _SendItem:
    '''Work item for a single message send.'''
    priority: int = field(default=DispatchPriority.NORMAL, init=False)
    seq: int = field(default=0)
    channel_id: int = 0
    content: str = ''
    delete_after: int | None = None
    allow_404: bool = False
    span_context: dict | None = None


@dataclass
class _ReadItem:
    '''Work item for a generic low-priority callable fetch.'''
    priority: int = field(default=DispatchPriority.LOW, init=False)
    seq: int = field(default=0)
    func: Callable = field(default=None)
    future: asyncio.Future = field(default=None)
    max_retries: int = 3
    allow_404: bool = False


@dataclass
class _HistoryReadItem:
    '''Work item for a channel history fetch submitted via the cog input queue.'''
    priority: int = field(default=DispatchPriority.LOW, init=False)
    seq: int = field(default=0)
    cog_name: str = ''
    guild_id: int = 0
    channel_id: int = 0
    limit: int = 100
    after: Any = None
    after_message_id: int | None = None
    oldest_first: bool = True
    dedup_key: tuple = field(default_factory=tuple)
    span_context: dict | None = None


@dataclass
class _EmojiReadItem:
    '''Work item for a guild emoji fetch submitted via the cog input queue.'''
    priority: int = field(default=DispatchPriority.LOW, init=False)
    seq: int = field(default=0)
    cog_name: str = ''
    guild_id: int = 0
    max_retries: int = 3
    dedup_key: tuple = field(default_factory=tuple)
    span_context: dict | None = None


@dataclass
class _MutablePending:
    '''Pending content for a mutable bundle update.'''
    content: List[str]
    guild_id: int
    channel_id: int | None
    sticky: bool
    delete_after: int | None


class MessageDispatcher(CogHelperBase):
    '''
    App-wide Discord message dispatcher.

    Owns one asyncio.PriorityQueue per guild and one worker task per active guild.
    Work items are processed at HIGH > NORMAL > LOW priority.

    HIGH   (_MutableSentinel)  – flush a mutable bundle update
    NORMAL (_SendItem)         – one-off channel.send calls
    NORMAL (_DeleteItem)       – message deletions
    LOW    (_ReadItem)         – generic background reads (fetch_object)
    LOW    (_HistoryReadItem)  – channel history fetches from cog input queue
    LOW    (_EmojiReadItem)    – guild emoji fetches from cog input queue

    Cogs submit typed requests via submit_request(), which feeds _cog_consumer.
    Identical history/emoji requests (same cog + channel/guild) are deduplicated.
    Results are delivered to per-cog result queues as typed result objects.
    '''

    def __init__(self, bot: Bot, settings: dict, db_engine=None):
        super().__init__(bot, settings, db_engine)
        if not settings.get('general', {}).get('include', {}).get('message_dispatcher', True):
            raise CogMissingRequiredArg('MessageDispatcher not enabled')

        self._guilds: dict[int, asyncio.PriorityQueue] = {}
        self._workers: dict[int, asyncio.Task] = {}

        # key -> MessageMutableBundle (created lazily on first update_mutable)
        self._bundles: dict[str, MessageMutableBundle] = {}
        # key -> latest pending content (de-duplicates rapid-fire calls)
        self._pending_mutable: dict[str, _MutablePending] = {}
        # keys that already have a sentinel in their guild queue
        self._sentinel_in_queue: set[str] = set()

        self._shutdown: asyncio.Event = asyncio.Event()
        self._seq = itertools.count()

        # Cog input queue: all cogs submit typed requests here
        self._cog_input: asyncio.Queue = asyncio.Queue()
        # Per-cog result queues: cog_name -> Queue
        self._cog_result_queues: dict[str, asyncio.Queue] = {}
        # Dedup tracking: (cog_name, guild_id, channel_id) or (cog_name, guild_id)
        self._pending_history: set[tuple] = set()
        self._pending_emojis: set[tuple] = set()
        self._cog_consumer_task: asyncio.Task | None = None
        self._redis_worker_tasks: list[asyncio.Task] = []

        create_observable_gauge(METER_PROVIDER, MetricNaming.DISPATCHER_QUEUE_DEPTH.value,
                                self.__queue_depth_callback, 'Message dispatcher total pending items')

    def __queue_depth_callback(self, _options):
        '''Total number of work items pending across all guild queues.'''
        depth = sum(q.qsize() for q in self._guilds.values())
        return [Observation(depth, attributes={AttributeNaming.BACKGROUND_JOB.value: 'message_dispatcher_queue'})]

    @cached_property
    def _redis(self):
        '''Return an async Redis client if redis_url is configured, else None.'''
        url = self.settings.get('general', {}).get('redis_url')
        if not url:
            return None
        return get_redis_client(url)

    # ------------------------------------------------------------------
    # Cog lifecycle
    # ------------------------------------------------------------------

    async def cog_load(self):
        '''Start the cog input consumer task and restore bundles from Redis if configured.'''
        self._cog_consumer_task = asyncio.create_task(self._cog_consumer())
        if self._redis:
            await self._restore_bundles()
        if self._http_mode:
            for _ in range(self._num_redis_workers):
                self._redis_worker_tasks.append(asyncio.create_task(self._redis_worker()))
            cfg = self.settings['general']['dispatch_server']
            # Import here to avoid circular import (server imports dispatcher types)
            from discord_bot.servers.dispatch_server import DispatchHttpServer  # pylint: disable=import-outside-toplevel
            server = DispatchHttpServer(
                self, self._redis_queue,
                host=cfg.get('host', '0.0.0.0'),
                port=int(cfg['port']),
            )
            asyncio.create_task(server.serve())

    async def cog_unload(self):
        '''Gracefully drain in-flight work and shut down.'''
        self.logger.info('MessageDispatcher :: cog_unload called, draining...')

        # Phase 1: stop accepting new cross-process input
        if self._redis_worker_tasks:
            self._shutdown.set()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._redis_worker_tasks, return_exceptions=True),
                    timeout=_DRAIN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                self.logger.warning('MessageDispatcher :: Redis worker drain timeout')
                for task in self._redis_worker_tasks:
                    task.cancel()
            self._redis_worker_tasks.clear()
        # Phase 2: signal shutdown — _cog_consumer and workers exit when their queues empty
        self._shutdown.set()

        # Phase 4: drain _cog_input into guild queues, then stop the consumer
        if self._cog_consumer_task:
            try:
                await asyncio.wait_for(self._cog_consumer_task, timeout=_DRAIN_TIMEOUT_SECONDS)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._cog_consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._cog_consumer_task

        # Phase 5: wait for guild workers to drain their PriorityQueues
        if self._workers:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._workers.values(), return_exceptions=True),
                    timeout=_DRAIN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                self.logger.warning('MessageDispatcher :: drain timeout, cancelling remaining workers')
                for task in self._workers.values():
                    task.cancel()

        self._workers.clear()
        self._guilds.clear()

        # Phase 6: flush all in-memory bundles to Redis as a fallback
        # (workers may have updated bundle state during drain without completing their save tasks)
        if self._redis and self._bundles:
            self.logger.info('MessageDispatcher :: flushing %d bundle(s) to Redis', len(self._bundles))
            await asyncio.gather(
                *[self._save_bundle_to_redis(key, bundle) for key, bundle in self._bundles.items()],
                return_exceptions=True,
            )

        if self.__dict__.get('_redis') is not None:
            await self._redis.aclose()
        self.logger.info('MessageDispatcher :: shutdown complete')

    async def _restore_bundles(self):
        '''Load all persisted bundles from Redis into _bundles on startup.'''
        async with async_otel_span_wrapper('message_dispatcher.restore_bundles') as span:
            try:
                data = await load_all_bundles(self._redis)
                for key, bundle_dict in data.items():
                    self._bundles[key] = MessageMutableBundle.from_dict(bundle_dict)
                self.logger.info('MessageDispatcher :: restored %d bundle(s) from Redis', len(data))
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                self.logger.error('MessageDispatcher :: failed to restore bundles: %s', exc)

    @cached_property
    def _process_id(self) -> str:
        return self.settings.get('general', {}).get('dispatch_process_id') or str(uuid.uuid4())

    @cached_property
    def _shard_id(self) -> int:
        return int(self.settings.get('general', {}).get('dispatch_shard_id', 0))

    @cached_property
    def _http_mode(self) -> bool:
        '''True when the dispatcher is configured to run as an HTTP server.'''
        return bool(self.settings.get('general', {}).get('dispatch_server'))

    @cached_property
    def _redis_queue(self) -> RedisDispatchQueue:
        '''Shared Redis work queue — only used in HTTP mode.'''
        return RedisDispatchQueue(self._redis, self._shard_id, self._process_id)

    @cached_property
    def _num_redis_workers(self) -> int:
        '''Number of Redis worker coroutines to run per pod (default 4).'''
        return int(self.settings.get('general', {}).get('dispatch_worker_count', 4))

    async def _dispatch_history_and_collect(self, payload: dict) -> dict:
        '''Execute a channel history fetch and return a JSON-safe result dict.'''
        after_dt = datetime.fromisoformat(payload['after']) if payload.get('after') else None
        item = _HistoryReadItem(
            guild_id=int(payload['guild_id']),
            channel_id=int(payload['channel_id']),
            limit=int(payload['limit']),
            after=after_dt,
            after_message_id=int(payload['after_message_id']) if payload.get('after_message_id') is not None else None,
            oldest_first=bool(payload.get('oldest_first', True)),
        )
        messages = await async_retry_discord_message_command(partial(self._fetch_channel_history, item))
        return {
            'guild_id': item.guild_id,
            'channel_id': item.channel_id,
            'after_message_id': item.after_message_id,
            'messages': [m.to_dict() for m in messages],
        }

    async def _dispatch_emojis_and_collect(self, payload: dict) -> dict:
        '''Execute a guild emoji fetch and return a JSON-safe result dict.'''
        item = _EmojiReadItem(
            guild_id=int(payload['guild_id']),
            max_retries=int(payload.get('max_retries', 3)),
        )
        emojis = await async_retry_discord_message_command(
            partial(self._fetch_guild_emojis, item), max_retries=item.max_retries
        )
        return {
            'guild_id': item.guild_id,
            'emojis': [{'id': e.id, 'name': e.name, 'animated': e.animated} for e in emojis],
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_queue(self, guild_id: int) -> asyncio.PriorityQueue:
        if guild_id not in self._guilds:
            self._guilds[guild_id] = asyncio.PriorityQueue()
        return self._guilds[guild_id]

    def _ensure_worker(self, guild_id: int):
        task = self._workers.get(guild_id)
        if task is None or task.done():
            self.logger.debug(f'MessageDispatcher :: starting worker for guild {guild_id}')
            loop = asyncio.get_event_loop()
            self._workers[guild_id] = loop.create_task(self._worker(guild_id))

    # ------------------------------------------------------------------
    # Cog input queue: public API for CogHelper
    # ------------------------------------------------------------------

    def register_cog_queue(self, cog_name: str) -> asyncio.Queue:
        '''Register a result delivery queue for the named cog.'''
        q: asyncio.Queue = asyncio.Queue()
        self._cog_result_queues[cog_name] = q
        return q

    async def submit_request(self, request) -> None:
        '''Submit a typed cog request to the consumer queue.'''
        await self._cog_input.put(request)

    # ------------------------------------------------------------------
    # Cog consumer: routes typed requests into the guild priority queues
    # ------------------------------------------------------------------

    async def _cog_consumer(self):
        '''
        Read typed requests from _cog_input, convert to internal work items,
        and drop them into the appropriate guild PriorityQueue.

        Identical history/emoji requests (same cog + channel/guild) are deduplicated:
        the first enqueues a work item; subsequent duplicates are dropped since the
        result will arrive on the cog's result queue regardless.
        '''
        while True:
            try:
                request = self._cog_input.get_nowait()
            except QueueEmpty:
                if self._shutdown.is_set():
                    return
                await asyncio.sleep(0.01)
                continue
            if isinstance(request, FetchChannelHistoryRequest):
                key = (request.cog_name, request.guild_id, request.channel_id)
                if key not in self._pending_history:
                    self._pending_history.add(key)
                    item = _HistoryReadItem(
                        seq=next(self._seq),
                        cog_name=request.cog_name,
                        guild_id=request.guild_id,
                        channel_id=request.channel_id,
                        limit=request.limit,
                        after=request.after,
                        after_message_id=request.after_message_id,
                        oldest_first=request.oldest_first,
                        dedup_key=key,
                        span_context=request.span_context,
                    )
                    queue = self._get_queue(request.guild_id)
                    queue.put_nowait((item.priority, item.seq, item))
                    self._ensure_worker(request.guild_id)
            elif isinstance(request, FetchGuildEmojisRequest):
                key = (request.cog_name, request.guild_id)
                if key not in self._pending_emojis:
                    self._pending_emojis.add(key)
                    item = _EmojiReadItem(
                        seq=next(self._seq),
                        cog_name=request.cog_name,
                        guild_id=request.guild_id,
                        max_retries=request.max_retries,
                        dedup_key=key,
                        span_context=request.span_context,
                    )
                    queue = self._get_queue(request.guild_id)
                    queue.put_nowait((item.priority, item.seq, item))
                    self._ensure_worker(request.guild_id)
            elif isinstance(request, SendRequest):
                self.send_message(request.guild_id, request.channel_id,
                                  request.content, delete_after=request.delete_after,
                                  span_context=request.span_context)
            elif isinstance(request, DeleteRequest):
                self.delete_message(request.guild_id, request.channel_id, request.message_id,
                                    span_context=request.span_context)

    # ------------------------------------------------------------------
    # Fetch helpers (called from _dispatch)
    # ------------------------------------------------------------------

    async def _fetch_channel_history(self, item: _HistoryReadItem) -> list:
        '''Fetch channel history and return as a list of FetchedMessage.'''
        channel = await self.bot.fetch_channel(item.channel_id)
        after_obj = item.after
        if item.after_message_id is not None:
            after_obj = await channel.fetch_message(item.after_message_id)
        messages = [m async for m in channel.history(
            limit=item.limit, after=after_obj, oldest_first=item.oldest_first
        )]
        return [
            FetchedMessage(id=m.id, content=m.content, created_at=m.created_at, author_bot=m.author.bot)
            for m in messages
        ]

    async def _fetch_guild_emojis(self, item: _EmojiReadItem) -> list:
        '''Fetch and return the emoji list for the given guild.'''
        guild = await self.bot.fetch_guild(item.guild_id)
        return await guild.fetch_emojis()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update_mutable(self, key: str, guild_id: int, content: List[str],
                       channel_id: int | None, sticky: bool = True, delete_after: int | None = None):
        '''
        Queue a mutable bundle update for *key* in *guild_id*.

        Rapid-fire calls collapse: only the latest content is kept, and only
        one sentinel is placed on the queue per key.
        '''
        if not content:
            self.logger.debug('update_mutable: empty content for key=%s, routing to remove_mutable', key)
            return self.remove_mutable(key)
        request_uuid = uuid.uuid4()
        trace.get_current_span().set_attribute(DispatchNaming.REQUEST_ID.value, str(request_uuid))
        self.logger.debug('update_mutable: key=%s dispatch.request_id=%s', key, request_uuid)

        if self._http_mode:
            asyncio.create_task(self._redis_queue.enqueue_unique(
                f'{_MEMBER_MUTABLE}{key}',
                {'key': key, 'guild_id': guild_id, 'content': content,
                 'channel_id': channel_id, 'sticky': sticky, 'delete_after': delete_after},
                DispatchPriority.HIGH,
            ))
            return request_uuid

        # Store the latest content (overwrite any previous pending)
        self._pending_mutable[key] = _MutablePending(
            content=content,
            guild_id=guild_id,
            channel_id=channel_id,
            sticky=sticky,
            delete_after=delete_after,
        )

        # Ensure bundle entry exists (create lazily)
        if key not in self._bundles:
            if channel_id is None:
                self.logger.info(f'MessageDispatcher :: cannot create bundle "{key}" without channel')
                return None
            self._create_bundle(key, guild_id, channel_id, sticky)

        # Add sentinel to queue only once per key
        if key not in self._sentinel_in_queue:
            self._sentinel_in_queue.add(key)
            queue = self._get_queue(guild_id)
            item = _MutableSentinel(seq=next(self._seq), key=key)
            queue.put_nowait((item.priority, item.seq, item))
            self._ensure_worker(guild_id)
        # Return random uuid so we know what called from where
        return request_uuid

    def remove_mutable(self, key: str):
        '''
        Delete all messages managed by *key* and remove its bundle.
        '''
        if self._http_mode:
            asyncio.create_task(self._redis_queue.enqueue_unique(
                f'{_MEMBER_REMOVE}{key}',
                {'key': key},
                DispatchPriority.HIGH,
            ))
            return
        with otel_span_wrapper('message_dispatcher.remove_mutable', attributes={'key': key}):
            bundle = self._bundles.pop(key, None)
            if bundle:
                delete_funcs = bundle.clear_all_messages(self.bot.get_partial_messageable)
                # Fire-and-forget: schedule the deletions as a task
                loop = asyncio.get_event_loop()
                loop.create_task(self._execute_funcs(delete_funcs))
                if self._redis:
                    loop.create_task(self._delete_bundle_from_redis(key))
            self._pending_mutable.pop(key, None)
            self._sentinel_in_queue.discard(key)

    def send_message(self, guild_id: int, channel_id: int, content: str,
                     delete_after: int | None = None, allow_404: bool = False,
                     span_context: dict | None = None):
        '''Enqueue a text message send at NORMAL priority.'''
        if self._http_mode:
            asyncio.create_task(self._redis_queue.enqueue(
                f'{_MEMBER_SEND}{uuid.uuid4()}',
                {'guild_id': guild_id, 'channel_id': channel_id, 'content': content,
                 'delete_after': delete_after, 'allow_404': allow_404, 'span_context': span_context},
                DispatchPriority.NORMAL,
            ))
            return
        queue = self._get_queue(guild_id)
        item = _SendItem(seq=next(self._seq), channel_id=channel_id,
                         content=content, delete_after=delete_after, allow_404=allow_404,
                         span_context=span_context)
        queue.put_nowait((item.priority, item.seq, item))
        self._ensure_worker(guild_id)

    def delete_message(self, guild_id: int, channel_id: int, message_id: int,
                       span_context: dict | None = None):
        '''Enqueue a message deletion at NORMAL priority.'''
        if self._http_mode:
            asyncio.create_task(self._redis_queue.enqueue(
                f'{_MEMBER_DELETE}{uuid.uuid4()}',
                {'guild_id': guild_id, 'channel_id': channel_id,
                 'message_id': message_id, 'span_context': span_context},
                DispatchPriority.NORMAL,
            ))
            return
        queue = self._get_queue(guild_id)
        item = _DeleteItem(seq=next(self._seq), channel_id=channel_id, message_id=message_id,
                           span_context=span_context)
        queue.put_nowait((item.priority, item.seq, item))
        self._ensure_worker(guild_id)

    async def fetch_object(self, guild_id: int, func: Callable,
                           max_retries: int = 3, allow_404: bool = False):
        '''
        Enqueue func at LOW priority and await its result.

        Runs after all HIGH and NORMAL items for this guild, ensuring that
        background reads (channel history, fetch_message) do not compete with
        higher-priority message sends and edits.
        '''
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        queue = self._get_queue(guild_id)
        item = _ReadItem(seq=next(self._seq), func=func, future=future,
                         max_retries=max_retries, allow_404=allow_404)
        queue.put_nowait((item.priority, item.seq, item))
        self._ensure_worker(guild_id)
        return await future

    async def enqueue_fetch_history(self, request_id: str, guild_id: int, channel_id: int,
                                    limit: int, after: str | None, after_message_id: int | None,
                                    oldest_first: bool) -> None:
        '''Enqueue a channel history fetch; result stored in Redis under request_id.'''
        await self._redis_queue.enqueue(
            f'{_MEMBER_FETCH_HISTORY}{request_id}',
            {'guild_id': guild_id, 'channel_id': channel_id, 'limit': limit,
             'after': after, 'after_message_id': after_message_id, 'oldest_first': oldest_first},
            DispatchPriority.LOW,
        )

    async def enqueue_fetch_emojis(self, request_id: str, guild_id: int, max_retries: int) -> None:
        '''Enqueue a guild emoji fetch; result stored in Redis under request_id.'''
        await self._redis_queue.enqueue(
            f'{_MEMBER_FETCH_EMOJIS}{request_id}',
            {'guild_id': guild_id, 'max_retries': max_retries},
            DispatchPriority.LOW,
        )

    def update_mutable_channel(self, key: str, guild_id: int, new_channel_id: int):
        '''
        Move an existing mutable bundle to *new_channel_id*.

        Immediately deletes old messages (fire-and-forget), then re-queues
        an update with the latest pending content in the new channel.
        '''
        if self._http_mode:
            asyncio.create_task(self._redis_queue.enqueue_unique(
                f'{_MEMBER_UPDATE_CHANNEL}{key}',
                {'key': key, 'guild_id': guild_id, 'new_channel_id': new_channel_id},
                DispatchPriority.HIGH,
            ))
            return
        with otel_span_wrapper('message_dispatcher.update_mutable_channel',
                               attributes={'key': key, 'discord.guild': guild_id,
                                           'discord.channel': new_channel_id}):
            bundle = self._bundles.get(key)
            if not bundle:
                return

            delete_funcs = bundle.update_text_channel(guild_id, new_channel_id, self.bot.get_partial_messageable)
            loop = asyncio.get_event_loop()
            loop.create_task(self._execute_funcs(delete_funcs))

            # Re-queue with new channel_id if there's pending content
            pending = self._pending_mutable.get(key)
            if pending:
                self.update_mutable(key, guild_id, pending.content, new_channel_id,
                                    sticky=pending.sticky, delete_after=pending.delete_after)

    # ------------------------------------------------------------------
    # Bundle creation helper
    # ------------------------------------------------------------------

    def _make_channel_funcs(self, channel_id: int):
        '''
        Build and return (check_last_message_func, send_function) as closures
        that resolve the channel at call-time via self.bot.get_partial_messageable(channel_id).
        '''
        async def check_last_message_func(count: int):
            channel = self.bot.get_partial_messageable(channel_id)
            async def fetch_messages():
                return [m async for m in channel.history(limit=count)]
            return await async_retry_discord_message_command(fetch_messages)

        async def send_function(**kwargs):
            channel = self.bot.get_partial_messageable(channel_id)
            return await async_retry_discord_message_command(partial(channel.send, **kwargs))

        return check_last_message_func, send_function

    def _create_bundle(self, key: str, guild_id: int, channel_id: int, sticky: bool) -> MessageMutableBundle:
        bundle = MessageMutableBundle(guild_id=guild_id, channel_id=channel_id, sticky_messages=sticky)
        self._bundles[key] = bundle
        return bundle

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker(self, guild_id: int):
        queue = self._get_queue(guild_id)
        while not self._shutdown.is_set() or not queue.empty():
            try:
                _, _, item = queue.get_nowait()
            except QueueEmpty:
                if self._shutdown.is_set():
                    self.logger.debug(f'MessageDispatcher :: worker exiting for guild {guild_id}')
                    return
                await asyncio.sleep(0.01)
                continue
            await self._dispatch(item, guild_id)

    async def _dispatch(self, item, guild_id: int):
        if isinstance(item, _MutableSentinel):
            self.logger.debug(f'MessageDispatcher :: processing mutable "{item.key}" for guild {guild_id}')
            async with async_otel_span_wrapper('message_dispatcher.process_mutable',
                                              attributes={'key': item.key, 'discord.guild': guild_id}):
                await self._process_mutable(item.key)
        elif isinstance(item, _DeleteItem):
            self.logger.debug(f'MessageDispatcher :: deleting message {item.message_id} in channel {item.channel_id} for guild {guild_id}')
            async with async_otel_span_wrapper('message_dispatcher.delete',
                                              attributes={'discord.channel': item.channel_id, 'discord.guild': guild_id},
                                              links=span_links_from_context(item.span_context)):
                try:
                    channel = await self.bot.fetch_channel(item.channel_id)
                    msg = channel.get_partial_message(item.message_id)
                    await msg.delete()
                except NotFound:
                    pass
        elif isinstance(item, _ReadItem):
            self.logger.debug(f'MessageDispatcher :: fetching object for guild {guild_id}')
            async with async_otel_span_wrapper('message_dispatcher.fetch',
                                              attributes={'discord.guild': guild_id}) as span:
                try:
                    result = await async_retry_discord_message_command(
                        item.func, max_retries=item.max_retries, allow_404=item.allow_404
                    )
                    if not item.future.done():
                        item.future.set_result(result)
                except Exception as exc:  # pylint: disable=broad-except
                    # Intentional broad catch: any uncaught exception here would leave
                    # item.future permanently pending, hanging the caller indefinitely.
                    # The exception is logged in full and forwarded via set_exception
                    # so it propagates normally at the caller's await site.
                    self.logger.error(
                        'MessageDispatcher :: fetch failed for guild %d: %s',
                        guild_id, exc, exc_info=True
                    )
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR)
                    if not item.future.done():
                        item.future.set_exception(exc)
        elif isinstance(item, _HistoryReadItem):
            self.logger.debug(f'MessageDispatcher :: fetching channel history {item.channel_id} for guild {guild_id}')
            async with async_otel_span_wrapper('message_dispatcher.channel_history',
                                              attributes={'discord.channel': item.channel_id, 'discord.guild': guild_id},
                                              links=span_links_from_context(item.span_context)) as span:
                try:
                    messages = await async_retry_discord_message_command(
                        partial(self._fetch_channel_history, item)
                    )
                    result = ChannelHistoryResult(
                        guild_id=item.guild_id,
                        channel_id=item.channel_id,
                        messages=messages,
                        after_message_id=item.after_message_id,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error(
                        'MessageDispatcher :: channel history fetch failed for guild %d: %s',
                        guild_id, exc, exc_info=True
                    )
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR)
                    result = ChannelHistoryResult(
                        guild_id=item.guild_id,
                        channel_id=item.channel_id,
                        messages=[],
                        after_message_id=item.after_message_id,
                        error=exc,
                    )
                self._pending_history.discard(item.dedup_key)
                q = self._cog_result_queues.get(item.cog_name)
                if q:
                    await q.put(result)
        elif isinstance(item, _EmojiReadItem):
            self.logger.debug(f'MessageDispatcher :: fetching guild emojis for guild {guild_id}')
            async with async_otel_span_wrapper('message_dispatcher.guild_emojis',
                                              attributes={'discord.guild': guild_id},
                                              links=span_links_from_context(item.span_context)) as span:
                try:
                    emojis = await async_retry_discord_message_command(
                        partial(self._fetch_guild_emojis, item),
                        max_retries=item.max_retries,
                    )
                    result = GuildEmojisResult(guild_id=item.guild_id, emojis=emojis)
                except Exception as exc:  # pylint: disable=broad-except
                    self.logger.error(
                        'MessageDispatcher :: guild emojis fetch failed for guild %d: %s',
                        guild_id, exc, exc_info=True
                    )
                    span.record_exception(exc)
                    span.set_status(StatusCode.ERROR)
                    result = GuildEmojisResult(guild_id=item.guild_id, emojis=[], error=exc)
                self._pending_emojis.discard(item.dedup_key)
                q = self._cog_result_queues.get(item.cog_name)
                if q:
                    await q.put(result)
        elif isinstance(item, _SendItem):
            channel = self.bot.get_partial_messageable(item.channel_id)
            self.logger.debug(f'MessageDispatcher :: sending to channel {item.channel_id} guild {guild_id}')
            async with async_otel_span_wrapper('message_dispatcher.send',
                                              attributes={'discord.channel': item.channel_id, 'discord.guild': guild_id},
                                              links=span_links_from_context(item.span_context)):
                await async_retry_discord_message_command(
                    partial(channel.send, content=item.content, delete_after=item.delete_after),
                    allow_404=item.allow_404,
                )

    async def _process_mutable(self, key: str):
        # Remove from in-queue sentinel set first
        self._sentinel_in_queue.discard(key)

        pending = self._pending_mutable.pop(key, None)
        if pending is None:
            self.logger.debug(f'MessageDispatcher :: no pending content for key "{key}", skipping')
            return

        bundle = self._bundles.get(key)
        if bundle is None:
            self.logger.debug(f'MessageDispatcher :: bundle "{key}" removed while sentinel in flight, skipping')
            return

        # If the channel changed in the pending update, clear old messages
        if bundle.channel_id != pending.channel_id:
            self.logger.debug(f'MessageDispatcher :: channel changed for "{key}": {bundle.channel_id} -> {pending.channel_id}')
            delete_funcs = bundle.update_text_channel(pending.guild_id, pending.channel_id, self.bot.get_partial_messageable)
            await self._execute_funcs(delete_funcs)

        check_func, send_func = self._make_channel_funcs(bundle.channel_id)

        content = pending.content
        delete_after = pending.delete_after

        # Sticky check: should we clear old messages?
        should_clear = False
        if bundle.sticky_messages or len(content) <= len(bundle.message_contexts):
            should_clear = await async_retry_discord_message_command(
                partial(bundle.should_clear_messages, check_func)
            )

        funcs = bundle.get_message_dispatch(content, send_func, self.bot.get_partial_messageable,
                                            clear_existing=should_clear, delete_after=delete_after)

        # Execute and collect new Message objects
        results = []
        for func in funcs:
            # Safety truncation: Discord max is 2000 chars
            if hasattr(func, 'keywords') and 'content' in func.keywords:
                content_val = func.keywords['content']
                if len(content_val) > 2000:
                    self.logger.info(
                        f'MessageDispatcher :: truncating message over 2000 chars for bundle "{key}"'
                    )
                    new_kwargs = func.keywords.copy()
                    new_kwargs['content'] = content_val[:1900]
                    func = partial(func.func, *func.args, **new_kwargs)
            result = await async_retry_discord_message_command(func)
            if result and hasattr(result, 'id'):
                results.append(result)

        # Update message references for newly sent messages
        contexts_needing_refs = [ctx for ctx in bundle.message_contexts if ctx.message_id is None]
        for i, message in enumerate(results):
            if i < len(contexts_needing_refs):
                contexts_needing_refs[i].set_message(message)

        self.logger.debug(
            f'MessageDispatcher :: mutable "{key}" dispatched {len(funcs)} op(s), '
            f'{len(results)} new message(s)'
        )

        if delete_after is not None:
            self._bundles.pop(key, None)
            if self._redis:
                asyncio.create_task(self._delete_bundle_from_redis(key))
        elif self._redis:
            asyncio.create_task(self._save_bundle_to_redis(key, bundle))

    # ------------------------------------------------------------------
    # Redis worker (HTTP mode)
    # ------------------------------------------------------------------

    async def _redis_worker(self):
        '''BZPOPMIN loop: pop work items from the shared Redis queue and execute them.'''
        while not self._shutdown.is_set():
            result = await self._redis_queue.dequeue(timeout=1.0)
            if result is None:
                continue
            member, payload = result
            await self._dispatch_redis_item(member, payload)

    async def _dispatch_redis_item(self, member: str, payload: dict):
        '''Route a Redis queue item to the appropriate handler based on its member prefix.'''
        if member.startswith(_MEMBER_MUTABLE):
            await self._process_mutable_redis(member[len(_MEMBER_MUTABLE):], payload)
        elif member.startswith(_MEMBER_REMOVE):
            await self._remove_mutable_redis(member[len(_MEMBER_REMOVE):])
        elif member.startswith(_MEMBER_SEND):
            await self._process_send_redis(payload)
        elif member.startswith(_MEMBER_DELETE):
            await self._process_delete_redis(payload)
        elif member.startswith(_MEMBER_UPDATE_CHANNEL):
            await self._process_update_channel_redis(member[len(_MEMBER_UPDATE_CHANNEL):], payload)
        elif member.startswith(_MEMBER_FETCH_HISTORY):
            await self._process_fetch_history_redis(member[len(_MEMBER_FETCH_HISTORY):], payload)
        elif member.startswith(_MEMBER_FETCH_EMOJIS):
            await self._process_fetch_emojis_redis(member[len(_MEMBER_FETCH_EMOJIS):], payload)
        else:
            self.logger.warning('MessageDispatcher :: unknown Redis queue member: %s', member)

    async def _process_mutable_redis(self, key: str, payload: dict):
        '''HTTP-mode: load bundle from Redis, acquire lock, execute mutable update, save.'''
        acquired = await self._redis_queue.acquire_lock(key)
        if not acquired:
            # Another pod is executing this bundle; re-enqueue so it runs after the lock releases
            await self._redis_queue.enqueue_unique(f'{_MEMBER_MUTABLE}{key}', payload, DispatchPriority.HIGH)
            return
        try:
            async with async_otel_span_wrapper('message_dispatcher.process_mutable_redis',
                                               attributes={'key': key, 'discord.guild': payload.get('guild_id', 0)}):
                bundle_dict = await redis_load_bundle(self._redis, key)
                if bundle_dict is not None:
                    bundle = MessageMutableBundle.from_dict(bundle_dict)
                else:
                    channel_id = payload.get('channel_id')
                    if channel_id is None:
                        self.logger.info('MessageDispatcher :: cannot create bundle "%s" without channel_id', key)
                        return
                    bundle = MessageMutableBundle(
                        guild_id=payload['guild_id'],
                        channel_id=channel_id,
                        sticky_messages=payload.get('sticky', True),
                    )

                new_channel_id = payload.get('channel_id')
                if new_channel_id and bundle.channel_id != new_channel_id:
                    delete_funcs = bundle.update_text_channel(
                        payload['guild_id'], new_channel_id, self.bot.get_partial_messageable
                    )
                    await self._execute_funcs(delete_funcs)

                check_func, send_func = self._make_channel_funcs(bundle.channel_id)
                content = payload['content']
                delete_after = payload.get('delete_after')

                should_clear = False
                if bundle.sticky_messages or len(content) <= len(bundle.message_contexts):
                    should_clear = await async_retry_discord_message_command(
                        partial(bundle.should_clear_messages, check_func)
                    )

                funcs = bundle.get_message_dispatch(
                    content, send_func, self.bot.get_partial_messageable,
                    clear_existing=should_clear, delete_after=delete_after,
                )

                results = []
                for func in funcs:
                    if hasattr(func, 'keywords') and 'content' in func.keywords:
                        content_val = func.keywords['content']
                        if len(content_val) > 2000:
                            self.logger.info('MessageDispatcher :: truncating message over 2000 chars for bundle "%s"', key)
                            new_kwargs = func.keywords.copy()
                            new_kwargs['content'] = content_val[:1900]
                            func = partial(func.func, *func.args, **new_kwargs)
                    result = await async_retry_discord_message_command(func)
                    if result and hasattr(result, 'id'):
                        results.append(result)

                contexts_needing_refs = [ctx for ctx in bundle.message_contexts if ctx.message_id is None]
                for i, message in enumerate(results):
                    if i < len(contexts_needing_refs):
                        contexts_needing_refs[i].set_message(message)

                if delete_after is not None:
                    await self._delete_bundle_from_redis(key)
                else:
                    await self._save_bundle_to_redis(key, bundle)
        finally:
            await self._redis_queue.release_lock(key)

    async def _remove_mutable_redis(self, key: str):
        '''HTTP-mode: delete all messages for a bundle and remove it from Redis.'''
        async with async_otel_span_wrapper('message_dispatcher.remove_mutable_redis', attributes={'key': key}):
            bundle_dict = await redis_load_bundle(self._redis, key)
            if bundle_dict:
                bundle = MessageMutableBundle.from_dict(bundle_dict)
                delete_funcs = bundle.clear_all_messages(self.bot.get_partial_messageable)
                await self._execute_funcs(delete_funcs)
            await self._delete_bundle_from_redis(key)

    async def _process_send_redis(self, payload: dict):
        '''HTTP-mode: execute a send_message from Redis queue payload.'''
        channel = self.bot.get_partial_messageable(payload['channel_id'])
        async with async_otel_span_wrapper('message_dispatcher.send',
                                           attributes={'discord.channel': payload['channel_id'],
                                                       'discord.guild': payload['guild_id']},
                                           links=span_links_from_context(payload.get('span_context'))):
            await async_retry_discord_message_command(
                partial(channel.send, content=payload['content'], delete_after=payload.get('delete_after')),
                allow_404=payload.get('allow_404', False),
            )

    async def _process_delete_redis(self, payload: dict):
        '''HTTP-mode: execute a delete_message from Redis queue payload.'''
        async with async_otel_span_wrapper('message_dispatcher.delete',
                                           attributes={'discord.channel': payload['channel_id'],
                                                       'discord.guild': payload['guild_id']},
                                           links=span_links_from_context(payload.get('span_context'))):
            try:
                channel = await self.bot.fetch_channel(payload['channel_id'])
                msg = channel.get_partial_message(payload['message_id'])
                await msg.delete()
            except NotFound:
                pass

    async def _process_update_channel_redis(self, key: str, payload: dict):
        '''HTTP-mode: move a bundle to a new channel and save updated state to Redis.'''
        async with async_otel_span_wrapper('message_dispatcher.update_mutable_channel',
                                           attributes={'key': key, 'discord.guild': payload['guild_id'],
                                                       'discord.channel': payload['new_channel_id']}):
            bundle_dict = await redis_load_bundle(self._redis, key)
            if not bundle_dict:
                return
            bundle = MessageMutableBundle.from_dict(bundle_dict)
            delete_funcs = bundle.update_text_channel(
                payload['guild_id'], payload['new_channel_id'], self.bot.get_partial_messageable
            )
            await self._execute_funcs(delete_funcs)
            await self._save_bundle_to_redis(key, bundle)

    async def _process_fetch_history_redis(self, request_id: str, payload: dict):
        '''HTTP-mode: execute channel history fetch and store result in Redis.'''
        async with async_otel_span_wrapper('message_dispatcher.fetch_history_redis',
                                           attributes={'discord.guild': payload['guild_id'],
                                                       'discord.channel': payload['channel_id']}):
            try:
                result = await self._dispatch_history_and_collect(payload)
            except Exception as exc:  # pylint: disable=broad-except
                # Intentional broad catch: result must always be written to Redis so the
                # polling client does not hang indefinitely waiting for a result that will never arrive.
                self.logger.error('MessageDispatcher :: fetch history failed: %s', exc, exc_info=True)
                result = {'error': str(exc)}
            await self._redis_queue.store_result(request_id, result)

    async def _process_fetch_emojis_redis(self, request_id: str, payload: dict):
        '''HTTP-mode: execute guild emoji fetch and store result in Redis.'''
        async with async_otel_span_wrapper('message_dispatcher.fetch_emojis_redis',
                                           attributes={'discord.guild': payload['guild_id']}):
            try:
                result = await self._dispatch_emojis_and_collect(payload)
            except Exception as exc:  # pylint: disable=broad-except
                # Intentional broad catch: same reasoning as _process_fetch_history_redis.
                self.logger.error('MessageDispatcher :: fetch emojis failed: %s', exc, exc_info=True)
                result = {'error': str(exc)}
            await self._redis_queue.store_result(request_id, result)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def _execute_funcs(self, funcs: List[Callable]):
        for func in funcs:
            await async_retry_discord_message_command(func)

    async def _save_bundle_to_redis(self, key: str, bundle: 'MessageMutableBundle') -> None:
        '''Persist *bundle* to Redis; logs and swallows any error.'''
        try:
            await redis_save_bundle(self._redis, key, bundle.to_dict())
        except Exception as exc:
            self.logger.error('MessageDispatcher :: failed to save bundle "%s": %s', key, exc)

    async def _delete_bundle_from_redis(self, key: str) -> None:
        '''Remove *key* from Redis; logs and swallows any error.'''
        try:
            await redis_delete_bundle(self._redis, key)
        except Exception as exc:
            self.logger.error('MessageDispatcher :: failed to delete bundle "%s": %s', key, exc)
