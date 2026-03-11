import asyncio
from asyncio import QueueEmpty
from dataclasses import dataclass, field
from enum import IntEnum
from functools import partial
import itertools
from typing import Callable, List

from discord import Message
from discord.errors import NotFound
from discord.ext.commands import Bot
from sqlalchemy.engine.base import Engine

from opentelemetry.metrics import Observation
from opentelemetry.trace.status import StatusCode

from discord_bot.cogs.common import CogHelper
from discord_bot.utils.discord_retry import async_retry_discord_message_command
from discord_bot.utils.otel import AttributeNaming, METER_PROVIDER, MetricNaming, otel_span_wrapper, create_observable_gauge


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
    message: Message | None = None
    message_content: str | None = None
    delete_after: int | None = None
    function: Callable | None = None

    def set_message(self, message: Message):
        '''Set the Discord message object after it has been sent.'''
        self.message = message
        self.message_id = message.id if message else None

    async def delete_message(self):
        '''Delete the message if it exists.'''
        if not self.message:
            return False
        try:
            await self.message.delete()
        except NotFound:
            return True
        return True

    async def edit_message(self, **kwargs):
        '''Edit the message contents.'''
        if not self.message:
            return False
        await self.message.edit(**kwargs)
        return True


class MessageMutableBundle:
    '''Collection of multiple mutable Discord messages.'''

    def __init__(self, guild_id: int, channel_id: int, check_last_message_func: Callable,
                 send_function: Callable, sticky_messages: bool = True):
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.check_last_message_func = check_last_message_func
        self.send_function = send_function
        self.sticky_messages = sticky_messages
        self.message_contexts: List[MessageContext] = []

    async def should_clear_messages(self) -> bool:
        '''Check if messages should be cleared (sticky check).'''
        if not self.message_contexts:
            return False
        if not self.sticky_messages:
            return False
        history_messages = await async_retry_discord_message_command(
            partial(self.check_last_message_func, len(self.message_contexts))
        )
        for count, hist_message in enumerate(history_messages):
            index = len(self.message_contexts) - 1 - count
            if index < 0:
                break
            context = self.message_contexts[index]
            if not context.message or context.message.id != hist_message.id:
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

    def get_message_dispatch(self, message_content: List[str], clear_existing: bool = False,
                             delete_after: int = None) -> List[Callable]:
        '''Return list of callables to sync Discord messages with new content.'''
        dispatch_functions = []

        if clear_existing and self.message_contexts:
            for context in self.message_contexts:
                if context.message:
                    dispatch_functions.append(partial(context.delete_message))
            self.message_contexts = []

        if not self.message_contexts:
            for content in message_content:
                mc = MessageContext(self.guild_id, self.channel_id)
                mc.message_content = content
                mc.delete_after = delete_after
                send_func = partial(self.send_function, content=content, delete_after=delete_after)
                mc.function = send_func
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
                    dispatch_functions.append(partial(item.delete_message))
                    delete_count += 1
                    continue
                edit_func = partial(item.edit_message, content=message_content[index], delete_after=delete_after)
                item.function = edit_func
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
            edit_func = partial(item.edit_message, content=message_content[index], delete_after=delete_after)
            item.function = edit_func
            item.delete_after = delete_after
            item.message_content = message_content[index]
            dispatch_functions.append(edit_func)
            new_contexts.append(item)
        self.message_contexts = new_contexts

        if new_count > existing_count:
            for i in range(existing_count, new_count):
                content = message_content[i]
                mc = MessageContext(self.guild_id, self.channel_id)
                mc.message_content = content
                mc.delete_after = delete_after
                send_func = partial(self.send_function, content=content, delete_after=delete_after)
                mc.function = send_func
                self.message_contexts.append(mc)
                dispatch_functions.append(send_func)

        return dispatch_functions

    def clear_all_messages(self) -> List[Callable]:
        '''Return callables to delete all managed messages and clear contexts.'''
        delete_functions = []
        for context in self.message_contexts:
            if context.message:
                delete_functions.append(partial(context.delete_message))
        self.message_contexts = []
        return delete_functions

    def update_text_channel(self, new_guild_id: int, new_channel_id: int,
                            check_last_message_func: Callable,
                            send_function: Callable) -> List[Callable]:
        '''Move this bundle to a different channel; returns delete funcs for old messages.'''
        dispatch_functions = list(self.clear_all_messages())
        self.guild_id = new_guild_id
        self.channel_id = new_channel_id
        self.check_last_message_func = check_last_message_func
        self.send_function = send_function
        return dispatch_functions


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
    priority: int = field(default=DispatchPriority.HIGH, init=False)
    seq: int = field(default=0)
    key: str = ''


@dataclass
class _DeleteItem:
    priority: int = field(default=DispatchPriority.NORMAL, init=False)
    seq: int = field(default=0)
    channel_id: int = 0
    message_id: int = 0


@dataclass
class _SendItem:
    priority: int = field(default=DispatchPriority.NORMAL, init=False)
    seq: int = field(default=0)
    channel_id: int = 0
    content: str = ''
    delete_after: int | None = None
    allow_404: bool = False


@dataclass
class _ReadItem:
    priority: int = field(default=DispatchPriority.LOW, init=False)
    seq: int = field(default=0)
    func: Callable = field(default=None)
    future: asyncio.Future = field(default=None)
    max_retries: int = 3
    allow_404: bool = False


@dataclass
class _MutablePending:
    content: List[str]
    guild_id: int
    channel_id: int | None
    sticky: bool
    delete_after: int | None


class MessageDispatcher(CogHelper):
    '''
    App-wide Discord message dispatcher.

    Owns one asyncio.PriorityQueue per guild and one worker task per active guild.
    Work items are processed at HIGH > NORMAL > LOW priority.

    HIGH   (_MutableSentinel) – flush a mutable bundle update
    NORMAL (_SendItem)        – one-off channel.send calls
    NORMAL (_DeleteItem)      – message deletions
    LOW    (_ReadItem)        – background reads (channel history, fetch_message)
    '''

    def __init__(self, bot: Bot, settings: dict, db_engine: Engine):
        super().__init__(bot, settings, db_engine)

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

        create_observable_gauge(METER_PROVIDER, MetricNaming.DISPATCHER_QUEUE_DEPTH.value,
                                self.__queue_depth_callback, 'Message dispatcher total pending items')

    def __queue_depth_callback(self, _options):
        '''Total number of work items pending across all guild queues.'''
        depth = sum(q.qsize() for q in self._guilds.values())
        return [Observation(depth, attributes={AttributeNaming.BACKGROUND_JOB.value: 'message_dispatcher_queue'})]

    # ------------------------------------------------------------------
    # Cog lifecycle
    # ------------------------------------------------------------------

    async def cog_load(self):
        pass  # workers are lazy – started on first use

    async def cog_unload(self):
        self.logger.info('MessageDispatcher :: cog_unload called')
        self._shutdown.set()
        for task in self._workers.values():
            task.cancel()
        self._workers.clear()
        self._guilds.clear()

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
    # Public API
    # ------------------------------------------------------------------

    def update_mutable(self, key: str, guild_id: int, content: List[str],
                       channel_id: int | None, sticky: bool = True, delete_after: int | None = None):
        '''
        Queue a mutable bundle update for *key* in *guild_id*.

        Rapid-fire calls collapse: only the latest content is kept, and only
        one sentinel is placed on the queue per key.
        '''
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
                self.logger.warning(f'MessageDispatcher :: cannot create bundle "{key}" without channel')
                return
            self._create_bundle(key, guild_id, channel_id, sticky)

        # Add sentinel to queue only once per key
        if key not in self._sentinel_in_queue:
            self._sentinel_in_queue.add(key)
            queue = self._get_queue(guild_id)
            item = _MutableSentinel(seq=next(self._seq), key=key)
            queue.put_nowait((item.priority, item.seq, item))
            self._ensure_worker(guild_id)

    def remove_mutable(self, key: str):
        '''
        Delete all messages managed by *key* and remove its bundle.
        '''
        bundle = self._bundles.pop(key, None)
        if bundle:
            delete_funcs = bundle.clear_all_messages()
            # Fire-and-forget: schedule the deletions as a task
            loop = asyncio.get_event_loop()
            loop.create_task(self._execute_funcs(delete_funcs))
        self._pending_mutable.pop(key, None)
        self._sentinel_in_queue.discard(key)

    def send_message(self, guild_id: int, channel_id: int, content: str,
                     delete_after: int | None = None, allow_404: bool = False):
        '''Enqueue a text message send at NORMAL priority.'''
        queue = self._get_queue(guild_id)
        item = _SendItem(seq=next(self._seq), channel_id=channel_id,
                         content=content, delete_after=delete_after, allow_404=allow_404)
        queue.put_nowait((item.priority, item.seq, item))
        self._ensure_worker(guild_id)

    def delete_message(self, guild_id: int, channel_id: int, message_id: int):
        '''Enqueue a message deletion at NORMAL priority.'''
        queue = self._get_queue(guild_id)
        item = _DeleteItem(seq=next(self._seq), channel_id=channel_id, message_id=message_id)
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

    def update_mutable_channel(self, key: str, guild_id: int, new_channel_id: int):
        '''
        Move an existing mutable bundle to *new_channel_id*.

        Immediately deletes old messages (fire-and-forget), then re-queues
        an update with the latest pending content in the new channel.
        '''
        bundle = self._bundles.get(key)
        if not bundle:
            return

        check_func, send_func = self._make_channel_funcs(new_channel_id)
        delete_funcs = bundle.update_text_channel(guild_id, new_channel_id, check_func, send_func)
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
        that resolve the channel at call-time via self.bot.get_channel(channel_id).
        '''
        async def check_last_message_func(count: int):
            channel = self.bot.get_channel(channel_id)
            if channel is None:
                return []
            async def fetch_messages():
                return [m async for m in channel.history(limit=count)]
            return await async_retry_discord_message_command(fetch_messages)

        async def send_function(**kwargs):
            channel = self.bot.get_channel(channel_id)
            if channel is None:
                self.logger.warning(f'MessageDispatcher :: channel {channel_id} not found for mutable send')
                return None
            return await async_retry_discord_message_command(partial(channel.send, **kwargs))

        return check_last_message_func, send_function

    def _create_bundle(self, key: str, guild_id: int, channel_id: int, sticky: bool) -> MessageMutableBundle:
        check_last_message_func, send_function = self._make_channel_funcs(channel_id)
        bundle = MessageMutableBundle(
            guild_id=guild_id,
            channel_id=channel_id,
            check_last_message_func=check_last_message_func,
            send_function=send_function,
            sticky_messages=sticky,
        )
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
            with otel_span_wrapper('message_dispatcher.process_mutable',
                                   attributes={'key': item.key, 'discord.guild': guild_id}):
                await self._process_mutable(item.key)
        elif isinstance(item, _DeleteItem):
            self.logger.debug(f'MessageDispatcher :: deleting message {item.message_id} in channel {item.channel_id} for guild {guild_id}')
            with otel_span_wrapper('message_dispatcher.delete',
                                   attributes={'discord.channel': item.channel_id, 'discord.guild': guild_id}):
                try:
                    channel = await self.bot.fetch_channel(item.channel_id)
                    msg = channel.get_partial_message(item.message_id)
                    await msg.delete()
                except NotFound:
                    pass
        elif isinstance(item, _ReadItem):
            self.logger.debug(f'MessageDispatcher :: fetching object for guild {guild_id}')
            with otel_span_wrapper('message_dispatcher.fetch',
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
        elif isinstance(item, _SendItem):
            channel = self.bot.get_channel(item.channel_id)
            if channel is None:
                self.logger.warning(f'MessageDispatcher :: channel {item.channel_id} not found for send')
                return
            self.logger.debug(f'MessageDispatcher :: sending to channel {item.channel_id} guild {guild_id}')
            with otel_span_wrapper('message_dispatcher.send',
                                   attributes={'discord.channel': item.channel_id, 'discord.guild': guild_id}):
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

        # If the channel changed in the pending update, rebuild the bundle functions
        if bundle.channel_id != pending.channel_id:
            self.logger.debug(f'MessageDispatcher :: channel changed for "{key}": {bundle.channel_id} -> {pending.channel_id}')
            check_func, send_func = self._make_channel_funcs(pending.channel_id)
            delete_funcs = bundle.update_text_channel(
                pending.guild_id, pending.channel_id, check_func, send_func
            )
            await self._execute_funcs(delete_funcs)

        content = pending.content
        delete_after = pending.delete_after

        # Sticky check: should we clear old messages?
        should_clear = False
        if bundle.sticky_messages or len(content) <= len(bundle.message_contexts):
            should_clear = await async_retry_discord_message_command(
                partial(bundle.should_clear_messages)
            )

        funcs = bundle.get_message_dispatch(content, clear_existing=should_clear,
                                            delete_after=delete_after)

        # Execute and collect new Message objects
        results = []
        for func in funcs:
            # Safety truncation: Discord max is 2000 chars
            if hasattr(func, 'keywords') and 'content' in func.keywords:
                content_val = func.keywords['content']
                if len(content_val) > 2000:
                    self.logger.warning(
                        f'MessageDispatcher :: truncating message over 2000 chars for bundle "{key}"'
                    )
                    new_kwargs = func.keywords.copy()
                    new_kwargs['content'] = content_val[:1900]
                    func = partial(func.func, *func.args, **new_kwargs)
            result = await async_retry_discord_message_command(func)
            if result and hasattr(result, 'id'):
                results.append(result)

        # Update message references for newly sent messages
        contexts_needing_refs = [ctx for ctx in bundle.message_contexts if not ctx.message]
        for i, message in enumerate(results):
            if i < len(contexts_needing_refs):
                contexts_needing_refs[i].set_message(message)

        self.logger.debug(
            f'MessageDispatcher :: mutable "{key}" dispatched {len(funcs)} op(s), '
            f'{len(results)} new message(s)'
        )

        # If delete_after is set the bundle is ephemeral – pop it
        if delete_after is not None:
            self.logger.debug(f'MessageDispatcher :: removing ephemeral bundle "{key}"')
            self._bundles.pop(key, None)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def _execute_funcs(self, funcs: List[Callable]):
        for func in funcs:
            await async_retry_discord_message_command(func)
