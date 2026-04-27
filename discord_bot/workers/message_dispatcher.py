import asyncio
import contextlib
import logging
from dataclasses import asdict, dataclass
from enum import IntEnum
from functools import cached_property, partial
from typing import Callable, List
import uuid

from discord import Message
from discord.errors import NotFound
from discord.ext.commands import Bot

from opentelemetry import trace

from discord_bot.clients.dispatch_client_base import DispatchClientBase, DispatchRemoteError
from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.interfaces.dispatch_protocols import BundleStore, WorkQueue
from discord_bot.types.fetched_message import FetchedMessage
from discord_bot.types.dispatch_request import DeleteRequest, SendRequest
from discord_bot.utils.discord_retry import async_retry_discord_message_command
from discord_bot.utils.otel import async_otel_span_wrapper, DispatchNaming, span_links_from_context


_DRAIN_TIMEOUT_SECONDS = 30

# Work queue member prefixes — used when building and routing queue members.
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
    LOW = 2     # background reads (channel history, emojis)


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
        '''Serialize this bundle to a JSON-safe dict for persistence.'''
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


class MessageDispatcher(DispatchClientBase):
    '''
    App-wide Discord message dispatcher.

    Owns a configurable number of worker tasks that dequeue items from work_queue
    and execute them.  Bundle state is persisted to bundle_store.

    Both work_queue and bundle_store are required; use AsyncioWorkQueue /
    AsyncioBundleStore for single-process deployments without Redis.
    '''

    def __init__(self, bot: Bot, settings: dict,
                 bundle_store: BundleStore,
                 work_queue: WorkQueue):
        if not settings.get('general', {}).get('include', {}).get('message_dispatcher', True):
            raise CogMissingRequiredArg('MessageDispatcher not enabled')

        self.bot = bot
        self.settings = settings
        self.logger = logging.getLogger('discord_bot.cogs.messagedispatcher')
        self._bundle_store = bundle_store
        self._work_queue = work_queue

        # DispatchClientBase: per-cog result delivery queues
        self._cog_queues: dict[str, asyncio.Queue] = {}
        # asyncio.Event per in-process fetch request, keyed by request_id
        self._result_events: dict[str, asyncio.Event] = {}

        self._shutdown: asyncio.Event = asyncio.Event()
        self._worker_tasks: list[asyncio.Task] = []

    # ------------------------------------------------------------------
    # Service lifecycle
    # ------------------------------------------------------------------

    async def start(self):
        '''Start worker tasks.'''
        for _ in range(self._num_workers):
            self._worker_tasks.append(asyncio.create_task(self._worker_loop()))

    async def stop(self):
        '''Gracefully drain in-flight work and shut down.'''
        self.logger.info('MessageDispatcher :: stop called, draining...')
        self._shutdown.set()
        if self._worker_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._worker_tasks, return_exceptions=True),
                    timeout=_DRAIN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                self.logger.warning('MessageDispatcher :: drain timeout, cancelling workers')
                for task in self._worker_tasks:
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks.clear()
        self.logger.info('MessageDispatcher :: shutdown complete')

    @cached_property
    def _num_workers(self) -> int:
        '''Number of worker coroutines to run (default 4).'''
        return int(self.settings.get('general', {}).get('dispatch_worker_count', 4))

    # ------------------------------------------------------------------
    # DispatchClientBase transport implementations
    # ------------------------------------------------------------------

    async def _do_fetch_history(self, params: dict) -> dict:
        '''Enqueue a fetch_history request and await the result in-process.'''
        request_id = str(uuid.uuid4())
        event = asyncio.Event()
        self._result_events[request_id] = event
        await self.enqueue_fetch_history(
            request_id,
            guild_id=int(params['guild_id']),
            channel_id=int(params['channel_id']),
            limit=int(params['limit']),
            after=params.get('after'),
            after_message_id=int(params['after_message_id']) if params.get('after_message_id') is not None else None,
            oldest_first=bool(params.get('oldest_first', True)),
        )
        await event.wait()
        self._result_events.pop(request_id, None)
        result = await self._work_queue.get_result(request_id)
        if result is None:
            raise DispatchRemoteError(f'no result stored for request_id={request_id}')
        if 'error' in result:
            raise DispatchRemoteError(result['error'])
        return result

    async def _do_fetch_emojis(self, params: dict) -> dict:
        '''Enqueue a fetch_emojis request and await the result in-process.'''
        request_id = str(uuid.uuid4())
        event = asyncio.Event()
        self._result_events[request_id] = event
        await self.enqueue_fetch_emojis(
            request_id,
            guild_id=int(params['guild_id']),
            max_retries=int(params.get('max_retries', 3)),
        )
        await event.wait()
        self._result_events.pop(request_id, None)
        result = await self._work_queue.get_result(request_id)
        if result is None:
            raise DispatchRemoteError(f'no result stored for request_id={request_id}')
        if 'error' in result:
            raise DispatchRemoteError(result['error'])
        return result

    # ------------------------------------------------------------------
    # Public API — fire-and-forget (routes to work_queue)
    # ------------------------------------------------------------------

    def _handle_send(self, request: SendRequest) -> None:
        self.send_message(request.guild_id, request.channel_id, request.content,
                          delete_after=request.delete_after, span_context=request.span_context)

    def _handle_delete(self, request: DeleteRequest) -> None:
        self.delete_message(request.guild_id, request.channel_id, request.message_id,
                            span_context=request.span_context)

    def update_mutable(self, key: str, guild_id: int, content: List[str],
                       channel_id: int | None, sticky: bool = True, delete_after: int | None = None):
        '''Enqueue a mutable bundle update at HIGH priority.'''
        if not content:
            self.logger.debug('update_mutable: empty content for key=%s, routing to remove_mutable', key)
            return self.remove_mutable(key)
        request_uuid = uuid.uuid4()
        trace.get_current_span().set_attribute(DispatchNaming.REQUEST_ID.value, str(request_uuid))
        self.logger.debug('update_mutable: key=%s dispatch.request_id=%s', key, request_uuid)
        asyncio.create_task(self._work_queue.enqueue_unique(
            f'{_MEMBER_MUTABLE}{key}',
            {'key': key, 'guild_id': guild_id, 'content': content,
             'channel_id': channel_id, 'sticky': sticky, 'delete_after': delete_after},
            DispatchPriority.HIGH,
        ))
        return request_uuid

    def remove_mutable(self, key: str):
        '''Enqueue a mutable bundle removal at HIGH priority.'''
        asyncio.create_task(self._work_queue.enqueue_unique(
            f'{_MEMBER_REMOVE}{key}',
            {'key': key},
            DispatchPriority.HIGH,
        ))

    def send_message(self, guild_id: int, channel_id: int, content: str,
                     delete_after: int | None = None, allow_404: bool = False,
                     span_context: dict | None = None):
        '''Enqueue a text message send at NORMAL priority.'''
        asyncio.create_task(self._work_queue.enqueue(
            f'{_MEMBER_SEND}{uuid.uuid4()}',
            {'guild_id': guild_id, 'channel_id': channel_id, 'content': content,
             'delete_after': delete_after, 'allow_404': allow_404, 'span_context': span_context},
            DispatchPriority.NORMAL,
        ))

    def delete_message(self, guild_id: int, channel_id: int, message_id: int,
                       span_context: dict | None = None):
        '''Enqueue a message deletion at NORMAL priority.'''
        asyncio.create_task(self._work_queue.enqueue(
            f'{_MEMBER_DELETE}{uuid.uuid4()}',
            {'guild_id': guild_id, 'channel_id': channel_id,
             'message_id': message_id, 'span_context': span_context},
            DispatchPriority.NORMAL,
        ))

    async def fetch_object(self, guild_id: int, func: Callable,  # pylint: disable=unused-argument
                           max_retries: int = 3, allow_404: bool = False):
        '''
        Fetch a Discord object by running *func* directly.

        guild_id is accepted for API compatibility but not used for routing;
        all in-process fetches run in the caller's task.
        '''
        return await async_retry_discord_message_command(func, max_retries=max_retries, allow_404=allow_404)

    async def enqueue_fetch_history(self, request_id: str, guild_id: int, channel_id: int,
                                    limit: int, after: str | None, after_message_id: int | None,
                                    oldest_first: bool) -> None:
        '''Enqueue a channel history fetch; result stored via work_queue under request_id.'''
        await self._work_queue.enqueue(
            f'{_MEMBER_FETCH_HISTORY}{request_id}',
            {'guild_id': guild_id, 'channel_id': channel_id, 'limit': limit,
             'after': after, 'after_message_id': after_message_id, 'oldest_first': oldest_first},
            DispatchPriority.LOW,
        )

    async def enqueue_fetch_emojis(self, request_id: str, guild_id: int, max_retries: int) -> None:
        '''Enqueue a guild emoji fetch; result stored via work_queue under request_id.'''
        await self._work_queue.enqueue(
            f'{_MEMBER_FETCH_EMOJIS}{request_id}',
            {'guild_id': guild_id, 'max_retries': max_retries},
            DispatchPriority.LOW,
        )

    def update_mutable_channel(self, key: str, guild_id: int, new_channel_id: int):
        '''Enqueue a mutable bundle channel move at HIGH priority.'''
        asyncio.create_task(self._work_queue.enqueue_unique(
            f'{_MEMBER_UPDATE_CHANNEL}{key}',
            {'key': key, 'guild_id': guild_id, 'new_channel_id': new_channel_id},
            DispatchPriority.HIGH,
        ))

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker_loop(self):
        '''Dequeue items from work_queue and dispatch them until shutdown.'''
        while not self._shutdown.is_set():
            result = await self._work_queue.dequeue(timeout=1.0)
            if result is None:
                continue
            member, payload = result
            await self._dispatch_item(member, payload)

    async def _dispatch_item(self, member: str, payload: dict):
        '''Route a work queue item to the appropriate handler based on its member prefix.'''
        if member.startswith(_MEMBER_MUTABLE):
            await self._process_mutable(member[len(_MEMBER_MUTABLE):], payload)
        elif member.startswith(_MEMBER_REMOVE):
            await self._remove_mutable(member[len(_MEMBER_REMOVE):])
        elif member.startswith(_MEMBER_SEND):
            await self._process_send(payload)
        elif member.startswith(_MEMBER_DELETE):
            await self._process_delete(payload)
        elif member.startswith(_MEMBER_UPDATE_CHANNEL):
            await self._process_update_channel(member[len(_MEMBER_UPDATE_CHANNEL):], payload)
        elif member.startswith(_MEMBER_FETCH_HISTORY):
            await self._process_fetch_history(member[len(_MEMBER_FETCH_HISTORY):], payload)
        elif member.startswith(_MEMBER_FETCH_EMOJIS):
            await self._process_fetch_emojis(member[len(_MEMBER_FETCH_EMOJIS):], payload)
        else:
            self.logger.warning('MessageDispatcher :: unknown queue member: %s', member)

    # ------------------------------------------------------------------
    # Item handlers
    # ------------------------------------------------------------------

    async def _process_mutable(self, key: str, payload: dict):
        '''Load bundle from store, acquire lock, execute mutable update, save.'''
        acquired = await self._work_queue.acquire_lock(key)
        if not acquired:
            await self._work_queue.enqueue_unique(f'{_MEMBER_MUTABLE}{key}', payload, DispatchPriority.HIGH)
            return
        try:
            async with async_otel_span_wrapper('message_dispatcher.process_mutable',
                                               attributes={'key': key, 'discord.guild': payload.get('guild_id', 0)}):
                bundle_dict = await self._bundle_store.load(key)
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
                    await self._delete_bundle_from_store(key)
                else:
                    await self._save_bundle_to_store(key, bundle)
        finally:
            await self._work_queue.release_lock(key)

    async def _remove_mutable(self, key: str):
        '''Delete all messages for a bundle and remove it from the store.'''
        async with async_otel_span_wrapper('message_dispatcher.remove_mutable', attributes={'key': key}):
            bundle_dict = await self._bundle_store.load(key)
            if bundle_dict:
                bundle = MessageMutableBundle.from_dict(bundle_dict)
                delete_funcs = bundle.clear_all_messages(self.bot.get_partial_messageable)
                await self._execute_funcs(delete_funcs)
            await self._delete_bundle_from_store(key)

    async def _process_send(self, payload: dict):
        '''Execute a send_message from queue payload.'''
        channel = self.bot.get_partial_messageable(payload['channel_id'])
        async with async_otel_span_wrapper('message_dispatcher.send',
                                           attributes={'discord.channel': payload['channel_id'],
                                                       'discord.guild': payload['guild_id']},
                                           links=span_links_from_context(payload.get('span_context'))):
            await async_retry_discord_message_command(
                partial(channel.send, content=payload['content'], delete_after=payload.get('delete_after')),
                allow_404=payload.get('allow_404', False),
            )

    async def _process_delete(self, payload: dict):
        '''Execute a delete_message from queue payload.'''
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

    async def _process_update_channel(self, key: str, payload: dict):
        '''Move a bundle to a new channel and save updated state to the store.'''
        async with async_otel_span_wrapper('message_dispatcher.update_mutable_channel',
                                           attributes={'key': key, 'discord.guild': payload['guild_id'],
                                                       'discord.channel': payload['new_channel_id']}):
            bundle_dict = await self._bundle_store.load(key)
            if not bundle_dict:
                return
            bundle = MessageMutableBundle.from_dict(bundle_dict)
            delete_funcs = bundle.update_text_channel(
                payload['guild_id'], payload['new_channel_id'], self.bot.get_partial_messageable
            )
            await self._execute_funcs(delete_funcs)
            await self._save_bundle_to_store(key, bundle)

    async def _process_fetch_history(self, request_id: str, payload: dict):
        '''Execute channel history fetch, store result, signal any in-process waiter.'''
        async with async_otel_span_wrapper('message_dispatcher.fetch_history',
                                           attributes={'discord.guild': payload['guild_id'],
                                                       'discord.channel': payload['channel_id']}):
            try:
                result = await self._dispatch_history_and_collect(payload)
            except Exception as exc:  # pylint: disable=broad-except
                # Intentional broad catch: result must always be written so callers do not hang.
                self.logger.error('MessageDispatcher :: fetch history failed: %s', exc, exc_info=True)
                result = {'error': str(exc)}
            await self._work_queue.store_result(request_id, result)
            event = self._result_events.pop(request_id, None)
            if event:
                event.set()

    async def _process_fetch_emojis(self, request_id: str, payload: dict):
        '''Execute guild emoji fetch, store result, signal any in-process waiter.'''
        async with async_otel_span_wrapper('message_dispatcher.fetch_emojis',
                                           attributes={'discord.guild': payload['guild_id']}):
            try:
                result = await self._dispatch_emojis_and_collect(payload)
            except Exception as exc:  # pylint: disable=broad-except
                # Intentional broad catch: same reasoning as _process_fetch_history.
                self.logger.error('MessageDispatcher :: fetch emojis failed: %s', exc, exc_info=True)
                result = {'error': str(exc)}
            await self._work_queue.store_result(request_id, result)
            event = self._result_events.pop(request_id, None)
            if event:
                event.set()

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------

    async def _dispatch_history_and_collect(self, payload: dict) -> dict:
        '''Execute a channel history fetch and return a JSON-safe result dict.'''
        from datetime import datetime  # pylint: disable=import-outside-toplevel
        after_dt = datetime.fromisoformat(payload['after']) if payload.get('after') else None
        channel = await self.bot.fetch_channel(int(payload['channel_id']))
        after_obj = after_dt
        if payload.get('after_message_id') is not None:
            after_obj = await channel.fetch_message(int(payload['after_message_id']))
        messages = [m async for m in channel.history(
            limit=int(payload['limit']),
            after=after_obj,
            oldest_first=bool(payload.get('oldest_first', True)),
        )]
        fetched = [
            FetchedMessage(id=m.id, content=m.content, created_at=m.created_at, author_bot=m.author.bot)
            for m in messages
        ]
        return {
            'guild_id': int(payload['guild_id']),
            'channel_id': int(payload['channel_id']),
            'after_message_id': payload.get('after_message_id'),
            'messages': [m.to_dict() for m in fetched],
        }

    async def _dispatch_emojis_and_collect(self, payload: dict) -> dict:
        '''Execute a guild emoji fetch and return a JSON-safe result dict.'''
        guild = await self.bot.fetch_guild(int(payload['guild_id']))
        emojis = await guild.fetch_emojis()
        return {
            'guild_id': int(payload['guild_id']),
            'emojis': [{'id': e.id, 'name': e.name, 'animated': e.animated} for e in emojis],
        }

    # ------------------------------------------------------------------
    # Channel function factories
    # ------------------------------------------------------------------

    def _make_channel_funcs(self, channel_id: int):
        '''
        Build and return (check_last_message_func, send_function) as closures
        that resolve the channel at call-time via self.bot.get_partial_messageable.
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

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    async def _execute_funcs(self, funcs: List[Callable]):
        for func in funcs:
            await async_retry_discord_message_command(func)

    async def _save_bundle_to_store(self, key: str, bundle: MessageMutableBundle) -> None:
        '''Persist *bundle* to the bundle store; logs and swallows any error.'''
        try:
            await self._bundle_store.save(key, bundle.to_dict())
        except Exception as exc:
            self.logger.error('MessageDispatcher :: failed to save bundle "%s": %s', key, exc)

    async def _delete_bundle_from_store(self, key: str) -> None:
        '''Remove *key* from the bundle store; logs and swallows any error.'''
        try:
            await self._bundle_store.delete(key)
        except Exception as exc:
            self.logger.error('MessageDispatcher :: failed to delete bundle "%s": %s', key, exc)
