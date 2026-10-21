'''
Runs accepted commands, one at a time per guild, in the order they were accepted.

``POST /commands/{command}`` answers 202 once a command is queued here, not once it has run
(routes/commands.py), so this is where "`play` then `skip` quickly" is settled: each guild has its
own FIFO and its own worker task, so the pod runs a guild's commands in arrival order and one
guild's slow command holds up no other guild.

A command is not idempotent (``play`` twice queues twice), so nothing here retries. The
``request_id`` the sender attaches is only so a replay of the same request, from a network retry or
a proxy, is dropped: ids seen in the last ``dedup_ttl`` seconds are remembered and a repeat is
reported as a duplicate and not queued.

A command that raises is logged and its span marked as an error; the guild's next command still
runs. The user sees nothing, which is what the gateway's ``on_command_error`` did for the same
exception.
'''
import time
from asyncio import Task, create_task, gather
from collections import OrderedDict, deque
from logging import Logger, getLogger
from typing import Awaitable, Callable

from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode
from pydantic import BaseModel

from discord_core.types.command_envelope import CommandEnvelope
from discord_core.utils.otel import async_otel_span_wrapper, span_links_from_context

OTEL_SPAN_PREFIX = 'commands'

# A handler runs one command: who sent it, then the arguments already validated against the
# command's model in discord_core.types.command_args.
Handler = Callable[[CommandEnvelope, BaseModel], Awaitable[None]]

# Long enough to cover a retry storm or a proxy replay, short enough that the set stays small.
DEFAULT_DEDUP_TTL_SECONDS = 600.0
DEFAULT_DEDUP_MAX_IDS = 10000


class CommandExecutor:
    '''Per-guild FIFO over a registry of command handlers.'''

    def __init__(self, handlers: dict[str, Handler], logger: Logger | None = None,
                 dedup_ttl: float = DEFAULT_DEDUP_TTL_SECONDS,
                 dedup_max_ids: int = DEFAULT_DEDUP_MAX_IDS, time_func=time.monotonic):
        self.handlers = handlers
        self.logger = logger or getLogger(__name__)
        self._dedup_ttl = dedup_ttl
        self._dedup_max_ids = dedup_max_ids
        self._time = time_func
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._pending: dict[int, deque] = {}
        self._workers: dict[int, Task] = {}

    def serves(self, command: str) -> bool:
        '''Whether this build has a handler for the command.'''
        return command in self.handlers

    def submit(self, command: str, envelope: CommandEnvelope, args: BaseModel, request_id: str,
               span_context: dict | None = None) -> bool:
        '''
        Queue a command on its guild's FIFO. Returns True, queueing nothing, when this
        ``request_id`` was already accepted. The command must be one this executor ``serves``.
        '''
        if self._is_duplicate(request_id):
            return True
        guild_id = envelope.guild_id
        self._pending.setdefault(guild_id, deque()).append(
            (command, envelope, args, request_id, span_context))
        if guild_id not in self._workers:
            self._workers[guild_id] = create_task(self._drain(guild_id))
        return False

    async def close(self):
        '''Stop working. Commands still queued are dropped: they were accepted, never run.'''
        workers = list(self._workers.values())
        for worker in workers:
            worker.cancel()
        # return_exceptions: a cancelled worker comes back as CancelledError instead of raising here.
        await gather(*workers, return_exceptions=True)
        self._workers.clear()
        self._pending.clear()

    def _is_duplicate(self, request_id: str) -> bool:
        '''Remember the id, and say whether it was already remembered.'''
        now = self._time()
        while self._seen:
            oldest_id, expires = next(iter(self._seen.items()))
            if expires > now and len(self._seen) <= self._dedup_max_ids:
                break
            del self._seen[oldest_id]
        if request_id in self._seen:
            return True
        self._seen[request_id] = now + self._dedup_ttl
        return False

    async def _drain(self, guild_id: int):
        '''Run one guild's commands, oldest first, until it has none left.'''
        pending = self._pending[guild_id]
        try:
            while pending:
                command, envelope, args, request_id, span_context = pending[0]
                try:
                    await self._run(command, envelope, args, request_id, span_context)
                finally:
                    pending.popleft()
        finally:
            del self._pending[guild_id]
            del self._workers[guild_id]

    async def _run(self, command: str, envelope: CommandEnvelope, args: BaseModel, request_id: str,
                   span_context: dict | None):
        '''Run one command's handler inside a span linked to the gateway span that sent it.'''
        attributes = {'music.guild_id': envelope.guild_id, 'command.name': command,
                      'command.request_id': request_id}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.run', kind=SpanKind.CONSUMER,
                                           attributes=attributes,
                                           links=span_links_from_context(span_context)) as span:
            try:
                await self.handlers[command](envelope, args)
            except Exception:  # pylint: disable=broad-exception-caught
                self.logger.exception(f'Command "{command}" failed in guild {envelope.guild_id}')
                span.set_status(StatusCode.ERROR)
