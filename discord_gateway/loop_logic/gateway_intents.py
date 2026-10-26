from asyncio import CancelledError, Task, create_task
from collections import deque
from logging import Logger

from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode

from discord_core.interfaces.broker_client_protocol import BrokerClient
from discord_core.types.gateway_intents import GatewayIntent, IntentKind
from discord_core.utils.otel import async_otel_span_wrapper

from discord_core.interfaces.player_gateway import PlayerGateway

OTEL_SPAN_PREFIX = 'music'

# What a caller is told when handling an intent raised: the detail is in the gateway's own log.
INTERNAL_ERROR = 'The bot hit an internal error'


class GatewayIntentsLoop:
    '''
    Carries out what other pods ask this gateway to do. Discord-free: see ``loop_logic``.

    The cog keeps the loop itself (the shutdown check, the idle sleep and the heartbeat); this class
    does what happens to what the broker handed out. One ``poll`` claims every pending intent for
    every guild. Each guild then works through its own intents in the order the broker took them, on
    its own task, so a slow join in one guild holds up neither another guild nor the next poll.

    Every intent is completed, including the fire and forget kinds: a claimed intent that is never
    completed is handed out again when its lease lapses. Redelivery is safe because every kind is
    idempotent, but an intent still running here is not run a second time.
    '''

    def __init__(self, logger: Logger, broker_client: BrokerClient, player_gateway: PlayerGateway,
                 gateway_id: str):
        self.logger = logger
        self.broker_client = broker_client
        self.player_gateway = player_gateway
        self.gateway_id = gateway_id
        self._pending: dict[int, deque[GatewayIntent]] = {}
        self._workers: dict[int, Task] = {}
        self._in_flight: set[str] = set()

    async def poll(self) -> int:
        '''Claim what is waiting and hand each intent to its guild. Returns how many were new.'''
        claimed = await self.broker_client.claim_intents(self.gateway_id)
        taken = 0
        for intent in claimed:
            if intent.id in self._in_flight:
                continue
            self._in_flight.add(intent.id)
            self._pending.setdefault(intent.guild_id, deque()).append(intent)
            if intent.guild_id not in self._workers:
                self._workers[intent.guild_id] = create_task(self._drain(intent.guild_id))
            taken += 1
        return taken

    async def close(self):
        '''Stop working on intents. Whatever was unfinished is handed out again by the broker.'''
        workers = list(self._workers.values())
        for worker in workers:
            worker.cancel()
        for worker in workers:
            try:
                await worker
            except CancelledError:
                pass
        self._workers.clear()
        self._pending.clear()
        self._in_flight.clear()

    async def _drain(self, guild_id: int):
        '''Run one guild's intents, oldest first, until it has none left.'''
        pending = self._pending[guild_id]
        try:
            while pending:
                intent = pending[0]
                try:
                    await self._run(intent)
                finally:
                    pending.popleft()
                    self._in_flight.discard(intent.id)
        finally:
            del self._pending[guild_id]
            del self._workers[guild_id]

    async def _run(self, intent: GatewayIntent):
        '''Carry out one intent and record how it went.'''
        attributes = {'music.guild_id': intent.guild_id, 'music.intent.kind': intent.kind.value}
        async with async_otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.gateway_intent', kind=SpanKind.INTERNAL,
                                           attributes=attributes) as span:
            try:
                ok, error = await self._handle(intent)
            except Exception:  # pylint: disable=broad-exception-caught
                self.logger.exception(f'Gateway intent {intent.id} ({intent.kind.value}) failed')
                ok, error = False, INTERNAL_ERROR
                span.set_status(StatusCode.ERROR)
            await self.broker_client.complete_intent(intent.id, ok, error)

    async def _handle(self, intent: GatewayIntent) -> tuple[bool, str | None]:
        '''What each kind means to the player. Returns (ok, text to tell the channel).'''
        params = intent.params
        if intent.kind == IntentKind.JOIN:
            result = await self.player_gateway.ensure(
                intent.guild_id, int(params['voice_channel_id']), int(params['text_channel_id']))
            return result.ok, result.error
        if intent.kind == IntentKind.STOP:
            await self.player_gateway.stop(intent.guild_id, int(params['text_channel_id']))
        elif intent.kind == IntentKind.MOVE_MESSAGES:
            await self.player_gateway.move_messages(intent.guild_id, int(params['text_channel_id']))
        else:
            await self.player_gateway.wake(intent.guild_id)
        return True, None
