'''
The gateway-intent half of the cog-facing broker surface, on its own.

Split from BrokerClient like GuildQueueClient is: it is gateway coordination the broker pod
happens to host, not part of the media-broker contract.  BrokerClient includes it, so a pod still
depends on one Protocol.

A request that cannot be honoured is a return value, not an exception (see routes/broker.py).
'''
from typing import Protocol

from discord_core.types.gateway_intents import GatewayIntent, IntentKind, IntentResult

__all__ = ['GatewayIntentClient']


class GatewayIntentClient(Protocol):
    '''Handle for asking a gateway to act, and for a gateway to take the request, via the broker.'''
    async def submit_intent(self, guild_id: int, kind: IntentKind, params: dict | None = None) -> str:
        '''Ask the guild's gateway to do something.  Returns the intent id.  Expires unclaimed.'''
    async def claim_intents(self, gateway_id: str) -> list[GatewayIntent]:
        '''Take every pending intent, in submission order, leasing them to this gateway.  Empty
        when there are none.  Non-blocking and polled, so the empty answer is cheap.'''
    async def complete_intent(self, intent_id: str, ok: bool, error: str | None = None) -> bool:
        '''The gateway is done with an intent.  False if it had already expired.'''
    async def get_intent_result(self, intent_id: str) -> IntentResult:
        '''Where an intent got to.  Not done yet is an answer, not an error.'''
