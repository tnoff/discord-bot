'''
HTTP half of the gateway-intent surface, split out of http_broker_client.

Same shape and reasoning as http_guild_queue: gateway coordination the broker pod hosts, so it
gets its own mixin rather than widening HttpBrokerClient further.  Mixed into HttpBrokerClient,
which supplies _call_route, _route_url, _validate, _trace_headers and _get_session.

Its routes are declared in routes/gateway_intents.py and are part of the broker seam: they are in
routes/broker.py's ALL, so in HttpBrokerClient.ROUTES_CALLED, and the peer route check covers them.

Like the guild-queue mixin, a 404 is NOT swallowed.  An intent that cannot be submitted means a
command silently does nothing, so a broker that predates these routes should fail loudly.
'''
from opentelemetry.trace import SpanKind

from discord_core.routes import gateway_intents as intent_routes
from discord_core.types.broker_responses import (
    ClaimIntentsResponse, CompleteIntentResponse, IntentResultResponse, SubmitIntentResponse,
)
from discord_core.types.gateway_intents import GatewayIntent, IntentKind, IntentResult
from discord_core.utils.otel import async_otel_span_wrapper

_NO_CONTENT_STATUS = 204


class HttpGatewayIntentsMixin:
    '''Gateway-intent calls against a remote BrokerHttpServer.'''

    async def submit_intent(self, guild_id: int, kind: IntentKind, params: dict | None = None) -> str:
        '''POST /guilds/{guild_id}/intents — ask the guild's gateway to act.  Returns the id.'''
        async with async_otel_span_wrapper(
            'broker.submit_intent', kind=SpanKind.CLIENT,
            attributes={'music.guild_id': guild_id, 'music.intent.kind': IntentKind(kind).value},
        ):
            payload = await self._call_route(
                intent_routes.SUBMIT_INTENT, {'kind': IntentKind(kind).value, 'params': params or {}},
                guild_id=guild_id)
        return self._validate(SubmitIntentResponse, payload).intent_id

    async def claim_intents(self, gateway_id: str) -> list[GatewayIntent]:
        '''
        POST /gateways/{gateway_id}/intents/claim — take every pending intent, leasing them.

        The empty (204) path intentionally mints NO span and skips the retry wrapper, like
        poll_guild_queue: this is polled several times a second even when idle, and a span per
        empty poll churns allocations for no signal.  The span is only opened once there is a
        body to parse.
        '''
        url = self._route_url(intent_routes.CLAIM_INTENTS, gateway_id=gateway_id)
        session = self._get_session()
        async with session.request(intent_routes.CLAIM_INTENTS.method, url,
                                   headers=self._trace_headers()) as resp:
            if resp.status == _NO_CONTENT_STATUS:
                return []
            resp.raise_for_status()
            payload = await resp.json()
        async with async_otel_span_wrapper('broker.claim_intents', kind=SpanKind.CLIENT,
                                           attributes={'music.gateway_id': gateway_id}):
            body = self._validate(ClaimIntentsResponse, payload)
        return [GatewayIntent(id=item.id, guild_id=item.guild_id, kind=IntentKind(item.kind),
                              params=item.params) for item in body.intents]

    async def complete_intent(self, intent_id: str, ok: bool, error: str | None = None) -> bool:
        '''POST /intents/{intent_id}/result — the gateway is done.  False if it had expired.'''
        async with async_otel_span_wrapper(
            'broker.complete_intent', kind=SpanKind.CLIENT,
            attributes={'music.intent.id': intent_id},
        ):
            payload = await self._call_route(intent_routes.COMPLETE_INTENT,
                                             {'ok': ok, 'error': error}, intent_id=intent_id)
        return self._validate(CompleteIntentResponse, payload).recorded

    async def get_intent_result(self, intent_id: str) -> IntentResult:
        '''GET /intents/{intent_id}/result — not done yet is an answer, not an error.'''
        async with async_otel_span_wrapper(
            'broker.get_intent_result', kind=SpanKind.CLIENT,
            attributes={'music.intent.id': intent_id},
        ):
            payload = await self._call_route(intent_routes.GET_INTENT_RESULT, intent_id=intent_id)
        body = self._validate(IntentResultResponse, payload)
        return IntentResult(done=body.done, ok=body.ok, error=body.error)
