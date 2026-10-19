'''
HTTP handlers for the gateway-intent routes, as a mixin for BrokerHttpServer.

Split out of broker_server for the reason the guild-queue handlers are (and the client side is a
mixin): this is gateway coordination the broker pod hosts, not media lifecycle.  Mixed into
BrokerHttpServer, which supplies _gateway_intents (a GatewayIntentRegistry or None) and inherits
_read_body from the server base.

Routes are declared in discord_core/routes/gateway_intents.py.  A result that is not ready, or an
intent that expired, is a 200 body, never an error status (see that module); the only error
statuses are 422, for a request the caller got wrong.
'''
from typing import Callable

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_core.routes import gateway_intents as intent_routes
from discord_core.routes.route import Route
from discord_core.types import broker_responses
from discord_core.types.gateway_intents import IntentKind
from discord_core.utils.otel import otel_span_wrapper


def _guild_id(request: web.Request) -> int:
    try:
        return int(request.match_info['guild_id'])
    except ValueError as exc:
        raise web.HTTPUnprocessableEntity() from exc


class GatewayIntentHandlersMixin:
    '''Gateway-intent route handlers, for a server that holds a GatewayIntentRegistry as _gateway_intents.'''

    def gateway_intent_handlers(self) -> dict[Route, Callable]:
        '''Route -> handler for every gateway-intent route; empty when no registry is configured.'''
        if self._gateway_intents is None:
            return {}
        return {
            intent_routes.SUBMIT_INTENT: self._handle_submit_intent,
            intent_routes.CLAIM_INTENTS: self._handle_claim_intents,
            intent_routes.COMPLETE_INTENT: self._handle_complete_intent,
            intent_routes.GET_INTENT_RESULT: self._handle_get_intent_result,
        }

    async def _handle_submit_intent(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        guild_id = _guild_id(request)
        try:
            kind = IntentKind(body['kind']).value
            params = body.get('params') or {}
            if not isinstance(params, dict):
                raise TypeError('params must be an object')
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.submit_intent', context=ctx, kind=SpanKind.SERVER):
            intent_id = await self._gateway_intents.submit(guild_id, kind, params)
        return web.json_response(broker_responses.SubmitIntentResponse(intent_id=intent_id).model_dump())

    async def _handle_claim_intents(self, request: web.Request) -> web.Response:
        '''POST /gateways/{gateway_id}/intents/claim — 204 when there is nothing to take.

        The empty answer opens no span: this is polled several times a second even when idle, and a
        span per empty poll is allocation churn with no signal (see _handle_poll_guild_queue).
        '''
        claimed = await self._gateway_intents.claim(request.match_info['gateway_id'])
        if not claimed:
            return web.Response(status=204)
        with otel_span_wrapper('broker.claim_intents', context=extract(request.headers),
                               kind=SpanKind.SERVER):
            return web.json_response(broker_responses.ClaimIntentsResponse(
                intents=[broker_responses.IntentBody(**item) for item in claimed]).model_dump())

    async def _handle_complete_intent(self, request: web.Request) -> web.Response:
        ctx, body = await self._read_body(request)
        try:
            ok = bool(body['ok'])
            error = body.get('error')
            error = None if error is None else str(error)
        except Exception as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('broker.complete_intent', context=ctx, kind=SpanKind.SERVER):
            recorded = await self._gateway_intents.complete(request.match_info['intent_id'], ok, error)
        return web.json_response(broker_responses.CompleteIntentResponse(recorded=recorded).model_dump())

    async def _handle_get_intent_result(self, request: web.Request) -> web.Response:
        with otel_span_wrapper('broker.get_intent_result', context=extract(request.headers),
                               kind=SpanKind.SERVER):
            result = await self._gateway_intents.result(request.match_info['intent_id'])
        return web.json_response(broker_responses.IntentResultResponse(**result).model_dump())
