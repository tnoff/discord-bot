'''
HTTP half of the corrupt-cache-object report, split out of http_broker_client.

Same shape and reasoning as http_gateway_intents: a call the broker pod answers, in its own mixin
so HttpBrokerClient does not grow further. Mixed into HttpBrokerClient, which supplies
_call_route and _validate.

Its route is declared in routes/cache_corrupt.py and is part of the broker seam: it is in
routes/broker.py's ALL, so in HttpBrokerClient.ROUTES_CALLED, and the peer route check covers it.
'''
from opentelemetry.trace import SpanKind

from discord_core.routes import cache_corrupt as corrupt_routes
from discord_core.types.broker_responses import ReportCorruptCacheObjectResponse
from discord_core.utils.otel import async_otel_span_wrapper


class HttpCacheCorruptMixin:
    '''The report-corrupt call against a remote BrokerHttpServer.'''

    async def report_corrupt_cache_object(self, media_request, key: str) -> bool:
        '''
        POST /cache/report-corrupt — tell the broker a cached object failed its digest.

        The broker reads the object again and evicts it, with its catalog row, only if that read
        fails too. Returns True if it did.

        media_request : The request the object was fetched for; names the catalog row
        key : The object key that failed, e.g. `cache/youtube.<id>.sha256-<digest>.pcm`
        '''
        async with async_otel_span_wrapper(
            'broker.report_corrupt_cache_object', kind=SpanKind.CLIENT,
            attributes={'music.media_request.uuid': str(media_request.uuid)},
        ):
            payload = await self._call_route(
                corrupt_routes.REPORT_CORRUPT_CACHE_OBJECT,
                {'key': key, 'media_request': media_request.model_dump(mode='json')},
            )
        if not payload:
            return False
        return bool(self._validate(ReportCorruptCacheObjectResponse, payload).evicted)
