'''
Shared wire handling for the persistence-tier HTTP stores.

Three classes now forward a database Protocol to the db pod, and a fourth is
coming. What they share is not the store's shape -- the four groups have almost
nothing in common -- but the envelope: every route is a POST under a group
prefix, every answer is a DatabaseResponse, and an error field in that response
is a failure the pod already retried and must not be retried again from here.

Extracted rather than repeated, and pylint's duplicate-code check is what forced
the timing: three copies of the same eleven lines is exactly the shape that
drifts. It is also the layer that would have to be edited if the envelope ever
gained a field, and doing that in four places is how the two halves of one
contract stop agreeing.

**Nothing here retries.** `_http` already wraps every call in
async_retry_broker_command, which handles the failure this side is nearest to --
the pod being absent or restarting. A DatabaseUnavailable inside the envelope
means the pod was reachable and its store had already exhausted its own retries
against the engine; re-running it from here turns one query into nine attempts.
See types/database_wire for the full split.
'''
import logging

from opentelemetry.trace import SpanKind

from discord_bot.clients.http_client_base import HttpClientMixin
from discord_bot.types.database_wire import DatabaseResponse
from discord_bot.utils.otel import async_otel_span_wrapper

logger = logging.getLogger(__name__)


class HttpStoreBase(HttpClientMixin):
    '''Base for the HTTP implementations of the persistence Protocols.'''

    # Set by each subclass: the span name prefix and the route group.
    SPAN_PREFIX = ''
    ROUTE_PREFIX = ''

    def __init__(self, base_url: str, session=None):
        '''
        base_url : Root URL of the db pod, e.g. http://discord-db:8085
        session : Pre-built aiohttp session; the mixin makes one lazily otherwise
        '''
        self._base_url = base_url.rstrip('/')
        self._session = session

    async def _call(self, route: str, body: dict = None):
        '''
        POST one store route and return its result, or raise its failure.

        route : Route name under the subclass's group prefix
        body : Request body; {} for the routes that take no arguments
        '''
        payload = await self._http('POST', f'{self._base_url}{self.ROUTE_PREFIX}/{route}',
                                   body if body is not None else {})
        response = DatabaseResponse.model_validate(payload)
        if response.error is not None:
            raise response.error.to_exception()
        return response.result

    def _span(self, route: str, attributes: dict = None):
        '''
        Open a client span named for the route being called.

        route : Route name, used as the span suffix
        attributes : Span attributes, or None
        '''
        return async_otel_span_wrapper(f'{self.SPAN_PREFIX}.{route}',
                                       kind=SpanKind.CLIENT, attributes=attributes)
