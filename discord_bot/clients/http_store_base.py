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

    # Set by each subclass: the span name prefix and the registry group it speaks.
    SPAN_PREFIX = ''
    #: One of routes/database.py's groups. ROUTE_PREFIX is derived from it by
    #: __init_subclass__ rather than declared -- it used to be a literal here and
    #: the identical string was generated server-side.
    GROUP = None
    ROUTE_PREFIX = ''
    #: For HttpClientMixin.start_seam_check. Every store client speaks this seam.
    SEAM = 'database'

    def __init_subclass__(cls, **kwargs):
        """Derive ROUTE_PREFIX and ROUTES_CALLED from GROUP.

        A real class attribute, because ROUTE_PREFIX was one and callers read it
        off the class. Deriving leaves one definition of the prefix instead of the
        two that used to agree by coincidence.

        ROUTES_CALLED is this group ONLY, never the whole seam: a markov client
        must not demand its peer serve the playlist routes. On this tier that is
        not hypothetical -- the groups are configured per pod, so a db pod
        legitimately serves a subset, and requiring all 33 would fire the subset
        check against a correctly-configured peer.
        """
        super().__init_subclass__(**kwargs)
        if cls.GROUP is not None:
            cls.ROUTE_PREFIX = cls.GROUP.prefix
            cls.ROUTES_CALLED = cls.GROUP.all

    def __init__(self, base_url: str, session=None, seam_contract=None):
        '''
        base_url : Root URL of the db pod, e.g. http://discord-db:8085
        session : Pre-built aiohttp session; the mixin makes one lazily otherwise
        seam_contract : a SeamContractConfig enabling the peer route check
        '''
        self._base_url = base_url.rstrip('/')
        self._session = session
        self._seam_contract_config = seam_contract

    async def _call(self, route: str, body: dict = None):
        '''
        POST one store route and return its result, or raise its failure.

        route : Route NAME under this client's group. Resolved through the
                registry, so a name this seam does not define raises KeyError here
                rather than 404ing against a route that never existed -- which on
                this seam is indistinguishable from the supported
                "that store is not configured on this pod".
        body : Request body; {} for the routes that take no arguments
        '''
        payload = await self._call_route(self.GROUP.routes[route],
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
