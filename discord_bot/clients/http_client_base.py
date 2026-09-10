'''Shared aiohttp session helpers used by HTTP client classes.'''
import aiohttp
from opentelemetry.propagate import inject

from discord_bot.clients.seam_contract import SeamContractCheck
from discord_bot.routes.route import Route
from discord_bot.utils.otel import METER_PROVIDER
from discord_bot.utils.retry import async_retry_broker_command


class HttpClientMixin:
    '''Mixin providing lazy aiohttp session management and trace header injection.'''
    _session: aiohttp.ClientSession | None = None
    # Set by every concrete client's __init__, already rstrip('/')ed. Annotated
    # rather than assigned so _route_url can rely on it without the mixin
    # pretending to own it.
    _base_url: str
    #: Set by subclasses that speak a seam with a route registry: the seam's
    #: low-cardinality name and the routes this client actually calls. Left None
    #: on clients whose seam has no registry yet, which makes start_seam_check a
    #: no-op for them rather than something each one has to know not to call.
    SEAM: str | None = None
    ROUTES_CALLED: tuple = ()
    _seam_check: SeamContractCheck | None = None
    #: Captured at construction, which is synchronous; the probe task is
    #: started later from an async context. Splitting it this way is what
    #: keeps the wiring off the pod's startup path — see start_seam_check.
    _seam_contract_config = None

    def _get_session(self) -> aiohttp.ClientSession:
        '''Return the shared session, creating it lazily on first use.'''
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Stop the seam check if one is running, then close the session.

        Ordered deliberately: a probe in flight against a session that has just
        been closed raises RuntimeError, and while SeamContractCheck survives
        that, not causing it is better than tolerating it.
        """
        if self._seam_check is not None:
            await self._seam_check.stop()
            self._seam_check = None
        if self._session and not self._session.closed:
            await self._session.close()

    def start_seam_check(self, meter_provider=METER_PROVIDER) -> SeamContractCheck | None:
        """Begin checking that this client's peer serves the routes it calls.

        **Call this from an async context**, not from the constructor. Clients
        are built in the pods' synchronous `run()` functions, before
        `asyncio.run`, so starting a task there would raise "no running event
        loop" — on the pod's startup path, which is the one place this mechanism
        must never be able to break. The config is captured at construction and
        the task starts here.

        Lives on the mixin rather than on each client because the bot, downloader
        and search pods all talk to the same broker and should not each
        re-implement the wiring. Returns None, harmlessly, for a client whose
        seam has no registry yet or that was built without a config.
        """
        if self.SEAM is None or not self.ROUTES_CALLED:
            return None
        if self._seam_contract_config is None:
            return None
        if self._seam_check is None:
            self._seam_check = SeamContractCheck(
                self.SEAM, self._base_url, self.ROUTES_CALLED, self._get_session,
                grace_seconds=self._seam_contract_config.grace_seconds)
            self._seam_check.register_gauge(meter_provider)
        self._seam_check.start(self._seam_contract_config.interval_seconds)
        return self._seam_check

    def _trace_headers(self) -> dict[str, str]:
        '''Return headers dict with W3C traceparent injected from the active span, if any.'''
        headers: dict[str, str] = {}
        inject(headers)
        return headers

    def _route_url(self, route: Route, **params: object) -> str:
        '''Absolute URL for one call on `route`, with path parameters filled in.

        The only place a client turns a route into a URL. Callers name the
        shared `routes/<seam>.py` symbol, never a path literal, so the string
        the client requests and the string the server registered come from one
        definition.
        '''
        return f'{self._base_url}{route.path(**params)}'

    async def _call_route(self, route: Route, body: dict | None = None,
                          traced: bool = True, **params: object):
        '''Execute `route` against this client's peer; returns parsed JSON or None.

        Named `_call_route`, not `_call`: `HttpStoreBase._call` already exists on
        the database seam and takes a route NAME string under a prefix. The two
        will converge when that seam gets its own registry; until then the
        distinct name keeps the override honest rather than shadowing it.

        Takes the method from the route rather than a separate argument — the
        verb was the second thing written twice on every seam, and it drifts the
        same way a path does. `**params` fills the route template, so a template
        placeholder may not be named `body` or `traced`; none is.
        '''
        return await self._http(route.method, self._route_url(route, **params),
                                body, traced=traced)

    async def _http(self, method: str, url: str, body: dict | None = None,
                    traced: bool = True):
        '''Execute an HTTP request with retry; returns parsed JSON or None.

        traced=False suppresses the retry wrapper's span; see
        async_retry_broker_command.'''
        session = self._get_session()
        async def _call():
            async with session.request(
                method, url,
                headers=self._trace_headers(),
                json=body,
            ) as resp:
                resp.raise_for_status()
                if resp.content_type == 'application/json':
                    return await resp.json()
                return None
        return await async_retry_broker_command(_call, traced=traced)
