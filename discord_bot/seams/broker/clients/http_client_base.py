'''Shared aiohttp session helpers used by HTTP client classes.'''
import logging
import weakref

import aiohttp
from opentelemetry.propagate import inject
from pydantic import ValidationError

from discord_bot.seams.broker.clients.seam_contract import SeamContractCheck
from discord_bot.core.exceptions import SeamResponseInvalid
from discord_bot.core.routes.route import Route
from discord_bot.core.utils.otel import AttributeNaming, METER_PROVIDER, MetricNaming
from discord_bot.utils.retry import async_retry_broker_command

logger = logging.getLogger(__name__)

#: Module level, and therefore created exactly once per process. A meter keeps
#: only the FIRST instrument registered under a given name and silently discards
#: the rest, which is how eight of twelve seam checks shipped dark in #951 --
#: every client built its own instrument and only the first pod-wide one
#: survived. One counter here, labelled per call, cannot repeat that.
_RESPONSE_INVALID_COUNTER = METER_PROVIDER.create_counter(
    name=MetricNaming.SEAM_RESPONSE_INVALID.value,
    description='Responses from a peer that this build could not parse',
    unit='1',
)


class SeamClientRegistry:
    """Every client in this process that was built with a seam contract config.

    Enrolment is automatic -- see the _SeamContractConfig descriptor -- and that
    is the whole design. This used to be an explicit argument list handed to
    start_seam_checks, which meant every pod restated which of its clients to
    check, and a client that was built but left off the list was checked by
    nothing while looking entirely healthy. The bot's eight checks are wired in
    TWO files (cli/bot.py and cogs/music.py), so it was the pod most exposed to
    that, and the bot is where the 2026-09-15 regression showed up.

    Holds WEAK references on purpose: a client is owned by the cog or entrypoint
    that built it, never by this registry, and the suite constructs hundreds.
    Enrolment must not be the thing keeping one alive.
    """

    def __init__(self):
        self._clients = weakref.WeakSet()

    def enroll(self, client) -> None:
        """Record a client as needing its peer route check started."""
        self._clients.add(client)

    def withdraw(self, client) -> None:
        """Drop a client so it is not started again. Safe if never enrolled."""
        self._clients.discard(client)

    @property
    def clients(self) -> tuple:
        """Everything currently enrolled, as a snapshot that is safe to iterate."""
        return tuple(self._clients)

    def reset(self) -> None:
        """Forget every client. For tests; no caller in the app."""
        self._clients.clear()


#: Process-wide, like GAUGE_REGISTRY and for the same reason.
SEAM_CLIENTS = SeamClientRegistry()


class _SeamContractConfig:
    """Descriptor enrolling a client the moment it is handed a seam config.

    Hooks the ASSIGNMENT rather than a constructor because there is no shared
    constructor to hook: HttpClientMixin is a mixin, and each of the five
    concrete clients writes `self._seam_contract_config = seam_contract` in its
    own __init__. Those five lines are untouched -- they now enrol as a side
    effect, so a new client enrols by doing the thing it already had to do, and
    there is no additional step for anyone to forget.

    "Was given a config" is exactly the right condition rather than a proxy for
    it: start_seam_check is already a no-op without one, so this enrols
    precisely the set that could ever have been started.
    """

    #: Instance-dict key. A data descriptor takes precedence over the instance
    #: dict, so the attribute name itself would be safe to reuse; a distinct key
    #: makes it obvious at a glance that reads come through here.
    SLOT = '_seam_contract_config_value'

    def __get__(self, client, owner=None):
        if client is None:
            return None
        return client.__dict__.get(self.SLOT)

    def __set__(self, client, config):
        client.__dict__[self.SLOT] = config
        if config is not None:
            SEAM_CLIENTS.enroll(client)


def start_seam_checks() -> None:
    """Start the peer route check on every client this process built.

    **Takes no arguments, and that is the point.** What this replaces is a
    client that exists, speaks a seam, and was simply left off a list -- a
    failure invisible at runtime, because a check that never started looks
    exactly like one that is passing. There is no longer a list to get wrong.

    Safe to call repeatedly and from more than one place: SeamContractCheck.start
    is a no-op while its task is live. The bot calls it from both cog_load and
    on_ready for that reason -- whichever runs first covers the clients built by
    then, and the later call picks up anything built after.

    Call from an async context; see HttpClientMixin.start_seam_check for why
    construction is the wrong place.
    """
    for client in SEAM_CLIENTS.clients:
        client.start_seam_check()


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
    #:
    #: A DESCRIPTOR, not a plain default: assigning it enrols the client with
    #: SEAM_CLIENTS, so start_seam_checks needs no argument list and a client
    #: cannot be built and then left unchecked. Concrete clients assign it
    #: exactly as before and need to know nothing about this.
    _seam_contract_config = _SeamContractConfig()

    @property
    def seam_check(self) -> SeamContractCheck | None:
        """This client's live route check, or None if none was started.

        Read-only, and public so a caller that ran start_seam_checks can see
        which clients actually took -- start_seam_check returns the check, but
        the fan-out helper deliberately returns nothing.
        """
        return self._seam_check

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
        SEAM_CLIENTS.withdraw(self)
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
                grace_seconds=self._seam_contract_config.grace_seconds,
                # Labels WHICH client on the seam. Read off the class because the
                # database stores and the two queue-worker clients derive it in
                # __init_subclass__; '' for the broker, which has no prefix.
                prefix=getattr(self, 'ROUTE_PREFIX', '') or '')
            self._seam_check.register_gauge(meter_provider)
        self._seam_check.start(self._seam_contract_config.interval_seconds)
        return self._seam_check

    def _validate(self, model, payload):
        """Parse a peer's response body, naming the peer if it does not fit.

        **Every `model_validate` on a response body in `clients/` goes through
        here**, enforced by tests/clients/test_response_validation.py. The point
        is not the try/except -- it is that `seam` and `prefix` are read off the
        client rather than passed in, so a caller cannot label a failure with the
        wrong peer, and a new client gets correct attribution by declaring the
        same `SEAM` the route check already needs.

        This is the *reactive* half of the seam contract. The route check in
        seam_contract asks "does my peer serve the routes I call" before any
        traffic; it cannot see a route that still exists but whose body gained a
        required field, because a route-set comparison has nothing to compare.
        Only a real response shows that, and only after it arrives.

        Control flow is unchanged: SeamResponseInvalid propagates exactly where
        the ValidationError did. `async_retry_broker_command` catches neither, so
        neither is mistaken for a retryable transport error, and both land in the
        same broad catch in return_loop_runner. What changes is that the failure
        now says which peer sent it.

        model : The pydantic model to parse into
        payload : The decoded body, or a fragment of it
        """
        try:
            return model.model_validate(payload)
        except ValidationError as error:
            prefix = getattr(self, 'ROUTE_PREFIX', '') or ''
            seam = self.SEAM or ''
            _RESPONSE_INVALID_COUNTER.add(1, {
                AttributeNaming.SEAM.value: seam,
                AttributeNaming.SEAM_PREFIX.value: prefix,
                AttributeNaming.SEAM_RESPONSE_MODEL.value: model.__name__,
            })
            logger.error('seam %s%s: peer sent a body this build cannot parse as %s. '
                         'The route is being served, so the route check cannot see '
                         'this -- suspect a model change that shipped on one side '
                         'only. %s', seam, prefix, model.__name__, error)
            raise SeamResponseInvalid(seam, prefix, model, error) from error

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
