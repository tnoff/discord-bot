'''
Client-side seam check: does my peer serve the routes I call, and for how long
has it not?

Acceptance criteria five and six of docs/projects/http-seam-contract.md. The
project exists because tolerating a skew is not the same as detecting one —
"peer not upgraded yet" is only benign if *yet* is bounded, and nothing bounded
it. A pin that never advances is indistinguishable, to the client, from a roll
that has not finished. This puts a clock on that.

**Subset, not equality.** A peer serving MORE routes than this client calls is
the normal steady state and must stay silent; pods update at different rates by
design. Only a route this client actually calls, absent from the peer, counts.

**Four states, and only two of them start the clock.** The distinction is the
whole design:

- OK — everything this client calls is served. Clock cleared.
- ROUTES_MISSING — the peer answered, and is missing something. Clock runs.
- NO_CONTRACT_ENDPOINT — the peer 404s the contract route, so it predates the
  advertisement. Clock runs, under its own reason label, because during the
  rollout of that feature this is expected everywhere and an operator will want
  to alert on it separately rather than not at all.
- UNREACHABLE — could not ask. Clock is left ALONE, neither started nor
  cleared. A peer being down is a different fault with its own heartbeat alert,
  and letting it clear the clock would let a flapping peer reset the deadline
  forever while a real mismatch sat underneath.

**It can never stop a pod from starting.** Every probe swallows its own
exceptions, the first probe is not awaited on the construction path, and a
verdict is only ever advisory. A hard failure here would turn a transient into
exactly the permanent outage the 2026-07-31 incident was.
'''
import asyncio
import logging
import time
from enum import Enum

import aiohttp
from opentelemetry.metrics import Observation

from discord_bot.routes import contract
from discord_bot.routes.route import Route
from discord_bot.utils.otel import AttributeNaming, MetricNaming, create_observable_gauge

logger = logging.getLogger(__name__)

# A peer that 404s the contract route is running a build from before the route
# advertisement landed, which is different from a peer that answers and is
# missing seam routes.
_NOT_FOUND = 404

# One rolling update's worth of slack. The 2026-07-31 skew window was ~20
# seconds; the 2026-08-03 pin skew was ~9.5 hours. Anything between a generous
# roll and the shortest outage worth paging on separates the two, and 5 minutes
# leaves room for an image pull on a cold node.
DEFAULT_GRACE_SECONDS = 300.0

DEFAULT_INTERVAL_SECONDS = 60.0


class PeerContractStatus(Enum):
    '''Outcome of one probe. See the module docstring for why UNREACHABLE differs.'''
    OK = 'ok'
    ROUTES_MISSING = 'routes_missing'
    NO_CONTRACT_ENDPOINT = 'no_contract_endpoint'
    UNREACHABLE = 'unreachable'


#: The two states that mean the peer gave a definite, wrong answer.
_MISMATCH_STATES = (PeerContractStatus.ROUTES_MISSING,
                    PeerContractStatus.NO_CONTRACT_ENDPOINT)


class SeamContractCheck:
    '''Ask a peer which routes it serves, and time how long any gap persists.'''

    def __init__(self, seam: str, base_url: str, routes_called: tuple[Route, ...],
                 session_factory, grace_seconds: float = DEFAULT_GRACE_SECONDS,
                 clock=time.monotonic):
        '''
        seam : Low-cardinality label for the metric, e.g. 'broker'
        base_url : Root URL of the peer
        routes_called : Routes this client actually calls, from the seam registry
        session_factory : Zero-arg callable returning an aiohttp ClientSession;
                          the owning client's, so sessions are not duplicated
        grace_seconds : How long a mismatch may persist before it is a breach
        clock : Monotonic time source, injectable for tests
        '''
        self._seam = seam
        self._base_url = base_url.rstrip('/')
        self._called = {(route.method, route.template) for route in routes_called}
        self._session_factory = session_factory
        self._grace = grace_seconds
        self._clock = clock
        self._status: PeerContractStatus | None = None
        self._missing: set[tuple[str, str]] = set()
        self._mismatch_since: float | None = None
        self._task: asyncio.Future | None = None

    @property
    def status(self) -> PeerContractStatus | None:
        '''Last probe outcome, or None before the first definite answer.'''
        return self._status

    @property
    def missing_routes(self) -> set[tuple[str, str]]:
        '''Routes this client calls that the peer did not advertise.'''
        return set(self._missing)

    @property
    def probe_task(self):
        """The running probe task, or None. Read-only, and public so callers and
        tests can see whether a check is live without reaching into the object."""
        return self._task

    @property
    def breached(self) -> bool:
        '''True once a mismatch has outlived the grace window.'''
        if self._mismatch_since is None:
            return False
        return (self._clock() - self._mismatch_since) >= self._grace

    async def probe(self) -> PeerContractStatus:
        '''Ask the peer once and fold the answer into the clock.

        Swallows the failure families a probe can realistically hit — connection
        errors, timeouts, malformed bodies, and a session closed underneath it
        during shutdown. This runs on a client's startup path, and the one thing
        it must not do is stop a pod from coming up.
        '''
        status, missing = await self._ask()
        self._status = status
        self._missing = missing
        if status is PeerContractStatus.OK:
            if self._mismatch_since is not None:
                logger.info('seam %s: peer contract satisfied again', self._seam)
            self._mismatch_since = None
        elif status in _MISMATCH_STATES:
            if self._mismatch_since is None:
                self._mismatch_since = self._clock()
                logger.warning('seam %s: peer contract mismatch (%s), missing %s; '
                               'clock started, %.0fs of grace',
                               self._seam, status.value, sorted(missing), self._grace)
            elif self.breached:
                logger.error('seam %s: peer contract mismatch (%s) has persisted %.0fs, '
                             'past the %.0fs grace; missing %s. A roll would have '
                             'finished by now — check the image pins.',
                             self._seam, status.value,
                             self._clock() - self._mismatch_since, self._grace,
                             sorted(missing))
        return status

    async def _ask(self) -> tuple[PeerContractStatus, set]:
        '''One HTTP call, mapped onto a status. Swallows every failure.'''
        url = f'{self._base_url}{contract.CONTRACT_ROUTE.template}'
        try:
            session = self._session_factory()
            async with session.get(url) as response:
                if response.status == _NOT_FOUND:
                    return PeerContractStatus.NO_CONTRACT_ENDPOINT, set(self._called)
                response.raise_for_status()
                served = contract.decode(await response.json())
        # RuntimeError covers aiohttp raising 'Session is closed' when the
        # owning client shuts down while a probe is in flight — a normal race on
        # the teardown path, not a seam fault.
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError,
                RuntimeError) as error:
            logger.debug('seam %s: could not read peer contract: %s', self._seam, error)
            return PeerContractStatus.UNREACHABLE, set()
        missing = self._called - served
        if missing:
            return PeerContractStatus.ROUTES_MISSING, missing
        return PeerContractStatus.OK, set()

    async def run(self, interval: float = DEFAULT_INTERVAL_SECONDS) -> None:
        """Probe forever. Intended to be launched as a task, never awaited inline."""
        while True:
            await self.probe()
            await asyncio.sleep(interval)

    def start(self, interval: float = DEFAULT_INTERVAL_SECONDS) -> None:
        """Launch the probe loop as a background task.

        Fire-and-forget on purpose, and the reason the whole class swallows its
        own failures: this is called on a client's construction path, and a peer
        that is simply not up yet is the NORMAL case during a roll. Blocking or
        raising here would turn a transient into exactly the permanent outage the
        2026-07-31 incident was.

        Calling it twice is a no-op rather than a second task, so a client that
        gets re-wired does not end up double-probing its peer.
        """
        if self._task is not None and not self._task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No RUNNING event loop. The pods build their clients in synchronous
            # run() functions, before asyncio.run, so calling start() there is the
            # easy mistake when wiring a new caller — and on a pod's startup path
            # it must degrade to "no check" rather than to a crash loop. Logged at
            # WARNING because the check silently not running is worth noticing.
            #
            # get_running_loop rather than ensure_future: ensure_future falls back
            # to get_event_loop, which on 3.12 returns a non-running loop instead
            # of raising and on 3.14 raises. Asking directly makes the branch
            # behave the same on every version the tox matrix covers.
            logger.warning('seam %s: contract check not started, no running event '
                           'loop — call start_seam_check from an async context',
                           self._seam)
            return
        self._task = loop.create_task(self.run(interval))

    async def stop(self) -> None:
        """Cancel the probe loop and wait for it to unwind. Safe if never started."""
        if self._task is None:
            return
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None

    def observations(self, _options=None):
        '''OTEL observable-gauge callback: 1 once a mismatch outlives the grace.

        Emits NOTHING before the first definite answer, rather than a 0. A
        permanently-0 series for a check that has never run reads as "verified
        healthy" on a dashboard, which is the opposite of the truth, and the
        same trap `loop_heartbeat_observations` avoids for loops that do not run
        in a given process.
        '''
        if self._status is None or self._status is PeerContractStatus.UNREACHABLE:
            return []
        reason = self._status.value if self.breached else PeerContractStatus.OK.value
        return [Observation(1 if self.breached else 0, attributes={
            AttributeNaming.SEAM.value: self._seam,
            AttributeNaming.SEAM_CONTRACT_REASON.value: reason,
        })]

    def register_gauge(self, meter_provider) -> None:
        '''Publish the breach gauge for this seam.'''
        create_observable_gauge(meter_provider, MetricNaming.SEAM_CONTRACT_BREACH.value,
                                self.observations,
                                '1 when a peer has been missing a called route longer '
                                'than the grace window')
