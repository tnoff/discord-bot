'''
HTTP client the gateway uses to forward commands to the commands pod (#1030, Phase 2).

Routes are declared in routes/commands.py (the teardown route in routes/guild_teardown.py, which it
includes) and are this client's `ROUTES_CALLED`, so the gateway checks that the commands pod
advertises `POST /commands/{command}` and the teardown route the same way it checks the broker
and dispatcher. The pod has served the route since the skeleton (#1116).

Unlike the broker and dispatch clients this one does not use `HttpClientMixin._http`, because that
retries transport errors with 1/2/4s backoff and a command is not idempotent. It makes one request
with a short timeout, behind a circuit breaker, so that a down pod costs one fast failure per
command instead of a timeout each.

The timeout is the pod's time to answer 202, not the command's run time; the pod only queues the
command. Past it the client cannot know whether the pod took the command, which is why it does not
retry and why the request carries an id the pod can deduplicate on.
'''
import asyncio
import logging
from uuid import uuid4

import aiohttp

from discord_core.clients.http_client_base import HttpClientMixin
from discord_core.interfaces.commands_client import CommandsUnavailable
from discord_core.routes import commands as command_routes
from discord_core.routes import guild_teardown
from discord_core.routes.route import Route
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import CommandAcceptedResponse, CommandRequestBody, parse_args
from discord_core.types.guild_teardown import GuildTeardownAccepted
from discord_core.utils.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

logger = logging.getLogger(__name__)

# How long the pod has to answer 202.
ACCEPT_TIMEOUT_SECONDS = 3.0

_BREAKER_FAILURE_THRESHOLD = 5
_BREAKER_RECOVERY_TIMEOUT = 30.0  # seconds

# One breaker per process, like the dispatch client's: every command goes to the same pod, so they
# should see the same up/down state.
_BREAKER = CircuitBreaker(
    name='commands',
    failure_threshold=_BREAKER_FAILURE_THRESHOLD,
    recovery_timeout=_BREAKER_RECOVERY_TIMEOUT,
)


class HttpCommandsClient(HttpClientMixin):
    '''CommandsClient that POSTs to a remote commands pod.'''

    #: The seam this client speaks, for HttpClientMixin.start_seam_check.
    SEAM = 'commands'
    ROUTES_CALLED = command_routes.ALL

    def __init__(self, base_url: str, session: aiohttp.ClientSession | None = None,
                 seam_contract=None):
        self._base_url = base_url.rstrip('/')
        self._session = session
        self._seam_contract_config = seam_contract

    async def forward(self, command: str, envelope: CommandEnvelope, args: dict | None = None,
                      request_id: str | None = None,
                      span_context: dict | None = None) -> CommandAcceptedResponse:
        '''POST /commands/{command} -- hand the pod one command. Raises CommandsUnavailable.'''
        # Parsed here as well as on the server: a typo in a stub fails in the gateway's tests and
        # never reaches the wire. It is also what fills in the defaults the model declares.
        body = CommandRequestBody(
            envelope=envelope, args=parse_args(command, args or {}).model_dump(),
            request_id=request_id or uuid4().hex, span_context=span_context).model_dump(mode='json')
        url = self._route_url(command_routes.FORWARD_COMMAND, command=command)
        payload = await self._post(command_routes.FORWARD_COMMAND, url, body, command)
        return self._validate(CommandAcceptedResponse, payload)

    async def teardown_guild(self, guild_id: int) -> GuildTeardownAccepted:
        '''
        POST /guilds/{guild_id}/teardown -- ask the pod to clear the guild's queued music work.

        Answers once the pod has taken the job. One attempt like `forward`; unlike a command it is
        idempotent, so the caller may repeat it. Raises CommandsUnavailable.
        '''
        url = self._route_url(guild_teardown.TEARDOWN_GUILD, guild_id=guild_id)
        payload = await self._post(guild_teardown.TEARDOWN_GUILD, url, {}, f'teardown of guild {guild_id}')
        return self._validate(GuildTeardownAccepted, payload)

    async def _post(self, route: Route, url: str, body: dict, label: str):
        '''One request behind the breaker. `label` names what was sent, for the errors.'''
        session = self._get_session()

        async def _call():
            async with session.request(
                    route.method, url, json=body,
                    headers=self._trace_headers(),
                    timeout=aiohttp.ClientTimeout(total=ACCEPT_TIMEOUT_SECONDS)) as resp:
                if 400 <= resp.status < 500:
                    # The sender's bug, not the pod's health: handed back rather than raised so the
                    # breaker, which counts an exception as a failure, does not count it.
                    try:
                        resp.raise_for_status()
                    except aiohttp.ClientResponseError as error:
                        return error
                resp.raise_for_status()
                return await resp.json()

        try:
            payload = await _BREAKER.call(_call)
        except CircuitBreakerOpenError as error:
            raise CommandsUnavailable(f'commands pod breaker is open ({label})') from error
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as error:
            raise CommandsUnavailable(f'commands pod did not take {label}: {error!r}') from error
        except aiohttp.ClientResponseError as error:
            raise CommandsUnavailable(f'commands pod answered {error.status} to {label}') from error
        if isinstance(payload, aiohttp.ClientResponseError):
            raise payload
        return payload
