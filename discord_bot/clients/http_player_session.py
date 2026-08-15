'''
HTTP half of the player-session surface, split out of http_broker_client.

Session persistence is player state the broker pod happens to host, not part of
the media-broker contract, so it gets its own mixin rather than widening
HttpBrokerClient further.  See interfaces/player_session_store for the engine
side and the reasoning.

Mixed into HttpBrokerClient, which supplies _base_url, _http and
_log_missing_route.
'''
import aiohttp
from opentelemetry.trace import SpanKind

from discord_bot.types.player_session import PlayerSession
from discord_bot.utils.otel import async_otel_span_wrapper

# A broker that 404s a session route is running a build from before the route
# existed.  The bot and broker pods roll independently, so this is an expected
# (and self-resolving) window during a deploy, not a client error.
PEER_ROUTE_MISSING_STATUS = 404


class HttpPlayerSessionMixin:
    '''Player-session calls against a remote BrokerHttpServer.'''

    async def save_player_session(self, session: PlayerSession) -> None:
        '''PUT /sessions/{guild_id} — persist a guild's player session.

        A 404 is swallowed rather than raised: this runs on the shutdown path,
        where failing costs an aborted teardown, and succeeding-into-nowhere
        costs only that the next startup finds nothing to resume.
        '''
        async with async_otel_span_wrapper(
            'broker.save_player_session', kind=SpanKind.CLIENT,
            attributes={'music.guild_id': session.guild_id},
        ):
            try:
                await self._http('PUT', f'{self._base_url}/sessions/{session.guild_id}',
                                 session.model_dump(mode='json'))
            except aiohttp.ClientResponseError as error:
                if error.status != PEER_ROUTE_MISSING_STATUS:
                    raise
                self._log_missing_route(error.status, '/sessions')

    async def list_player_sessions(self) -> list[PlayerSession]:
        '''GET /sessions — every stored player session.

        A 404 reads as "no sessions", so a bot on a newer image than the broker
        starts up normally with nothing to resume instead of failing its init.
        '''
        async with async_otel_span_wrapper('broker.list_player_sessions', kind=SpanKind.CLIENT):
            try:
                payload = await self._http('GET', f'{self._base_url}/sessions')
            except aiohttp.ClientResponseError as error:
                if error.status != PEER_ROUTE_MISSING_STATUS:
                    raise
                self._log_missing_route(error.status, '/sessions')
                return []
        if payload is None:
            return []
        return [PlayerSession.model_validate(s) for s in payload.get('sessions', [])]

    async def delete_player_session(self, guild_id: int) -> None:
        '''DELETE /sessions/{guild_id}.  A 404 is a no-op for the same reason as save.'''
        async with async_otel_span_wrapper(
            'broker.delete_player_session', kind=SpanKind.CLIENT,
            attributes={'music.guild_id': guild_id},
        ):
            try:
                await self._http('DELETE', f'{self._base_url}/sessions/{guild_id}')
            except aiohttp.ClientResponseError as error:
                if error.status != PEER_ROUTE_MISSING_STATUS:
                    raise
                self._log_missing_route(error.status, '/sessions')
