'''
HTTP handler for the guild-teardown route, as a mixin for CommandsHttpServer.

Split out of commands_server for the same reason the broker's guild-queue and gateway-intent
handlers are: it is not a command, and its route is declared in discord_core/routes/guild_teardown.py
ahead of the commands registry. Mixed into CommandsHttpServer, which supplies ``_teardown`` (a
GuildTeardown).

A teardown is not a command: it skips the guild's FIFO, and 202 means the pod has started it, not
that the guild's queues are clear (workers/guild_teardown.py). A guild id that is not a number is a
422, the sender's bug.
'''
from typing import Callable

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_core.routes import guild_teardown
from discord_core.routes.route import Route
from discord_core.types.guild_teardown import GuildTeardownAccepted
from discord_core.utils.otel import otel_span_wrapper

from discord_commands.workers.guild_teardown import GuildTeardown


class GuildTeardownHandlersMixin:
    '''The teardown route's handler, for a server that holds a ``_teardown``.'''

    _teardown: GuildTeardown

    def guild_teardown_handlers(self) -> dict[Route, Callable]:
        '''Route -> handler for every guild-teardown route.'''
        return {guild_teardown.TEARDOWN_GUILD: self._handle_teardown}

    async def _handle_teardown(self, request: web.Request) -> web.Response:
        try:
            guild_id = int(request.match_info['guild_id'])
        except ValueError as exc:
            raise web.HTTPUnprocessableEntity() from exc
        with otel_span_wrapper('commands.teardown', context=extract(request.headers), kind=SpanKind.SERVER,
                               attributes={'music.guild_id': guild_id}):
            self._teardown.submit(guild_id)
        return web.json_response(GuildTeardownAccepted().model_dump(), status=202)
