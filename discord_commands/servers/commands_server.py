'''
HTTP server for the commands pod: the gateway's way to hand it a parsed command.
Schedule with asyncio.create_task(server.serve()).

    POST /commands/{command}  -> 202 {status, duplicate}

202 means the pod has queued the command on its guild's FIFO, not that it has run. The reply to the
user goes through the dispatcher. Bodies are validated here and nowhere else:

- 422 for a name that is not a command, a body that does not parse, or arguments that do not fit the
  command's model. It is the sender's bug, and the client does not retry a 4xx.
- 501 for a command the contract knows but this build has no handler for. A 5xx, so a gateway
  rolled ahead of this pod gets "commands unavailable" and not a silently dropped command.
'''
import logging

from aiohttp import web
from opentelemetry.propagate import extract
from opentelemetry.trace import SpanKind

from discord_core.routes import commands as command_routes
from discord_core.servers.base import AiohttpServerBase
from discord_core.types.command_wire import CommandAcceptedResponse, CommandRequestBody, parse_args
from discord_core.utils.otel import otel_span_wrapper

from discord_commands.workers.command_executor import CommandExecutor

logger = logging.getLogger(__name__)


class CommandsHttpServer(AiohttpServerBase):
    '''aiohttp server in front of a CommandExecutor.'''

    # bandit B104: '0.0.0.0' default is intentional — the gateway reaches this pod across the docker/k8s network; callers override host via constructor arg
    def __init__(self, executor: CommandExecutor, host: str = '0.0.0.0', port: int = 8086):  # nosec B104
        super().__init__()
        self._executor = executor
        self._host = host
        self._port = port

    def route_handlers(self) -> dict:
        '''Map every route on the commands seam to the handler that serves it.'''
        return {command_routes.FORWARD_COMMAND: self._handle_forward}

    def build_app(self) -> web.Application:
        '''Build and return the aiohttp Application. Exposed for testing.'''
        app = web.Application(middlewares=[self._get_drain_middleware()])
        self.register_seam_routes(app, self.route_handlers())
        self.add_contract_route(app)
        return app

    async def _handle_forward(self, request: web.Request) -> web.Response:
        command = request.match_info['command']
        ctx = extract(request.headers)
        try:
            body = CommandRequestBody.model_validate(await request.json())
            args = parse_args(command, body.args)
        except Exception as exc:
            # A non-JSON body, a body that does not fit, an unknown command and bad arguments are
            # all the sender's: one 422, never retried by async_retry_broker_command.
            raise web.HTTPUnprocessableEntity() from exc
        if not self._executor.serves(command):
            raise web.HTTPNotImplemented()
        with otel_span_wrapper('commands.accept', context=ctx, kind=SpanKind.SERVER,
                               attributes={'command.name': command}):
            duplicate = self._executor.submit(command, body.envelope, args, body.request_id,
                                              body.span_context)
        return web.json_response(CommandAcceptedResponse(duplicate=duplicate).model_dump(), status=202)
