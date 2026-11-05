'''
The commands seam: command forwarding and guild teardown, as the commands pod serves them.

``args`` in the forward body is validated against a model that depends on the ``{command}`` in
the path (``COMMAND_ARGS``), so the document types it as an object and the per-command shapes
live in ``discord_core/types/command_args.py``.
'''
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

from discord_core.routes import commands as command_routes
from discord_core.routes import guild_teardown as teardown_routes
from discord_core.types.command_envelope import CommandEnvelope
from discord_core.types.command_wire import CommandAcceptedResponse, CommandRequestBody
from discord_core.types.guild_teardown import GuildTeardownAccepted

from discord_commands.servers.commands_server import CommandsHttpServer
from discord_commands.workers.command_executor import CommandExecutor

from tests.routes.specs._model import Call, Operation, reply, serving

ENVELOPE = CommandEnvelope(guild_id=1, channel_id=2, author_id=3, author_display_name='tyler')


async def _hello(_envelope, _args):
    '''A command that does nothing.'''


@asynccontextmanager
async def environment():
    '''A commands pod serving `hello` only, over a real executor and a stand-in teardown.'''
    executor = CommandExecutor({'hello': _hello})
    try:
        async with serving(CommandsHttpServer(executor, MagicMock())) as env:
            yield env
    finally:
        await executor.close()


def forward(request_id: str = 'r-1') -> dict:
    '''A forward body for a command that takes no arguments.'''
    return CommandRequestBody(envelope=ENVELOPE, request_id=request_id).model_dump(mode='json')


async def already_sent(env) -> dict:
    '''The same request id accepted once already.'''
    resp = await env.client.post('/commands/hello', json=forward())
    assert resp.status == 202
    return {}


OPERATIONS = {
    command_routes.FORWARD_COMMAND: Operation(
        'Hand a command to the pod. 202 means accepted, not run.', request=CommandRequestBody,
        replies=(reply(202, CommandAcceptedResponse, doc='Taken. `duplicate` is true when this '
                                                          'request_id was accepted before.'),
                 reply(422, doc='Not JSON, does not fit the body, unknown command, or bad arguments.'),
                 reply(501, doc='A known command this build has no handler for.')),
        calls=(Call(202, body=forward(), path={'command': 'hello'}),
               Call(202, body=forward(), path={'command': 'hello'}, arrange=already_sent),
               Call(422, body={}, path={'command': 'hello'}),
               Call(501, body=forward(), path={'command': 'meta'}))),
    teardown_routes.TEARDOWN_GUILD: Operation(
        "Start releasing a guild's queues. 202 means started, not finished.", path={'guild_id': int},
        replies=(reply(202, GuildTeardownAccepted), reply(422, doc='guild_id is not an integer.')),
        calls=(Call(202, path={'guild_id': 5}), Call(422, path={'guild_id': 'abc'}))),
}
