'''
Request and response bodies for POST /commands/{command} (routes/commands.py).

The request carries the envelope, the command's already-parsed arguments, the gateway's trace
context (so the pod can recreate the `command_wrapper` span) and a `request_id`. The id is for
the pod to drop a duplicate if the network or a proxy replays the request; it is not an invitation
to retry, because commands are not idempotent (`play` twice queues twice) and the client does not.

`args` is a plain dict here and is validated against `COMMAND_ARGS[command]` by `parse_args`,
because which model applies depends on the route's `{command}` and a body model cannot see that.
'''
from typing import Literal

from pydantic import BaseModel

from discord_core.types.command_args import COMMAND_ARGS
from discord_core.types.command_envelope import CommandEnvelope


class UnknownCommand(ValueError):
    '''The name is not in COMMAND_ARGS.'''


class CommandRequestBody(BaseModel):
    '''POST /commands/{command}'''
    envelope: CommandEnvelope
    args: dict = {}
    request_id: str
    span_context: dict | None = None


class CommandAcceptedResponse(BaseModel):
    '''
    202 from POST /commands/{command}: the pod has taken the command, not run it.

    `duplicate` is True when this request_id was seen already and the command was not queued again.
    '''
    status: Literal['ok'] = 'ok'
    duplicate: bool = False


def parse_args(command: str, args: dict) -> BaseModel:
    '''
    The argument model for `command`, filled from `args`.

    Raises UnknownCommand for a name this build does not serve, and pydantic's ValidationError when
    the arguments do not fit. Both are the sender's bug, so the server answers 422 for either.
    '''
    model = COMMAND_ARGS.get(command)
    if model is None:
        raise UnknownCommand(command)
    return model.model_validate(args)
