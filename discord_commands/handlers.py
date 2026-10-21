'''
The commands this pod serves: command name (a key of discord_core's COMMAND_ARGS) to its handler.

Empty in the skeleton. A command group's handlers are added here in the change that moves its logic
into this package, in the same change that makes the gateway forward it, so a command is never
served by neither pod. Until then the server answers a known command with 501 and the gateway,
which forwards nothing yet, never asks.
'''
from discord_commands.workers.command_executor import Handler


def build_handlers() -> dict[str, Handler]:
    '''The handler registry the executor is built with.'''
    return {}
