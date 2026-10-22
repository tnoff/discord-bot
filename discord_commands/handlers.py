'''
The commands this pod serves: command name (a key of discord_core's COMMAND_ARGS) to its handler.

A command group's handlers are added here in the change that moves its logic into this package,
in the same change that makes the gateway forward it, so a command is never served by neither
pod. A known command with no handler here answers 501 and the gateway, which forwards only the
groups that have moved, never asks for it.
'''
from logging import Logger

from discord_core.types.command_args import RollArgs, UrbanArgs
from discord_core.types.command_envelope import CommandEnvelope

from discord_commands.command_logic.general import GeneralCommands
from discord_commands.command_logic.reply import Reply
from discord_commands.command_logic.urban import UrbanCommands
from discord_commands.workers.command_executor import Handler


def build_handlers(reply: Reply, logger: Logger) -> dict[str, Handler]:
    '''The handler registry the executor is built with.'''
    general = GeneralCommands(reply)
    urban = UrbanCommands(reply, logger)

    async def hello(env: CommandEnvelope, _args) -> None:
        await general.hello(env)

    async def roll(env: CommandEnvelope, args: RollArgs) -> None:
        await general.roll(env, args.input_value)

    async def meta(env: CommandEnvelope, _args) -> None:
        await general.meta(env)

    async def urban_lookup(env: CommandEnvelope, args: UrbanArgs) -> None:
        await urban.word_lookup(env, args.word)

    return {
        'hello': hello,
        'roll': roll,
        'meta': meta,
        'urban': urban_lookup,
    }
