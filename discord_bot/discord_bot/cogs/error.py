import sys
import traceback

from discord.ext import commands

# https://gist.github.com/EvieePy/7822af90858ef65012ea500bcecf1612
class CommandErrorHandler(commands.Cog):
    '''
    Handle command errors
    '''

    def __init__(self, bot, logger):
        self.bot = bot
        self.logger = logger

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        """The event triggered when an error is raised while invoking a command.
        Parameters
        ------------
        ctx: commands.Context
            The context used for command invocation.
        error: commands.CommandError
            The Exception raised.
        """
        if hasattr(ctx.command, 'on_error'):
            return

        cog = ctx.cog
        if cog:
            if cog._get_overridden_method(cog.cog_command_error) is not None: #pylint:disable=protected-access
                return

        error = getattr(error, 'original', error)

        if isinstance(error, commands.CommandNotFound):
            return await ctx.send('Unknown command, use !help to show all commands')

        self.logger.exception(f'Exception on command "{ctx.command.name}", exception {str(error)}')
        traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)
