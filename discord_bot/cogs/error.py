from traceback import format_exc

from discord.ext import commands
from discord_bot.utils.common import get_logger

# https://gist.github.com/EvieePy/7822af90858ef65012ea500bcecf1612
class CommandErrorHandler(commands.Cog):
    '''
    Handle command errors
    '''

    def __init__(self, bot: commands.Bot, settings: dict):
        self.bot = bot
        self.logger = get_logger(type(self).__name__, settings['general'].get('logging', {}))

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

        if ctx.cog:
            if ctx.cog._get_overridden_method(ctx.cog.cog_command_error) is not None: #pylint:disable=protected-access
                return

        error_type = getattr(error, 'original', error)

        if isinstance(error_type, commands.CommandNotFound):
            return await ctx.send('Unknown command, use !help to show all commands')
        if isinstance(error_type, commands.MissingRequiredArgument):
            return await ctx.send(f'Missing required arguments: {error}')
        self.logger.exception(f'Exception on command "{ctx.command.name}", exception {error}', exc_info=True)
        self.logger.error(format_exc())
