from discord.ext import commands
from discord_bot.cogs.common import CogHelperBase

# https://gist.github.com/EvieePy/7822af90858ef65012ea500bcecf1612
class CommandErrorHandler(CogHelperBase):
    '''
    Handle command errors
    '''

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
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,
                                               f'"{error}", use !help to show all commands')
        if isinstance(error_type, commands.MissingRequiredArgument):
            return await self.dispatch_message(ctx.guild.id, ctx.channel.id,
                                               f'Missing required arguments: {error}')
        self.logger.error('Exception on command "%s" in guild %s: %s',
                          ctx.command, ctx.guild.id, str(error_type), exc_info=error_type)
