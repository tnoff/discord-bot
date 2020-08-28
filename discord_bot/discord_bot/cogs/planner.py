from discord.ext import commands

from discord_bot.cogs.common import CogHelper


class Planner(CogHelper):
    '''
    Assistant for planning events
    '''

    @commands.group(name='planner', invoke_without_command=False)
    async def planner(self, ctx):
        '''
        Planner functions
        '''
        if ctx.invoked_subcommand is None:
            await ctx.send('Invalid sub command passed...')
