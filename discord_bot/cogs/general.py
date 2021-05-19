from discord.ext import commands

from discord_bot import functions

class General(commands.Cog):
    '''
    General use commands
    '''

    __slots__ = ('bot', 'logger')

    def __init__(self, bot, logger):
        self.bot = bot
        self.logger = logger

    @commands.command(name='hello')
    async def hello(self, ctx):
        '''
        Say hello to the server
        '''
        _, message = functions.hello(ctx, self.logger)
        await ctx.send(message)

    @commands.command(name='roll')
    async def roll(self, ctx, *, number):
        '''
        Get a random number between 1 and number given
        '''
        _status, message = functions.roll(ctx, self.logger, number)
        await ctx.send(message)

    @commands.command(name='windows')
    async def windows(self, ctx):
        '''
        Get an inspirational note about your operating system
        '''
        _, message = functions.windows(ctx, self.logger)
        await ctx.send(message)

    @commands.command(name='meta')
    async def meta(self, ctx):
        '''
        Get meta information for channel and server
        '''
        message = f'```Server id: {ctx.guild.id}\n'\
                  f'Channel id: {ctx.channel.id}\n'\
                  f'User id: {ctx.author.id}```'
        await ctx.send(message)
