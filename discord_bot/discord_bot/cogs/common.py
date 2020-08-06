from discord.ext import commands

class CogHelper(commands.Cog):
    '''
    Cogs usually have the following bits
    '''

    def __init__(self, bot, db_session, logger):
        self.bot = bot
        self.db_session = db_session
        self.logger = logger
