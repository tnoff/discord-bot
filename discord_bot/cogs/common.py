from discord.ext import commands
from sqlalchemy.orm import sessionmaker

from discord_bot.utils import RetryingQuery

class CogHelper(commands.Cog):
    '''
    Cogs usually have the following bits
    '''

    def __init__(self, bot, db_engine, logger, settings):
        self.bot = bot
        self.logger = logger
        self.settings = settings
        self.db_engine = db_engine
        self.db_session = None
        if self.db_engine:
            self.db_session = sessionmaker(bind=db_engine, query_cls=RetryingQuery)()
