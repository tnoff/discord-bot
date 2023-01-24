from discord.ext import commands
from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm import sessionmaker

DEFAULT_DB_EXCEPTIONS = (OperationalError, PendingRollbackError)

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
            self.db_session = sessionmaker(bind=db_engine)()

    async def check_user_role(self, ctx):
        '''
        Check if user has proper role to run command

        ctx : Standard context
        '''
        try:
            allowed_roles = self.settings['general_allowed_roles'][ctx.guild.id]
        except KeyError:
            # No settings set, assume true
            return True
        # First check if channel key set, if not see if we have an all
        try:
            channel_role = allowed_roles[ctx.channel.id]
        except KeyError:
            try:
                channel_role = allowed_roles['all']
            except KeyError:
                self.logger.warning(f'No role settings for channel {ctx.channel.id}, assuming false')
                return False
        channel_roles = channel_role.split(';;;')
        for role in ctx.author.roles:
            if role.name in channel_roles:
                return True
        return False
