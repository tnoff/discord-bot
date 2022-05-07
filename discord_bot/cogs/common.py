from time import sleep

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

    async def retry_command(self, func, *args, **kwargs):
        '''
        Use retries for the command, mostly deals with db issues
        '''
        max_retries = kwargs.pop('max_retries', 3)
        db_exceptions = kwargs.pop('db_exceptions', DEFAULT_DB_EXCEPTIONS)
        non_db_exceptions = kwargs.pop('non_db_exceptions', ())
        retry = 0
        while True:
            retry += 1
            try:
                return await func(*args, **kwargs)
            except db_exceptions as ex:
                self.logger.exception(f'Hit DB Exception, attempting to retry "{str(ex)}"')
                self.db_session.rollback()
                if retry <= max_retries:
                    sleep_for = 2 ** (retry - 1)
                    sleep(sleep_for)
                    continue
                raise
            except non_db_exceptions as ex:
                self.logger.exception(f'Hit Non-DB Exception, attempting to retry "{str(ex)}"')
                if retry <= max_retries:
                    sleep_for = 2 ** (retry - 1)
                    sleep(sleep_for)
                    continue
                raise
