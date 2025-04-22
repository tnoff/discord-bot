from contextlib import contextmanager
from functools import partial

from discord.ext.commands import Cog, Bot
from jsonschema import ValidationError

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session


from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.common import validate_config, get_logger
from discord_bot.utils.sql_retry import retry_database_commands

class CogHelper(Cog):
    '''
    Cogs usually have the following bits
    '''

    def __init__(self, bot: Bot, settings: dict, db_engine: Engine,
                 settings_prefix: str = None, section_schema: dict  = None):
        '''
        Init a basic cog
        bot                 :   Discord bot object
        logger              :   Common python logger obj
        settings            :   Common settings config
        db_engine           :   (Optional) Sqlalchemy db engine
        settings_prefix     :   (Optional) Settings prefix, will load settings if given
        section_schema      :   (Optional) Json schema to use to validate config. settings_prefix must also be given
        '''
        # Check that prefix given if schema also given
        if section_schema and not settings_prefix:
            raise CogMissingRequiredArg('Section schema given but settings prefix not given')

        self._cog_name = (type(self).__name__).lower()
        self.bot = bot
        self.logger = get_logger(self._cog_name, settings.get('general', {}).get('logging', {}))
        self.settings = settings
        self.db_engine = db_engine

        # Setup config
        if section_schema:
            try:
                validate_config(self.settings.get(settings_prefix, {}), section_schema)
            except ValidationError as exc:
                raise CogMissingRequiredArg(f'Invalid config given for {settings_prefix}', str(exc)) from exc

    @contextmanager
    def with_db_session(self):
        '''
        Yield a db session from engine
        '''
        db_session = sessionmaker(bind=self.db_engine)()
        try:
            yield db_session
        finally:
            db_session.close()

    def retry_commit(self, db_session: Session):
        '''
        Common function to retry db_session commit
        db_session: Sqlalchmy db session
        '''
        def commit_changes():
            return db_session.commit()

        return retry_database_commands(db_session, partial(commit_changes))
