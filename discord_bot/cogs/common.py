import asyncio
from contextlib import contextmanager
from functools import cached_property, partial
from typing import Optional

from discord.ext.commands import Cog, Bot
from pydantic import BaseModel, ValidationError as PydanticValidationError

from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm.session import Session


from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.common import get_logger, LoggingConfig
from discord_bot.utils.sql_retry import retry_database_commands

class CogHelper(Cog):
    '''
    Cogs usually have the following bits
    '''

    def __init__(self, bot: Bot, settings: dict, db_engine: Engine,
                 settings_prefix: str = None,
                 config_model: Optional[type[BaseModel]] = None):
        '''
        Init a basic cog
        bot                 :   Discord bot object
        logger              :   Common python logger obj
        settings            :   Common settings config
        db_engine           :   (Optional) Sqlalchemy db engine
        settings_prefix     :   (Optional) Settings prefix, will load settings if given
        config_model        :   (Optional) Pydantic model to use to validate config. settings_prefix must also be given
        '''
        # Check that prefix given if model also given
        if config_model and not settings_prefix:
            raise CogMissingRequiredArg('Config model given but settings prefix not given')

        self._cog_name = (type(self).__name__).lower()
        self.bot = bot
        logging_dict = settings.get('general', {}).get('logging', {})
        self.logging_config = LoggingConfig.model_validate(logging_dict) if logging_dict else None
        self.logger = get_logger(self._cog_name, self.logging_config)
        self.settings = settings
        self.db_engine = db_engine
        self.config: Optional[BaseModel] = None

        # Setup config validation
        if config_model:
            try:
                self.config = config_model.model_validate(settings.get(settings_prefix, {}))
            except PydanticValidationError as exc:
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

    @cached_property
    def _dispatcher(self):
        dispatcher = self.bot.get_cog('MessageDispatcher')
        if dispatcher is None:
            raise RuntimeError('MessageDispatcher cog is required but not loaded')
        return dispatcher

    async def dispatch_fetch(self, guild_id: int, func, **retry_kwargs):
        '''
        Fetch a Discord object through MessageDispatcher (LOW priority).
        '''
        return await self._dispatcher.fetch_object(guild_id, func, **retry_kwargs)

    async def send_funcs(self, guild_id: int, funcs: list):
        '''
        Enqueue a list of callables through MessageDispatcher (NORMAL priority).
        '''
        result = self._dispatcher.send_single(guild_id, funcs)
        if asyncio.iscoroutine(result):
            await result

    async def dispatch_message(self, guild_id: int, channel_id: int, content: str) -> str:
        '''
        Send *content* to the given channel and return *content*.

        Routes through MessageDispatcher (NORMAL priority, with retry).
        Returns content so callers can use ``return await self.dispatch_message(...)``
        as an early-exit that also signals which message was sent.
        '''
        self._dispatcher.send_message(guild_id, channel_id, content)
        return content

    def retry_commit(self, db_session: Session):
        '''
        Common function to retry db_session commit
        db_session: Sqlalchmy db session
        '''
        def commit_changes():
            return db_session.commit()

        return retry_database_commands(db_session, partial(commit_changes))
