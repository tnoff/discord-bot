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

_UNSET = object()

class CogHelper(Cog):
    '''
    Cogs usually have the following bits
    '''

    _message_delete_after: int | None = None

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

    async def dispatch_message(self, guild_id: int, channel_id: int, content: str,
                               delete_after=_UNSET) -> str:
        '''
        Send *content* to the given channel and return *content*.

        Routes through MessageDispatcher (NORMAL priority, with retry).
        If delete_after is not provided, falls back to self._message_delete_after.
        Returns content so callers can use ``return await self.dispatch_message(...)``
        as an early-exit that also signals which message was sent.
        '''
        if delete_after is _UNSET:
            delete_after = self._message_delete_after
        self._dispatcher.send_message(guild_id, channel_id, content, delete_after=delete_after)
        return content

    async def dispatch_delete(self, guild_id: int, channel_id: int, message_id: int) -> None:
        '''
        Delete a Discord message by ID through MessageDispatcher (NORMAL priority).
        '''
        self._dispatcher.delete_message(guild_id, channel_id, message_id)

    async def dispatch_guild_emojis(self, guild_id: int, **retry_kwargs) -> list:
        '''
        Fetch guild emojis through MessageDispatcher (LOW priority).
        '''
        async def _fetch():
            guild = await self.bot.fetch_guild(guild_id)
            return await guild.fetch_emojis()
        return await self._dispatcher.fetch_object(guild_id, _fetch, **retry_kwargs)

    async def dispatch_channel_history(
        self,
        guild_id: int,
        channel_id: int,
        limit: int = 100,
        after=None,
        after_message_id: int | None = None,
        oldest_first: bool = True,
    ) -> list:
        '''
        Fetch channel history through MessageDispatcher (LOW priority).

        after               :   datetime or discord.Message; passed directly to channel.history
        after_message_id    :   if given, fetches that message first and uses it as `after`
        '''
        async def _fetch():
            channel = await self.bot.fetch_channel(channel_id)
            after_obj = after
            if after_message_id is not None:
                after_obj = await channel.fetch_message(after_message_id)
            return [m async for m in channel.history(limit=limit, after=after_obj, oldest_first=oldest_first)]
        return await self._dispatcher.fetch_object(guild_id, _fetch)

    def retry_commit(self, db_session: Session):
        '''
        Common function to retry db_session commit
        db_session: Sqlalchmy db session
        '''
        def commit_changes():
            return db_session.commit()

        return retry_database_commands(db_session, partial(commit_changes))
