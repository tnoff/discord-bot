import asyncio
from contextlib import asynccontextmanager
from functools import cached_property
from typing import Optional

from discord.ext.commands import Cog, Bot
from pydantic import BaseModel, ValidationError as PydanticValidationError

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncEngine


from discord_bot.exceptions import CogMissingRequiredArg
from discord_bot.utils.common import get_logger, LoggingConfig
from discord_bot.utils.sql_retry import async_retry_database_commands
from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
)

_UNSET = object()

class CogHelper(Cog):
    '''
    Cogs usually have the following bits
    '''

    _message_delete_after: int | None = None
    REQUIRED_TABLES: list[str] = []

    def __init__(self, bot: Bot, settings: dict, db_engine: AsyncEngine,
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
        self._init_task = None
        self._result_queue: asyncio.Queue | None = None

        # Setup config validation
        if config_model:
            try:
                self.config = config_model.model_validate(settings.get(settings_prefix, {}))
            except PydanticValidationError as exc:
                raise CogMissingRequiredArg(f'Invalid config given for {settings_prefix}', str(exc)) from exc

    async def gate_tasks_on_db_restore(self, start_tasks_fn):
        '''
        If DatabaseBackup is loaded and exposes wait_for_tables, wait for this cog's
        REQUIRED_TABLES to be restored before calling start_tasks_fn.
        Otherwise call start_tasks_fn immediately.
        '''
        backup_cog = self.bot.get_cog('DatabaseBackup')
        if backup_cog and hasattr(backup_cog, 'wait_for_tables'):
            self._init_task = self.bot.loop.create_task(
                self._await_restore_then_start(backup_cog, start_tasks_fn)
            )
        else:
            start_tasks_fn()

    async def _await_restore_then_start(self, backup_cog, start_tasks_fn):
        await backup_cog.wait_for_tables(self.REQUIRED_TABLES)
        self.logger.info(f'{type(self).__name__} :: Required tables restored, starting DB tasks')
        start_tasks_fn()

    @asynccontextmanager
    async def with_db_session(self):
        '''
        Yield an async db session from engine
        '''
        session_factory = async_sessionmaker(bind=self.db_engine, class_=AsyncSession, expire_on_commit=False)
        async with session_factory() as db_session:
            yield db_session

    @cached_property
    def _dispatcher(self):
        dispatcher = self.bot.get_cog('MessageDispatcher')
        if dispatcher is None:
            raise RuntimeError('MessageDispatcher cog is required but not loaded')
        return dispatcher

    def register_result_queue(self) -> None:
        '''Register this cog with MessageDispatcher to receive a result queue.'''
        self._result_queue = self._dispatcher.register_cog_queue(self._cog_name)

    async def dispatch_fetch(self, guild_id: int, func, **retry_kwargs):
        '''
        Fetch a Discord object through MessageDispatcher (LOW priority).
        '''
        return await self._dispatcher.fetch_object(guild_id, func, **retry_kwargs)

    async def dispatch_channel_history(
        self,
        guild_id: int,
        channel_id: int,
        limit: int = 100,
        after=None,
        after_message_id: int | None = None,
        oldest_first: bool = True,
    ) -> None:
        '''
        Submit a channel history fetch request (fire-and-forget).

        Results are delivered to self._result_queue as ChannelHistoryResult objects.
        Call register_result_queue() before using this method.
        '''
        await self._dispatcher.submit_request(FetchChannelHistoryRequest(
            guild_id=guild_id,
            channel_id=channel_id,
            limit=limit,
            after=after,
            after_message_id=after_message_id,
            oldest_first=oldest_first,
            cog_name=self._cog_name,
        ))

    async def dispatch_guild_emojis(self, guild_id: int, max_retries: int = 3) -> None:
        '''
        Submit a guild emoji fetch request (fire-and-forget).

        Results are delivered to self._result_queue as GuildEmojisResult objects.
        Call register_result_queue() before using this method.
        '''
        await self._dispatcher.submit_request(FetchGuildEmojisRequest(
            guild_id=guild_id,
            cog_name=self._cog_name,
            max_retries=max_retries,
        ))

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
        await self._dispatcher.submit_request(SendRequest(
            guild_id=guild_id,
            channel_id=channel_id,
            content=content,
            delete_after=delete_after,
        ))
        return content

    async def dispatch_delete(self, guild_id: int, channel_id: int, message_id: int) -> None:
        '''
        Delete a Discord message by ID through MessageDispatcher (NORMAL priority).
        '''
        await self._dispatcher.submit_request(DeleteRequest(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
        ))

    async def retry_commit(self, db_session: AsyncSession):
        '''
        Common function to retry db_session commit
        db_session: Sqlalchemy async db session
        '''
        await async_retry_database_commands(db_session, db_session.commit)
