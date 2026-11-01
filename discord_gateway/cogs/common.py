import asyncio

from discord.ext.commands import Cog, Bot, Context
from pydantic import BaseModel, ValidationError as PydanticValidationError

from discord_core.clients.http_commands_client import HttpCommandsClient
from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException
from discord_core.interfaces.commands_client import CommandsClient, CommandsUnavailable
from discord_core.utils.common import get_logger, LoggingConfig, seam_contract_from_settings
from discord_core.utils.otel import capture_span_context

from discord_core.clients.dispatch_client_base import DispatchClientBase
from discord_core.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
)

from discord_core.types.command_envelope import CommandEnvelope

_UNSET = object()

COMMANDS_UNAVAILABLE_MESSAGE = 'Commands are unavailable right now, please try again in a moment'


def build_commands_client(settings: dict) -> HttpCommandsClient:
    '''The client a stub forwards its command through, from ``general.commands_http_url``.'''
    commands_http_url = settings.get('general', {}).get('commands_http_url')
    if not commands_http_url:
        raise DiscordBotException('commands_http_url required to forward commands')
    return HttpCommandsClient(commands_http_url, seam_contract=seam_contract_from_settings(settings))


def envelope_from_ctx(ctx: Context) -> CommandEnvelope:
    '''Read the fields command logic needs off a discord.py Context.'''
    voice = ctx.author.voice
    voice_channel = voice.channel if voice else None
    return CommandEnvelope(
        guild_id=ctx.guild.id,
        channel_id=ctx.channel.id,
        author_id=ctx.author.id,
        author_display_name=ctx.author.display_name,
        channel_name=ctx.channel.name,
        author_voice_channel_id=voice_channel.id if voice_channel else None,
        author_voice_channel_name=voice_channel.name if voice_channel else None,
        author_voice_guild_id=voice_channel.guild.id if voice_channel else None,
        author_role_ids=tuple(role.id for role in getattr(ctx.author, 'roles', ())),
    )


class CogHelperBase(Cog):
    '''Base cog class. Requires an injected dispatcher — production passes
    HttpDispatchClient; tests may pass a MessageDispatcher.'''

    _message_delete_after: int | None = None
    # Set by the cogs whose commands run in the commands pod; see forward_command.
    commands_client: CommandsClient

    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 settings_prefix: str | None = None,
                 config_model: type[BaseModel] | None = None,
                 redis_manager=None):
        '''
        Init a basic cog
        bot                 :   Discord bot object
        settings            :   Common settings config
        dispatcher          :   Dispatch service (MessageDispatcher or HttpDispatchClient)
        settings_prefix     :   (Optional) Settings prefix, will load settings if given
        config_model        :   (Optional) Pydantic model to validate config against;
                                settings_prefix must also be given
        redis_manager       :   (Optional) Shared RedisManager instance for the process
        '''
        if config_model and not settings_prefix:
            raise CogMissingRequiredArg('Config model given but settings prefix not given')

        self._cog_name = (type(self).__name__).lower()
        self.bot = bot
        logging_dict = settings.get('general', {}).get('logging', {})
        self.logging_config = LoggingConfig.model_validate(logging_dict) if logging_dict else None
        self.logger = get_logger(self._cog_name, self.logging_config)
        self.settings = settings
        self.config: BaseModel | None = None
        self._init_task = None
        self._result_queue: asyncio.Queue | None = None
        self.dispatcher = dispatcher
        self.redis_manager = redis_manager

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
        await backup_cog.wait_for_tables(self.REQUIRED_TABLES)  # pylint: disable=no-member
        self.logger.info(f'{type(self).__name__} :: Required tables restored, starting DB tasks')
        start_tasks_fn()

    def register_result_queue(self) -> None:
        '''Register this cog with the dispatcher to receive a result queue.'''
        self._result_queue = self.dispatcher.register_cog_queue(self._cog_name)

    async def dispatch_fetch(self, guild_id: int, func, **retry_kwargs):
        '''Fetch a Discord object through the dispatcher (LOW priority).'''
        return await self.dispatcher.fetch_object(guild_id, func, **retry_kwargs)

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
        await self.dispatcher.submit_request(FetchChannelHistoryRequest(
            guild_id=guild_id,
            channel_id=channel_id,
            limit=limit,
            after=after,
            after_message_id=after_message_id,
            oldest_first=oldest_first,
            cog_name=self._cog_name,
            span_context=capture_span_context(),
        ))

    async def dispatch_guild_emojis(self, guild_id: int, max_retries: int = 3) -> None:
        '''
        Submit a guild emoji fetch request (fire-and-forget).

        Results are delivered to self._result_queue as GuildEmojisResult objects.
        Call register_result_queue() before using this method.
        '''
        await self.dispatcher.submit_request(FetchGuildEmojisRequest(
            guild_id=guild_id,
            cog_name=self._cog_name,
            max_retries=max_retries,
            span_context=capture_span_context(),
        ))

    async def dispatch_message(self, guild_id: int, channel_id: int, content: str,
                               delete_after=_UNSET) -> str:
        '''
        Send *content* to the given channel and return *content*.

        Routes through the dispatcher (NORMAL priority, with retry).
        If delete_after is not provided, falls back to self._message_delete_after.
        Returns content so callers can use ``return await self.dispatch_message(...)``
        as an early-exit that also signals which message was sent.
        '''
        if delete_after is _UNSET:
            delete_after = self._message_delete_after
        await self.dispatcher.submit_request(SendRequest(
            guild_id=guild_id,
            channel_id=channel_id,
            content=content,
            delete_after=delete_after,
            span_context=capture_span_context(),
        ))
        return content

    async def forward_command(self, ctx: Context, command: str, **args) -> None:
        '''
        Hand a parsed command to the commands pod and return once it has accepted it.

        The pod runs the command and replies through the dispatcher, so nothing is returned here.
        If the pod cannot take it the user is told once and the command is dropped: a command is
        not idempotent, so it is not retried (see interfaces/commands_client.py). Must be called
        inside the command's span so the pod's span links back to it. A rejected command (4xx) is
        a bug in the stub and propagates to ``on_command_error``.
        '''
        try:
            await self.commands_client.forward(
                command, envelope_from_ctx(ctx), args, span_context=capture_span_context())
        except CommandsUnavailable as error:
            self.logger.warning(f'Commands unavailable for "{command}" in guild "{ctx.guild.id}": {error}')
            await self.dispatch_message(ctx.guild.id, ctx.channel.id, COMMANDS_UNAVAILABLE_MESSAGE)

    async def dispatch_delete(self, guild_id: int, channel_id: int, message_id: int) -> None:
        '''Delete a Discord message by ID through the dispatcher (NORMAL priority).'''
        await self.dispatcher.submit_request(DeleteRequest(
            guild_id=guild_id,
            channel_id=channel_id,
            message_id=message_id,
            span_context=capture_span_context(),
        ))
