from asyncio import sleep
from datetime import datetime, timedelta, timezone
from logging import Logger
from typing import Awaitable, Callable

from opentelemetry.trace import SpanKind

from discord_core.utils.discord_context import DiscordContextNaming
from discord_core.utils.otel import async_otel_span_wrapper, span_links_from_context

from discord_core.types.dispatch_result import ChannelHistoryResult

# Default for deleting messages after X days
DELETE_AFTER_DEFAULT = 7

# (guild_id, channel_id): ask for the channel's recent history; the answer arrives later as a
# ChannelHistoryResult on the cog's result queue.
RequestHistory = Callable[[int, int], Awaitable[None]]
# (guild_id, channel_id, message_id)
DeleteMessage = Callable[[int, int, int], Awaitable[None]]


class DeleteMessagesLoop:
    '''
    One iteration of each delete-messages loop. Discord-free: see ``loop_logic``.

    ``discord_channels`` is the validated per-channel config as plain dicts
    (``server_id``, ``channel_id``, ``delete_after``).
    '''

    def __init__(self, logger: Logger, discord_channels: list[dict], loop_sleep_interval: float,
                 request_history: RequestHistory, delete_message: DeleteMessage):
        self.logger = logger
        self.discord_channels = discord_channels
        self.loop_sleep_interval = loop_sleep_interval
        self.request_history = request_history
        self.delete_message = delete_message

    def get_channel_config(self, channel_id: int) -> dict:
        '''Return config dict for the given channel_id, or empty dict if not found.'''
        for channel in self.discord_channels:
            if channel['channel_id'] == channel_id:
                return channel
        return {}

    async def request(self) -> None:
        '''
        Producer iteration: submit channel history fetch requests for each configured channel.
        '''
        await sleep(self.loop_sleep_interval)
        async with async_otel_span_wrapper('delete_messages.check'):
            for channel_dict in self.discord_channels:
                guild_id = channel_dict['server_id']
                channel_id = channel_dict['channel_id']
                async with async_otel_span_wrapper('delete_messages.channel_check', kind=SpanKind.CONSUMER, attributes={'discord.channel': channel_id}):
                    self.logger.debug(f'Checking Channel ID {channel_id}')
                    await self.request_history(guild_id, channel_id)

    async def process_result(self, result: ChannelHistoryResult) -> None:
        '''
        Process a single channel history result, deleting old messages.

        Opens its own span linked back to the requesting one: this runs in the
        result-consumer task, by which point the span that dispatched the fetch
        has long closed and these logs would otherwise carry no trace.
        '''
        async with async_otel_span_wrapper('delete_messages.history_result', kind=SpanKind.CONSUMER,
                                           attributes={DiscordContextNaming.CHANNEL.value: result.channel_id,
                                                       DiscordContextNaming.GUILD.value: result.guild_id},
                                           links=span_links_from_context(result.span_context)):
            return await self._apply_result(result)

    async def _apply_result(self, result: ChannelHistoryResult) -> None:
        '''Body of process_result, run inside the consumer span.'''
        if result.error:
            self.logger.error(
                f'DeleteMessages :: Failed to fetch history for channel {result.channel_id} '
                f'in server {result.guild_id}: {result.error}'
            )
            return
        channel_config = self.get_channel_config(result.channel_id)
        delete_after = channel_config.get('delete_after', DELETE_AFTER_DEFAULT)
        cutoff_period = (datetime.now(timezone.utc) - timedelta(days=delete_after))
        for message in result.messages:
            if message.created_at < cutoff_period:
                self.logger.info(
                    f'Deleting message id {message.id}, in channel {result.channel_id}, '
                    f'in server {result.guild_id}'
                )
                await self.delete_message(result.guild_id, result.channel_id, message.id)
