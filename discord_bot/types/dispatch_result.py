from dataclasses import dataclass
from typing import Optional

from discord_bot.types.fetched_message import FetchedMessage


@dataclass
class ChannelHistoryResult:
    '''Result of a channel history fetch, delivered to a cog result queue.'''
    guild_id: int
    channel_id: int
    messages: list
    after_message_id: Optional[int] = None
    error: Optional[Exception] = None


@dataclass
class GuildEmojisResult:
    '''Result of a guild emoji fetch, delivered to a cog result queue.'''
    guild_id: int
    emojis: list
    error: Optional[Exception] = None


def decode_history_result(payload: dict) -> ChannelHistoryResult:
    '''Decode a raw fetch_history result payload into a ChannelHistoryResult.'''
    messages = [FetchedMessage.from_dict(m) for m in payload.get('messages', [])]
    return ChannelHistoryResult(
        guild_id=payload['guild_id'],
        channel_id=payload['channel_id'],
        messages=messages,
        after_message_id=payload.get('after_message_id'),
    )


def decode_emojis_result(payload: dict) -> GuildEmojisResult:
    '''Decode a raw fetch_emojis result payload into a GuildEmojisResult.'''
    return GuildEmojisResult(
        guild_id=payload['guild_id'],
        emojis=payload.get('emojis', []),
    )
