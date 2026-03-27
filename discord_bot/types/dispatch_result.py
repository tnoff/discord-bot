from dataclasses import dataclass
from typing import Optional


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
