import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class FetchChannelHistoryRequest:
    '''Request to fetch channel message history.'''

    guild_id: int
    channel_id: int
    limit: int
    cog_name: str
    after: Optional[datetime] = None
    after_message_id: Optional[int] = None
    oldest_first: bool = True
    type: str = field(default='fetch_history', init=False)


@dataclass
class FetchGuildEmojisRequest:
    '''Request to fetch guild emoji list.'''

    guild_id: int
    cog_name: str
    max_retries: int = 3
    type: str = field(default='fetch_emojis', init=False)


@dataclass
class SendRequest:
    '''Request to send a message to a channel.'''

    guild_id: int
    channel_id: int
    content: str
    delete_after: Optional[int] = None
    type: str = field(default='send', init=False)


@dataclass
class DeleteRequest:
    '''Request to delete a message by ID.'''

    guild_id: int
    channel_id: int
    message_id: int
    type: str = field(default='delete', init=False)


def to_dict(request):
    '''Serialize a request dataclass to a plain dict (for JSON transport in Phase 2).'''
    return dataclasses.asdict(request)
