from dataclasses import dataclass
from typing import Optional

from discord_bot.types.fetched_message import FetchedMessage

# HTTP status Discord returns for a resource that no longer exists.
NOT_FOUND_STATUS = 404
# Discord API error code for "Unknown Message" — the message ID we tracked is gone.
UNKNOWN_MESSAGE_CODE = 10008


@dataclass
class ChannelHistoryResult:
    '''Result of a channel history fetch, delivered to a cog result queue.'''
    guild_id: int
    channel_id: int
    messages: list
    after_message_id: Optional[int] = None
    error: Optional[Exception] = None
    span_context: Optional[dict] = None


@dataclass
class GuildEmojisResult:
    '''Result of a guild emoji fetch, delivered to a cog result queue.'''
    guild_id: int
    emojis: list
    error: Optional[Exception] = None
    span_context: Optional[dict] = None


def encode_error(exc: Exception) -> dict:
    '''
    Serialize *exc* into a JSON-safe detail dict for transport back to the caller.

    The dispatcher runs in a separate process from the cogs, so the exception
    object itself cannot cross the boundary — only str(exc) used to survive,
    which erased the type and left callers unable to tell a recoverable 404 from
    a real failure.  Carrying status/code explicitly is what makes the failure
    mode matchable on the far side; see is_not_found_error.
    '''
    return {
        'message': str(exc),
        'type': type(exc).__name__,
        'status': getattr(exc, 'status', None),
        'code': getattr(exc, 'code', None),
    }


def is_not_found_error(error) -> bool:
    '''
    True when *error* represents a Discord 404.

    Deliberately duck-typed on .status rather than isinstance(error, NotFound):
    a result delivered through the dispatcher carries a DispatchRemoteError
    rebuilt from JSON, never the original discord.NotFound, so an isinstance
    check silently never matches in the split deployment.  Both types expose
    .status, so this matches whichever side raised it.
    '''
    return getattr(error, 'status', None) == NOT_FOUND_STATUS


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
