'''
Shared decode helpers for dispatch result payloads.

Used by both HttpDispatchClient and RedisDispatchClient so the decoding
logic lives in exactly one place.
'''
from discord_bot.types.dispatch_result import ChannelHistoryResult, GuildEmojisResult
from discord_bot.types.fetched_message import FetchedMessage


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
