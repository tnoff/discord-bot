import dataclasses
import inspect
from datetime import datetime, timezone

from discord_bot.types.dispatch_request import (
    FetchChannelHistoryRequest,
    FetchGuildEmojisRequest,
    SendRequest,
    DeleteRequest,
    to_dict,
)


# ---------------------------------------------------------------------------
# FetchChannelHistoryRequest
# ---------------------------------------------------------------------------

def test_fetch_channel_history_request_type_discriminator():
    '''type field is automatically set to fetch_history'''
    req = FetchChannelHistoryRequest(guild_id=1, channel_id=2, limit=10, cog_name='markov')
    assert req.type == 'fetch_history'


def test_fetch_channel_history_request_type_not_in_init():
    '''type field is set automatically and cannot be passed to __init__'''
    sig = inspect.signature(FetchChannelHistoryRequest.__init__)
    assert 'type' not in sig.parameters


def test_fetch_channel_history_request_defaults():
    '''Optional fields default to None/True'''
    req = FetchChannelHistoryRequest(guild_id=1, channel_id=2, limit=50, cog_name='markov')
    assert req.after is None
    assert req.after_message_id is None
    assert req.oldest_first is True


def test_fetch_channel_history_request_asdict_structure():
    '''to_dict includes all fields including type discriminator'''
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    req = FetchChannelHistoryRequest(
        guild_id=10, channel_id=20, limit=5, cog_name='markov',
        after=dt, after_message_id=99, oldest_first=False,
    )
    d = to_dict(req)
    assert d['guild_id'] == 10
    assert d['channel_id'] == 20
    assert d['limit'] == 5
    assert d['cog_name'] == 'markov'
    assert d['after'] == dt
    assert d['after_message_id'] == 99
    assert d['oldest_first'] is False
    assert d['type'] == 'fetch_history'


def test_fetch_channel_history_request_roundtrip():
    '''asdict output can reconstruct an equivalent request'''
    req = FetchChannelHistoryRequest(guild_id=1, channel_id=2, limit=10, cog_name='markov')
    d = dataclasses.asdict(req)
    req2 = FetchChannelHistoryRequest(
        guild_id=d['guild_id'],
        channel_id=d['channel_id'],
        limit=d['limit'],
        cog_name=d['cog_name'],
        after=d['after'],
        after_message_id=d['after_message_id'],
        oldest_first=d['oldest_first'],
    )
    assert req2.guild_id == req.guild_id
    assert req2.channel_id == req.channel_id
    assert req2.type == req.type


# ---------------------------------------------------------------------------
# FetchGuildEmojisRequest
# ---------------------------------------------------------------------------

def test_fetch_guild_emojis_request_type_discriminator():
    '''type field is automatically set to fetch_emojis'''
    req = FetchGuildEmojisRequest(guild_id=5, cog_name='markov')
    assert req.type == 'fetch_emojis'


def test_fetch_guild_emojis_request_default_max_retries():
    '''max_retries defaults to 3'''
    req = FetchGuildEmojisRequest(guild_id=5, cog_name='markov')
    assert req.max_retries == 3


def test_fetch_guild_emojis_request_asdict():
    '''to_dict includes all fields'''
    req = FetchGuildEmojisRequest(guild_id=5, cog_name='markov', max_retries=5)
    d = to_dict(req)
    assert d['guild_id'] == 5
    assert d['cog_name'] == 'markov'
    assert d['max_retries'] == 5
    assert d['type'] == 'fetch_emojis'


# ---------------------------------------------------------------------------
# SendRequest
# ---------------------------------------------------------------------------

def test_send_request_type_discriminator():
    '''type field is automatically set to send'''
    req = SendRequest(guild_id=1, channel_id=2, content='hello')
    assert req.type == 'send'


def test_send_request_delete_after_default():
    '''delete_after defaults to None'''
    req = SendRequest(guild_id=1, channel_id=2, content='hello')
    assert req.delete_after is None


def test_send_request_asdict():
    '''to_dict includes content, delete_after, and type'''
    req = SendRequest(guild_id=1, channel_id=2, content='hi', delete_after=30)
    d = to_dict(req)
    assert d['content'] == 'hi'
    assert d['delete_after'] == 30
    assert d['type'] == 'send'


# ---------------------------------------------------------------------------
# DeleteRequest
# ---------------------------------------------------------------------------

def test_delete_request_type_discriminator():
    '''type field is automatically set to delete'''
    req = DeleteRequest(guild_id=1, channel_id=2, message_id=3)
    assert req.type == 'delete'


def test_delete_request_asdict():
    '''to_dict includes all three ID fields and type'''
    req = DeleteRequest(guild_id=10, channel_id=20, message_id=30)
    d = to_dict(req)
    assert d['guild_id'] == 10
    assert d['channel_id'] == 20
    assert d['message_id'] == 30
    assert d['type'] == 'delete'
