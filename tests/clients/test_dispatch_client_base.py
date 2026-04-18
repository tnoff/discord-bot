'''Tests for DispatchClientBase — the shared base class for dispatch clients.'''
import asyncio

import pytest

from discord_bot.clients.dispatch_client_base import DispatchClientBase
from discord_bot.types.dispatch_request import FetchChannelHistoryRequest, FetchGuildEmojisRequest


class _ConcreteClient(DispatchClientBase):
    '''Minimal concrete subclass for testing the base class.'''

    def __init__(self):
        self._cog_queues = {}

    async def _do_fetch_history(self, params: dict) -> dict:
        return {'guild_id': params['guild_id'], 'channel_id': params['channel_id'], 'messages': []}

    async def _do_fetch_emojis(self, params: dict) -> dict:
        return {'guild_id': params['guild_id'], 'emojis': []}


# ---------------------------------------------------------------------------
# Abstract transport stubs (lines 50, 54)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_do_fetch_history_raises_not_implemented():
    '''_do_fetch_history raises NotImplementedError when not overridden by a subclass.'''
    base = DispatchClientBase.__new__(DispatchClientBase)
    with pytest.raises(NotImplementedError):
        await base._do_fetch_history({})  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_do_fetch_emojis_raises_not_implemented():
    '''_do_fetch_emojis raises NotImplementedError when not overridden by a subclass.'''
    base = DispatchClientBase.__new__(DispatchClientBase)
    with pytest.raises(NotImplementedError):
        await base._do_fetch_emojis({})  # pylint: disable=protected-access


# ---------------------------------------------------------------------------
# Unregistered cog queue early-return guards (lines 58, 82)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_submit_history_request_drops_when_cog_not_registered():
    '''_submit_history_request returns immediately when the cog queue is not registered.'''
    client = _ConcreteClient()
    request = FetchChannelHistoryRequest(cog_name='unregistered', guild_id=1, channel_id=2, limit=10)
    # Should return without error and without putting anything in any queue
    await client._submit_history_request(request)  # pylint: disable=protected-access
    assert len(client._cog_queues) == 0  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_submit_emojis_request_drops_when_cog_not_registered():
    '''_submit_emojis_request returns immediately when the cog queue is not registered.'''
    client = _ConcreteClient()
    request = FetchGuildEmojisRequest(cog_name='unregistered', guild_id=1)
    await client._submit_emojis_request(request)  # pylint: disable=protected-access
    assert len(client._cog_queues) == 0  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_register_cog_queue_returns_asyncio_queue():
    '''register_cog_queue stores and returns an asyncio.Queue for the named cog.'''
    client = _ConcreteClient()
    q = client.register_cog_queue('my_cog')
    assert isinstance(q, asyncio.Queue)
    assert client._cog_queues['my_cog'] is q  # pylint: disable=protected-access
