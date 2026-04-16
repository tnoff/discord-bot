'''Tests for DispatchClientBase — the shared base class for dispatch clients.'''
import pytest

from discord_bot.utils.dispatch_client_base import DispatchClientBase


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
