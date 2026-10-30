from unittest.mock import AsyncMock

import pytest

from discord_core.exceptions import ExitEarlyException

from discord_gateway.cogs.music import Music
from discord_gateway.tests.cogs.test_music import BASE_MUSIC_CONFIG

from tests.helpers import fake_engine, fake_context #pylint:disable=unused-import


@pytest.mark.asyncio()
async def test_process_gateway_intents_idles_when_nothing_is_claimed(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """process_gateway_intents sleeps the short poll interval when the broker has no intents."""
    mock_sleep = mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.intents_logic.poll = AsyncMock(return_value=0)
    await cog.process_gateway_intents()
    mock_sleep.assert_awaited_once_with(0.25)


@pytest.mark.asyncio()
async def test_process_gateway_intents_polls_again_at_once_after_work(mocker, fake_context):  #pylint:disable=redefined-outer-name
    """A poll that took intents does not sleep: more may already be waiting."""
    mock_sleep = mocker.patch('discord_gateway.cogs.music.sleep', return_value=True)
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.intents_logic.poll = AsyncMock(return_value=2)
    await cog.process_gateway_intents()
    mock_sleep.assert_not_called()


@pytest.mark.asyncio()
async def test_process_gateway_intents_bot_shutdown(fake_context):  #pylint:disable=redefined-outer-name
    """process_gateway_intents exits early once the bot is shutting down."""
    cog = Music(fake_context['bot'], BASE_MUSIC_CONFIG, fake_context['dispatcher'])
    cog.bot_shutdown_event.set()
    with pytest.raises(ExitEarlyException):
        await cog.process_gateway_intents()
