import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock

import pytest
from discord.ext.commands import Bot

from discord_core.cli._lib import gateway
from discord_core.utils.common import GeneralConfig


def _fake_bot(closed=False):
    '''A Bot stand-in supporting `async with` and the close/add_cog calls.'''
    bot = MagicMock()
    bot.__aenter__ = AsyncMock(return_value=bot)
    bot.__aexit__ = AsyncMock(return_value=False)
    bot.add_cog = AsyncMock()
    bot.close = AsyncMock()
    bot.is_closed.return_value = closed
    return bot


def _cog(unload=None):
    '''A cog stand-in whose cog_unload is the given coroutine function (or absent).'''
    cog = MagicMock(spec=['cog_unload'] if unload else [])
    if unload:
        cog.cog_unload = unload
    return cog


def test_build_bot_enables_extra_intents():
    '''Configured intent names are switched on over the default set.'''
    bot = gateway.build_bot(GeneralConfig(intents=['members', 'message_content']))
    assert isinstance(bot, Bot)
    assert bot.intents.members is True
    assert bot.intents.message_content is True
    assert bot.description == 'Discord bot'


def test_build_bot_defaults_leave_privileged_intents_off():
    '''With no extra intents, privileged ones stay at their defaults.'''
    bot = gateway.build_bot(GeneralConfig())
    assert bot.intents.members is False
    assert bot.intents.message_content is False


def test_shutdown_state_truthiness_follows_flag():
    '''ShutdownState is falsy until triggered.'''
    state = gateway.ShutdownState()
    assert not state
    state.triggered = True
    assert state


@pytest.mark.asyncio
async def test_handle_shutdown_signals_closes_bot_once(captured_signals):
    '''A signal flips the state and schedules bot.close once; repeats are ignored.'''
    bot = _fake_bot()
    with gateway.handle_shutdown_signals(bot) as state:
        assert set(captured_signals) == {signal.SIGTERM, signal.SIGINT}
        assert not state
        captured_signals[signal.SIGTERM](signal.SIGTERM, None)
        captured_signals[signal.SIGINT](signal.SIGINT, None)
        await asyncio.sleep(0)
    assert state.triggered is True
    bot.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_handle_shutdown_signals_skips_close_when_closed(captured_signals):
    '''An already-closed bot is not closed again but the state still flips.'''
    bot = _fake_bot(closed=True)
    with gateway.handle_shutdown_signals(bot) as state:
        captured_signals[signal.SIGTERM](signal.SIGTERM, None)
        await asyncio.sleep(0)
    assert state.triggered is True
    bot.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_unload_cogs_continues_past_failures():
    '''Every cog with cog_unload is called; one raising does not stop the rest.'''
    calls = []

    async def good():
        calls.append('good')

    async def bad():
        calls.append('bad')
        raise RuntimeError('boom')

    await gateway.unload_cogs([_cog(bad), _cog(), _cog(good)])
    assert calls == ['bad', 'good']


@pytest.mark.asyncio
async def test_bot_lifecycle_adds_cogs_and_starts_health_server(captured_signals):
    '''Cogs are added and the health server is served; a clean exit does no teardown.'''
    bot = _fake_bot()
    cogs = [MagicMock(), MagicMock()]
    health = MagicMock()
    serving = asyncio.Event()

    async def serve():
        serving.set()

    health.serve = serve
    on_shutdown = AsyncMock()
    async with gateway.bot_lifecycle(bot, cogs, health_server=health, on_shutdown=on_shutdown) as shutdown:
        await asyncio.wait_for(serving.wait(), timeout=2)
        assert not shutdown
    assert [c.args[0] for c in bot.add_cog.await_args_list] == cogs
    assert signal.SIGTERM in captured_signals
    bot.close.assert_not_awaited()
    on_shutdown.assert_not_awaited()


@pytest.mark.asyncio
async def test_bot_lifecycle_signal_triggers_full_teardown(captured_signals):
    '''When shutdown was triggered, cogs unload, the bot closes and on_shutdown runs.'''
    bot = _fake_bot()
    unloaded = AsyncMock()
    on_shutdown = AsyncMock()
    async with gateway.bot_lifecycle(bot, [_cog(unloaded)], on_shutdown=on_shutdown) as shutdown:
        shutdown.triggered = True
    unloaded.assert_awaited_once()
    assert bot.close.await_count == 1
    on_shutdown.assert_awaited_once()
    assert captured_signals


@pytest.mark.asyncio
async def test_bot_lifecycle_keyboard_interrupt_is_graceful(captured_signals):
    '''KeyboardInterrupt inside the block is swallowed and treated as a shutdown request.'''
    bot = _fake_bot(closed=True)
    async with gateway.bot_lifecycle(bot, []) as shutdown:
        raise KeyboardInterrupt
    assert shutdown.triggered is True
    bot.close.assert_not_awaited()
    assert captured_signals


@pytest.mark.asyncio
async def test_bot_lifecycle_swallows_errors_without_teardown(captured_signals):
    '''An ordinary exception ends the block quietly; without a shutdown flag nothing is torn down.'''
    bot = _fake_bot()
    async with gateway.bot_lifecycle(bot, []) as shutdown:
        raise ValueError('loop ended')
    assert not shutdown
    bot.close.assert_not_awaited()
    assert captured_signals


@pytest.mark.asyncio
async def test_register_on_ready_leaves_rejectlisted_guilds():
    '''on_ready leaves guilds on the rejectlist and keeps the others.'''
    bot = MagicMock()
    bot.user.id = 1
    bad, good = MagicMock(id=10), MagicMock(id=20)
    bad.leave = AsyncMock()
    good.leave = AsyncMock()

    async def fetch_guilds(limit):
        assert limit == 150
        for guild in (bad, good):
            yield guild

    bot.fetch_guilds = fetch_guilds
    captured = {}
    bot.event = lambda func: captured.setdefault('on_ready', func)
    gateway.register_on_ready(bot, GeneralConfig(rejectlist_guilds=[10]), MagicMock())
    await captured['on_ready']()
    bad.leave.assert_awaited_once()
    good.leave.assert_not_awaited()
