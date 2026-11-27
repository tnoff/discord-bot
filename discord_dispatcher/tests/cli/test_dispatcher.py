'''Tests for the dispatcher's CLI entrypoint (discord_dispatcher.cli.dispatcher).'''
import asyncio
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, AsyncMock

import pytest
from yaml import dump


from discord_dispatcher.cli.dispatcher import main as dispatcher_main
from discord_dispatcher.cli.dispatcher import main_loop as dispatcher_main_loop
from discord_dispatcher.cli.dispatcher import run_bot as dispatcher_run_bot

from tests.pod_cli_support import invoke_main_with_health_server, invoke_with_config


class _FakeDispatcher:
    '''Minimal dispatcher stand-in for cli main_loop tests.'''
    async def start(self):
        pass
    async def stop(self):
        pass


@pytest.mark.asyncio(loop_scope="session")
async def test_main_loop_dispatch_gateway_false():
    '''dispatcher_main_loop uses bot.login() and polls is_closed() (HTTP-only / HA mode)'''
    class _FakeBotHttpOnly:
        def __init__(self):
            self._calls = 0
            self.bot_closed = False
        def is_closed(self):
            self._calls += 1
            return self._calls > 1  # False on first poll, True on second
        async def login(self, _token):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def close(self):
            self.bot_closed = True

    mock_manager = MagicMock()
    mock_manager.start = AsyncMock()
    mock_manager.close = AsyncMock()

    bot = _FakeBotHttpOnly()
    await dispatcher_main_loop(bot, 'token', mock_manager, _FakeDispatcher())
    assert bot._calls >= 2  # polled at least twice  #pylint:disable=protected-access


@pytest.mark.asyncio(loop_scope="session")
async def test_main_loop_closes_redis_manager_on_shutdown():
    '''dispatcher_main_loop calls redis_manager.close() after shutdown (KeyboardInterrupt).'''
    class _FakeBotInterrupt:
        def __init__(self):
            self.bot_closed = False
        def event(self, _func):
            pass
        def is_closed(self):
            return self.bot_closed
        async def login(self, _token):
            raise KeyboardInterrupt()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def close(self):
            self.bot_closed = True

    mock_manager = MagicMock()
    mock_manager.start = AsyncMock()
    mock_manager.close = AsyncMock()

    await dispatcher_main_loop(_FakeBotInterrupt(), 'token', mock_manager, _FakeDispatcher())
    mock_manager.start.assert_awaited_once()
    mock_manager.close.assert_awaited_once()


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatcher_main_loop_with_dispatch_http_server():
    '''dispatcher_main_loop creates a task for dispatch_http_server.serve() when provided.'''
    mock_dispatch_server = MagicMock()
    mock_dispatch_server.serve = AsyncMock()

    class _FakeBotQuick:
        def __init__(self):
            self._calls = 0
            self.bot_closed = False
        def is_closed(self):
            self._calls += 1
            return self._calls > 1  # False on first poll, True on second
        async def login(self, _token):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def close(self):
            self.bot_closed = True

    mock_manager = MagicMock()
    mock_manager.start = AsyncMock()
    mock_manager.close = AsyncMock()

    await dispatcher_main_loop(
        _FakeBotQuick(), 'token', mock_manager, _FakeDispatcher(),
        dispatch_http_server=mock_dispatch_server,
    )
    mock_dispatch_server.serve.assert_called_once()


@pytest.mark.asyncio(loop_scope="session")
async def test_dispatcher_main_loop_drains_http_server_before_closing_redis():
    '''
    On shutdown the dispatch server stops accepting BEFORE the Redis handle closes.

    The reverse order left port 8082 listening through dispatcher.stop() and
    redis_manager.close(), so a POST landing in that window got a 202 for work
    that was never queued — send_message() fires its enqueue into a detached
    task, so the failure never reached the caller.
    '''
    order = []

    mock_dispatch_server = MagicMock()
    mock_dispatch_server.serve = AsyncMock()
    mock_dispatch_server.drain_and_stop = AsyncMock(side_effect=lambda: order.append('drain'))

    class _FakeBotInterrupt:
        def __init__(self):
            self.bot_closed = False
        def is_closed(self):
            return self.bot_closed
        async def login(self, _token):
            raise KeyboardInterrupt()
        async def __aenter__(self):
            return self
        async def __aexit__(self, *_args):
            pass
        async def close(self):
            self.bot_closed = True

    class _OrderedDispatcher:
        async def start(self):
            pass
        async def stop(self):
            order.append('dispatcher_stop')

    mock_manager = MagicMock()
    mock_manager.start = AsyncMock()
    mock_manager.close = AsyncMock(side_effect=lambda: order.append('redis_close'))

    await dispatcher_main_loop(
        _FakeBotInterrupt(), 'token', mock_manager, _OrderedDispatcher(),
        dispatch_http_server=mock_dispatch_server,
    )

    assert order == ['drain', 'dispatcher_stop', 'redis_close']


def test_dispatcher_run_bot_schedules_main_loop(mocker):
    '''dispatcher.run_bot schedules main_loop on the event loop via run_loop().'''
    mock_run_loop = mocker.patch('discord_dispatcher.cli.dispatcher.run_loop')

    general_config = MagicMock()
    general_config.discord_token = 'token'
    bot = MagicMock()
    redis_manager = MagicMock()
    dispatcher = MagicMock()

    dispatcher_run_bot(general_config, bot, redis_manager, dispatcher)
    mock_run_loop.assert_called_once()
    # Ensure run_loop was passed the awaitable from main_loop
    coro = mock_run_loop.call_args[0][0]
    assert asyncio.iscoroutine(coro)
    coro.close()


@pytest.mark.asyncio
async def test_dispatcher_main_with_health_server(mocker):
    '''DispatchHealthServer is instantiated by the dispatcher entry-point when health_server.enabled is true.'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
                'redis_url': 'redis://localhost:6379/0',
                'monitoring': {
                    'otlp': {'enabled': False},
                    'health_server': {'enabled': True},
                },
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        result = await invoke_main_with_health_server(
            mocker, dispatcher_main, temp_config.name, 'discord_dispatcher.cli.dispatcher.DispatchHealthServer',
            extra_patches=('discord_dispatcher.cli.dispatcher.run_bot',))
        assert result.exception is None


def test_dispatcher_run_raises_when_redis_url_missing():
    '''cli/dispatcher.py run() raises DiscordBotException when redis_url is absent.'''
    result = invoke_with_config(dispatcher_main, {'general': {'discord_token': 'foo'}})
    assert 'Redis required' in str(result.exception)
