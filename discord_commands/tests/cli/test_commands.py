import asyncio
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner

from discord_core.exceptions import DiscordBotException

from discord_commands.cli import commands as commands_cli


DISPATCH_URL = 'http://dispatcher:8082'
DATABASE_URL = 'http://db:8085'
GENERAL = {'dispatch_http_url': DISPATCH_URL, 'database_http_url': DATABASE_URL}


def general_config(health=False):
    health_server = SimpleNamespace(enabled=True, port=18080, bind_address='127.0.0.1') if health else None
    return SimpleNamespace(monitoring=SimpleNamespace(health_server=health_server) if health else None,
                           seam_contract=None)


@pytest.fixture(name='run_loop')
def fixture_run_loop(mocker):
    '''run() hands main_loop's coroutine to run_loop; capture its arguments instead of running it.'''
    mocker.patch.object(commands_cli, 'setup_observability')
    mocker.patch.object(commands_cli, 'main_loop', new=lambda *args, **kwargs: (args, kwargs))
    return mocker.patch.object(commands_cli, 'run_loop')


def test_run_builds_the_server_on_the_configured_port(run_loop):
    commands_cli.run({'general': {**GENERAL, 'commands_server': {'host': '127.0.0.1', 'port': 9999}}}, general_config())
    (http_server, executor), kwargs = run_loop.call_args.args[0]
    assert (http_server._host, http_server._port) == ('127.0.0.1', 9999)  # pylint: disable=protected-access
    assert http_server._executor is executor  # pylint: disable=protected-access
    assert kwargs['health_server'] is None
    assert kwargs['dispatcher']._base_url == DISPATCH_URL  # pylint: disable=protected-access
    assert kwargs['markov_store']._base_url == DATABASE_URL  # pylint: disable=protected-access


def test_run_requires_a_dispatcher_url(run_loop):
    with pytest.raises(DiscordBotException, match='dispatch_http_url'):
        commands_cli.run({'general': {}}, general_config())
    run_loop.assert_not_called()


def test_run_requires_a_database_url(run_loop):
    with pytest.raises(DiscordBotException, match='database_http_url'):
        commands_cli.run({'general': {'dispatch_http_url': DISPATCH_URL}}, general_config())
    run_loop.assert_not_called()


def test_run_gives_the_markov_reject_list_to_the_handlers(run_loop, mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    commands_cli.run({'general': GENERAL, 'markov': {'server_reject_list': [5, 6]}}, general_config())
    assert build.call_args.args[3] == [5, 6]
    run_loop.assert_called_once()


@pytest.mark.usefixtures('run_loop')
def test_run_defaults_to_an_empty_markov_reject_list(mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    commands_cli.run({'general': GENERAL}, general_config())
    assert build.call_args.args[3] == []


def test_run_defaults_to_all_interfaces_on_8086(run_loop):
    commands_cli.run({'general': GENERAL}, general_config())
    (http_server, _), _ = run_loop.call_args.args[0]
    assert (http_server._host, http_server._port) == ('0.0.0.0', 8086)  # pylint: disable=protected-access


def test_run_adds_a_health_server_when_monitoring_enables_one(run_loop):
    commands_cli.run({'general': GENERAL}, general_config(health=True))
    _, kwargs = run_loop.call_args.args[0]
    assert (kwargs['health_server'].port, kwargs['health_server'].bind_address) == (18080, '127.0.0.1')


@pytest.mark.asyncio
async def test_main_loop_serves_until_stopped_then_drains_before_closing_the_executor(mocker):
    order = []
    stop_event = asyncio.Event()

    @contextlib.contextmanager
    def fake_signals():
        yield stop_event

    mocker.patch.object(commands_cli, 'shutdown_event_signals', fake_signals)
    mocker.patch.object(commands_cli, 'start_seam_checks', side_effect=lambda: order.append('seams'))
    http_server = MagicMock()
    http_server.serve = AsyncMock(side_effect=lambda: order.append('serve'))
    http_server.drain_and_stop = AsyncMock(side_effect=lambda: order.append('drain'))
    executor = MagicMock()
    executor.close = AsyncMock(side_effect=lambda: order.append('close'))
    health_server = MagicMock()
    health_server.serve = AsyncMock(side_effect=lambda: order.append('health'))
    dispatcher = MagicMock()
    dispatcher.close = AsyncMock(side_effect=lambda: order.append('dispatcher'))
    markov_store = MagicMock()
    markov_store.close = AsyncMock(side_effect=lambda: order.append('store'))

    loop_task = asyncio.create_task(commands_cli.main_loop(http_server, executor, health_server, dispatcher, markov_store))
    await asyncio.sleep(0.01)
    assert not loop_task.done()
    stop_event.set()
    await loop_task
    assert order[-4:] == ['drain', 'close', 'dispatcher', 'store']
    assert {'serve', 'health', 'seams'} <= set(order)


@pytest.mark.asyncio
async def test_main_loop_runs_without_a_health_server(mocker):
    stop_event = asyncio.Event()
    stop_event.set()

    @contextlib.contextmanager
    def fake_signals():
        yield stop_event

    mocker.patch.object(commands_cli, 'shutdown_event_signals', fake_signals)
    mocker.patch.object(commands_cli, 'start_seam_checks')
    http_server = MagicMock()
    http_server.serve = AsyncMock()
    http_server.drain_and_stop = AsyncMock()
    executor = MagicMock()
    executor.close = AsyncMock()
    await commands_cli.main_loop(http_server, executor)
    http_server.drain_and_stop.assert_awaited_once()
    executor.close.assert_awaited_once()


def test_main_reads_the_config_file_and_runs(mocker):
    parse = mocker.patch.object(commands_cli, 'parse_and_validate_config', return_value=({'general': {}}, 'cfg'))
    run = mocker.patch.object(commands_cli, 'run')
    result = CliRunner().invoke(commands_cli.main, ['/tmp/discord.cnf'])
    assert result.exit_code == 0
    parse.assert_called_once_with('/tmp/discord.cnf')
    run.assert_called_once_with({'general': {}}, 'cfg')
