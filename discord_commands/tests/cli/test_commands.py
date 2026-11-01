import asyncio
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from discord_core.exceptions import DiscordBotException

from discord_commands.cli import commands as commands_cli
from discord_commands.clients.http_guild_roles_client import HttpGuildRolesClient
from discord_commands.workers.result_loops import ResultLoops


DISPATCH_URL = 'http://dispatcher:8082'
DATABASE_URL = 'http://db:8085'
GENERAL = {'dispatch_http_url': DISPATCH_URL, 'database_http_url': DATABASE_URL}
MUSIC = {
    'broker_client': {'url': 'http://broker:8081'},
    'download_client': {'url': 'http://downloader:8083'},
    'youtube_music_search_client': {'url': 'http://search:8084'},
    'media_search_client': {'url': 'http://search:8084'},
}


def settings_with(music=None, **sections):
    '''Settings with the required sections; ``music`` is merged over the required music peers.'''
    return {'general': GENERAL, 'music': {**MUSIC, **(music or {})}, **sections}


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
    settings = settings_with()
    settings['general'] = {**GENERAL, 'commands_server': {'host': '127.0.0.1', 'port': 9999}}
    commands_cli.run(settings, general_config())
    (http_server, executor, teardown), kwargs = run_loop.call_args.args[0]
    assert (http_server._host, http_server._port) == ('127.0.0.1', 9999)  # pylint: disable=protected-access
    assert http_server._executor is executor  # pylint: disable=protected-access
    assert http_server._teardown is teardown  # pylint: disable=protected-access
    assert teardown.download_client is kwargs['download_client']
    assert kwargs['health_server'] is None
    assert kwargs['dispatcher']._base_url == DISPATCH_URL  # pylint: disable=protected-access
    assert len(kwargs['closers']) == 7
    assert kwargs['download_client']._base_url == 'http://downloader:8083'  # pylint: disable=protected-access
    assert isinstance(kwargs['result_loops'], ResultLoops)


@pytest.mark.parametrize('tracing,expected', [
    ({'trace_queue_worker_status_poll': True}, True),
    ({'trace_queue_worker_status_poll': False}, False),
    (None, False),
], ids=['on', 'off', 'no-tracing-block'])
def test_run_forwards_the_status_poller_toggle_to_the_download_client(run_loop, tracing, expected):
    '''The pod is where the client is built, so this is where the toggle either reaches it or is
    quietly replaced by the default -- the failure this fleet has actually shipped.'''
    settings = settings_with()
    if tracing is not None:
        settings['general'] = {**GENERAL, 'monitoring': {'otlp': {'enabled': False}, 'tracing': tracing}}
    commands_cli.run(settings, general_config())
    _, kwargs = run_loop.call_args.args[0]
    assert kwargs['download_client']._trace_status_poll is expected  # pylint: disable=protected-access


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
    commands_cli.run(settings_with(markov={'server_reject_list': [5, 6]}), general_config())
    assert build.call_args.args[3] == [5, 6]
    run_loop.assert_called_once()


@pytest.mark.usefixtures('run_loop')
def test_run_defaults_to_an_empty_markov_reject_list(mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    commands_cli.run(settings_with(), general_config())
    assert build.call_args.args[3] == []


def test_run_gives_the_music_settings_to_the_logic(run_loop, mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    music = {'general': {'message_delete_after': 60}, 'playlist': {'server_playlist_max_size': 512},
             'player': {'queue_max_size': 9},
             'download': {'server_queue_priority': [{'server_id': '5', 'priority': 2}]}}
    commands_cli.run(settings_with(music), general_config())
    playlist, music_commands = build.call_args.args[4:6]
    assert (playlist.message_delete_after, playlist.server_playlist_max_size) == (60, 512)
    assert music_commands.message_delete_after == 60
    enqueuer = music_commands.enqueuer
    assert (enqueuer.queue_max_size, enqueuer.server_queue_priority) == (9, {5: 2})
    assert enqueuer.player_gateway.queuer.queue_max_size == 9
    run_loop.assert_called_once()


@pytest.mark.usefixtures('run_loop')
def test_run_defaults_the_music_settings(mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    commands_cli.run(settings_with(), general_config())
    playlist, music_commands = build.call_args.args[4:6]
    assert (playlist.message_delete_after, playlist.server_playlist_max_size) == (300, 64)
    enqueuer = music_commands.enqueuer
    assert (enqueuer.queue_max_size, enqueuer.server_queue_priority) == (128, {})


def test_run_gives_the_role_config_and_the_dispatcher_to_the_role_logic(run_loop, mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    settings = settings_with()
    settings['role'] = {11: {'self_service_role_list': [5], 7: {'manages_roles': [8]}}}
    commands_cli.run(settings, general_config())
    role = build.call_args.args[6]
    assert role.settings == {11: {'self_service_role_list': [5], 7: {'manages_roles': [8]}}}
    assert isinstance(role.role_gateway, HttpGuildRolesClient)
    run_loop.assert_called_once()


@pytest.mark.usefixtures('run_loop')
def test_run_without_role_config_gives_the_role_logic_no_rules(mocker):
    build = mocker.patch.object(commands_cli, 'build_handlers', return_value={})
    commands_cli.run(settings_with(), general_config())
    assert build.call_args.args[6].settings == {}


def test_run_refuses_a_role_rule_that_manages_nothing(run_loop):
    settings = settings_with()
    settings['role'] = {11: {7: {'manages_roles': []}}}
    with pytest.raises(ValidationError):
        commands_cli.run(settings, general_config())
    run_loop.assert_not_called()


@pytest.mark.parametrize('client', list(MUSIC))
def test_run_requires_the_music_peers(run_loop, client):
    music = {key: value for key, value in MUSIC.items() if key != client}
    with pytest.raises(DiscordBotException, match=f'music.{client}.url'):
        commands_cli.run({'general': GENERAL, 'music': music}, general_config())
    run_loop.assert_not_called()


def test_run_defaults_to_all_interfaces_on_8086(run_loop):
    commands_cli.run(settings_with(), general_config())
    (http_server, _, _), _ = run_loop.call_args.args[0]
    assert (http_server._host, http_server._port) == ('0.0.0.0', 8086)  # pylint: disable=protected-access


def test_run_adds_a_health_server_when_monitoring_enables_one(run_loop):
    commands_cli.run(settings_with(), general_config(health=True))
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
    teardown = MagicMock()
    teardown.close = AsyncMock(side_effect=lambda: order.append('teardown'))
    health_server = MagicMock()
    health_server.serve = AsyncMock(side_effect=lambda: order.append('health'))
    dispatcher = MagicMock()
    dispatcher.close = AsyncMock(side_effect=lambda: order.append('dispatcher'))
    download_client = MagicMock()
    download_client.start = AsyncMock(side_effect=lambda: order.append('download-start'))
    result_loops = MagicMock()
    result_loops.start = MagicMock(side_effect=lambda event: order.append(('loops-start', event is stop_event)))
    result_loops.stop = AsyncMock(side_effect=lambda: order.append('loops-stop'))

    async def close_store():
        order.append('store')

    async def close_playlist():
        order.append('playlist')

    loop_task = asyncio.create_task(commands_cli.main_loop(
        http_server, executor, teardown, health_server, dispatcher, [close_store, close_playlist], download_client,
        result_loops))
    await asyncio.sleep(0.01)
    assert not loop_task.done()
    stop_event.set()
    await loop_task
    assert order[-6:] == ['drain', 'close', 'teardown', 'dispatcher', 'store', 'playlist']
    assert {'serve', 'health', 'seams', 'download-start'} <= set(order)
    assert order.index('seams') < order.index('download-start')
    # The loops start once the clients they use are up, and stop first: a popped result is not
    # dropped while the dispatcher and the other clients close under it.
    assert order.index('download-start') < order.index(('loops-start', True))
    assert order.index('loops-stop') < order.index('drain')


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
    teardown = MagicMock()
    teardown.close = AsyncMock()
    await commands_cli.main_loop(http_server, executor, teardown)
    http_server.drain_and_stop.assert_awaited_once()
    executor.close.assert_awaited_once()
    teardown.close.assert_awaited_once()


def test_main_reads_the_config_file_and_runs(mocker):
    parse = mocker.patch.object(commands_cli, 'parse_and_validate_config', return_value=({'general': {}}, 'cfg'))
    run = mocker.patch.object(commands_cli, 'run')
    result = CliRunner().invoke(commands_cli.main, ['/tmp/discord.cnf'])
    assert result.exit_code == 0
    parse.assert_called_once_with('/tmp/discord.cnf')
    run.assert_called_once_with({'general': {}}, 'cfg')
