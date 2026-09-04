'''Tests for the standalone persistence CLI entrypoint.

The construction tests here deliberately patch as little as possible. On
media_search MR 3 both suites patched the same factory, so `diff-cover` read
100% while nothing exercised the real construction at all — the failure this
file is written against. `test_run_builds_stores_that_serve` patches only the
process-level collaborators (observability, the engine context manager, and the
blocking run) and lets every store, the server and the wiring be real, then
drives a route through the object `run()` actually built.
'''
# NOTE: test names here deliberately avoid being exactly 40 characters long.
# trufflehog's Lob detector matches `test_` + 35 chars, so a 40-char test
# function name is reported as a VERIFIED secret and fails the secrets scan.
import asyncio

import pytest
from aiohttp.test_utils import TestClient, TestServer

from discord_bot.cli import database as database_cli
from discord_bot.cli._lib import common as cli_common
from discord_bot.clients.guild_analytics_client import GuildAnalyticsClient
from discord_bot.clients.markov_client import MarkovClient
from discord_bot.clients.playlist_client import PlaylistClient
from discord_bot.cogs.music_helpers.video_cache_client import VideoCacheClient
from discord_bot.exceptions import DiscordBotException

from tests.helpers import fake_engine  # pylint: disable=unused-import

GUILD_ID = 8675


class _HealthServerConfig:
    def __init__(self, enabled):
        self.enabled = enabled
        self.port = 18080
        self.bind_address = '127.0.0.1'


class _Monitoring:
    def __init__(self, enabled):
        self.health_server = _HealthServerConfig(enabled)


class _GeneralConfig:
    '''Stand-in for GeneralConfig with the fields run() reads.'''
    def __init__(self, *, monitoring=None):
        self.monitoring = monitoring


def _settings(*, cache=None, server=None):
    '''Build a minimal raw settings dict for run().'''
    return {
        'general': {'database_server': server or {}},
        'music': {'download': {'cache': cache if cache is not None else {}}},
    }


def _patch_process_collaborators(mocker, engine):
    '''Patch only what would talk to the outside world; leave construction real.'''
    mocker.patch.object(database_cli, 'setup_observability')
    mocker.patch.object(database_cli, 'instrument_sqlalchemy')
    managed = mocker.patch.object(database_cli, 'managed_db')
    managed.return_value.__enter__.return_value = engine
    return mocker.patch.object(database_cli, 'run_database')


def test_run_wires_all_four_store_groups(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''All four Protocol groups are served, each by its real in-process store.'''
    run_database = _patch_process_collaborators(mocker, fake_engine)

    database_cli.run(_settings(cache={'enable_cache_files': True, 'max_cache_files': 10}),
                     _GeneralConfig())

    server = run_database.call_args.args[0]
    assert isinstance(server._guild_analytics_store, GuildAnalyticsClient)  # pylint: disable=protected-access
    assert isinstance(server._markov_store, MarkovClient)  # pylint: disable=protected-access
    assert isinstance(server._playlist_store, PlaylistClient)  # pylint: disable=protected-access
    assert isinstance(server._video_cache_store, VideoCacheClient)  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_run_builds_stores_that_serve(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''
    The server `run()` builds answers a real request against real postgres.

    This is the test that would have caught the media_search MR 3 gap: every
    store, the session generator and the route table are the real ones, and the
    only things patched are the process-level collaborators. A wiring mistake --
    a store built on the wrong generator, a group left unregistered -- fails
    here rather than at pod start.
    '''
    run_database = _patch_process_collaborators(mocker, fake_engine)
    database_cli.run(_settings(), _GeneralConfig())
    server = run_database.call_args.args[0]

    async with TestClient(TestServer(server.build_app())) as client:
        created = await client.post('/database/playlist/create_playlist',
                                    json={'guild_id': GUILD_ID, 'name': 'road trip'})
        listed = await client.post('/database/playlist/list_playlists',
                                   json={'guild_id': GUILD_ID})
        assert created.status == 200
        assert listed.status == 200
        names = [item['name'] for item in (await listed.json())['result']]

    assert 'road trip' in names


@pytest.mark.asyncio
async def test_disabled_cache_leaves_routes_off(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''
    With the catalog disabled the video_cache routes are not registered at all.

    404 rather than a 500 or an empty answer is the designed behaviour for a
    group with no store: `async_retry_broker_command` propagates it immediately
    instead of laddering, so a misconfigured pod fails loudly and fast.
    '''
    run_database = _patch_process_collaborators(mocker, fake_engine)
    database_cli.run(_settings(cache={'enable_cache_files': False}), _GeneralConfig())
    server = run_database.call_args.args[0]
    assert server._video_cache_store is None  # pylint: disable=protected-access

    async with TestClient(TestServer(server.build_app())) as client:
        response = await client.post('/database/video_cache/get_cache_count', json={})

    assert response.status == 404


def test_run_raises_when_no_dsn_is_set(mocker):
    '''No DSN is fatal here, not a warning.

    Every other pod can degrade without a database. This one IS the database, so
    coming up and serving 404s would present a configuration error as an empty
    catalog -- the bot would render "no playlists" rather than fail.
    '''
    run_database = _patch_process_collaborators(mocker, None)

    with pytest.raises(DiscordBotException) as exc:
        database_cli.run(_settings(), _GeneralConfig())

    assert 'sql_connection_statement' in str(exc.value)
    run_database.assert_not_called()


def test_video_cache_policy_is_pod_side(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''max_cache_files and storage_type come from this pod's config, not the wire.

    They describe the catalog the persistence tier owns. Sending them per request
    would let two callers disagree about one catalog's policy, which is why the
    Protocol does not carry them.
    '''
    run_database = _patch_process_collaborators(mocker, fake_engine)

    database_cli.run(
        _settings(cache={'enable_cache_files': True, 'max_cache_files': 42,
                         'max_cache_size_mb': 8}),
        _GeneralConfig())

    store = run_database.call_args.args[0]._video_cache_store  # pylint: disable=protected-access
    assert store.max_cache_files == 42
    assert store.max_cache_size_bytes == 8 * 1024 * 1024
    assert store.storage_type == 's3'


def test_run_binds_the_configured_host_port(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''An explicit general.database_server overrides the 0.0.0.0:8085 default.'''
    run_database = _patch_process_collaborators(mocker, fake_engine)

    database_cli.run(_settings(server={'host': '127.0.0.1', 'port': 9999}), _GeneralConfig())

    server = run_database.call_args.args[0]
    assert server._host == '127.0.0.1'  # pylint: disable=protected-access
    assert server._port == 9999  # pylint: disable=protected-access


def test_default_port_is_the_servers_own(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''The default comes from database_server.DEFAULT_PORT, not a second copy.'''
    run_database = _patch_process_collaborators(mocker, fake_engine)

    database_cli.run(_settings(), _GeneralConfig())

    server = run_database.call_args.args[0]
    assert server._port == database_cli.DEFAULT_PORT  # pylint: disable=protected-access
    assert server._host == '0.0.0.0'  # pylint: disable=protected-access


def test_health_server_built_when_enabled(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''An enabled health_server gets the pod's engine, which is what it probes.'''
    run_database = _patch_process_collaborators(mocker, fake_engine)

    database_cli.run(_settings(), _GeneralConfig(monitoring=_Monitoring(True)))

    health_server = run_database.call_args.args[1]
    assert health_server._db_engine is fake_engine  # pylint: disable=protected-access
    assert health_server.port == 18080
    assert health_server.bind_address == '127.0.0.1'


@pytest.mark.parametrize('monitoring', [None, _Monitoring(False)],
                         ids=['no-monitoring', 'disabled'])
def test_health_server_skipped_when_off(mocker, fake_engine, monitoring):  # pylint: disable=redefined-outer-name
    '''No health server when monitoring is absent or the server is disabled.'''
    run_database = _patch_process_collaborators(mocker, fake_engine)

    database_cli.run(_settings(), _GeneralConfig(monitoring=monitoring))

    assert run_database.call_args.args[1] is None


@pytest.mark.asyncio
async def test_session_generator_opens_a_session(fake_engine):  # pylint: disable=redefined-outer-name
    '''The generator yields a usable AsyncSession bound to the pod's engine.'''
    from sqlalchemy import text  # pylint: disable=import-outside-toplevel

    generator = database_cli.build_session_generator(fake_engine)
    async with generator() as session:
        result = await session.execute(text('SELECT 1'))

    assert result.scalar() == 1


def _shutdown_collaborators(mocker):
    '''Build the async collaborator mocks main_loop drives.'''
    server = mocker.Mock()
    server.serve = mocker.AsyncMock()
    server.drain_and_stop = mocker.AsyncMock()
    return server


def _capture_signals(mocker):
    '''Intercept signal registration so the test can fire SIGTERM itself.'''
    captured = {}

    def _fake_signal(signum, handler):
        captured[signum] = handler

    mocker.patch.object(cli_common.signal, 'signal', new=_fake_signal)
    return captured


async def _fire_sigterm(captured):
    for _ in range(4):
        await asyncio.sleep(0)
    captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)


@pytest.mark.asyncio
async def test_main_loop_drains_on_shutdown(mocker):
    '''SIGTERM sets the stop event and the server is drained in the finally.'''
    server = _shutdown_collaborators(mocker)
    health_server = mocker.Mock()
    health_server.serve = mocker.AsyncMock()
    captured = _capture_signals(mocker)

    await asyncio.gather(database_cli.main_loop(server, health_server),
                         _fire_sigterm(captured))

    server.serve.assert_awaited_once()
    health_server.serve.assert_awaited_once()
    server.drain_and_stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_runs_without_health(mocker):
    '''A pod with no health server still serves and still drains.'''
    server = _shutdown_collaborators(mocker)
    captured = _capture_signals(mocker)

    await asyncio.gather(database_cli.main_loop(server, None), _fire_sigterm(captured))

    server.serve.assert_awaited_once()
    server.drain_and_stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_server_drains_even_if_serve_dies(mocker):
    '''
    A serve() that raises still reaches drain_and_stop.

    The drain lives in a finally for this reason: a bind failure on 8085 that
    skipped the drain would leave the aiohttp runner holding the port while the
    process lingers, and the restart then fails on address-in-use rather than on
    the original error.
    '''
    server = _shutdown_collaborators(mocker)
    server.serve.side_effect = OSError('address already in use')
    captured = _capture_signals(mocker)

    await asyncio.gather(database_cli.main_loop(server, None), _fire_sigterm(captured))

    server.drain_and_stop.assert_awaited_once()


def test_run_database_schedules_the_loop(mocker):
    '''run_database hands main_loop to run_loop rather than driving it itself.'''
    run_loop = mocker.patch.object(database_cli, 'run_loop')
    # MagicMock, not the AsyncMock autospec would give: patching a coroutine
    # function with an AsyncMock makes every call return a fresh coroutine, so
    # the identity assertion below would compare two different objects and the
    # unawaited coroutine would warn.
    main_loop = mocker.patch.object(database_cli, 'main_loop', new=mocker.MagicMock())

    database_cli.run_database('server', 'health')

    main_loop.assert_called_once_with('server', 'health')
    run_loop.assert_called_once_with(main_loop.return_value)


def test_main_reads_config_then_runs(mocker):
    '''The click command parses the config file and hands both halves to run().'''
    parse = mocker.patch.object(database_cli, 'parse_and_validate_config',
                                return_value=({'general': {}}, 'general-config'))
    run = mocker.patch.object(database_cli, 'run')

    database_cli.main.callback('/etc/discord.cnf')

    parse.assert_called_once_with('/etc/discord.cnf')
    run.assert_called_once_with({'general': {}}, 'general-config')
