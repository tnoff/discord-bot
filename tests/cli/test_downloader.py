'''Tests for the standalone downloader CLI entrypoint.'''
import asyncio
import logging

import pytest

from discord_bot.cli import downloader as downloader_cli
from discord_bot.cli._lib import common as cli_common
from discord_bot.exceptions import DiscordBotException, ExitEarlyException


def _settings(*, broker_url='http://broker:8081',
              bucket='media-bucket', extra_download=None, extra_general=None):
    '''Build a minimal raw settings dict for run().'''
    download = {'storage': {'bucket_name': bucket}}
    if extra_download:
        download.update(extra_download)
    general = {}
    if extra_general:
        general.update(extra_general)
    settings = {
        'general': general,
        'music': {
            'broker_client': {'url': broker_url},
            'download': download,
        },
    }
    return settings


class _GeneralConfig:
    '''Stand-in for GeneralConfig with the fields run() reads.'''
    def __init__(self, *, redis_url='redis://localhost:6379', redis_sentinel=None,
                 monitoring=None, logging_config=None):
        self.redis_url = redis_url
        self.redis_sentinel = redis_sentinel
        self.monitoring = monitoring
        self.logging = logging_config


def _patch_collaborators(mocker):
    '''Patch every constructor/helper run() touches; return the mocks.'''
    return {
        'setup_observability': mocker.patch.object(downloader_cli, 'setup_observability'),
        'RedisManager': mocker.patch.object(downloader_cli, 'RedisManager'),
        'HttpBrokerClient': mocker.patch.object(downloader_cli, 'HttpBrokerClient'),
        'RedisDownloadWorker': mocker.patch.object(downloader_cli, 'RedisDownloadWorker'),
        'DownloadHttpServer': mocker.patch.object(downloader_cli, 'DownloadHttpServer'),
        'RedisPingHealthServer': mocker.patch.object(downloader_cli, 'RedisPingHealthServer'),
        'DownloadMetrics': mocker.patch.object(downloader_cli, 'DownloadMetrics'),
        'run_downloader': mocker.patch.object(downloader_cli, 'run_downloader'),
    }


def test_run_wires_collaborators(mocker):
    '''run() builds RedisManager/HttpBrokerClient/worker/server and calls run_downloader.'''
    mocks = _patch_collaborators(mocker)
    redis_manager = mocks['RedisManager'].from_general_config.return_value
    broker_client = mocks['HttpBrokerClient'].return_value
    worker = mocks['RedisDownloadWorker'].return_value
    server = mocks['DownloadHttpServer'].return_value

    settings = _settings()
    general = _GeneralConfig()
    downloader_cli.run(settings, general)

    mocks['setup_observability'].assert_called_once_with(general)
    mocks['RedisManager'].from_general_config.assert_called_once_with(general)
    mocks['HttpBrokerClient'].assert_called_once_with(
        'http://broker:8081', bucket_name='media-bucket')

    # Worker wiring: broker client + redis manager + bucket forwarded.
    _, worker_kwargs = mocks['RedisDownloadWorker'].call_args
    assert worker_kwargs['redis_manager'] is redis_manager
    assert worker_kwargs['broker'] is broker_client
    assert worker_kwargs['bucket_name'] == 'media-bucket'

    # Server wiring: worker + default port 8083 + default host.
    server_args, server_kwargs = mocks['DownloadHttpServer'].call_args
    assert server_args[0] is worker
    assert server_kwargs['port'] == 8083
    assert server_kwargs['host'] == '0.0.0.0'

    # Metrics collector built from the worker and forwarded.
    mocks['DownloadMetrics'].assert_called_once_with(worker)
    download_metrics = mocks['DownloadMetrics'].return_value

    # No monitoring -> no health server.
    mocks['RedisPingHealthServer'].assert_not_called()
    mocks['run_downloader'].assert_called_once_with(
        worker, server, None, redis_manager, download_metrics)


def test_run_respects_configured_server_host_and_port(mocker):
    '''general.downloader_server overrides host/port.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_general={'downloader_server': {'host': '127.0.0.1', 'port': 9099}})
    downloader_cli.run(settings, _GeneralConfig())
    _, server_kwargs = mocks['DownloadHttpServer'].call_args
    assert server_kwargs['host'] == '127.0.0.1'
    assert server_kwargs['port'] == 9099


def test_run_builds_health_server_when_enabled(mocker):
    '''A monitoring.health_server.enabled config builds a RedisPingHealthServer.'''
    mocks = _patch_collaborators(mocker)
    health_cfg = mocker.Mock(enabled=True, port=8080, bind_address='0.0.0.0')
    monitoring = mocker.Mock(health_server=health_cfg)
    general = _GeneralConfig(monitoring=monitoring)
    downloader_cli.run(_settings(), general)
    mocks['RedisPingHealthServer'].assert_called_once()
    _, hs_kwargs = mocks['RedisPingHealthServer'].call_args
    assert hs_kwargs['port'] == 8080
    assert hs_kwargs['bind_address'] == '0.0.0.0'
    health_server = mocks['RedisPingHealthServer'].return_value
    run_args = mocks['run_downloader'].call_args[0]
    assert run_args[2] is health_server


def test_run_missing_redis_raises(mocker):
    '''No redis_url and no redis_sentinel -> DiscordBotException.'''
    _patch_collaborators(mocker)
    general = _GeneralConfig(redis_url=None, redis_sentinel=None)
    with pytest.raises(DiscordBotException, match='Redis required'):
        downloader_cli.run(_settings(), general)


def test_run_missing_broker_url_raises(mocker):
    '''No music.broker_client.url -> DiscordBotException.'''
    _patch_collaborators(mocker)
    settings = _settings()
    settings['music']['broker_client'] = {}
    with pytest.raises(DiscordBotException, match='broker_client.url required'):
        downloader_cli.run(settings, _GeneralConfig())


def test_run_uses_configured_download_dir(mocker, tmp_path):
    '''download_dir_path is used (and created) instead of a temp dir.'''
    mocks = _patch_collaborators(mocker)
    target = tmp_path / 'downloads' / 'nested'
    settings = _settings(extra_download={'download_dir_path': str(target)})
    downloader_cli.run(settings, _GeneralConfig())
    assert target.is_dir()
    worker_args, _ = mocks['RedisDownloadWorker'].call_args
    assert str(worker_args[1]) == str(target)


@pytest.mark.asyncio
async def test_drive_worker_guards_broad_exception(mocker):
    '''A worker.run() exception is logged and the loop continues (does not propagate).'''
    stop_event = asyncio.Event()
    calls = []

    async def _run(_evt):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('redis blip')
        # Second call: request shutdown so the loop exits cleanly.
        stop_event.set()

    worker = mocker.Mock()
    worker.run = _run
    mocker.patch.object(downloader_cli.asyncio, 'sleep', new=mocker.AsyncMock())
    log = mocker.Mock(spec=logging.Logger)

    # Must not raise despite the RuntimeError on the first iteration.
    await downloader_cli._drive_worker(worker, stop_event, log)  # pylint: disable=protected-access
    assert len(calls) == 2
    log.exception.assert_called_once()


@pytest.mark.asyncio
async def test_drive_worker_exits_on_exit_early(mocker):
    '''ExitEarlyException breaks the loop without logging an error.'''
    stop_event = asyncio.Event()

    async def _run(_evt):
        raise ExitEarlyException('shutdown')

    worker = mocker.Mock()
    worker.run = _run
    log = mocker.Mock(spec=logging.Logger)
    await downloader_cli._drive_worker(worker, stop_event, log)  # pylint: disable=protected-access
    log.exception.assert_not_called()


@pytest.mark.asyncio
async def test_drive_worker_skips_when_already_stopped(mocker):
    '''A pre-set stop_event means run() is never called.'''
    stop_event = asyncio.Event()
    stop_event.set()
    worker = mocker.Mock()
    worker.run = mocker.AsyncMock()
    await downloader_cli._drive_worker(worker, stop_event, mocker.Mock())  # pylint: disable=protected-access
    worker.run.assert_not_called()


@pytest.mark.asyncio
async def test_main_loop_shutdown_drains_and_closes(mocker):
    '''main_loop sets stop_event on SIGTERM and drains server + closes redis in finally.'''
    redis_manager = mocker.Mock()
    redis_manager.start = mocker.AsyncMock()
    redis_manager.close = mocker.AsyncMock()
    server = mocker.Mock()
    server.serve = mocker.AsyncMock()
    server.drain_and_stop = mocker.AsyncMock()
    health_server = mocker.Mock()
    health_server.serve = mocker.AsyncMock()

    worker = mocker.Mock()

    async def _run(evt):
        # Block on the shutdown event so the driver task doesn't busy-loop and
        # cleanly exits its while-guard once shutdown is requested.
        await evt.wait()

    worker.run = _run

    metrics = mocker.Mock()
    metrics.run = mocker.AsyncMock()

    captured = {}

    def _fake_signal(signum, handler):
        captured[signum] = handler

    mocker.patch.object(cli_common.signal, 'signal', new=_fake_signal)

    async def _fire_sigterm():
        # Give main_loop a chance to register handlers + start tasks, then fire.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        handler = captured[cli_common.signal.SIGTERM]
        handler(cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        downloader_cli.main_loop(worker, server, health_server, redis_manager, metrics),
        _fire_sigterm(),
    )

    redis_manager.start.assert_awaited_once()
    server.serve.assert_awaited_once()
    health_server.serve.assert_awaited_once()
    metrics.run.assert_awaited_once()
    server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_without_health_server(mocker):
    '''main_loop tolerates a None health_server (still drains on shutdown).'''
    redis_manager = mocker.Mock()
    redis_manager.start = mocker.AsyncMock()
    redis_manager.close = mocker.AsyncMock()
    server = mocker.Mock()
    server.serve = mocker.AsyncMock()
    server.drain_and_stop = mocker.AsyncMock()
    worker = mocker.Mock()

    async def _run(evt):
        await evt.wait()

    worker.run = _run

    metrics = mocker.Mock()
    metrics.run = mocker.AsyncMock()

    captured = {}

    def _capture(signum, handler):
        captured[signum] = handler

    mocker.patch.object(cli_common.signal, 'signal', new=_capture)

    async def _fire():
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        captured[cli_common.signal.SIGINT](cli_common.signal.SIGINT, None)

    await asyncio.gather(
        downloader_cli.main_loop(worker, server, None, redis_manager, metrics),
        _fire(),
    )
    server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


def test_run_downloader_delegates_to_run_loop(mocker):
    '''run_downloader hands the main_loop coroutine to run_loop.'''
    run_loop = mocker.patch.object(downloader_cli, 'run_loop')
    sentinel = object()
    main_loop = mocker.Mock(return_value=sentinel)
    mocker.patch.object(downloader_cli, 'main_loop', new=main_loop)
    worker, server, health, redis_manager, metrics = (
        object(), object(), object(), object(), object())
    downloader_cli.run_downloader(worker, server, health, redis_manager, metrics)
    main_loop.assert_called_once_with(worker, server, health, redis_manager, metrics)
    run_loop.assert_called_once_with(sentinel)


def test_main_parses_config_and_runs(mocker):
    '''main() parses the config file then calls run().'''
    parse = mocker.patch.object(downloader_cli, 'parse_and_validate_config',
                                return_value=({'k': 'v'}, 'general'))
    run = mocker.patch.object(downloader_cli, 'run')
    downloader_cli.main.callback('cfg.yml')
    parse.assert_called_once_with('cfg.yml')
    run.assert_called_once_with({'k': 'v'}, 'general')
