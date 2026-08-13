'''Tests for the standalone downloader CLI entrypoint.'''
import asyncio
import logging

import pytest

from discord_bot.cli import downloader as downloader_cli
from discord_bot.cli._lib import common as cli_common
# RedisManager + the health server are constructed by the shared worker-pod
# scaffolding now (cli/_lib/worker_pod.py), so that is where they are patched.
from discord_bot.cli._lib import worker_pod
from discord_bot.exceptions import DiscordBotException, ExitEarlyException
from discord_bot.utils.loop_health import LOOP_HEALTH, LoopHealth, LoopStatus


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
        'RedisManager': mocker.patch.object(worker_pod, 'RedisManager'),
        'HttpBrokerClient': mocker.patch.object(downloader_cli, 'HttpBrokerClient'),
        'RedisDownloadWorker': mocker.patch.object(downloader_cli, 'RedisDownloadWorker'),
        'DownloadHttpServer': mocker.patch.object(downloader_cli, 'DownloadHttpServer'),
        'RedisPingHealthServer': mocker.patch.object(worker_pod, 'RedisPingHealthServer'),
        'DownloadMetrics': mocker.patch.object(downloader_cli, 'DownloadMetrics'),
        'build_exit_probe': mocker.patch.object(downloader_cli, 'build_exit_probe'),
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

    # Egress exit probe built (default: unconfigured -> no type, no proxy) and wired.
    mocks['build_exit_probe'].assert_called_once_with(None, None)
    exit_probe = mocks['build_exit_probe'].return_value
    worker.set_exit_probe.assert_called_once_with(exit_probe)

    # No monitoring -> no health server.
    mocks['RedisPingHealthServer'].assert_not_called()
    # broker_client is forwarded so the drain path can close its session.
    mocks['run_downloader'].assert_called_once_with(
        worker, server, None, redis_manager, download_metrics, exit_probe, driver_count=1,
        broker_client=broker_client)


def test_run_builds_exit_probe_from_config(mocker):
    '''The egress probe is built from music.download.egress_probe + the yt-dlp proxy.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'egress_probe': 'mullvad',
        'extra_ytdlp_options': {'proxy': 'http://discord-vpn:8888'}})
    downloader_cli.run(settings, _GeneralConfig())
    mocks['build_exit_probe'].assert_called_once_with('mullvad', 'http://discord-vpn:8888')
    worker = mocks['RedisDownloadWorker'].return_value
    worker.set_exit_probe.assert_called_once_with(mocks['build_exit_probe'].return_value)


def test_run_skips_probe_in_pool_mode(mocker):
    '''In pool mode each download leases + names its own exit, so no am.i.mullvad
    probe is built or wired and run_downloader receives exit_probe=None.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'egress_mode': 'mullvad-socks5', 'egress_exits': ['us-lax-wg-001']})
    downloader_cli.run(settings, _GeneralConfig())
    mocks['build_exit_probe'].assert_not_called()
    mocks['RedisDownloadWorker'].return_value.set_exit_probe.assert_not_called()
    assert mocks['run_downloader'].call_args[0][5] is None  # exit_probe positional


def test_run_builds_probe_in_fixed_http_proxy_mode(mocker):
    '''Fixed http-proxy mode still probes the shared exit and wires it to the worker.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'egress_mode': 'http-proxy',
        'egress_probe': 'mullvad',
        'extra_ytdlp_options': {'proxy': 'http://discord-vpn:8888'}})
    downloader_cli.run(settings, _GeneralConfig())
    mocks['build_exit_probe'].assert_called_once_with('mullvad', 'http://discord-vpn:8888')
    exit_probe = mocks['build_exit_probe'].return_value
    mocks['RedisDownloadWorker'].return_value.set_exit_probe.assert_called_once_with(exit_probe)
    assert mocks['run_downloader'].call_args[0][5] is exit_probe


def test_run_pool_mode_fans_out_to_worker_count(mocker):
    '''Pool mode drives worker_count concurrent loops (each leases a distinct exit).'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'egress_mode': 'mullvad-socks5', 'egress_exits': ['a', 'b', 'c'], 'worker_count': 2})
    downloader_cli.run(settings, _GeneralConfig())
    assert mocks['run_downloader'].call_args.kwargs['driver_count'] == 2


def test_run_pool_driver_count_capped_at_exit_count(mocker):
    '''More workers than exits is capped so no driver is permanently starved.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'egress_mode': 'mullvad-socks5', 'egress_exits': ['a', 'b'], 'worker_count': 5})
    downloader_cli.run(settings, _GeneralConfig())
    assert mocks['run_downloader'].call_args.kwargs['driver_count'] == 2


def test_run_http_proxy_mode_is_single_driver(mocker):
    '''Fixed-proxy mode stays single-driver — one egress IP is one rate-limit bucket.'''
    mocks = _patch_collaborators(mocker)
    downloader_cli.run(_settings(), _GeneralConfig())
    assert mocks['run_downloader'].call_args.kwargs['driver_count'] == 1


def test_run_defaults_egress_mode_to_http_proxy(mocker):
    '''With no egress config, the worker is built in http-proxy mode with no exits.'''
    mocks = _patch_collaborators(mocker)
    downloader_cli.run(_settings(), _GeneralConfig())
    _, kwargs = mocks['RedisDownloadWorker'].call_args
    assert kwargs['egress_mode'] == 'http-proxy'
    assert kwargs['egress_exits'] is None


def test_run_forwards_pool_egress_config(mocker):
    '''music.download.egress_mode/egress_exits reach the worker verbatim.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'egress_mode': 'mullvad-socks5', 'egress_exits': ['us-lax-wg-001', 'us-nyc-wg-301']})
    downloader_cli.run(settings, _GeneralConfig())
    _, kwargs = mocks['RedisDownloadWorker'].call_args
    assert kwargs['egress_mode'] == 'mullvad-socks5'
    assert kwargs['egress_exits'] == ['us-lax-wg-001', 'us-nyc-wg-301']


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

    health = LoopHealth('test_pool')

    # Must not raise despite the RuntimeError on the first iteration.
    await downloader_cli._drive_worker(worker, stop_event, log, health)  # pylint: disable=protected-access
    assert len(calls) == 2
    log.exception.assert_called_once()
    # The failed iteration was recorded, the following one re-armed the window.
    assert health.consecutive_errors == 0
    assert health.status == LoopStatus.OK


@pytest.mark.asyncio
async def test_drive_worker_exits_on_exit_early(mocker):
    '''ExitEarlyException breaks the loop without logging an error.'''
    stop_event = asyncio.Event()

    async def _run(_evt):
        raise ExitEarlyException('shutdown')

    worker = mocker.Mock()
    worker.run = _run
    log = mocker.Mock(spec=logging.Logger)
    health = LoopHealth('test_pool')
    await downloader_cli._drive_worker(worker, stop_event, log, health)  # pylint: disable=protected-access
    log.exception.assert_not_called()


@pytest.mark.asyncio
async def test_drive_worker_skips_when_already_stopped(mocker):
    '''A pre-set stop_event means run() is never called.'''
    stop_event = asyncio.Event()
    stop_event.set()
    worker = mocker.Mock()
    worker.run = mocker.AsyncMock()
    health = LoopHealth('test_pool')
    await downloader_cli._drive_worker(worker, stop_event, mocker.Mock(), health)  # pylint: disable=protected-access
    worker.run.assert_not_called()


@pytest.mark.asyncio
async def test_drive_worker_leaves_pool_health_running_for_siblings(mocker):
    '''
    One driver returning must NOT mark the shared pool health stopped.

    The drivers share a single LoopHealth; if each marked it stopped on exit, the
    first driver to finish would report the whole pool stopped while its siblings
    were still downloading, and a wedged pool would then be excluded from the
    health calculation instead of failing the probe.
    '''
    stop_event = asyncio.Event()

    async def _run(_evt):
        raise ExitEarlyException('shutdown')

    worker = mocker.Mock()
    worker.run = _run
    health = LoopHealth('test_pool')

    await downloader_cli._drive_worker(worker, stop_event, mocker.Mock(), health)  # pylint: disable=protected-access

    assert health.status != LoopStatus.STOPPED


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
    exit_probe = mocker.Mock()
    exit_probe.run = mocker.AsyncMock()

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
        downloader_cli.main_loop(worker, server, health_server, redis_manager, metrics,
                                 exit_probe),
        _fire_sigterm(),
    )

    redis_manager.start.assert_awaited_once()
    server.serve.assert_awaited_once()
    health_server.serve.assert_awaited_once()
    metrics.run.assert_awaited_once()
    exit_probe.run.assert_awaited_once()
    server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_closes_the_broker_session_on_drain(mocker):
    '''
    The downloader closes its outbound broker session on the way out too.

    Same gap as the search pod (both share cli/_lib/worker_pod): the session was
    never closed, so aiohttp logged `Unclosed client session` at ERROR on every
    roll. Covered here as well because the two pods drift apart exactly where
    only one of them is tested.
    '''
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
    broker_client = mocker.Mock()
    broker_client.close = mocker.AsyncMock()

    captured = {}

    def _fake_signal(signum, handler):
        captured[signum] = handler

    mocker.patch.object(cli_common.signal, 'signal', new=_fake_signal)

    async def _fire_sigterm():
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        downloader_cli.main_loop(worker, server, None, redis_manager, metrics, None,
                                 broker_client=broker_client),
        _fire_sigterm(),
    )

    broker_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_registers_one_shared_health_for_the_driver_pool(mocker):
    '''
    driver_count drivers share ONE LoopHealth entry, registered by main_loop.

    Per-driver entries would make the registry's all()-of-loops health test fail
    this pod's liveness probe whenever a single driver sat in a slow download —
    routine in pool mode, where a driver can block on a flagged exit.
    '''
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

    handles = []
    real_drive = downloader_cli._drive_worker  # pylint: disable=protected-access

    async def _spy_drive(drv_worker, evt, log, health):
        handles.append(health)
        await real_drive(drv_worker, evt, log, health)

    mocker.patch.object(downloader_cli, '_drive_worker', new=_spy_drive)

    captured = {}
    mocker.patch.object(cli_common.signal, 'signal', new=captured.__setitem__)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        downloader_cli.main_loop(worker, server, None, redis_manager, metrics, None,
                                 driver_count=3),
        _fire_sigterm(),
    )

    # Three drivers, one shared handle, one registry entry.
    assert len(handles) == 3
    assert all(h is handles[0] for h in handles)
    assert list(LOOP_HEALTH.snapshot()) == [downloader_cli.LOOP_DOWNLOADER_WORKER]


@pytest.mark.asyncio
async def test_main_loop_owns_marking_the_pool_stopped(mocker):
    '''
    main_loop marks the pool stopped on shutdown — the drivers never do.

    Asserted on the call rather than the resulting status: a driver finishing its
    final iteration after the drain calls record_success(), which re-arms the
    entry to 'ok'. That is harmless (both 'ok' and 'stopped' pass the probe;
    mark_stopped only guards against a false 'stalled'), but it makes the end
    state racy, whereas "who calls it" is exactly the regression under test.
    '''
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
    mocker.patch.object(cli_common.signal, 'signal', new=captured.__setitem__)

    # Spy on the registry-level call so we can attribute it to main_loop, and on
    # the per-loop call so we can prove no driver makes it.
    registry_stops = []
    mocker.patch.object(LOOP_HEALTH, 'mark_stopped',
                        new=lambda *names: registry_stops.extend(names))
    per_loop_stop = mocker.patch.object(LoopHealth, 'mark_stopped')

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        downloader_cli.main_loop(worker, server, None, redis_manager, metrics, None,
                                 driver_count=2),
        _fire_sigterm(),
    )

    assert registry_stops == [downloader_cli.LOOP_DOWNLOADER_WORKER]
    # Two drivers exited; neither marked the shared entry stopped.
    per_loop_stop.assert_not_called()


@pytest.mark.asyncio
async def test_main_loop_without_health_server(mocker):
    '''main_loop tolerates a None health_server and a None exit_probe (still drains).'''
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

    # exit_probe=None (attribution disabled) -> the guard skips scheduling it.
    await asyncio.gather(
        downloader_cli.main_loop(worker, server, None, redis_manager, metrics, None),
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
    worker, server, health, redis_manager, metrics, exit_probe = (
        object(), object(), object(), object(), object(), object())
    downloader_cli.run_downloader(worker, server, health, redis_manager, metrics, exit_probe)
    main_loop.assert_called_once_with(worker, server, health, redis_manager, metrics,
                                      exit_probe, driver_count=1, broker_client=None)
    run_loop.assert_called_once_with(sentinel)


def test_main_parses_config_and_runs(mocker):
    '''main() parses the config file then calls run().'''
    parse = mocker.patch.object(downloader_cli, 'parse_and_validate_config',
                                return_value=({'k': 'v'}, 'general'))
    run = mocker.patch.object(downloader_cli, 'run')
    downloader_cli.main.callback('cfg.yml')
    parse.assert_called_once_with('cfg.yml')
    run.assert_called_once_with({'k': 'v'}, 'general')
