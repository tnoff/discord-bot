'''Tests for the standalone search CLI entrypoint (the shared search pod).'''
import asyncio
import logging

import pytest

from discord_bot.cli import search as search_cli
from discord_bot.cli._lib import common as cli_common
# RedisManager + the health server are constructed by the shared worker-pod
# scaffolding now (cli/_lib/worker_pod.py), so that is where they are patched.
from discord_bot.cli._lib import worker_pod
from discord_bot.exceptions import DiscordBotException, ExitEarlyException
from discord_bot.utils.loop_health import LOOP_HEALTH, LoopHealth, LoopStatus


def _settings(*, broker_url='http://broker:8081', extra_download=None, extra_general=None):
    '''Build a minimal raw settings dict for run().'''
    download = dict(extra_download or {})
    general = dict(extra_general or {})
    return {
        'general': general,
        'music': {
            'broker_client': {'url': broker_url},
            'download': download,
        },
    }


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
        'setup_observability': mocker.patch.object(search_cli, 'setup_observability'),
        'RedisManager': mocker.patch.object(worker_pod, 'RedisManager'),
        'HttpBrokerClient': mocker.patch.object(search_cli, 'HttpBrokerClient'),
        'YoutubeMusicClient': mocker.patch.object(search_cli, 'YoutubeMusicClient'),
        'RedisYoutubeMusicSearchWorker': mocker.patch.object(
            search_cli, 'RedisYoutubeMusicSearchWorker'),
        'YoutubeMusicSearchHttpServer': mocker.patch.object(
            search_cli, 'YoutubeMusicSearchHttpServer'),
        'RedisPingHealthServer': mocker.patch.object(worker_pod, 'RedisPingHealthServer'),
        'SearchMetrics': mocker.patch.object(search_cli, 'SearchMetrics'),
        'YoutubeMusicSearchDriver': mocker.patch.object(search_cli, 'YoutubeMusicSearchDriver'),
        'run_search': mocker.patch.object(search_cli, 'run_search'),
    }


def test_run_wires_collaborators(mocker):
    '''run() builds redis/broker/worker/server/driver and calls run_search.'''
    mocks = _patch_collaborators(mocker)
    redis_manager = mocks['RedisManager'].from_general_config.return_value
    broker_client = mocks['HttpBrokerClient'].return_value
    worker = mocks['RedisYoutubeMusicSearchWorker'].return_value
    server = mocks['YoutubeMusicSearchHttpServer'].return_value
    driver = mocks['YoutubeMusicSearchDriver'].return_value

    general = _GeneralConfig()
    search_cli.run(_settings(), general)

    mocks['setup_observability'].assert_called_once_with(general)
    mocks['RedisManager'].from_general_config.assert_called_once_with(general)
    # No bucket_name: the search pod never checks media out of S3.
    mocks['HttpBrokerClient'].assert_called_once_with('http://broker:8081')

    # Worker wiring: the injected ytmusic client + redis manager + backoff defaults.
    worker_args, worker_kwargs = mocks['RedisYoutubeMusicSearchWorker'].call_args
    assert worker_args[1] is mocks['YoutubeMusicClient'].return_value
    assert worker_args[3] == 30  # youtube_wait_period_minimum
    assert worker_args[4] == 10  # youtube_wait_period_max_variance
    assert worker_kwargs['redis_manager'] is redis_manager

    # The driver pops straight off the worker (no client indirection in the pod)
    # and reports back through the HTTP broker client.
    driver_args, driver_kwargs = mocks['YoutubeMusicSearchDriver'].call_args
    assert driver_args[0] is worker
    assert driver_args[1] is broker_client
    assert driver_kwargs['max_retries'] == 3
    assert driver_kwargs['queue_priority'] == {}

    # Server wiring: worker + default port 8084 + default host.
    server_args, server_kwargs = mocks['YoutubeMusicSearchHttpServer'].call_args
    assert server_args[0] is worker
    assert server_kwargs['port'] == 8084
    assert server_kwargs['host'] == '0.0.0.0'

    mocks['SearchMetrics'].assert_called_once_with(worker)
    # No monitoring -> no health server.
    mocks['RedisPingHealthServer'].assert_not_called()
    # broker_client is forwarded so the drain path can close its session, and the
    # worker so the pod can sweep stale guild blocks before it serves.
    mocks['run_search'].assert_called_once_with(
        driver, server, None, redis_manager, mocks['SearchMetrics'].return_value,
        broker_client=broker_client, worker=worker)


def test_run_forwards_backoff_and_retry_config(mocker):
    '''music.download backoff / retry / failure-tracking config reaches the worker.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={
        'youtube_wait_period_minimum': 45,
        'youtube_wait_period_max_variance': 20,
        'max_youtube_music_search_retries': 5,
        'failure_tracking_max_size': 42,
        'failure_tracking_max_age_seconds': 900,
    })
    search_cli.run(settings, _GeneralConfig())

    worker_args, _ = mocks['RedisYoutubeMusicSearchWorker'].call_args
    assert worker_args[3] == 45
    assert worker_args[4] == 20
    failure_queue = worker_args[2]
    assert failure_queue.queue.maxsize == 42
    assert failure_queue.max_age_seconds == 900
    assert mocks['YoutubeMusicSearchDriver'].call_args.kwargs['max_retries'] == 5


def test_run_forwards_server_queue_priority(mocker):
    '''
    Guild priorities reach the driver, so a retried request keeps its priority.

    The re-enqueue happens on the pod now, out of reach of the cog's own priority
    map — without this a prioritised guild's retries would silently drop to the
    default bucket.
    '''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_download={'server_queue_priority': [
        {'server_id': '42', 'priority': 1},
        {'server_id': 77, 'priority': 5},
    ]})
    search_cli.run(settings, _GeneralConfig())
    assert mocks['YoutubeMusicSearchDriver'].call_args.kwargs['queue_priority'] == {42: 1, 77: 5}


def test_run_respects_configured_server_host_and_port(mocker):
    '''general.search_server overrides host/port.'''
    mocks = _patch_collaborators(mocker)
    settings = _settings(extra_general={'search_server': {'host': '127.0.0.1', 'port': 9099}})
    search_cli.run(settings, _GeneralConfig())
    _, server_kwargs = mocks['YoutubeMusicSearchHttpServer'].call_args
    assert server_kwargs['host'] == '127.0.0.1'
    assert server_kwargs['port'] == 9099


def test_run_builds_health_server_when_enabled(mocker):
    '''A monitoring.health_server.enabled config builds a RedisPingHealthServer.'''
    mocks = _patch_collaborators(mocker)
    health_cfg = mocker.Mock(enabled=True, port=8080, bind_address='0.0.0.0')
    monitoring = mocker.Mock(health_server=health_cfg)
    search_cli.run(_settings(), _GeneralConfig(monitoring=monitoring))
    mocks['RedisPingHealthServer'].assert_called_once()
    _, hs_kwargs = mocks['RedisPingHealthServer'].call_args
    assert hs_kwargs['port'] == 8080
    assert hs_kwargs['bind_address'] == '0.0.0.0'
    assert mocks['run_search'].call_args[0][2] is mocks['RedisPingHealthServer'].return_value


def test_run_missing_redis_raises(mocker):
    '''No redis_url and no redis_sentinel -> DiscordBotException.'''
    _patch_collaborators(mocker)
    general = _GeneralConfig(redis_url=None, redis_sentinel=None)
    with pytest.raises(DiscordBotException, match='Redis required'):
        search_cli.run(_settings(), general)


def test_run_missing_broker_url_raises(mocker):
    '''No music.broker_client.url -> DiscordBotException.'''
    _patch_collaborators(mocker)
    settings = _settings()
    settings['music']['broker_client'] = {}
    with pytest.raises(DiscordBotException, match='broker_client.url required'):
        search_cli.run(settings, _GeneralConfig())


# NOTE: `_loop_guards_` rather than the name symmetric with the downloader's
# equivalent test, purely to dodge a trufflehog false positive: its Lob detector
# reads a 40-character `test_`-prefixed token as an API key and *verifies* it,
# failing pr-check:secrets. The symmetric name is exactly 40 characters long.
# Keep any rename off that length — and don't spell the 40-character form out in
# a comment either, since trufflehog scans the diff, not just the code.
@pytest.mark.asyncio
async def test_drive_search_loop_guards_broad_exception(mocker):
    '''A run_once() exception is logged and the loop continues (does not propagate).'''
    stop_event = asyncio.Event()
    calls = []

    async def _run_once(_evt):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError('redis blip')
        stop_event.set()

    driver = mocker.Mock()
    driver.run_once = _run_once
    mocker.patch.object(search_cli.asyncio, 'sleep', new=mocker.AsyncMock())
    log = mocker.Mock(spec=logging.Logger)
    health = LoopHealth('test_search')

    await search_cli._drive_search(driver, stop_event, log, health)  # pylint: disable=protected-access

    assert len(calls) == 2
    log.exception.assert_called_once()
    # The failed iteration was recorded, the following one re-armed the window.
    assert health.consecutive_errors == 0
    assert health.status == LoopStatus.OK


@pytest.mark.asyncio
async def test_drive_search_exits_on_exit_early(mocker):
    '''ExitEarlyException (shutdown mid-backoff) breaks the loop without logging.'''
    stop_event = asyncio.Event()

    async def _run_once(_evt):
        raise ExitEarlyException('shutdown')

    driver = mocker.Mock()
    driver.run_once = _run_once
    log = mocker.Mock(spec=logging.Logger)
    await search_cli._drive_search(driver, stop_event, log, LoopHealth('test_search'))  # pylint: disable=protected-access
    log.exception.assert_not_called()


@pytest.mark.asyncio
async def test_drive_search_skips_when_already_stopped(mocker):
    '''A pre-set stop_event means run_once() is never called.'''
    stop_event = asyncio.Event()
    stop_event.set()
    driver = mocker.Mock()
    driver.run_once = mocker.AsyncMock()
    await search_cli._drive_search(driver, stop_event, mocker.Mock(), LoopHealth('test_search'))  # pylint: disable=protected-access
    driver.run_once.assert_not_called()


def _shutdown_collaborators(mocker):
    '''Build the async collaborator mocks main_loop drives.'''
    redis_manager = mocker.Mock()
    redis_manager.start = mocker.AsyncMock()
    redis_manager.close = mocker.AsyncMock()
    server = mocker.Mock()
    server.serve = mocker.AsyncMock()
    server.drain_and_stop = mocker.AsyncMock()
    metrics = mocker.Mock()
    metrics.run = mocker.AsyncMock()
    driver = mocker.Mock()

    async def _run_once(evt):
        # Block on the shutdown event so the driver task doesn't busy-loop.
        await evt.wait()

    driver.run_once = _run_once
    return driver, server, redis_manager, metrics


def _capture_signals(mocker):
    '''Intercept signal registration so the test can fire SIGTERM/SIGINT itself.'''
    captured = {}

    def _fake_signal(signum, handler):
        captured[signum] = handler

    mocker.patch.object(cli_common.signal, 'signal', new=_fake_signal)
    return captured


@pytest.mark.asyncio
async def test_main_loop_shutdown_drains_and_closes(mocker):
    '''main_loop sets stop_event on SIGTERM and drains server + closes redis in finally.'''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    health_server = mocker.Mock()
    health_server.serve = mocker.AsyncMock()
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, health_server, redis_manager, metrics),
        _fire_sigterm(),
    )

    redis_manager.start.assert_awaited_once()
    server.serve.assert_awaited_once()
    health_server.serve.assert_awaited_once()
    metrics.run.assert_awaited_once()
    server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_registers_the_search_loop_health(mocker):
    '''
    The POD-side driver owns the search LoopHealth, and it is the only one here.

    The bot-side status poller deliberately registers none (a search-pod outage
    must not restart the bot pod); this loop is the one that genuinely wedges, so
    a stalled one has to fail this pod's own probe.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics),
        _fire_sigterm(),
    )

    assert list(LOOP_HEALTH.snapshot()) == [search_cli.LOOP_SEARCH_WORKER]


@pytest.mark.asyncio
async def test_main_loop_publishes_the_loop_heartbeat_gauge(mocker):
    '''
    The pod publishes its consumer loop's heartbeat, under the loop's own name.

    Without this the only heartbeat series the pod emitted was the HTTP server's
    (`youtube_music_search_server`), which reports is_serving — the TCP site is up,
    not that the loop is turning — so a wedged loop read green in Mimir until the
    liveness probe restarted the pod. It also means the cog's
    `youtube_music_search` series MOVES job labels at the MR 6 cutover instead of
    disappearing, which is the whole reason the pod reuses the cog's loop name.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    gauge = mocker.patch.object(worker_pod, 'create_observable_gauge')
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics),
        _fire_sigterm(),
    )

    gauge.assert_called_once()
    assert gauge.call_args.args[1] == 'heartbeat'
    # The bound loop name is what becomes the background_job label.
    assert gauge.call_args.args[2].args == (search_cli.LOOP_SEARCH_WORKER,)


@pytest.mark.asyncio
async def test_pod_loop_heartbeat_reports_the_registered_loop(mocker):
    '''
    The published gauge reads the pod's real LoopHealth, so it reports 1 while the
    loop is completing iterations and 0 once it has gone stale — the same bit the
    /health probe reads, rather than a second, independent notion of liveness.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    observations = {}
    mocker.patch.object(
        worker_pod, 'create_observable_gauge',
        new=lambda _meter, name, callback, _desc: observations.setdefault(name, callback))
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        # Read the gauge while the loop is still registered and healthy.
        observations['result'] = observations['heartbeat']()
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics),
        _fire_sigterm(),
    )

    [observation] = observations['result']
    assert observation.value == 1
    assert observation.attributes['background_job'] == search_cli.LOOP_SEARCH_WORKER


@pytest.mark.asyncio
async def test_main_loop_closes_the_broker_session_on_drain(mocker):
    '''
    The pod's outbound broker session is closed on the way out.

    Without this aiohttp logs `Unclosed client session` at ERROR on every pod
    roll — observed in prod on the search-cutover rollout. The process is exiting
    so nothing leaks for long, but it is recurring ERROR noise in exactly the
    window an operator reads logs during a deploy.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    broker_client = mocker.Mock()
    broker_client.close = mocker.AsyncMock()
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics,
                             broker_client=broker_client),
        _fire_sigterm(),
    )

    broker_client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_drains_without_a_broker_client(mocker):
    '''
    The broker client is optional, so a pod built without one still drains
    cleanly rather than tripping over None in the shutdown path.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics),
        _fire_sigterm(),
    )

    server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_marks_the_loop_stopped_on_drain(mocker):
    '''
    A deliberate drain marks the loop stopped, so a draining pod doesn't fail its
    own liveness probe on the way out.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    captured = _capture_signals(mocker)
    registry_stops = []
    mocker.patch.object(LOOP_HEALTH, 'mark_stopped',
                        new=lambda *names: registry_stops.extend(names))

    async def _fire_sigint():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGINT](cli_common.signal.SIGINT, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics),
        _fire_sigint(),
    )

    assert registry_stops == [search_cli.LOOP_SEARCH_WORKER]


@pytest.mark.asyncio
async def test_main_loop_sweeps_stale_blocks_when_given_a_worker(mocker):
    '''
    The search pod sweeps its own legacy blocks on start.

    Covered on both pods because they thread the worker through separately —
    cli/_lib/worker_pod is shared, but the wiring that reaches it is not, and a
    default that silently stays None is exactly how this arrives dead in prod.
    '''
    driver, server, redis_manager, metrics = _shutdown_collaborators(mocker)
    worker = mocker.Mock()
    worker.clear_stale_guild_blocks = mocker.AsyncMock(return_value=1)
    captured = _capture_signals(mocker)

    async def _fire_sigterm():
        for _ in range(4):
            await asyncio.sleep(0)
        captured[cli_common.signal.SIGTERM](cli_common.signal.SIGTERM, None)

    await asyncio.gather(
        search_cli.main_loop(driver, server, None, redis_manager, metrics, worker=worker),
        _fire_sigterm(),
    )

    worker.clear_stale_guild_blocks.assert_awaited_once()


def test_run_search_delegates_to_run_loop(mocker):
    '''run_search hands the main_loop coroutine to run_loop.'''
    run_loop = mocker.patch.object(search_cli, 'run_loop')
    sentinel = object()
    main_loop = mocker.Mock(return_value=sentinel)
    mocker.patch.object(search_cli, 'main_loop', new=main_loop)
    driver, server, health, redis_manager, metrics = (
        object(), object(), object(), object(), object())
    search_cli.run_search(driver, server, health, redis_manager, metrics)
    main_loop.assert_called_once_with(driver, server, health, redis_manager, metrics,
                                      broker_client=None, worker=None)
    run_loop.assert_called_once_with(sentinel)


def test_main_parses_config_and_runs(mocker):
    '''main() parses the config file then calls run().'''
    parse = mocker.patch.object(search_cli, 'parse_and_validate_config',
                                return_value=({'k': 'v'}, 'general'))
    run = mocker.patch.object(search_cli, 'run')
    search_cli.main.callback('cfg.yml')
    parse.assert_called_once_with('cfg.yml')
    run.assert_called_once_with({'k': 'v'}, 'general')
