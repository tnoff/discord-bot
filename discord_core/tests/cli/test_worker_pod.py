import asyncio
import signal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from discord_core.cli._lib import worker_pod
from discord_core.clients.redis_client import RedisManager
from discord_core.exceptions import DiscordBotException, ExitEarlyException
from discord_core.utils.common import GeneralConfig
from discord_core.utils.loop_health import LOOP_HEALTH, LoopHealth


@pytest.fixture(autouse=True)
def clean_registry():
    '''Keep the process-wide loop-health registry empty around each test.'''
    LOOP_HEALTH.reset()
    yield
    LOOP_HEALTH.reset()


def test_require_redis_manager_needs_redis():
    '''Without a redis url or sentinel the pod refuses to start.'''
    with pytest.raises(DiscordBotException, match='Redis required for search HA mode'):
        worker_pod.require_redis_manager(GeneralConfig(), 'search')


def test_require_redis_manager_builds_manager():
    '''A configured redis url yields a RedisManager.'''
    manager = worker_pod.require_redis_manager(GeneralConfig(redis_url='redis://localhost:6379'), 'search')
    assert isinstance(manager, RedisManager)


def test_require_redis_manager_accepts_sentinel():
    '''A sentinel config alone also satisfies the redis requirement.'''
    config = GeneralConfig(redis_sentinel={'sentinels': ['sentinel-a:26379']})
    assert isinstance(worker_pod.require_redis_manager(config, 'downloader'), RedisManager)


def test_require_broker_url_returns_url():
    '''The configured broker url is returned.'''
    settings = {'music': {'broker_client': {'url': 'http://broker:8080'}}}
    assert worker_pod.require_broker_url(settings, 'search') == 'http://broker:8080'


@pytest.mark.parametrize('settings', [
    {},
    {'music': {}},
    {'music': {'broker_client': None}},
    {'music': {'broker_client': {'url': ''}}},
])
def test_require_broker_url_missing_raises(settings):
    '''Any shape lacking music.broker_client.url fails with a clear message.'''
    with pytest.raises(DiscordBotException, match='music.broker_client.url required for search HA mode'):
        worker_pod.require_broker_url(settings, 'search')


@pytest.mark.parametrize('monitoring', [
    None,
    {'otlp': {'enabled': False}},
    {'otlp': {'enabled': False}, 'health_server': {'enabled': False}},
])
def test_build_redis_health_server_disabled_returns_none(monitoring):
    '''No enabled health_server block means no server.'''
    config = GeneralConfig(monitoring=monitoring)
    assert worker_pod.build_redis_health_server(config, MagicMock(), 'search') is None


def test_build_redis_health_server_enabled_uses_config():
    '''An enabled health_server builds the ping server with the configured port, bind address and pod.'''
    config = GeneralConfig(monitoring={
        'otlp': {'enabled': False},
        'health_server': {'enabled': True, 'port': 9191, 'bind_address': '127.0.0.1'},
    })
    manager = MagicMock()
    with patch.object(worker_pod, 'RedisPingHealthServer') as server_cls:
        result = worker_pod.build_redis_health_server(config, manager, 'search')
    assert result is server_cls.return_value
    server_cls.assert_called_once_with(manager, port=9191, bind_address='127.0.0.1', pod='search')


@pytest.mark.asyncio
async def test_drive_loop_records_success_until_stopped():
    '''Each iteration records success; the loop ends once the stop event is set.'''
    stop = asyncio.Event()
    health = MagicMock(spec=LoopHealth)
    calls = []

    async def iteration(event):
        calls.append(event)
        if len(calls) == 3:
            stop.set()

    await worker_pod.drive_loop(iteration, stop, MagicMock(), health, 'failed')
    assert calls == [stop] * 3
    assert health.record_success.call_count == 3
    health.record_error.assert_not_called()


@pytest.mark.asyncio
async def test_drive_loop_exit_early_breaks_without_success():
    '''ExitEarlyException ends the loop without recording success or error.'''
    health = MagicMock(spec=LoopHealth)
    iteration = AsyncMock(side_effect=ExitEarlyException)
    await worker_pod.drive_loop(iteration, asyncio.Event(), MagicMock(), health, 'failed')
    iteration.assert_awaited_once()
    health.record_success.assert_not_called()
    health.record_error.assert_not_called()


@pytest.mark.asyncio
async def test_drive_loop_logs_error_and_retries():
    '''An unexpected error is logged, recorded, backed off, and the loop carries on.'''
    stop = asyncio.Event()
    health = MagicMock(spec=LoopHealth)
    driver_logger = MagicMock()
    attempts = []

    async def iteration(_event):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError('redis blip')
        stop.set()

    with patch.object(worker_pod.asyncio, 'sleep', new=AsyncMock()) as sleep:
        await worker_pod.drive_loop(iteration, stop, driver_logger, health, 'consumer failed')
    assert len(attempts) == 2
    health.record_error.assert_called_once_with()
    health.record_success.assert_called_once_with()
    driver_logger.exception.assert_called_once_with('consumer failed')
    sleep.assert_awaited_once_with(1)


def _servers():
    '''Fake http and health servers whose serve/drain are awaitable.'''
    http_server = MagicMock()
    http_server.serve = AsyncMock()
    http_server.drain_and_stop = AsyncMock()
    health_server = MagicMock()
    health_server.serve = AsyncMock()
    return http_server, health_server


def _redis_manager():
    '''A RedisManager stand-in with awaitable start/close.'''
    manager = MagicMock()
    manager.start = AsyncMock()
    manager.close = AsyncMock()
    return manager


@pytest.mark.asyncio
async def test_main_loop_runs_until_signal_then_drains(captured_signals):
    '''Startup runs in order, tasks are scheduled, and a SIGTERM drains http, broker and redis.'''
    http_server, health_server = _servers()
    redis_manager = _redis_manager()
    broker = MagicMock()
    broker.close = AsyncMock()
    worker = MagicMock()
    worker.clear_stale_guild_blocks = AsyncMock(return_value=2)
    seen = {}

    def task_factory(_stop_event, loop_health):
        seen['health'] = loop_health

        async def extra():
            seen['extra'] = True
            captured_signals[signal.SIGTERM](signal.SIGTERM, None)
        return [extra()]

    with patch.object(worker_pod, 'start_seam_checks') as seam, \
            patch.object(worker_pod, 'create_observable_gauge') as gauge:
        await asyncio.wait_for(worker_pod.worker_pod_main_loop(
            http_server, health_server, redis_manager, 'search_loop', 'Search', task_factory,
            broker_client=broker, worker=worker), timeout=5)

    redis_manager.start.assert_awaited_once()
    worker.clear_stale_guild_blocks.assert_awaited_once()
    seam.assert_called_once_with()
    gauge.assert_called_once()
    assert gauge.call_args.args[3] == 'Search consumer loop heartbeat'
    assert gauge.call_args.args[2].args == ('search_loop',)
    assert seen['extra'] is True
    assert seen['health'] is LOOP_HEALTH.get('search_loop')
    assert seen['health'].status == 'stopped'
    http_server.serve.assert_awaited_once()
    health_server.serve.assert_awaited_once()
    http_server.drain_and_stop.assert_awaited_once()
    broker.close.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_minimal_pod_without_optionals(captured_signals):
    '''With no health server, broker client or worker, the loop still starts and drains.'''
    http_server, _ = _servers()
    redis_manager = _redis_manager()

    def task_factory(_stop_event, _loop_health):
        async def trigger():
            captured_signals[signal.SIGINT](signal.SIGINT, None)
        return [trigger()]

    with patch.object(worker_pod, 'start_seam_checks'), \
            patch.object(worker_pod, 'create_observable_gauge'):
        await asyncio.wait_for(worker_pod.worker_pod_main_loop(
            http_server, None, redis_manager, 'dl_loop', 'Downloader', task_factory), timeout=5)

    http_server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_main_loop_sweep_finding_nothing_is_quiet(captured_signals):
    '''A sweep that removes nothing logs no warning.'''
    http_server, _ = _servers()
    worker = MagicMock()
    worker.clear_stale_guild_blocks = AsyncMock(return_value=0)

    def task_factory(_stop_event, _loop_health):
        async def trigger():
            captured_signals[signal.SIGTERM](signal.SIGTERM, None)
        return [trigger()]

    with patch.object(worker_pod, 'start_seam_checks'), \
            patch.object(worker_pod, 'create_observable_gauge'), \
            patch.object(worker_pod, 'logger') as log:
        await asyncio.wait_for(worker_pod.worker_pod_main_loop(
            http_server, None, _redis_manager(), 'quiet_loop', 'Search', task_factory, worker=worker), timeout=5)
    log.warning.assert_not_called()


@pytest.mark.asyncio
async def test_main_loop_drains_even_when_task_factory_fails(captured_signals):
    '''A failure while scheduling still runs the drain path and propagates the error.'''
    http_server, _ = _servers()
    redis_manager = _redis_manager()

    def task_factory(_stop_event, _loop_health):
        raise RuntimeError('factory broke')

    with patch.object(worker_pod, 'start_seam_checks'), \
            patch.object(worker_pod, 'create_observable_gauge'):
        with pytest.raises(RuntimeError, match='factory broke'):
            await worker_pod.worker_pod_main_loop(
                http_server, None, redis_manager, 'broken_loop', 'Search', task_factory)
    assert captured_signals
    http_server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()
    assert LOOP_HEALTH.get('broken_loop').status == 'stopped'
