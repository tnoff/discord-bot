'''Tests for the standalone broker entrypoint (discord_bot.cli.broker).'''
import asyncio
import signal as _signal
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.cli import broker as broker_cli
from discord_bot.clients.http_video_cache_store import HttpVideoCacheStore


def _general_config(health_enabled=True):
    gc = MagicMock()
    gc.redis_url = 'redis://x'
    gc.monitoring.health_server.enabled = health_enabled
    gc.monitoring.health_server.port = 8080
    gc.monitoring.health_server.bind_address = '0.0.0.0'
    return gc


def _settings(dispatch_url='http://disp'):
    return {
        'general': {
            'dispatch_http_url': dispatch_url,
            'broker_server': {'host': '0.0.0.0', 'port': 8081},
        },
        'music': {
            'storage': {'bucket_name': 'my-bucket'},
            'download': {'max_download_retries': 5, 'max_youtube_music_search_retries': 4},
        },
    }


def _patch_run_deps(mocker, video_cache=None):
    '''Patch every heavy dependency cli.broker.run touches; return the mocks.'''
    return {
        'observability': mocker.patch('discord_bot.cli.broker.setup_observability'),
        'redis_manager': mocker.patch('discord_bot.cli.broker.RedisManager', return_value=MagicMock()),
        'registry': mocker.patch('discord_bot.cli.broker.RedisBrokerRegistry', return_value=MagicMock()),
        'result_queue': mocker.patch('discord_bot.cli.broker.RedisDownloadResultQueue', return_value=MagicMock()),
        'search_result_queue': mocker.patch('discord_bot.cli.broker.RedisSearchResultQueue', return_value=MagicMock()),
        'metrics': mocker.patch('discord_bot.cli.broker.BrokerMetrics', return_value=MagicMock()),
        'video_cache': mocker.patch('discord_bot.cli.broker._build_video_cache', return_value=video_cache),
        'dispatch': mocker.patch('discord_bot.cli.broker.HttpDispatchClient', return_value=MagicMock()),
        'broker': mocker.patch('discord_bot.cli.broker.RedisBroker', return_value=MagicMock()),
        'server': mocker.patch('discord_bot.cli.broker.BrokerHttpServer', return_value=MagicMock()),
        'health': mocker.patch('discord_bot.cli.broker.BrokerHealthServer', return_value=MagicMock()),
        'run_broker': mocker.patch('discord_bot.cli.broker.run_broker'),
    }


def test_run_constructs_broker_with_dispatcher_and_health(mocker):
    m = _patch_run_deps(mocker, video_cache='VC')
    broker_cli.run(_settings(), _general_config(health_enabled=True))
    m['run_broker'].assert_called_once()
    m['dispatch'].assert_called_once_with('http://disp')
    m['health'].assert_called_once()
    # RedisBroker built with the config-derived retry limits + video_cache + bucket.
    kwargs = m['broker'].call_args.kwargs
    assert kwargs['video_cache'] == 'VC'
    assert kwargs['bucket_name'] == 'my-bucket'
    assert kwargs['download_max_retries'] == 5
    assert kwargs['search_max_retries'] == 4
    # Server gets the Redis-backed result queues (download + search), no ha_mode.
    assert m['server'].call_args.kwargs['result_queue'] is m['result_queue'].return_value
    assert m['server'].call_args.kwargs['search_result_queue'] is m['search_result_queue'].return_value
    # Metrics poller built from the result queue + registry + search queue, handed to run_broker.
    m['metrics'].assert_called_once_with(
        m['result_queue'].return_value, m['registry'].return_value,
        search_result_queue=m['search_result_queue'].return_value)
    assert m['run_broker'].call_args.args[3] is m['metrics'].return_value


def test_run_without_dispatcher_or_health(mocker):
    m = _patch_run_deps(mocker)
    broker_cli.run(_settings(dispatch_url=None), _general_config(health_enabled=False))
    m['dispatch'].assert_not_called()
    m['health'].assert_not_called()
    assert m['broker'].call_args.kwargs['dispatcher'] is None
    m['run_broker'].assert_called_once()


def test_build_video_cache_returns_none_when_disabled(caplog):
    '''Cache off, no db pod, or no bucket each yield no catalog client.'''
    with caplog.at_level('WARNING'):
        assert broker_cli._build_video_cache({}, 'http://discord-db:8085', 'b') is None  # pylint: disable=protected-access
        assert broker_cli._build_video_cache({'enable_cache_files': True}, None, 'b') is None  # pylint: disable=protected-access
        assert broker_cli._build_video_cache({'enable_cache_files': True}, 'http://d:8085', None) is None  # pylint: disable=protected-access


def test_build_video_cache_constructs_the_http_store():
    '''
    The catalog is an HTTP client now, pointed at the configured pod.

    This replaces the session-generator test that stood here: there is no session
    to generate any more, and driving the closure end-to-end was only ever a proxy
    for "the client can reach the rows".
    '''
    result = broker_cli._build_video_cache(  # pylint: disable=protected-access
        {'enable_cache_files': True, 'max_cache_files': 10, 'max_cache_size_mb': 5},
        'http://discord-db:8085', 'bucket',
    )
    assert isinstance(result, HttpVideoCacheStore)
    assert result._base_url == 'http://discord-db:8085'  # pylint: disable=protected-access
    # The eviction knobs were accepted and deliberately dropped: they describe the
    # catalog, which this process no longer owns. The db pod reads them from its
    # own config. See tests/cli/test_database.py for the guard that moved there.
    assert not hasattr(result, 'max_cache_files')


@pytest.mark.asyncio
async def test_main_loop_drains_on_signal(mocker):
    captured = {}
    mocker.patch('discord_bot.cli.broker.signal.signal', side_effect=captured.__setitem__)
    broker_server = MagicMock()
    broker_server.serve = AsyncMock()
    broker_server.drain_and_stop = AsyncMock()
    health_server = MagicMock()
    health_server.serve = AsyncMock()
    redis_manager = MagicMock()
    redis_manager.start = AsyncMock()
    redis_manager.close = AsyncMock()
    broker_metrics = MagicMock()
    broker_metrics.run = AsyncMock()

    task = asyncio.create_task(
        broker_cli.main_loop(broker_server, health_server, redis_manager, broker_metrics))
    await asyncio.sleep(0)  # let main_loop reach stop_event.wait() and register handlers
    captured[_signal.SIGTERM](_signal.SIGTERM, None)  # simulate SIGTERM
    await task

    redis_manager.start.assert_awaited_once()
    broker_server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()
    broker_metrics.run.assert_called_once()  # metrics poller was started


def test_run_broker_invokes_run_loop(mocker):
    mock_run_loop = mocker.patch('discord_bot.cli.broker.run_loop')
    sentinel = object()
    # Force a sync mock so main_loop(...) returns the sentinel rather than a coroutine.
    mocker.patch('discord_bot.cli.broker.main_loop', new=MagicMock(return_value=sentinel))
    broker_cli.run_broker(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    mock_run_loop.assert_called_once_with(sentinel)


def test_main_parses_config_and_runs(mocker):
    mocker.patch('discord_bot.cli.broker.parse_and_validate_config',
                 return_value=({'k': 'v'}, 'gc'))
    mock_run = mocker.patch('discord_bot.cli.broker.run')
    broker_cli.main.callback('config.cnf')
    mock_run.assert_called_once_with({'k': 'v'}, 'gc')
