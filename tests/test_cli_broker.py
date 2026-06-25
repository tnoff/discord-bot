'''Tests for the standalone broker entrypoint (discord_bot.cli.broker).'''
import asyncio
import signal as _signal
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_bot.cli import broker as broker_cli


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
    db_engine = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=db_engine)
    cm.__exit__ = MagicMock(return_value=False)
    return {
        'observability': mocker.patch('discord_bot.cli.broker.setup_observability'),
        'instrument': mocker.patch('discord_bot.cli.broker.instrument_sqlalchemy'),
        'managed_db': mocker.patch('discord_bot.cli.broker.managed_db', return_value=cm),
        'redis_manager': mocker.patch('discord_bot.cli.broker.RedisManager', return_value=MagicMock()),
        'registry': mocker.patch('discord_bot.cli.broker.RedisBrokerRegistry', return_value=MagicMock()),
        'result_queue': mocker.patch('discord_bot.cli.broker.RedisDownloadResultQueue', return_value=MagicMock()),
        'video_cache': mocker.patch('discord_bot.cli.broker._build_video_cache', return_value=video_cache),
        'dispatch': mocker.patch('discord_bot.cli.broker.HttpDispatchClient', return_value=MagicMock()),
        'broker': mocker.patch('discord_bot.cli.broker.RedisBroker', return_value=MagicMock()),
        'server': mocker.patch('discord_bot.cli.broker.BrokerHttpServer', return_value=MagicMock()),
        'health': mocker.patch('discord_bot.cli.broker.BrokerHealthServer', return_value=MagicMock()),
        'run_broker': mocker.patch('discord_bot.cli.broker.run_broker'),
        'db_engine': db_engine,
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
    # Server gets the Redis-backed result queue, no ha_mode.
    assert m['server'].call_args.kwargs['result_queue'] is m['result_queue'].return_value


def test_run_without_dispatcher_or_health(mocker):
    m = _patch_run_deps(mocker)
    broker_cli.run(_settings(dispatch_url=None), _general_config(health_enabled=False))
    m['dispatch'].assert_not_called()
    m['health'].assert_not_called()
    assert m['broker'].call_args.kwargs['dispatcher'] is None
    m['run_broker'].assert_called_once()


def test_build_video_cache_returns_none_when_disabled():
    assert broker_cli._build_video_cache({}, MagicMock(), 'b') is None  # pylint: disable=protected-access
    assert broker_cli._build_video_cache({'enable_cache_files': True}, None, 'b') is None  # pylint: disable=protected-access
    assert broker_cli._build_video_cache({'enable_cache_files': True}, MagicMock(), None) is None  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_build_video_cache_constructs_and_session_generator_works(mocker):
    mock_vc = mocker.patch('discord_bot.cli.broker.VideoCacheClient', return_value='VC')
    mock_session = AsyncMock()
    factory_cm = MagicMock()
    factory_cm.__aenter__ = AsyncMock(return_value=mock_session)
    factory_cm.__aexit__ = AsyncMock(return_value=False)
    mocker.patch('discord_bot.cli.broker.async_sessionmaker', return_value=MagicMock(return_value=factory_cm))

    result = broker_cli._build_video_cache(  # pylint: disable=protected-access
        {'enable_cache_files': True, 'max_cache_files': 10, 'max_cache_size_mb': 5},
        MagicMock(), 'bucket',
    )
    assert result == 'VC'
    assert mock_vc.call_args.kwargs['max_cache_size_bytes'] == 5 * 1024 * 1024
    # Drive the session-generator closure end-to-end.
    session_generator = mock_vc.call_args.args[1]
    async with session_generator() as session:
        assert session is mock_session


@pytest.mark.asyncio
async def test_build_video_cache_no_size_limit(mocker):
    mock_vc = mocker.patch('discord_bot.cli.broker.VideoCacheClient', return_value='VC')
    broker_cli._build_video_cache(  # pylint: disable=protected-access
        {'enable_cache_files': True, 'max_cache_files': 3, 'max_cache_size_mb': None},
        MagicMock(), 'bucket',
    )
    assert mock_vc.call_args.kwargs['max_cache_size_bytes'] is None


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

    task = asyncio.create_task(broker_cli.main_loop(broker_server, health_server, redis_manager))
    await asyncio.sleep(0)  # let main_loop reach stop_event.wait() and register handlers
    captured[_signal.SIGTERM](_signal.SIGTERM, None)  # simulate SIGTERM
    await task

    redis_manager.start.assert_awaited_once()
    broker_server.drain_and_stop.assert_awaited_once()
    redis_manager.close.assert_awaited_once()


def test_run_broker_invokes_run_loop(mocker):
    mock_run_loop = mocker.patch('discord_bot.cli.broker.run_loop')
    sentinel = object()
    # Force a sync mock so main_loop(...) returns the sentinel rather than a coroutine.
    mocker.patch('discord_bot.cli.broker.main_loop', new=MagicMock(return_value=sentinel))
    broker_cli.run_broker(MagicMock(), MagicMock(), MagicMock())
    mock_run_loop.assert_called_once_with(sentinel)


def test_main_parses_config_and_runs(mocker):
    mocker.patch('discord_bot.cli.broker.parse_and_validate_config',
                 return_value=({'k': 'v'}, 'gc'))
    mock_run = mocker.patch('discord_bot.cli.broker.run')
    broker_cli.main.callback('config.cnf')
    mock_run.assert_called_once_with({'k': 'v'}, 'gc')
