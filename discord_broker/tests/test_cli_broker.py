'''Tests for the standalone broker entrypoint (discord_bot.cli.broker).'''
import asyncio
import signal as _signal
from unittest.mock import AsyncMock, MagicMock

import pytest

from discord_broker.cli import broker as broker_cli
from discord_broker.clients.http_video_cache_store import HttpVideoCacheStore


def _general_config(health_enabled=True):
    gc = MagicMock()
    gc.redis_url = 'redis://x'
    gc.monitoring.health_server.enabled = health_enabled
    gc.monitoring.health_server.port = 8080
    gc.monitoring.health_server.bind_address = '0.0.0.0'
    return gc


def _settings(dispatch_url='http://disp', database_url='http://db', playlist=None):
    settings = {
        'general': {
            'dispatch_http_url': dispatch_url,
            'broker_server': {'host': '0.0.0.0', 'port': 8081},
        },
        'music': {
            'storage': {'bucket_name': 'my-bucket'},
            'download': {'max_download_retries': 5, 'max_youtube_music_search_retries': 4},
        },
    }
    if database_url:
        settings['general']['database_http_url'] = database_url
    if playlist is not None:
        settings['music']['playlist'] = playlist
    return settings


def _patch_run_deps(mocker, video_cache=None):
    '''Patch every heavy dependency cli.broker.run touches; return the mocks.'''
    return {
        'observability': mocker.patch('discord_broker.cli.broker.setup_observability'),
        'redis_manager': mocker.patch('discord_broker.cli.broker.RedisManager', return_value=MagicMock()),
        'registry': mocker.patch('discord_broker.cli.broker.RedisBrokerRegistry', return_value=MagicMock()),
        'result_queue': mocker.patch('discord_broker.cli.broker.RedisDownloadResultQueue', return_value=MagicMock()),
        'search_result_queue': mocker.patch('discord_broker.cli.broker.RedisSearchResultQueue', return_value=MagicMock()),
        'metrics': mocker.patch('discord_broker.cli.broker.BrokerMetrics', return_value=MagicMock()),
        'video_cache': mocker.patch('discord_broker.cli.broker._build_video_cache', return_value=video_cache),
        'dispatch': mocker.patch('discord_broker.cli.broker.HttpDispatchClient', return_value=MagicMock()),
        'broker': mocker.patch('discord_broker.cli.broker.RedisBroker', return_value=MagicMock()),
        'guild_queue': mocker.patch('discord_broker.cli.broker.GuildQueueBroker', return_value=MagicMock()),
        'guild_queue_registry': mocker.patch('discord_broker.cli.broker.GuildQueueRegistry', return_value=MagicMock()),
        'history_worker': mocker.patch('discord_broker.cli.broker.HistoryWorker', return_value=MagicMock()),
        'history_playlists': mocker.patch('discord_broker.cli.broker.HttpHistoryPlaylistStore',
                                          return_value=MagicMock()),
        'history_analytics': mocker.patch('discord_broker.cli.broker.HttpPlayAnalyticsStore',
                                          return_value=MagicMock()),
        'server': mocker.patch('discord_broker.cli.broker.BrokerHttpServer', return_value=MagicMock()),
        'health': mocker.patch('discord_broker.cli.broker.BrokerHealthServer', return_value=MagicMock()),
        'run_broker': mocker.patch('discord_broker.cli.broker.run_broker'),
    }


def test_run_constructs_broker_with_dispatcher_and_health(mocker):
    m = _patch_run_deps(mocker, video_cache='VC')
    general_config = _general_config(health_enabled=True)
    broker_cli.run(_settings(), general_config)
    m['run_broker'].assert_called_once()
    m['dispatch'].assert_called_once_with(
        'http://disp', seam_contract=general_config.seam_contract)
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
    # The guild queue is built over the same broker and Redis manager, and handed to the server.
    m['guild_queue_registry'].assert_called_once_with(m['redis_manager'].from_general_config.return_value)
    m['guild_queue'].assert_called_once_with(
        m['broker'].return_value, m['guild_queue_registry'].return_value, m['dispatch'].return_value,
        record_plays=True)
    assert m['server'].call_args.kwargs['guild_queue'] is m['guild_queue'].return_value
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
    # No dispatcher means no play-order message; the queue itself is still built.
    assert m['guild_queue'].call_args.args[2] is None
    m['run_broker'].assert_called_once()


def test_run_builds_the_history_worker_over_the_db_pod(mocker):
    '''Plays are recorded through the db pod, into the same registry the queue writes to.'''
    m = _patch_run_deps(mocker)
    general_config = _general_config(health_enabled=False)
    broker_cli.run(_settings(database_url='http://discord-db:8085'), general_config)

    m['history_playlists'].assert_called_once_with(
        'http://discord-db:8085', seam_contract=general_config.seam_contract)
    m['history_analytics'].assert_called_once_with(
        'http://discord-db:8085', seam_contract=general_config.seam_contract)
    m['history_worker'].assert_called_once_with(
        m['guild_queue_registry'].return_value, m['history_playlists'].return_value,
        m['history_analytics'].return_value, max_size=64)
    assert m['run_broker'].call_args.kwargs['history_worker'] is m['history_worker'].return_value


def test_run_reads_the_history_ceiling_from_the_playlist_config(mocker):
    '''The same setting the gateway reads, so a guild's history keeps the size it always did.'''
    m = _patch_run_deps(mocker)
    broker_cli.run(_settings(playlist={'server_playlist_max_size': 10}), _general_config(health_enabled=False))
    assert m['history_worker'].call_args.kwargs['max_size'] == 10


def test_run_without_a_db_pod_records_nothing_and_says_so(mocker, caplog):
    '''No db pod means nothing could drain the records, so none are queued.'''
    m = _patch_run_deps(mocker)
    with caplog.at_level('WARNING'):
        broker_cli.run(_settings(database_url=None), _general_config(health_enabled=False))
    m['history_worker'].assert_not_called()
    m['history_playlists'].assert_not_called()
    assert m['guild_queue'].call_args.kwargs['record_plays'] is False
    assert m['run_broker'].call_args.kwargs['history_worker'] is None
    assert 'cannot record plays' in caplog.text


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
    mocker.patch('discord_broker.cli.broker.signal.signal', side_effect=captured.__setitem__)
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


@pytest.mark.asyncio
async def test_main_loop_runs_and_closes_the_history_worker(mocker):
    '''The worker runs until the stop event, and its db clients are closed on the way out.'''
    captured = {}
    mocker.patch('discord_broker.cli.broker.signal.signal', side_effect=captured.__setitem__)
    broker_server = MagicMock(serve=AsyncMock(), drain_and_stop=AsyncMock())
    redis_manager = MagicMock(start=AsyncMock(), close=AsyncMock())
    broker_metrics = MagicMock(run=AsyncMock())
    history_worker = MagicMock(run=AsyncMock(), close=AsyncMock())

    task = asyncio.create_task(broker_cli.main_loop(
        broker_server, None, redis_manager, broker_metrics, history_worker=history_worker))
    await asyncio.sleep(0)
    captured[_signal.SIGTERM](_signal.SIGTERM, None)
    await task

    history_worker.run.assert_called_once()
    history_worker.close.assert_awaited_once()


def test_run_broker_invokes_run_loop(mocker):
    mock_run_loop = mocker.patch('discord_broker.cli.broker.run_loop')
    sentinel = object()
    # Force a sync mock so main_loop(...) returns the sentinel rather than a coroutine.
    mocker.patch('discord_broker.cli.broker.main_loop', new=MagicMock(return_value=sentinel))
    broker_cli.run_broker(MagicMock(), MagicMock(), MagicMock(), MagicMock())
    mock_run_loop.assert_called_once_with(sentinel)


def test_main_parses_config_and_runs(mocker):
    mocker.patch('discord_broker.cli.broker.parse_and_validate_config',
                 return_value=({'k': 'v'}, 'gc'))
    mock_run = mocker.patch('discord_broker.cli.broker.run')
    broker_cli.main.callback('config.cnf')
    mock_run.assert_called_once_with({'k': 'v'}, 'gc')
