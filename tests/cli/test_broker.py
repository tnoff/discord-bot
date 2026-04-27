'''Tests for the discord-broker CLI entry point.'''
import asyncio
import os
import signal as signal_module
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from unittest.mock import AsyncMock, MagicMock, patch

from click.testing import CliRunner
import pytest
from yaml import dump

from discord_bot.cli.broker import main, main_loop, run, run_broker
from discord_bot.cli.db import require_postgres
from discord_bot.exceptions import DiscordBotException
from discord_bot.utils.common import GeneralConfig, MonitoringConfig, MonitoringHealthServerConfig, MonitoringOtlpConfig


# ---------------------------------------------------------------------------
# require_postgres guard
# ---------------------------------------------------------------------------

def _general_config(sql_url: str) -> GeneralConfig:
    return GeneralConfig(discord_token='tok', sql_connection_statement=sql_url)


def test_require_postgres_accepts_postgres():
    '''require_postgres does not raise for a postgresql:// URL.'''
    require_postgres(_general_config('postgresql://user:pw@host/db'))


def test_require_postgres_rejects_sqlite():
    '''require_postgres raises DiscordBotException for a sqlite:// URL.'''
    with pytest.raises(DiscordBotException, match='PostgreSQL'):
        require_postgres(_general_config('sqlite:///local.db'))


def test_require_postgres_raises_when_no_sql():
    '''require_postgres raises when sql_connection_statement is None.'''
    cfg = GeneralConfig(discord_token='tok')
    with pytest.raises(DiscordBotException):
        require_postgres(cfg)


# ---------------------------------------------------------------------------
# CLI validation via CliRunner
# ---------------------------------------------------------------------------

def test_run_with_no_args():
    '''Invoke with no args produces click usage error.'''
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert "Error: Missing argument 'CONFIG_FILE'" in result.output


def test_run_no_file():
    '''Invoke with an empty YAML raises "General config section required".'''
    with NamedTemporaryFile() as tmp:
        runner = CliRunner()
        result = runner.invoke(main, [tmp.name])
        assert 'General config section required' in str(result.exception)


def test_run_sqlite_db_raises():
    '''Broker rejects a SQLite DB URL at startup.'''
    with NamedTemporaryFile(suffix='.yml') as tmp:
        config_data = {
            'general': {
                'discord_token': 'tok',
                'sql_connection_statement': 'sqlite:///local.db',
                'redis_url': 'redis://localhost',
            }
        }
        with open(tmp.name, 'w', encoding='utf-8') as f:
            dump(config_data, f)
        runner = CliRunner()
        result = runner.invoke(main, [tmp.name])
        assert 'PostgreSQL' in str(result.exception)


def test_run_missing_redis_raises():
    '''Broker rejects a config with no redis_url.'''
    with NamedTemporaryFile(suffix='.yml') as tmp:
        config_data = {
            'general': {
                'discord_token': 'tok',
                'sql_connection_statement': 'postgresql://u:p@h/db',
            }
        }
        with open(tmp.name, 'w', encoding='utf-8') as f:
            dump(config_data, f)
        runner = CliRunner()
        result = runner.invoke(main, [tmp.name])
        assert 'Redis required' in str(result.exception)


# ---------------------------------------------------------------------------
# main_loop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_main_loop_starts_and_stops():
    '''main_loop starts servers and shuts down cleanly when stop_event fires.'''
    broker_server = MagicMock()
    broker_server.serve = AsyncMock(return_value=None)
    broker_server.drain_and_stop = AsyncMock(return_value=None)

    redis_manager = MagicMock()
    redis_manager.start = AsyncMock(return_value=None)
    redis_manager.close = AsyncMock(return_value=None)

    async def _run():
        loop_task = asyncio.create_task(
            main_loop(broker_server, health_server=None, redis_manager=redis_manager)
        )
        await asyncio.sleep(0.05)
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass

    await _run()
    redis_manager.start.assert_called_once()


@pytest.mark.asyncio
async def test_main_loop_starts_health_server_when_provided():
    '''main_loop calls health_server.serve() when a health server is provided.'''
    broker_server = MagicMock()
    broker_server.serve = AsyncMock(return_value=None)
    broker_server.drain_and_stop = AsyncMock(return_value=None)

    health_server = MagicMock()
    health_server.serve = AsyncMock(return_value=None)

    redis_manager = MagicMock()
    redis_manager.start = AsyncMock(return_value=None)
    redis_manager.close = AsyncMock(return_value=None)

    loop_task = asyncio.create_task(
        main_loop(broker_server, health_server=health_server, redis_manager=redis_manager)
    )
    await asyncio.sleep(0.05)
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass

    health_server.serve.assert_called_once()


@pytest.mark.asyncio
async def test_main_loop_handles_sigterm():
    '''Sending SIGTERM while main_loop is running triggers graceful shutdown via _handle_signal.'''
    broker_server = MagicMock()
    broker_server.serve = AsyncMock(return_value=None)
    broker_server.drain_and_stop = AsyncMock(return_value=None)

    redis_manager = MagicMock()
    redis_manager.start = AsyncMock(return_value=None)
    redis_manager.close = AsyncMock(return_value=None)

    loop_task = asyncio.create_task(
        main_loop(broker_server, health_server=None, redis_manager=redis_manager)
    )
    await asyncio.sleep(0.05)  # let main_loop install signal handlers
    os.kill(os.getpid(), signal_module.SIGTERM)
    await asyncio.wait_for(loop_task, timeout=2.0)
    broker_server.drain_and_stop.assert_called_once()


@pytest.mark.asyncio
async def test_main_loop_drains_broker_on_shutdown():
    '''main_loop calls drain_and_stop on the broker server during shutdown.'''
    broker_server = MagicMock()
    broker_server.serve = AsyncMock(return_value=None)
    broker_server.drain_and_stop = AsyncMock(return_value=None)

    redis_manager = MagicMock()
    redis_manager.start = AsyncMock(return_value=None)
    redis_manager.close = AsyncMock(return_value=None)

    loop_task = asyncio.create_task(
        main_loop(broker_server, health_server=None, redis_manager=redis_manager)
    )
    await asyncio.sleep(0.05)
    loop_task.cancel()
    try:
        await loop_task
    except asyncio.CancelledError:
        pass

    broker_server.drain_and_stop.assert_called_once()
    redis_manager.close.assert_called_once()


# ---------------------------------------------------------------------------
# run_broker
# ---------------------------------------------------------------------------

def test_run_broker_delegates_to_run_loop():
    '''run_broker calls run_loop with the main_loop coroutine.'''
    broker_server = MagicMock()
    redis_manager = MagicMock()

    with patch('discord_bot.cli.broker.run_loop') as mock_run_loop:
        run_broker(broker_server, health_server=None, redis_manager=redis_manager)
    mock_run_loop.assert_called_once()
    # Close the coroutine so it doesn't trigger "coroutine was never awaited" on GC.
    mock_run_loop.call_args[0][0].close()


# ---------------------------------------------------------------------------
# run() — integration-level wiring
# ---------------------------------------------------------------------------

def _minimal_settings(with_cache: bool = False, with_health: bool = False,
                      broker_host: str = '0.0.0.0', broker_port: int = 8081) -> tuple[dict, GeneralConfig]:
    monitoring = None
    if with_health:
        monitoring = MonitoringConfig(
            otlp=MonitoringOtlpConfig(enabled=False),
            health_server=MonitoringHealthServerConfig(enabled=True, port=9090),
        )
    general_config = GeneralConfig(
        discord_token='tok',
        sql_connection_statement='postgresql://u:p@h/db',
        redis_url='redis://localhost',
        monitoring=monitoring,
    )
    settings: dict = {
        'general': {'broker_server': {'host': broker_host, 'port': str(broker_port)}},
        'music': {},
    }
    if with_cache:
        settings['music']['download'] = {'cache': {'enable_cache_files': True, 'max_cache_files': 50}}
    return settings, general_config


@contextmanager
def _mock_run_env(settings, general_config, db_engine=None):
    '''Patch all external side-effects so run() can be called synchronously.'''
    with patch('discord_bot.cli.broker.parse_and_validate_config', return_value=(settings, general_config)), \
         patch('discord_bot.cli.broker.require_postgres'), \
         patch('discord_bot.cli.broker.setup_redis_observability'), \
         patch('discord_bot.cli.broker.instrument_sqlalchemy'), \
         patch('discord_bot.cli.broker.managed_db') as mock_db, \
         patch('discord_bot.cli.broker.run_broker') as mock_run_broker:
        mock_db.return_value.__enter__ = MagicMock(return_value=db_engine)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)
        yield mock_run_broker


def test_run_creates_broker_http_server_in_ha_mode():
    '''run() passes ha_mode=True to BrokerHttpServer.'''
    settings, general_config = _minimal_settings()
    with _mock_run_env(settings, general_config), \
         patch('discord_bot.cli.broker.BrokerHttpServer') as mock_server_cls:
        mock_server_cls.return_value = MagicMock()
        run(settings, general_config)
    _, kwargs = mock_server_cls.call_args
    assert kwargs.get('ha_mode') is True


def test_run_passes_broker_server_config():
    '''run() reads host and port from settings and passes them to BrokerHttpServer.'''
    settings, general_config = _minimal_settings(broker_host='127.0.0.1', broker_port=9001)
    with _mock_run_env(settings, general_config), \
         patch('discord_bot.cli.broker.BrokerHttpServer') as mock_server_cls:
        mock_server_cls.return_value = MagicMock()
        run(settings, general_config)
    _, kwargs = mock_server_cls.call_args
    assert kwargs['host'] == '127.0.0.1'
    assert kwargs['port'] == 9001


def test_run_no_health_server_when_monitoring_not_configured():
    '''run() passes health_server=None to run_broker when monitoring is absent.'''
    settings, general_config = _minimal_settings(with_health=False)
    with _mock_run_env(settings, general_config) as mock_run_broker:
        run(settings, general_config)
    args, _ = mock_run_broker.call_args
    # run_broker(broker_server, health_server, redis_manager)
    assert args[1] is None


def test_run_creates_health_server_when_monitoring_enabled():
    '''run() instantiates BrokerHealthServer when health monitoring is configured.'''
    settings, general_config = _minimal_settings(with_health=True)
    with _mock_run_env(settings, general_config) as mock_run_broker, \
         patch('discord_bot.cli.broker.BrokerHealthServer') as mock_hs_cls:
        mock_hs_cls.return_value = MagicMock()
        run(settings, general_config)
    mock_hs_cls.assert_called_once()
    args, _ = mock_run_broker.call_args
    assert args[1] is mock_hs_cls.return_value


def test_run_creates_video_cache_when_configured():
    '''run() instantiates VideoCacheClient when enable_cache_files is True.'''
    settings, general_config = _minimal_settings(with_cache=True)
    fake_engine = MagicMock()
    with _mock_run_env(settings, general_config, db_engine=fake_engine) as _, \
         patch('discord_bot.cli.broker.VideoCacheClient') as mock_vc_cls, \
         patch('discord_bot.cli.broker.async_sessionmaker'), \
         patch('discord_bot.cli.broker.BrokerHttpServer') as mock_server_cls:
        mock_server_cls.return_value = MagicMock()
        mock_vc_cls.return_value = MagicMock()
        run(settings, general_config)
    mock_vc_cls.assert_called_once()


def test_run_no_video_cache_when_not_configured():
    '''run() does not instantiate VideoCacheClient when cache is not enabled.'''
    settings, general_config = _minimal_settings(with_cache=False)
    with _mock_run_env(settings, general_config) as _, \
         patch('discord_bot.cli.broker.VideoCacheClient') as mock_vc_cls:
        run(settings, general_config)
    mock_vc_cls.assert_not_called()
