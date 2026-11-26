import asyncio
import logging
import signal
from unittest.mock import MagicMock, patch

import pytest

from discord_core.cli._lib import common
from discord_core.exceptions import DiscordBotException
from discord_core.utils.common import GeneralConfig


def _monitoring(**sections):
    '''Build a GeneralConfig whose monitoring block carries the given sections.'''
    return GeneralConfig(monitoring={'otlp': {'enabled': False}, **sections})


def test_read_config_without_file_returns_empty():
    '''No config path means an empty settings dict.'''
    assert not common.read_config(None)


def test_read_config_requires_general_section(tmp_path):
    '''A file with no general section is rejected.'''
    path = tmp_path / 'config.yml'
    path.write_text('other: {a: 1}\n')
    with pytest.raises(DiscordBotException, match='General config section required'):
        common.read_config(str(path))


def test_read_config_empty_file_is_rejected(tmp_path):
    '''An empty yaml file parses to nothing and fails the general-section check.'''
    path = tmp_path / 'config.yml'
    path.write_text('')
    with pytest.raises(DiscordBotException, match='General config section required'):
        common.read_config(str(path))


def test_read_config_substitutes_environment(tmp_path, monkeypatch):
    '''Environment placeholders in the file are expanded.'''
    monkeypatch.setenv('SHARED_SETUP_TOKEN', 'test-token')
    path = tmp_path / 'config.yml'
    path.write_text('general:\n  discord_token: !ENV ${SHARED_SETUP_TOKEN}\n')
    assert common.read_config(str(path)) == {'general': {'discord_token': 'test-token'}}


def test_setup_otlp_disabled_returns_none():
    '''With otlp off, nothing is configured and no provider is returned.'''
    assert common.setup_otlp(GeneralConfig()) is None
    assert common.setup_otlp(_monitoring()) is None


def test_setup_otlp_enabled_wires_traces_metrics_logs():
    '''With otlp on, trace, metric and log pipelines are built and the log provider returned.'''
    config = GeneralConfig(monitoring={'otlp': {'enabled': True}})
    names = ['trace', 'TracerProvider', 'RequestsInstrumentor', 'RedisInstrumentor', 'OTLPSpanExporter',
             'BatchSpanProcessor', 'get_aggregated_resources', 'OTLPMetricExporter',
             'PeriodicExportingMetricReader', 'MeterProvider', 'set_meter_provider', 'LoggerProvider',
             'set_logger_provider', 'OTLPLogExporter', 'BatchLogRecordProcessor', 'LoggingHandler']
    mocks = {name: patch.object(common, name) for name in names}
    root = logging.getLogger()
    handlers_before = list(root.handlers)
    started = {name: mock.start() for name, mock in mocks.items()}
    try:
        result = common.setup_otlp(config)
        added = [h for h in root.handlers if h not in handlers_before]
        for handler in added:
            root.removeHandler(handler)
    finally:
        for mock in mocks.values():
            mock.stop()
    assert result is started['LoggerProvider'].return_value
    assert added == [started['LoggingHandler'].return_value]
    started['trace'].set_tracer_provider.assert_called_once_with(started['TracerProvider'].return_value)
    started['RequestsInstrumentor'].return_value.instrument.assert_called_once_with(
        tracer_provider=started['TracerProvider'].return_value)
    started['RedisInstrumentor'].return_value.instrument.assert_called_once_with(
        tracer_provider=started['TracerProvider'].return_value)
    started['trace'].get_tracer_provider.return_value.add_span_processor.assert_called_once_with(
        started['BatchSpanProcessor'].return_value)
    started['MeterProvider'].assert_called_once_with(
        resource=started['get_aggregated_resources'].return_value,
        metric_readers=[started['PeriodicExportingMetricReader'].return_value])
    started['set_meter_provider'].assert_called_once_with(started['MeterProvider'].return_value)
    started['set_logger_provider'].assert_called_once_with(started['LoggerProvider'].return_value)
    started['LoggerProvider'].return_value.add_log_record_processor.assert_called_once_with(
        started['BatchLogRecordProcessor'].return_value)


def test_setup_profiling_starts_memory_and_process_profilers():
    '''Enabled memory and process-metric sections start their profilers with the configured knobs.'''
    config = _monitoring(memory_profiling={'enabled': True, 'interval_seconds': 7, 'top_n_lines': 3},
                         process_metrics={'enabled': True, 'interval_seconds': 9})
    logger = MagicMock()
    with patch.object(common, 'MemoryProfiler') as memory, \
            patch.object(common, 'ProcessMetricsProfiler') as process, \
            patch.object(common, 'GcCensusProfiler') as census:
        common.setup_profiling(config, logger)
    memory.assert_called_once_with(interval_seconds=7, top_n_lines=3)
    memory.return_value.start.assert_called_once_with()
    process.assert_called_once_with(interval_seconds=9)
    process.return_value.start.assert_called_once_with()
    census.assert_not_called()


def test_setup_profiling_without_monitoring_starts_nothing():
    '''No monitoring block means no profilers.'''
    with patch.object(common, 'MemoryProfiler') as memory, \
            patch.object(common, 'ProcessMetricsProfiler') as process:
        common.setup_profiling(GeneralConfig(), MagicMock())
    memory.assert_not_called()
    process.assert_not_called()


def test_setup_loop_health_applies_stale_window():
    '''A configured loop_health block sets the registry staleness window.'''
    config = _monitoring(loop_health={'stale_after_seconds': 42})
    with patch.object(common, 'LOOP_HEALTH') as registry:
        common.setup_loop_health(config)
    registry.configure.assert_called_once_with(42)


def test_setup_loop_health_unconfigured_leaves_default():
    '''Without a loop_health block the registry is untouched.'''
    with patch.object(common, 'LOOP_HEALTH') as registry:
        common.setup_loop_health(GeneralConfig())
        common.setup_loop_health(_monitoring())
    registry.configure.assert_not_called()


def test_run_loop_starts_new_loop_when_none_running():
    '''Outside a running loop the coroutine is handed to asyncio.run.'''
    coro = MagicMock()
    with patch.object(common.asyncio, 'run') as run:
        common.run_loop(coro)
    run.assert_called_once_with(coro)


@pytest.mark.asyncio
async def test_run_loop_schedules_on_running_loop():
    '''Inside a running loop the coroutine is scheduled as a task instead of nesting asyncio.run.'''
    ran = asyncio.Event()

    async def work():
        ran.set()

    with patch.object(common.asyncio, 'run') as run:
        common.run_loop(work())
        await asyncio.wait_for(ran.wait(), timeout=2)
    run.assert_not_called()


@pytest.mark.asyncio
async def test_shutdown_event_signals_sets_event_on_signal(captured_signals):
    '''SIGTERM and SIGINT handlers are installed and either one sets the stop event.'''
    with common.shutdown_event_signals() as stop_event:
        assert set(captured_signals) == {signal.SIGTERM, signal.SIGINT}
        assert not stop_event.is_set()
        captured_signals[signal.SIGTERM](signal.SIGTERM, None)
        await asyncio.wait_for(stop_event.wait(), timeout=2)
    assert stop_event.is_set()
