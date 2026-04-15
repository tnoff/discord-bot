'''
Tests for discord_bot/cli/download_worker.py
'''
import asyncio
import signal
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from discord_bot.cli.download_worker import _worker_task_loop, _run_worker, run
from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.common import GeneralConfig


# ---------------------------------------------------------------------------
# _worker_task_loop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio(loop_scope='session')
async def test_worker_task_loop_calls_function_until_shutdown():
    '''_worker_task_loop calls function repeatedly until shutdown_event is set.'''
    call_count = 0
    shutdown = asyncio.Event()

    async def fake_function():
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            shutdown.set()

    logger = Mock()
    await _worker_task_loop(fake_function, shutdown, logger)
    assert call_count == 3


@pytest.mark.asyncio(loop_scope='session')
async def test_worker_task_loop_exits_on_exit_early_exception():
    '''_worker_task_loop returns cleanly when function raises ExitEarlyException.'''
    async def fake_function():
        raise ExitEarlyException('done')

    shutdown = asyncio.Event()
    logger = Mock()
    await _worker_task_loop(fake_function, shutdown, logger)
    logger.exception.assert_not_called()


@pytest.mark.asyncio(loop_scope='session')
async def test_worker_task_loop_exits_and_logs_on_unexpected_exception():
    '''_worker_task_loop logs the exception and returns on any other error.'''
    async def fake_function():
        raise RuntimeError('boom')

    shutdown = asyncio.Event()
    logger = Mock()
    await _worker_task_loop(fake_function, shutdown, logger)
    logger.exception.assert_called_once()
    assert 'RuntimeError' in logger.exception.call_args[0][1]


@pytest.mark.asyncio(loop_scope='session')
async def test_worker_task_loop_does_not_call_when_shutdown_set():
    '''_worker_task_loop does not call function when shutdown_event is already set.'''
    called = False

    async def fake_function():
        nonlocal called
        called = True

    shutdown = asyncio.Event()
    shutdown.set()
    logger = Mock()
    await _worker_task_loop(fake_function, shutdown, logger)
    assert not called


# ---------------------------------------------------------------------------
# _run_worker
# ---------------------------------------------------------------------------

def _base_settings(download_dir=None, **extra_download):
    settings = {
        'general': {
            'redis_url': 'redis://localhost:6379',
            'download_worker_process_id': 'test-worker-1',
        },
        'download': {
            **extra_download,
        },
    }
    if download_dir:
        settings['download']['download_dir_path'] = str(download_dir)
    return settings


@pytest.mark.asyncio(loop_scope='session')
async def test_run_worker_shuts_down_cleanly(tmp_path):
    '''_run_worker starts tasks and shuts down cleanly when shutdown_event is set.'''
    settings = _base_settings(download_dir=tmp_path)
    general_config = GeneralConfig()

    mock_redis = MagicMock()

    async def fake_feeder(shutdown_event):
        raise ExitEarlyException('done')

    async def fake_runner(shutdown_event):
        raise ExitEarlyException('done')

    with patch('discord_bot.cli.download_worker.get_redis_client', return_value=mock_redis):
        with patch('discord_bot.cli.download_worker.DownloadClient') as mock_client_cls:
            mock_client = MagicMock()
            mock_client.run_redis_feeder = fake_feeder
            mock_client.run = fake_runner
            mock_client_cls.return_value = mock_client
            await _run_worker(settings, general_config)


@pytest.mark.asyncio(loop_scope='session')
async def test_run_worker_uses_temp_dir_when_no_download_dir():
    '''_run_worker creates a temp directory when download_dir_path is not configured.'''
    settings = _base_settings()  # no download_dir_path
    general_config = GeneralConfig()

    captured_dir = {}

    async def fake_feeder(shutdown_event):
        raise ExitEarlyException('done')

    async def fake_runner(shutdown_event):
        raise ExitEarlyException('done')

    with patch('discord_bot.cli.download_worker.get_redis_client', return_value=MagicMock()):
        with patch('discord_bot.cli.download_worker.DownloadClient') as mock_client_cls:
            mock_client = MagicMock()
            mock_client.run_redis_feeder = fake_feeder
            mock_client.run = fake_runner
            mock_client_cls.return_value = mock_client

            def capture_init(*args, **_kwargs):
                captured_dir['path'] = args[1]  # second positional arg is download_dir
                return mock_client

            mock_client_cls.side_effect = capture_init
            await _run_worker(settings, general_config)

    assert 'path' in captured_dir
    assert isinstance(captured_dir['path'], Path)


@pytest.mark.asyncio(loop_scope='session')
async def test_run_worker_signal_handler_sets_shutdown(tmp_path):
    '''Signal handler sets shutdown_event, which stops the task loops.'''
    settings = _base_settings(download_dir=tmp_path)
    general_config = GeneralConfig()

    async def fake_feeder(_shutdown_event):
        await asyncio.sleep(0)
        raise ExitEarlyException('done')

    async def fake_runner(_shutdown_event):
        # Send SIGTERM to ourselves so the signal handler fires
        signal.raise_signal(signal.SIGTERM)
        await asyncio.sleep(0)
        raise ExitEarlyException('done')

    with patch('discord_bot.cli.download_worker.get_redis_client', return_value=MagicMock()):
        with patch('discord_bot.cli.download_worker.DownloadClient') as mock_client_cls:
            mock_client = MagicMock()
            mock_client.run_redis_feeder = fake_feeder
            mock_client.run = fake_runner
            mock_client_cls.return_value = mock_client
            await _run_worker(settings, general_config)


# ---------------------------------------------------------------------------
# run (entry point)
# ---------------------------------------------------------------------------

def test_run_entry_point():
    '''run() calls asyncio.run with _run_worker.'''
    settings = {
        'general': {
            'redis_url': 'redis://localhost:6379',
            'download_worker_process_id': 'w1',
        },
        'download': {},
    }
    general_config = GeneralConfig()

    def _close_coro(coro):
        coro.close()

    with patch('discord_bot.cli.download_worker.asyncio.run', side_effect=_close_coro) as mock_run:
        run(settings, general_config)

    mock_run.assert_called_once()
