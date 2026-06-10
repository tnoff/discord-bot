import logging
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

from opentelemetry.instrumentation.logging.handler import LoggingHandler

from discord_bot.cli.common import setup_logging
from discord_bot.utils.common import GeneralConfig


def _drop_handlers(*names):
    for name in names:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)
            try:
                h.close()
            except Exception:  # pylint: disable=broad-except
                pass
        lg.propagate = True


def _make_config(tmp_dir):
    return GeneralConfig(
        discord_token='abctoken',
        logging={
            'log_dir': tmp_dir,
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 10,
        },
    )


def test_setup_logging_attaches_otlp_to_main_and_discord_bot():
    '''main + discord_bot get a LoggingHandler when logger_provider passed.'''
    _drop_handlers('main', 'discord_bot', 'discord')
    with TemporaryDirectory() as tmp_dir:
        cfg = _make_config(tmp_dir)
        setup_logging(cfg, logger_provider=MagicMock())
        for name in ('main', 'discord_bot'):
            assert any(isinstance(h, LoggingHandler) for h in logging.getLogger(name).handlers), \
                f'expected LoggingHandler on {name}'
    _drop_handlers('main', 'discord_bot', 'discord')


def test_setup_logging_disables_propagate_to_avoid_double_export():
    '''main + discord_bot don't propagate so root's OTLP handler doesn't double-export WARNING+.'''
    _drop_handlers('main', 'discord_bot', 'discord')
    with TemporaryDirectory() as tmp_dir:
        cfg = _make_config(tmp_dir)
        setup_logging(cfg, logger_provider=MagicMock())
        assert logging.getLogger('main').propagate is False
        assert logging.getLogger('discord_bot').propagate is False
    _drop_handlers('main', 'discord_bot', 'discord')


def test_setup_logging_no_otlp_logger_still_works():
    '''Calling setup_logging without a logger_provider does not attach LoggingHandler.'''
    _drop_handlers('main', 'discord_bot', 'discord')
    with TemporaryDirectory() as tmp_dir:
        cfg = _make_config(tmp_dir)
        setup_logging(cfg, logger_provider=None)
        for name in ('main', 'discord_bot'):
            assert not any(isinstance(h, LoggingHandler) for h in logging.getLogger(name).handlers)
    _drop_handlers('main', 'discord_bot', 'discord')
