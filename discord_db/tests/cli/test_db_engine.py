'''setup_db / dispose_db_engine edge cases not reached through the serving path.'''
import asyncio
from unittest.mock import MagicMock

import pytest

from discord_core.utils.common import GeneralConfig

from discord_db.cli._lib.db import dispose_db_engine, setup_db


def test_setup_db_without_connection_statement_returns_none(capsys):
    '''No DSN configured means no engine, and stderr says why.'''
    assert setup_db(GeneralConfig()) is None
    assert 'assuming no db' in capsys.readouterr().err


def test_dispose_db_engine_without_engine_is_a_no_op():
    dispose_db_engine(None)


@pytest.mark.asyncio
async def test_dispose_db_engine_inside_running_loop_schedules_a_task():
    '''From inside a running loop, dispose cannot asyncio.run; it schedules dispose() instead.'''
    disposed = asyncio.Event()

    async def _dispose():
        disposed.set()

    engine = MagicMock()
    engine.dispose = _dispose
    dispose_db_engine(engine)
    await asyncio.wait_for(disposed.wait(), timeout=1)


def test_dispose_db_engine_outside_loop_releases_without_closing():
    '''With no running loop, the pool is released with close=False (its loop is already gone).'''
    calls = []

    async def _dispose(**kwargs):
        calls.append(kwargs)

    engine = MagicMock()
    engine.dispose = _dispose
    dispose_db_engine(engine)
    assert calls == [{'close': False}]
