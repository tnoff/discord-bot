'''Tests for DatabasePingHealthServer — the db pod's postgres-backed /health.'''
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text

from discord_bot.servers.database_health_server import DatabasePingHealthServer

from tests.helpers import fake_engine  # pylint: disable=unused-import


def _server(engine):
    return DatabasePingHealthServer(engine, port=19500)


def _failing_engine(error):
    '''An engine whose connect() raises, standing in for postgres being gone.'''
    engine = MagicMock()
    engine.connect.side_effect = error
    return engine


@pytest.mark.asyncio
async def test_check_ok_against_real_postgres(fake_engine):  # pylint: disable=redefined-outer-name
    '''
    The ok path runs against a REAL engine, not a mock that returns True.

    A MagicMock satisfies `async with engine.connect()` and `conn.execute`
    whatever the SELECT says, so a mocked ok path asserts the test's own
    scaffolding. This asserts postgres actually answered.
    '''
    ok, payload = await _server(fake_engine)._check()  # pylint: disable=protected-access
    assert ok is True
    assert payload == {'db': 'ok'}


@pytest.mark.asyncio
async def test_check_unavailable_when_db_raises():
    '''A dead database fails the probe rather than propagating the error.

    The kubelet gets a 503 and restarts the pod; an exception escaping here
    would kill the health server itself and leave the probe hanging instead.
    '''
    ok, payload = await _server(_failing_engine(ConnectionError('pg down')))._check()  # pylint: disable=protected-access
    assert ok is False
    assert payload == {'db': 'unavailable'}


@pytest.mark.asyncio
async def test_check_counts_the_ok_outcome(mocker, fake_engine):  # pylint: disable=redefined-outer-name
    '''Probe outcomes are counted, so a flapping database is visible before the kill.'''
    counter = mocker.patch('discord_bot.servers.database_health_server._READY_CHECK_COUNTER')
    ok, _ = await _server(fake_engine)._check()  # pylint: disable=protected-access
    assert ok is True
    counter.add.assert_called_once_with(1, {'outcome': 'ok'})


@pytest.mark.asyncio
async def test_check_counts_a_bad_outcome(mocker):
    '''The unavailable outcome is counted under its own label.'''
    counter = mocker.patch('discord_bot.servers.database_health_server._READY_CHECK_COUNTER')
    ok, _ = await _server(_failing_engine(ConnectionError('pg down')))._check()  # pylint: disable=protected-access
    assert ok is False
    counter.add.assert_called_once_with(1, {'outcome': 'unavailable'})


@pytest.mark.asyncio
async def test_ping_survives_an_execute_failure():
    '''A connection that opens but fails mid-SELECT still reports unavailable.

    Not the same path as connect() raising: postgres accepting the socket and
    then erroring is what a failing-over or read-only instance looks like, and
    the probe has to fail on that too rather than only on a refused connection.
    '''
    conn = AsyncMock()
    conn.execute.side_effect = ConnectionError('server closed the connection')
    engine = MagicMock()
    engine.connect.return_value.__aenter__ = AsyncMock(return_value=conn)
    engine.connect.return_value.__aexit__ = AsyncMock(return_value=False)

    ok, payload = await _server(engine)._check()  # pylint: disable=protected-access

    assert ok is False
    assert payload == {'db': 'unavailable'}
    conn.execute.assert_awaited_once()
    assert str(conn.execute.await_args.args[0]) == str(text('SELECT 1'))
