'''Tests for db_ping — the database liveness probe both health servers run.

Moved here from tests/servers/test_health_server.py when the eight duplicated
lines were extracted: the behaviour is unchanged, it just lives where the code
does now rather than being asserted through one of its two callers.
'''
from unittest.mock import AsyncMock, MagicMock

import pytest
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from sqlalchemy import text

from discord_bot.servers.db_probe import db_ping

from tests.helpers import fake_engine  # pylint: disable=unused-import


def _engine(*, connect_error=None, execute_error=None):
    '''Return (engine, conn) where the mock fails at connect, at execute, or not at all.'''
    engine = MagicMock()
    if connect_error:
        engine.connect.return_value = AsyncMock(
            __aenter__=AsyncMock(side_effect=connect_error),
            __aexit__=AsyncMock(return_value=False),
        )
        return engine, None
    conn = AsyncMock()
    if execute_error:
        conn.execute.side_effect = execute_error
    engine.connect.return_value = AsyncMock(
        __aenter__=AsyncMock(return_value=conn),
        __aexit__=AsyncMock(return_value=False),
    )
    return engine, conn


@pytest.mark.asyncio
async def test_ping_true_against_real_postgres(fake_engine):  # pylint: disable=redefined-outer-name
    '''The success path runs against a real engine, not a mock returning True.'''
    assert await db_ping(fake_engine) is True


@pytest.mark.asyncio
async def test_ping_false_when_connect_fails():
    '''A refused connection is False, not an exception.'''
    engine, _ = _engine(connect_error=Exception('db down'))
    assert await db_ping(engine) is False


@pytest.mark.asyncio
async def test_ping_false_when_execute_fails():
    '''A connection that opens and then errors is False too.

    Not the same path as a refused connection: postgres accepting the socket and
    failing the statement is what a failing-over instance looks like.
    '''
    engine, _ = _engine(execute_error=Exception('read-only'))
    assert await db_ping(engine) is False


@pytest.mark.asyncio
async def test_ping_issues_a_select_one():
    '''The probe is a SELECT 1 -- cheap enough for the kubelet's interval.'''
    engine, conn = _engine()
    assert await db_ping(engine) is True
    assert str(conn.execute.await_args.args[0]) == str(text('SELECT 1'))


@pytest.mark.asyncio
async def test_ping_runs_with_auto_instrumentation_suppressed():
    """The query runs with auto-instrumentation off, so it emits no spans.

    Asserted on `is_instrumentation_enabled()` -- the flag every auto-instrumentor
    consults -- rather than by counting exported spans. SQLAlchemyInstrumentor is
    a process-global singleton: a test that instruments it silently no-ops when
    another test has already done so, and its uninstrument() then tears down that
    other test's instrumentation. The resulting failure is order-dependent, which
    is worse than the narrower assertion.

    That the SQLAlchemy instrumentation really does honour this flag was checked
    directly against a live engine when the wrapper was added: the same
    connect + SELECT emitted two spans outside suppress_instrumentation() and
    none inside it.
    """
    engine, conn = _engine()
    seen = {}

    async def _record(*_args, **_kwargs):
        seen['enabled'] = is_instrumentation_enabled()
        return MagicMock()

    conn.execute.side_effect = _record

    assert is_instrumentation_enabled() is True, 'control: not suppressed to begin with'
    assert await db_ping(engine) is True
    assert seen['enabled'] is False
    assert is_instrumentation_enabled() is True, 'suppression is scoped to the probe'


@pytest.mark.asyncio
async def test_ping_emits_when_suppression_is_turned_off():
    """The suppression is a toggle, and turning it off really does re-enable spans.

    The mirror of the test above, and the half that matters: it is what stops
    monitoring.tracing.suppress_db_probe_auto_instrumentation from being a config
    key that is read, plumbed, and has no effect. Asserting only the default
    would pass against exactly that bug.

    Turning it off is what an operator does while postgres is flapping -- the
    per-probe spans are the record of it, and the alert docker-apps raises on
    database.ready_check has no detail view without them.
    """
    engine, conn = _engine()
    seen = {}

    async def _record(*_args, **_kwargs):
        seen['enabled'] = is_instrumentation_enabled()
        return MagicMock()

    conn.execute.side_effect = _record

    assert await db_ping(engine, False) is True
    assert seen['enabled'] is True, 'auto-instrumentation should be live inside the probe'


@pytest.mark.asyncio
async def test_ping_default_still_suppresses():
    '''The argument defaults to the shipped behaviour, so callers that predate it
    are unaffected -- including the two health servers before their own wiring
    was added.'''
    engine, conn = _engine()
    seen = {}

    async def _record(*_args, **_kwargs):
        seen['enabled'] = is_instrumentation_enabled()
        return MagicMock()

    conn.execute.side_effect = _record

    assert await db_ping(engine) is True
    assert seen['enabled'] is False
