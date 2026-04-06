from functools import partial

import pytest
from pytest import raises
from sqlalchemy.exc import OperationalError, PendingRollbackError

from discord_bot.database import MarkovChannel
from discord_bot.utils.sql_retry import retry_database_commands, async_retry_database_commands

class FakeSession():
    def __init__(self, error_always: bool = False):
        self.counter = -1
        self.rollback_called = False
        self.error_always = error_always

    def query(self, *_, **__):
        self.counter += 1
        if self.counter == 0 or self.error_always:
            raise OperationalError('Mock', None, None)
        if self.counter == 1:
            raise PendingRollbackError('Mock', None, None)

    def rollback(self, *_, **__):
        self.rollback_called = True

def query_markov(db_session):
    return db_session.query(MarkovChannel)


def test_sql_retry(mocker):
    mocker.patch('discord_bot.utils.sql_retry.sleep', return_value=True)
    session = FakeSession()
    func = partial(query_markov, session)
    retry_database_commands(session, func)
    assert session.counter == 2
    assert session.rollback_called

def test_sql_retry_fails(mocker):
    mocker.patch('discord_bot.utils.sql_retry.sleep', return_value=True)
    session = FakeSession(error_always=True)
    func = partial(query_markov, session)
    with raises(OperationalError) as exc:
        retry_database_commands(session, func)
    assert 'Mock' in str(exc.value)

def test_sql_retry_operational_error_triggers_rollback(mocker):
    '''OperationalError must call rollback before sleeping and retrying'''
    mocker.patch('discord_bot.utils.sql_retry.sleep', return_value=True)

    class OpErrorOnceSession:
        def __init__(self):
            self.counter = -1
            self.rollback_called = False

        def query(self, *_, **__):
            self.counter += 1
            if self.counter == 0:
                raise OperationalError('Mock', None, None)

        def rollback(self, *_, **__):
            self.rollback_called = True

    session = OpErrorOnceSession()
    func = partial(query_markov, session)
    retry_database_commands(session, func)
    assert session.rollback_called

def test_sql_retry_pending_rollback_error_exhausts(mocker):
    '''PendingRollbackError exhausts all attempts and re-raises'''
    mocker.patch('discord_bot.utils.sql_retry.sleep', return_value=True)

    class AlwaysPendingSession:
        def __init__(self):
            self.rollback_called = False

        def query(self, *_, **__):
            raise PendingRollbackError('Pending', None, None)

        def rollback(self, *_, **__):
            self.rollback_called = True

    session = AlwaysPendingSession()
    func = partial(query_markov, session)
    with raises(PendingRollbackError):
        retry_database_commands(session, func)
    assert session.rollback_called


# ---------------------------------------------------------------------------
# async_retry_database_commands
# ---------------------------------------------------------------------------

class FakeAsyncSession():
    def __init__(self, error_always: bool = False):
        self.counter = -1
        self.rollback_called = False
        self.error_always = error_always

    async def rollback(self, *_, **__):
        self.rollback_called = True


@pytest.mark.asyncio
async def test_async_sql_retry_succeeds_after_errors(mocker):
    '''async_retry_database_commands retries on OperationalError then PendingRollbackError'''
    mocker.patch('asyncio.sleep', return_value=None)
    session = FakeAsyncSession()
    counter = [-1]

    async def flaky():
        counter[0] += 1
        if counter[0] == 0:
            raise OperationalError('Mock', None, None)
        if counter[0] == 1:
            raise PendingRollbackError('Mock', None, None)
        return 'ok'

    result = await async_retry_database_commands(session, flaky)
    assert result == 'ok'
    assert counter[0] == 2
    assert session.rollback_called


@pytest.mark.asyncio
async def test_async_sql_retry_exhausts_attempts(mocker):
    '''async_retry_database_commands re-raises after max attempts'''
    mocker.patch('asyncio.sleep', return_value=None)
    session = FakeAsyncSession(error_always=True)

    async def always_fails():
        raise OperationalError('Mock', None, None)

    with raises(OperationalError):
        await async_retry_database_commands(session, always_fails)


@pytest.mark.asyncio
async def test_async_sql_retry_operational_error_triggers_rollback(mocker):
    '''OperationalError must call async rollback before sleeping and retrying'''
    mocker.patch('asyncio.sleep', return_value=None)
    session = FakeAsyncSession()
    counter = [0]

    async def once_then_ok():
        if counter[0] == 0:
            counter[0] += 1
            raise OperationalError('Mock', None, None)
        return 'ok'

    await async_retry_database_commands(session, once_then_ok)
    assert session.rollback_called


@pytest.mark.asyncio
async def test_async_sql_retry_pending_rollback_exhausts(mocker):
    '''PendingRollbackError exhausts all attempts and re-raises'''
    mocker.patch('asyncio.sleep', return_value=None)
    session = FakeAsyncSession()

    async def always_pending():
        raise PendingRollbackError('Pending', None, None)

    with raises(PendingRollbackError):
        await async_retry_database_commands(session, always_pending)
    assert session.rollback_called
