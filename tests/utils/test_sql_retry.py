import pytest
from pytest import raises
from sqlalchemy.exc import OperationalError, PendingRollbackError

from discord_bot.utils.sql_retry import async_retry_database_commands


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
