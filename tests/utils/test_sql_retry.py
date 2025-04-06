from functools import partial

from pytest import raises
from sqlalchemy.exc import OperationalError, PendingRollbackError

from discord_bot.database import MarkovChannel
from discord_bot.utils.sql_retry import retry_database_commands

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
