# https://stackoverflow.com/questions/53287215/retry-failed-sqlalchemy-queries

from contextlib import contextmanager
from time import sleep

from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm.query import Query as _Query


@contextmanager
def retry_database_commands(db_session, attempts: int = 3, interval: int = 1):
    '''
    Wrap database commands in a retry class

    db_session : Valid db_session being used
    attempts : Number of attempts
    interval : Sleep interval
    '''
    print('Running retry manager')
    for _ in range(attempts):
        print('Attempt')
        try:
            yield
            print('Ran fine, im returning')
            return
        except PendingRollbackError:
            db_session.rollback()
        except OperationalError:
            continue

class RetryingQuery(_Query): #pylint:disable=abstract-method
    '''
    Attempt retry for sqlalchemy calls
    '''
    __max_retry_count__ = 3

    def __init__(self, *args, **kwargs):
        print('Init on retry class')
        super().__init__(*args, **kwargs)

    def __iter__(self):
        print('Iter attempt on query')
        attempts = 0
        while True:
            attempts += 1
            try:
                return super().__iter__()
            except OperationalError:
                if attempts <= self.__max_retry_count__:
                    sleep_for = attempts
                    sleep(sleep_for)
                    continue
                raise
            except PendingRollbackError:
                self.session.rollback()
