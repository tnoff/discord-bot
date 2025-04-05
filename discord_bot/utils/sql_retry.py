# https://stackoverflow.com/questions/53287215/retry-failed-sqlalchemy-queries

from time import sleep
from typing import Callable

from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm.session import Session


def retry_database_commands(db_session: Session, function: Callable, attempts: int = 3, interval: int = 1):
    '''
    Wrap database commands in a retry class

    db_session : Valid db_session being used
    function   : Function with db_session attached
    attempts : Number of attempts
    interval : Sleep interval
    '''
    count = 0
    while True:
        count += 1
        try:
            return function()
        except PendingRollbackError:
            if count > attempts:
                raise
            db_session.rollback()
            continue
        except OperationalError:
            if count > attempts:
                raise
            sleep(0.5 * (attempts * interval))
            continue
        return False
