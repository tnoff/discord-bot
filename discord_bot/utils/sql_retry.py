# https://stackoverflow.com/questions/53287215/retry-failed-sqlalchemy-queries

from time import sleep
from typing import Callable

from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode
from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm.session import Session

from discord_bot.utils.otel import otel_span_wrapper, AttributeNaming

OTEL_SPAN_PREFIX = 'sql_retry'

def retry_database_commands(db_session: Session, function: Callable, attempts: int = 3, interval: int = 1):
    '''
    Wrap database commands in a retry class

    db_session : Valid db_session being used
    function   : Function with db_session attached
    attempts : Number of attempts
    interval : Sleep interval
    '''
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_db_command', kind=SpanKind.CLIENT) as span:
        count = 0
        while True:
            span.set_attributes({
                AttributeNaming.RETRY_COUNT.value: count,
            })
            count += 1
            try:
                result = function()
                span.set_status(StatusCode.OK)
                return result
            except PendingRollbackError as error:
                if count > attempts:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(error)
                    raise
                db_session.rollback()
                continue
            except OperationalError as error:
                if count > attempts:
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(error)
                    raise
                sleep(0.5 * (attempts * interval))
                continue
