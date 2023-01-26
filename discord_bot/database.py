from datetime import datetime, date
from json import JSONEncoder, dumps
from time import sleep

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.orm.decl_api import registry

from discord_bot.exceptions import UnhandledColumnType
from discord_bot.utils import DATETIME_FORMAT

BASE = declarative_base()

# https://coded3.com/how-to-serialize-sqlalchemy-result-to-json/
class AlchemyEncoder(JSONEncoder):
    '''
    Encode sqlalchemy data as JSON
    '''
    def default(self, o):
        if isinstance(o.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(o) if not x.startswith('_') and x != 'metadata']:
                data = getattr(o, field)
                if isinstance(data, registry):
                    continue
                try:
                    dumps(data)
                    fields[field] = data
                except TypeError as exc:
                    if isinstance(data, (date, datetime)):
                        fields[field] = data.strftime(DATETIME_FORMAT)
                    else:
                        raise UnhandledColumnType(f'Field {field} and data {data} are not handled by AlchemyEncoder') from exc
            # a json-encodable dict
            return fields
        return JSONEncoder.default(self, o)

# https://stackoverflow.com/questions/53287215/retry-failed-sqlalchemy-queries
class RetryingQuery(Query):
    '''
    Add some basic retry logic to the queries
    '''
    __max_retry_count__ = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __iter__(self):
        attempts = 0
        while True:
            attempts += 1
            try:
                return super().__iter__()
            except OperationalError as ex:
                print(f'SQL Operational error {str(ex)}')
                if "server closed the connection unexpectedly" not in str(ex):
                    raise
                if attempts <= self.__max_retry_count__:
                    print(f'Retrying SQL Error, attempt {attempts}')
                    sleep_for = 2 ** (attempts - 1)
                    sleep(sleep_for)
                    continue
                raise
