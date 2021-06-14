
import logging
from logging.handlers import RotatingFileHandler
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import InvalidRequestError, OperationalError, StatementError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query as _Query

from discord_bot.database import BASE

# https://stackoverflow.com/questions/53287215/retry-failed-sqlalchemy-queries
class RetryingQuery(_Query): #pylint:disable=too-many-ancestors
    '''
    Retry logic for sqlalchemy
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
                if "server closed the connection unexpectedly" not in str(ex):
                    raise
                if attempts <= self.__max_retry_count__:
                    sleep_for = 2 ** (attempts - 1)
                    logging.error(f'Database connection error, sleeping for {sleep_for} seconds')
                    sleep(sleep_for)
                    continue
                raise
            except StatementError as ex:
                if "reconnect until invalid transaction is rolled back" not in str(ex):
                    raise
                self.session.rollback()
            except InvalidRequestError as ex:
                logging.error(f'Invalid request error {str(ex)}')
                self.session.rollback()

def get_logger(logger_name, log_file):
    '''
    Generic logger
    '''
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logger.setLevel(logging.DEBUG)
    fh = RotatingFileHandler(log_file,
                             backupCount=2,
                             maxBytes=((2 ** 20) * 10))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def get_mysql_database_session(mysql_user, mysql_password, mysql_database, mysql_host):
    '''
    Mysql database session
    '''
    sql_statement = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}'
    engine = create_engine(sql_statement, encoding='utf-8')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine
    return sessionmaker(bind=engine, query_cls=RetryingQuery)()

def get_sqlite_database_session(sqlite_file):
    '''
    Return sqlite database session
    '''
    engine = create_engine(f'sqlite:///{sqlite_file}', encoding='utf-8')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine
    return sessionmaker(bind=engine, query_cls=RetryingQuery)()

def get_db_session(settings):
    '''
    Use settings to return db_session
    '''
    if settings['db_type'] == 'mysql':
        return get_mysql_database_session(settings['mysql_user'],
                                          settings['mysql_password'],
                                          settings['mysql_database'],
                                          settings['mysql_host'])
    if settings['db_type'] == 'sqlite':
        return get_sqlite_database_session(settings['sqlite_file'])
    return None
