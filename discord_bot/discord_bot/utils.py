from configparser import NoSectionError, NoOptionError, SafeConfigParser
import logging
from logging.handlers import RotatingFileHandler
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query as _Query


from discord_bot.database import BASE

# https://stackoverflow.com/questions/53287215/retry-failed-sqlalchemy-queries
class RetryingQuery(_Query):
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

def get_database_session(mysql_user, mysql_password, mysql_database, mysql_host):
    '''
    Mysql database session
    '''
    sql_statement = f'mysql+pymysql://{mysql_user}:{mysql_password}@localhost'
    sql_statement += f'/{mysql_database}?host={mysql_host}?port=3306'
    engine = create_engine(sql_statement, encoding='utf-8')
    BASE.metadata.create_all(engine)
    BASE.metadata.bind = engine
    return sessionmaker(bind=engine, query_cls=RetryingQuery)()

def read_config(config_file):
    '''
    Get values from config file
    '''
    if config_file is None:
        return dict()
    parser = SafeConfigParser()
    parser.read(config_file)
    mapping = {
        # General
        'log_file' : ['general', 'log_file'],
        'discord_token' : ['general', 'discord_token'],
        # Mysql
        'mysql_user' : ['mysql', 'user'],
        'mysql_password' : ['mysql', 'password'],
        'mysql_database' : ['mysql', 'database'],
        'mysql_host'     : ['mysql', 'host'],
        # Twitter
        'twitter_api_key' : ['twitter', 'api_key'],
        'twitter_api_key_secret' : ['twitter', 'api_key_secret'],
        'twitter_access_token' : ['twitter', 'access_token'],
        'twitter_access_token_secret' : ['twitter', 'access_token_secret'],
    }
    return_data = dict()
    for key_name, args in mapping.items():
        try:
            value = parser.get(*args)
        except (NoSectionError, NoOptionError):
            value = None
        return_data[key_name] = value
    return return_data
