from configparser import NoSectionError, NoOptionError, SafeConfigParser
import logging
from logging.handlers import RotatingFileHandler
from time import sleep

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query as _Query


from discord_bot.exceptions import DiscordBotException
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

def get_mysql_database_session(mysql_user, mysql_password, mysql_database, mysql_host):
    '''
    Mysql database session
    '''
    sql_statement = f'mysql+pymysql://{mysql_user}:{mysql_password}@localhost' \
                    f'/{mysql_database}?host={mysql_host}?port=3306'
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
    return get_sqlite_database_session(settings['sqlite_file'])

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
        'db_type' : ['general', 'db_type'],
        'download_dir': ['general', 'download_dir'],
        'message_delete_after': ['general', 'message_delete_after'],
        'queue_max_size': ['general', 'queue_max_size'],
        'max_song_length': ['general', 'max_song_length'],
        # Mysql
        'mysql_user' : ['mysql', 'user'],
        'mysql_password' : ['mysql', 'password'],
        'mysql_database' : ['mysql', 'database'],
        'mysql_host'     : ['mysql', 'host'],
        # Sqlite settings
        'sqlite_file' : ['sqlite', 'file'],
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

def validate_config(settings):
    '''
    Validate some settings are set properly
    '''
    if settings['discord_token'] is None:
        raise DiscordBotException('No discord token given')
    if settings['db_type'] not in ['sqlite', 'mysql']:
        raise DiscordBotException(f'Invalid db_type {settings["db_type"]}')

    if settings['message_delete_after']:
        try:
            settings['message_delete_after'] = int(settings['message_delete_after'])
        except Exception as e:
            raise DiscordBotException(f'Invalid message after '
                                      f'type {settings["message_delete_after"]}') from e

    if settings['queue_max_size']:
        try:
            settings['queue_max_size'] = int(settings['queue_max_size'])
        except Exception as e:
            raise DiscordBotException(f'Invalid message after type '
                                      f'{settings["queue_max_size"]}') from e

def load_args(args):
    '''
    Load args from config file and command line
    '''
    settings = read_config(args.pop('config_file'))
    # Override settings if cli args passed
    for key, item in args.items():
        if key not in settings:
            settings[key] = item
        elif item is not None:
            settings[key] = item
    validate_config(settings)
    return settings
