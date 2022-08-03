from logging import getLogger, Formatter, DEBUG
from logging.handlers import RotatingFileHandler

from sqlalchemy import create_engine

def get_logger(logger_name, log_file):
    '''
    Generic logger
    '''
    logger = getLogger(logger_name)
    formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    logger.setLevel(DEBUG)
    fh = RotatingFileHandler(log_file,
                             backupCount=2,
                             maxBytes=((2 ** 20) * 10))
    fh.setLevel(DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def get_mysql_database_engine(mysql_user, mysql_password, mysql_database, mysql_host):
    '''
    Mysql database session
    '''
    sql_statement = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}'
    engine = create_engine(sql_statement, encoding='utf-8')
    return engine

def get_sqlite_database_engine(sqlite_file):
    '''
    Return sqlite database session
    '''
    engine = create_engine(f'sqlite:///{sqlite_file}', encoding='utf-8')
    return engine

def get_db_engine(settings):
    '''
    Use settings to return db_session
    '''
    if settings['general_db_type'] == 'mysql':
        return get_mysql_database_engine(settings['mysql_user'],
                                          settings['mysql_password'],
                                          settings['mysql_database'],
                                          settings['mysql_host'])
    if settings['general_db_type'] == 'sqlite':
        return get_sqlite_database_engine(settings['sqlite_file'])
    return None
