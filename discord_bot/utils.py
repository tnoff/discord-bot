from logging import getLogger, Formatter, DEBUG
from logging.handlers import RotatingFileHandler
from time import sleep

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

def get_logger(logger_name, log_file):
    '''
    Generic logger
    '''
    logger = getLogger(logger_name)
    formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt=DATETIME_FORMAT)
    logger.setLevel(DEBUG)
    fh = RotatingFileHandler(log_file,
                             backupCount=2,
                             maxBytes=((2 ** 20) * 10))
    fh.setLevel(DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def retry_command(func, *args, **kwargs):
    '''
    Use retries for the command, mostly deals with db issues
    '''
    max_retries = kwargs.pop('max_retries', 3)
    accepted_exceptions = kwargs.pop('accepted_exceptions', (Exception))
    post_functions = kwargs.pop('post_exception_functions', [])
    retry = 0
    while True:
        retry += 1
        try:
            return func(*args, **kwargs)
        except accepted_exceptions as ex:
            for pf in post_functions:
                pf(ex)
            if retry <= max_retries:
                sleep_for = 2 ** (retry - 1)
                sleep(sleep_for)
                continue
            raise

async def async_retry_command(func, *args, **kwargs):
    '''
    Use retries for the command, mostly deals with db issues
    '''
    max_retries = kwargs.pop('max_retries', 3)
    accepted_exceptions = kwargs.pop('accepted_exceptions', (Exception))
    post_functions = kwargs.pop('post_exception_functions', [])
    retry = 0
    while True:
        retry += 1
        try:
            return await func(*args, **kwargs)
        except accepted_exceptions as ex:
            for pf in post_functions:
                pf(ex)
            if retry <= max_retries:
                sleep_for = 2 ** (retry - 1)
                sleep(sleep_for)
                continue
            raise
