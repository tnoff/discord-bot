from asyncio import sleep as async_sleep
from logging import getLogger, Formatter, DEBUG, StreamHandler
from logging.handlers import RotatingFileHandler
from sys import stdout
from time import sleep

from discord.errors import HTTPException, DiscordServerError, RateLimited
from jsonschema import validate

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

GENERAL_SECTION_SCHEMA = {
    'type': 'object',
    'properties': {
        'log_file': {
            'type': 'string'
        },
        'discord_token': {
            'type': 'string'
        },
        'sql_connection_statement': {
            'type': 'string',
        },
        'logging': {
            'type': 'object',
            'properties': {
                'log_file': {
                    'type': 'string',
                },
                'log_file_count': {
                    'type': 'integer',
                },
                'log_file_max_bytes': {
                    'type': 'integer',
                }
            },
            'required': [
                'log_file',
                'log_file_count',
                'log_file_max_bytes',
            ]
        },
        'include': {
            'type': 'object',
            'properties': {
                'default': {
                    'type': 'boolean',
                },
                'markov': {
                    'type': 'boolean',
                },
                'urban': {
                    'type': 'boolean',
                },
                'music': {
                    'type': 'boolean',
                },
                'delete_messages': {
                    'type': 'boolean',
                }
            }
        },
        'intents': {
            'type': 'array',
            'items': {
                'type': 'string',
            },
        },
    },
    'required': [
        'discord_token',
    ]
}

def validate_config(config_section, schema):
    '''
    Validate config against a JSON schema
    '''
    return validate(instance=config_section, schema=schema)

def get_logger(logger_name, logging_section):
    '''
    Generic logger
    '''
    logger = getLogger(logger_name)
    formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s',
                          datefmt=DATETIME_FORMAT)
    logger.setLevel(DEBUG)
    if not logging_section:
        ch = StreamHandler(stdout)
        ch.setLevel(DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    else:
        fh = RotatingFileHandler(logging_section['log_file'],
                                backupCount=logging_section['log_file_count'],
                                maxBytes=logging_section['log_file_max_bytes'])
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
                await async_sleep(sleep_for)
                continue
            raise

def retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    def check_429(ex):
        if '429' not in str(ex):
            raise #pylint:disable=misplaced-bare-raise
    post_exception_functions = [check_429]
    exceptions = (HTTPException, RateLimited, DiscordServerError)
    return retry_command(func, *args, **kwargs, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)

async def async_retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    def check_429(ex):
        if '429' not in str(ex):
            raise #pylint:disable=misplaced-bare-raise
    post_exception_functions = [check_429]
    exceptions = (HTTPException, RateLimited, DiscordServerError)
    return await async_retry_command(func, *args, **kwargs, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)
