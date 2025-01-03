from asyncio import sleep as async_sleep
from logging import getLogger, Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from sys import stdout
from time import sleep

from jsonschema import validate

from discord.errors import DiscordServerError, RateLimited

GENERAL_SECTION_SCHEMA = {
    'type': 'object',
    'properties': {
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
                },
                'log_level': {
                    'type': 'integer',
                    'enum': [0, 10, 20, 30, 40, 50],
                }
            },
            'required': [
                'log_file',
                'log_level',
                'log_file_count',
                'log_file_max_bytes',
            ]
        },
        'include': {
            'type': 'object',
            'properties': {
                'default': {
                    'type': 'boolean',
                    'default': True,
                },
                'markov': {
                    'type': 'boolean',
                    'default': False,
                },
                'urban': {
                    'type': 'boolean',
                    'default': False,
                },
                'music': {
                    'type': 'boolean',
                    'default': False,
                },
                'delete_messages': {
                    'type': 'boolean',
                    'default': False,
                }
            },
        },
        'intents': {
            'type': 'array',
            'items': {
                'type': 'string',
            },
        },
        'rejectlist_guilds': {
            'type': 'array',
            'items': {
                'type': 'string',
            }
        }
    },
    'required': [
        'discord_token',
    ]
}

class SkipRetrySleep(Exception):
    '''
    Call this to skip generic retry logic
    '''

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
                          datefmt='%Y-%m-%dT%H.%M.%S')
    # If no logging section given, return generic logger
    # That logs to stdout
    if not logging_section:
        ch = StreamHandler(stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(10)
        return logger
    # Else set more proper rotated file logging
    fh = RotatingFileHandler(logging_section['log_file'],
                            backupCount=logging_section['log_file_count'],
                            maxBytes=logging_section['log_file_max_bytes'])
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.setLevel(logging_section['log_level'])
    return logger

def retry_command(func, *args, **kwargs):
    '''
    Use retries for the command, mostly deals with db issues
    '''
    max_retries = kwargs.pop('max_retries', 3)
    accepted_exceptions = kwargs.pop('accepted_exceptions', ())
    post_functions = kwargs.pop('post_exception_functions', [])
    retry = -1
    while True:
        retry += 1
        should_sleep = True
        try:
            return func(*args, **kwargs)
        except accepted_exceptions as ex:
            try:
                for pf in post_functions:
                    pf(ex, retry == max_retries)
            except SkipRetrySleep:
                should_sleep = False
            if retry < max_retries:
                if should_sleep:
                    sleep_for = 2 ** (retry - 1)
                    sleep(sleep_for)
                continue
            raise

async def async_retry_command(func, *args, **kwargs):
    '''
    Use retries for the command, mostly deals with db issues
    '''
    print('Retry command', func, args, kwargs)
    max_retries = kwargs.pop('max_retries', 3)
    accepted_exceptions = kwargs.pop('accepted_exceptions', ())
    post_functions = kwargs.pop('post_exception_functions', [])
    retry = -1
    while True:
        retry += 1
        should_sleep = True
        try:
            return await func(*args, **kwargs)
        except accepted_exceptions as ex:
            try:
                for pf in post_functions:
                    await pf(ex, retry == max_retries)
            except SkipRetrySleep:
                should_sleep = False
            if retry < max_retries:
                if should_sleep:
                    sleep_for = 2 ** (retry - 1)
                    await async_sleep(sleep_for)
                continue
            raise

def retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    def check_429(ex, is_last_retry):
        if isinstance(ex, RateLimited) and not is_last_retry:
            sleep(ex.retry_after)
            raise SkipRetrySleep('Skip sleep since we slept already')
    post_exception_functions = [check_429]
    exceptions = (RateLimited, DiscordServerError, TimeoutError)
    return retry_command(func, *args, **kwargs, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)

async def async_retry_discord_message_command(func, *args, **kwargs):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    async def check_429(ex, is_last_retry):
        if isinstance(ex, RateLimited) and not is_last_retry:
            await async_sleep(ex.retry_after)
            raise SkipRetrySleep('Skip sleep since we slept already')
    post_exception_functions = [check_429]
    exceptions = (RateLimited, DiscordServerError, TimeoutError)
    return await async_retry_command(func, *args, **kwargs, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)

def rm_tree(pth: Path) -> bool:
    '''
    Remove all files in a tree
    '''
    # https://stackoverflow.com/questions/50186904/pathlib-recursively-remove-directory
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()
    return True
