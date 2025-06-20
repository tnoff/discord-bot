from asyncio import sleep as async_sleep
from logging import getLogger, Formatter, StreamHandler, RootLogger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from sys import stdout
from time import sleep
from traceback import format_exc
from typing import Callable

from jsonschema import validate
from discord.errors import DiscordServerError, RateLimited
from discord.ext.commands import Bot
from opentelemetry.trace import SpanKind
from opentelemetry.trace.status import StatusCode
from opentelemetry.sdk._logs import LoggingHandler
from sqlalchemy.orm.session import Session

from discord_bot.exceptions import ExitEarlyException
from discord_bot.utils.otel import otel_span_wrapper, AttributeNaming

OTEL_SPAN_PREFIX = 'utils'

GENERAL_SECTION_SCHEMA = {
    'type': 'object',
    'properties': {
        'discord_token': {
            'type': 'string'
        },
        'sql_connection_statement': {
            'type': 'string',
        },
        'otlp': {
            'type': 'object',
            'properties': {
                'log_endpoint': {
                    'type': 'string',
                },
                'trace_endpoint': {
                    'type': 'string'
                },
                'metric_endpoint': {
                    'type': 'string',
                },
            },
            'required': [
                'log_endpoint',
                'trace_endpoint',
                'metric_endpoint',
            ],
        },
        'logging': {
            'type': 'object',
            'properties': {
                'log_dir': {
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
                },
                'logging_format': {
                    'type': 'string',
                },
                'logging_date_format': {
                    'type': 'string',
                }
            },
            'required': [
                'log_dir',
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

def get_logger(logger_name, logging_section, otlp_logger=None):
    '''
    Generic logger
    '''
    logger = getLogger(logger_name)
    logging_format = logging_section.get('logging_format', '%(asctime)s - %(levelname)s - %(message)s')
    logging_date_format = logging_section.get('logging_date_format', '%Y-%m-%dT%H-%M-%S')
    formatter = Formatter(logging_format, datefmt=logging_date_format)
    # If no logging section given, return generic logger
    # That logs to stdout
    if not logging_section:
        ch = StreamHandler(stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(10)
        return logger
    # Else set more proper rotated file logging
    log_file = Path(logging_section['log_dir']) / f'{logger_name}.log'
    fh = RotatingFileHandler(str(log_file),
                             backupCount=logging_section['log_file_count'],
                             maxBytes=logging_section['log_file_max_bytes'])
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.setLevel(logging_section['log_level'])
    if otlp_logger:
        handler = LoggingHandler(level=logging_section['log_level'], logger_provider=otlp_logger)
        logger.addHandler(handler)

    return logger

def retry_command(func, max_retries: int = 3, accepted_exceptions=None, post_exception_functions=None):
    '''
    Use retries for the command, mostly deals with db issues
    '''
    accepted_exceptions = accepted_exceptions or ()
    post_functions = post_exception_functions or ()
    retry = -1
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_command_synchronous', kind=SpanKind.CLIENT) as span:
        while True:
            retry += 1
            should_sleep = True
            span.set_attributes({
                AttributeNaming.RETRY_COUNT.value: retry
            })
            try:
                result = func()
                span.set_status(StatusCode.OK)
                return result
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
                span.set_status(StatusCode.ERROR)
                span.record_exception(ex)
                raise

async def async_retry_command(func, max_retries: int = 3, accepted_exceptions=None, post_exception_functions=None):
    '''
    Use retries for the command, mostly deals with db issues
    '''
    accepted_exceptions = accepted_exceptions or ()
    post_functions = post_exception_functions or ()
    retry = -1
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.retry_command_async', kind=SpanKind.CLIENT) as span:
        while True:
            retry += 1
            should_sleep = True
            span.set_attributes({
                AttributeNaming.RETRY_COUNT.value: retry
            })
            try:
                result = await func()
                span.set_status(StatusCode.OK)
                return result
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
                span.set_status(StatusCode.ERROR)
                span.record_exception(ex)
                raise

def retry_discord_message_command(func, max_retries: int = 3):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    def check_429(ex, is_last_retry):
        if isinstance(ex, RateLimited) and not is_last_retry:
            sleep(ex.retry_after)
            raise SkipRetrySleep('Skip sleep since we slept already')
    post_exception_functions = [check_429]
    exceptions = (RateLimited, DiscordServerError, TimeoutError)
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.message_send_synchronous', kind=SpanKind.CLIENT):
        return retry_command(func, max_retries=max_retries, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)

async def async_retry_discord_message_command(func, max_retries: int = 3):
    '''
    Retry discord send message command, catch case of rate limiting
    '''
    async def check_429(ex, is_last_retry):
        if isinstance(ex, RateLimited) and not is_last_retry:
            await async_sleep(ex.retry_after)
            raise SkipRetrySleep('Skip sleep since we slept already')
    post_exception_functions = [check_429]
    exceptions = (RateLimited, DiscordServerError, TimeoutError)
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.message_send_async', kind=SpanKind.CLIENT):
        return await async_retry_command(func, max_retries=max_retries, accepted_exceptions=exceptions, post_exception_functions=post_exception_functions)

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

def return_loop_runner(function: Callable, bot: Bot, logger: RootLogger, checkfile: Path, continue_exceptions=None, exit_exceptions=ExitEarlyException):
    '''
    Return a basic standard bot loop

    function : Function to run, must by async
    bot : Bot object
    logger : Logger for exceptions
    checkfile: Writes 1 to file when loop active, writes 0 when its not
    continue_exceptions: Do not exit on these exceptions
    exit_exceptions : Exit on these exceptions
    '''
    continue_exceptions = continue_exceptions or ()
    if checkfile:
        checkfile.write_text('1')
    async def loop_runner(): #pylint:disable=duplicate-code
        await bot.wait_until_ready()

        while not bot.is_closed():
            try:
                await function()
            except continue_exceptions:
                continue
            except exit_exceptions:
                if checkfile:
                    checkfile.write_text('0')
                return False
            except Exception as e:
                logger.exception(e)
                logger.error(format_exc())
                logger.error(str(e))
                print(f'Player loop exception {str(e)}')
                print('Formatted exception:', format_exc())
                if checkfile:
                    checkfile.write_text('0')
                return False
        if checkfile:
            checkfile.write_text('0')
    return loop_runner

def run_commit(db_session: Session):
    '''
    Run commit on a db_session, useful for using in retries
    '''
    db_session.commit()

def create_observable_gauge(meter_provider, name: str, function, description: str, unit: str = '1'):
    '''
    Yield a loop callback method for heartbeat
    '''
    meter_provider.create_observable_gauge(
        name=name,
        callbacks=[function],
        unit=unit,
        description=description,
    )
