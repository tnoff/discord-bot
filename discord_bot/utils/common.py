from asyncio import sleep as async_sleep
from logging import getLogger, Formatter, StreamHandler, RootLogger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from re import sub
from sys import stdout
from typing import Awaitable, Callable

from aiohttp.client_exceptions import ServerDisconnectedError
from jsonschema import validate
from discord.errors import DiscordServerError, RateLimited, NotFound
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
        'monitoring': {
            'type': 'object',
            'properties': {
                'otlp': {
                    'type': 'object',
                    'properties': {
                        'enabled': {
                            'type': 'boolean',
                        },
                    },
                    'required': [
                        'enabled'
                    ]
                },
                'memory_profiling': {
                    'type': 'object',
                    'properties': {
                        'enabled': {
                            'type': 'boolean',
                            'default': False,
                        },
                        'interval_seconds': {
                            'type': 'integer',
                            'minimum': 1,
                            'default': 60,
                        },
                        'top_n_lines': {
                            'type': 'integer',
                            'minimum': 1,
                            'default': 25,
                        },
                    },
                },
            },
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

async def async_retry_command(func: Callable[[], Awaitable], max_retries: int = 3,
                              retry_exceptions=None, post_exception_functions=None,
                              accepted_exceptions=None):
    '''
    Use retries for the command, mostly deals with db issues

    func: Callable partial function to run
    max_retries : Max retries until we fail
    retry_exceptions: Retry on these exceptions
    post_exception_functions: On retry_exceptions, run these functions
    accepted_exceptions: Exceptions that are swallowed
    '''
    retry_exceptions = retry_exceptions or ()
    post_functions = post_exception_functions or []
    accepted_exceptions = accepted_exceptions or ()
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
                span.record_exception(ex)
                span.set_status(StatusCode.OK)
                return False
            except retry_exceptions as ex:
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

async def async_retry_discord_message_command(func: Callable[[], Awaitable], max_retries: int = 3, allow_404: bool = False):
    '''
    Retry discord send message command, catch case of rate limiting

    func: Function to retry
    max_retries: Max retry before failing
    allow_404 : 404 exceptions are fine and we can skip
    '''
    # For 429s, there is a 'retry_after' arg that tells how long to sleep before trying again
    async def check_429(ex, is_last_retry):
        if isinstance(ex, RateLimited) and not is_last_retry:
            await async_sleep(ex.retry_after)
            raise SkipRetrySleep('Skip sleep since we slept already')
    post_exception_functions = [check_429]
    # These are common discord api exceptions we can retry on
    retry_exceptions = (RateLimited, DiscordServerError, TimeoutError, ServerDisconnectedError)
    accepted_exceptions = ()
    if allow_404:
        accepted_exceptions = NotFound
    with otel_span_wrapper(f'{OTEL_SPAN_PREFIX}.message_send_async', kind=SpanKind.CLIENT):
        return await async_retry_command(func, max_retries=max_retries,
                                         retry_exceptions=retry_exceptions, post_exception_functions=post_exception_functions,
                                         accepted_exceptions=accepted_exceptions)

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

def return_loop_runner(function: Callable, bot: Bot, logger: RootLogger, continue_exceptions=None, exit_exceptions=ExitEarlyException):
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
    async def loop_runner(): #pylint:disable=duplicate-code
        await bot.wait_until_ready()

        while not bot.is_closed():
            try:
                await function()
            except continue_exceptions as e:
                logger.exception('Continue exception in loop runner: %s', type(e).__name__, exc_info=True)
                continue
            except exit_exceptions:
                return False
            except Exception as e:
                logger.exception('Exception in loop runner: %s', type(e).__name__, exc_info=True)
                return False
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

def discord_format_string_embed(stringy: str) -> str:
    '''
    Format discord string so it is not embedded
    '''
    # Regex to match URLs and wrap them in angle brackets to prevent embedding
    # This matches https:// followed by non-whitespace characters
    url_pattern = r'(https://\S+)'
    return sub(url_pattern, r'<\1>', stringy)
