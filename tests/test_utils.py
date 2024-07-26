from tempfile import NamedTemporaryFile
from time import sleep

from jsonschema.exceptions import ValidationError
from discord.errors import HTTPException, DiscordServerError, RateLimited
import pytest

from discord_bot.utils import GENERAL_SECTION_SCHEMA
from discord_bot.utils import validate_config
from discord_bot.utils import get_logger
from discord_bot.utils import retry_command
from discord_bot.utils import retry_discord_message_command

def test_validate_minimal_config():
    minimal_input = {
        'discord_token': 'abctoken'
    }
    validate_config(minimal_input, GENERAL_SECTION_SCHEMA)

def test_sql_statement_config():
    sql_input = {
        'discord_token': 'abctoken',
        'sql_connection_statement': 'sqlite:///foo.sql'
    }
    validate_config(sql_input, GENERAL_SECTION_SCHEMA)

def test_logging_config():
    logging_input = {
        'discord_token': 'abctoken',
        'logging': {},
    }
    with pytest.raises(ValidationError) as exc:
        validate_config(logging_input, GENERAL_SECTION_SCHEMA)
    assert "'log_file' is a required property" in str(exc.value)

    logging_input = {
        'discord_token': 'abctoken',
        'logging': {
            'log_file': '/var/foo.log',
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 30,
        }
    }
    validate_config(logging_input, GENERAL_SECTION_SCHEMA)

    logging_input = {
        'discord_token': 'abctoken',
        'logging': {
            'log_file': '/var/foo.log',
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 123,
        }
    }
    with pytest.raises(ValidationError) as exc:
        validate_config(logging_input, GENERAL_SECTION_SCHEMA)
    assert '123 is not one of [0, 10, 20, 30, 40, 50]' in str(exc.value)

def test_includes_config():
    include_input = {
        'discord_token': 'abctoken',
        'include': {
            'default': False,
        }
    }
    validate_config(include_input, GENERAL_SECTION_SCHEMA)

def test_intents_config():
    intents_input = {
        'discord_token': 'abctoken',
        'intents': [
            'message'
        ]
    }
    validate_config(intents_input, GENERAL_SECTION_SCHEMA)

def test_get_logger():
    # Test default options
    logger = get_logger('foo', {})
    assert logger.getEffectiveLevel() == 10
    assert logger.hasHandlers() == True


    with NamedTemporaryFile() as temp_file:
        # Test some more specific options
        log_args =  {
            'log_file': temp_file.name,
            'log_file_count': 1,
            'log_file_max_bytes': 10 * 1024,
            'log_level': 30,
        }
        logger = get_logger('foo', log_args)
        assert logger.getEffectiveLevel() == 30
        assert logger.hasHandlers() == True

def test_retry_command(mocker):
    # Test very basic call command
    def test_command(x, y: 2):
        return x + y
    val = retry_command(test_command, *[3], **{'y': 1})
    assert val == 4

    # Pass a funcion that should fail everytime, make sure it retries
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    def test_command(x, y):
        raise Exception('Test Exception')
    with pytest.raises(Exception) as exc:
        retry_command(test_command, *[2, 3])
    assert mock_time.call_count == 3
    # Same test but set max retries this time
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    with pytest.raises(Exception) as exc:
        retry_command(test_command, *[2, 3], **{'max_retries': 1})
    assert mock_time.call_count == 1

    # Lets try with a specific exception to pass in
    class TestException(Exception):
        pass
    def test_command(x):
        raise TestException('foo')
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    with pytest.raises(TestException) as exc:
        retry_command(test_command, *[2], **{'accepted_exceptions': (TestException)})
    assert mock_time.call_count == 3

    # Check does not retry when another exception type passed
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    with pytest.raises(TestException) as exc:
        retry_command(test_command, *[2], **{'accepted_exceptions': (TypeError)})
    assert mock_time.call_count == 0

    # Test post exceptions
    stub = mocker.stub(name='test_post')
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    with pytest.raises(TestException) as exc:
        retry_command(test_command, *[2], **{'accepted_exceptions': (TestException), 'post_exception_functions': [stub]})
    assert mock_time.call_count == 3
    assert stub.called == True

    # Try a post exception that throws another error
    def test_throw(exc):
        raise Exception('Test')
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    with pytest.raises(Exception) as exc:
        retry_command(test_command, *[2], **{'accepted_exceptions': (TestException), 'post_exception_functions': [test_throw]})
    assert mock_time.call_count == 0

def test_retry_discord(mocker):
    class FakeResponse():
        def __init__(self):
            self.status = 500
            self.reason = 'Cat unplugged the machines'
    def test_send_message():
        raise DiscordServerError(FakeResponse(), 'bar')
    mock_time = mocker.patch('discord_bot.utils.sleep', return_value=False)
    with pytest.raises(DiscordServerError) as exc:
        retry_discord_message_command(test_send_message)
    assert mock_time.call_count == 3

