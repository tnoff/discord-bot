import asyncio
from tempfile import NamedTemporaryFile

from click.testing import CliRunner
import pytest
from yaml import dump

from discord_bot.cli import main

from tests.helpers import fake_bot_yielder, FakeGuild

def test_run_with_no_args():
    '''
    Throw error with no config options
    '''
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert "Error: Missing argument 'CONFIG_FILE'" in result.output

def test_run_no_file():
    '''
    Test with no config file
    '''
    with NamedTemporaryFile() as temp_config:
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert 'General config section required' in str(result.exception)

def test_run_config_but_no_data():
    '''
    Test with empty config
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {},
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        assert 'Unable to run bot without token' in str(result.exception)

@pytest.mark.asyncio
async def test_run_config_only_token(mocker):
    '''
    Run with only token
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo'
            },
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder())
        runner = CliRunner()
        result = runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert result.exception is None

@pytest.mark.asyncio
async def test_run_config_reject_list(mocker):
    '''
    Leave server within rejectlist
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        fake_guild = FakeGuild()
        guilds = [fake_guild]
        config_data = {
            'general': {
                'discord_token': 'foo',
                'rejectlist_guilds': [
                    fake_guild.id,
                ],
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)

        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=guilds))
        runner = CliRunner()
        runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert guilds[0].left_guild is True

@pytest.mark.asyncio
async def test_run_config_no_reject_list(mocker):
    '''
    Run config with no checklist
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        config_data = {
            'general': {
                'discord_token': 'foo',
            }
        }
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        guilds = [FakeGuild()]
        mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder(guilds=guilds))
        runner = CliRunner()
        runner.invoke(main, [temp_config.name])
        await asyncio.sleep(.01)
        assert guilds[0].left_guild is False

@pytest.mark.asyncio
async def test_run_config_with_db(mocker):
    '''
    Run config with sqlite db
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            config_data = {
                'general': {
                    'discord_token': 'foo',
                    'sql_connection_statement': f'sqlite:///{temp_db.name}',
                    'rejectlist_guilds': [
                        'fake-guild-1234',
                    ],
                }
            }
            with open(temp_config.name, 'w', encoding='utf-8') as writer:
                dump(config_data, writer)
            mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder())
            runner = CliRunner()
            result = runner.invoke(main, [temp_config.name])
            await asyncio.sleep(.01)
            assert result.exception is None

@pytest.mark.asyncio
async def test_run_config_with_intents(mocker):
    '''
    Run config with intents
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            config_data = {
                'general': {
                    'discord_token': 'foo',
                    'sql_connection_statement': f'sqlite:///{temp_db.name}',
                    'intents': [
                        'members',
                    ]
                },
            }
            with open(temp_config.name, 'w', encoding='utf-8') as writer:
                dump(config_data, writer)
            mocker.patch('discord_bot.cli.Bot', side_effect=fake_bot_yielder())
            runner = CliRunner()
            result = runner.invoke(main, [temp_config.name])
            await asyncio.sleep(.01)
            assert result.exception is None

def test_run_markov_clear():
    '''
    Run the markov clear command
    '''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        with NamedTemporaryFile(suffix='.sql') as temp_db:
            config_data = {
                'general': {
                    'discord_token': 'foo',
                    'sql_connection_statement': f'sqlite:///{temp_db.name}',
                },
            }
            with open(temp_config.name, 'w', encoding='utf-8') as writer:
                dump(config_data, writer)
            runner = CliRunner()
            result = runner.invoke(main, [temp_config.name, '--execute', 'clear-markov-relations'])
            assert result
