'''Shared by the pods' CLI tests: run a pod's `main` with its health server and the discord Bot replaced.'''
import asyncio
from tempfile import NamedTemporaryFile
from unittest.mock import AsyncMock, MagicMock

from click.testing import CliRunner
from yaml import dump

from testing.helpers import fake_bot_yielder


async def invoke_main_with_health_server(mocker, main, config_file, health_server_target, extra_patches=()):
    '''Invoke the pod's click `main` on `config_file` with its HealthServer (patched at `health_server_target`) and the
    shared gateway Bot faked, plus any `extra_patches`, and return the CliRunner result once the loop has had a turn.'''
    mock_hs = MagicMock()
    mock_hs.serve = AsyncMock()
    mocker.patch(health_server_target, return_value=mock_hs)
    mocker.patch('discord_core.cli._lib.gateway.Bot', side_effect=fake_bot_yielder(guilds=[]))
    for target in extra_patches:
        mocker.patch(target)
    result = CliRunner().invoke(main, [config_file])
    await asyncio.sleep(.01)
    return result


def invoke_with_config(main, config_data):
    '''Write `config_data` to a temporary YAML file, invoke the pod's click `main` on it, return the CliRunner result.'''
    with NamedTemporaryFile(suffix='.yml') as temp_config:
        with open(temp_config.name, 'w', encoding='utf-8') as writer:
            dump(config_data, writer)
        return CliRunner().invoke(main, [temp_config.name])
