import logging

import pytest

from discord_bot.cogs.role import RoleAssignment
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import fake_bot_yielder, FakeContext, FakeGuild, FakeEmjoi, FakeChannel, FakeMessage


def test_role_no_enabled():
    fake_bot = fake_bot_yielder()()
    with pytest.raises(CogMissingRequiredArg) as exc:
        RoleAssignment(fake_bot, logging, {}, None)
    assert 'Role not enabled' in str(exc.value)

def test_role_invalid_config():
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
    }
    fake_bot = fake_bot_yielder()()
    with pytest.raises(CogMissingRequiredArg) as exc:
        RoleAssignment(fake_bot, logging, config, None)
    assert 'Invalid config given' in str(exc.value)