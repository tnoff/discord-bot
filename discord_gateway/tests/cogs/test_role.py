from unittest.mock import AsyncMock

import pytest

from discord_core.exceptions import CogMissingRequiredArg, DiscordBotException

from discord_gateway.cogs.role import RoleAssignment

from tests.helpers import fake_context #pylint:disable=unused-import

CONFIG = {
    'general': {
        'commands_http_url': 'http://commands:8086',
        'include': {
            'role': True
        }
    },
}


def _cog(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], CONFIG, fake_context['dispatcher'])
    cog.commands_client = AsyncMock()
    return cog


def test_role_no_enabled(fake_context):  #pylint:disable=redefined-outer-name
    with pytest.raises(CogMissingRequiredArg) as exc:
        RoleAssignment(fake_context['bot'], {}, fake_context['dispatcher'])
    assert 'Role not enabled' in str(exc.value)


def test_role_needs_the_commands_pod_url(fake_context):  #pylint:disable=redefined-outer-name
    with pytest.raises(DiscordBotException, match='commands_http_url'):
        RoleAssignment(fake_context['bot'], {'general': {'include': {'role': True}}}, fake_context['dispatcher'])


def test_role_no_longer_needs_the_members_intent(fake_context):  #pylint:disable=redefined-outer-name
    fake_context['bot'].intents.members = False
    assert RoleAssignment(fake_context['bot'], CONFIG, fake_context['dispatcher']) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('method,command,args', [
    ('role_list', 'role.list', {}),
    ('role_managed', 'role.available', {}),
    ('role_list_users', 'role.users', {'role_input': 'staff'}),
    ('role_add', 'role.add', {'inputs': '<@2> <@&200>'}),
    ('role_remove', 'role.remove', {'inputs': '<@2> <@&200>'}),
])
async def test_subcommands_forward_to_the_commands_pod(fake_context, method, command, args):  #pylint:disable=redefined-outer-name
    cog = _cog(fake_context)
    await getattr(cog, method)(cog, fake_context['context'], **args)
    cog.commands_client.forward.assert_awaited_once()
    forwarded = cog.commands_client.forward.await_args
    assert forwarded.args[0] == command
    assert forwarded.args[2] == args
    assert forwarded.args[1].guild_id == fake_context['guild'].id


@pytest.mark.asyncio
async def test_role_group_no_subcommand(fake_context):  #pylint:disable=redefined-outer-name
    cog = _cog(fake_context)
    fake_context['context'].invoked_subcommand = None
    await cog.role(cog, fake_context['context'])  #pylint:disable=too-many-function-args
    assert 'Invalid sub command passed...' in fake_context['context'].messages_sent
    cog.commands_client.forward.assert_not_awaited()
