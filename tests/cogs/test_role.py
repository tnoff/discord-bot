import logging

import pytest

from discord_bot.cogs.role import RoleAssignment
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import fake_bot_yielder, FakeContext, FakeGuild, FakeAuthor, FakeRole


VALID_BASIC_CONFIG = {
    'general': {
        'include': {
            'role': True
        }
    },
    'role': {
        'foo_bar': {
            'bar_foo': []
        }
    }
}

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

def test_clean_string():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    assert 'foo' == cog.clean_input('foo')
    assert cog.clean_input('“foo“') == 'foo'

def test_rejected_roles_no_data():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    assert [] == cog.get_rejected_roles_list(FakeContext())

def test_required_roles_no_data():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    assert [] == cog.get_required_roles(FakeContext())

def test_override_roles_no_data():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    assert [] == cog.get_override_role(FakeContext())

def test_self_service_roles_no_data():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    assert [] == cog.get_self_service_roles(FakeContext())

def test_rejected_roles_with_data():
    fake_guild = FakeGuild()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'rejected_roles_list': [
                    'reject-role-1234',
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    assert ['reject-role-1234'] == cog.get_rejected_roles_list(FakeContext(fake_guild=fake_guild))

def test_required_roles_with_data():
    fake_guild = FakeGuild()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    'required-role-1234',
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    assert ['required-role-1234'] == cog.get_required_roles(FakeContext(fake_guild=fake_guild))

def test_override_roles_with_data():
    fake_guild = FakeGuild()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'admin_override_role_list': [
                    'admin-role-1234',
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    assert ['admin-role-1234'] == cog.get_override_role(FakeContext(fake_guild=fake_guild))

def test_self_service_roles_with_data():
    fake_guild = FakeGuild()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    'self-service-role-1234',
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    assert ['self-service-role-1234'] == cog.get_self_service_roles(FakeContext(fake_guild=fake_guild))

@pytest.mark.asyncio
async def test_get_user_invalid_input():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = await cog.get_user(FakeContext(), 'foo bar')
    assert result is None

@pytest.mark.asyncio
async def test_get_user_valid_user():
    fake_user = FakeAuthor(id='123456789')
    fake_guild = FakeGuild(members=[fake_user])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = await cog.get_user(FakeContext(fake_guild=fake_guild), '<@123456789>')
    assert result.id == '123456789'

@pytest.mark.asyncio
async def test_get_user_not_found():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = await cog.get_user(FakeContext(), '<@123456789>')
    assert result is None

def test_get_role_valid_id():
    fake_role = FakeRole(id='123456789')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = cog.get_role(FakeContext(fake_guild=fake_guild), '123456789')
    assert result.id == '123456789'

def test_get_role_valid_id_but_no_roles():
    fake_role = FakeRole(id='123456789')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = cog.get_role(FakeContext(fake_guild=fake_guild), '9875423')
    assert result is None

def test_get_role_valid_name():
    fake_role = FakeRole(id='123456789', name='fake-role')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = cog.get_role(FakeContext(fake_guild=fake_guild), 'fake-role')
    assert result.id == '123456789'

def test_get_role_valid_name_with_space():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = cog.get_role(FakeContext(fake_guild=fake_guild), 'fake role')
    assert result.id == '123456789'

def test_get_role_invalid_name():
    fake_role = FakeRole(id='123456789', name='fake-role')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = cog.get_role(FakeContext(fake_guild=fake_guild), 'another-fake-role')
    assert result is None

@pytest.mark.asyncio
async def test_get_user_roles_with_spaces_for_role():
    fake_user = FakeAuthor(id='987654321')
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild(roles=[fake_role], members=[fake_user])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = await cog.get_user_or_role(FakeContext(fake_guild=fake_guild), '<@987654321> fake role')
    assert result[0][0].id == fake_user.id
    assert result[1].id == fake_role.id

def test_required_roles_with_no_config():
    fake_bot = fake_bot_yielder()()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    result = cog.check_required_roles(FakeContext())
    assert result is True

def test_required_roles_just_author():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])

    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_required_roles(FakeContext(author=fake_user))
    assert result is True

def test_required_roles_just_author_with_invalid():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])

    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_required_roles(FakeContext(author=fake_user))
    assert result is False

def test_required_roles_author_user_but_author_invalid():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_user2 = FakeAuthor(id='1234')
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_required_roles(FakeContext(author=fake_user), user=fake_user2)
    assert result is False

def test_required_roles_both():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_user2 = FakeAuthor(id='1234', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_required_roles(FakeContext(author=fake_user), user=fake_user2)
    assert result is True

def test_required_roles_user_no_roles():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_user2 = FakeAuthor(id='1234', roles=[])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_required_roles(FakeContext(author=fake_user), user=fake_user2)
    assert result is False

def test_check_override_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_guild = FakeGuild()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'admin_override_role_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_override_role(FakeContext(author=fake_user))
    assert result is True

def test_check_override_role_with_no_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_guild = FakeGuild()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'admin_override_role_list': [
                    '1234'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = cog.check_override_role(FakeContext(author=fake_user))
    assert result is False

@pytest.mark.asyncio
async def test_role_list_with_no_roles():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])

    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_list(cog, FakeContext(author=fake_user)) #pylint:disable=too-many-function-args
    assert result == 'No roles found'

@pytest.mark.asyncio
async def test_role_list_with_invalid_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])

    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_list(cog, FakeContext(author=fake_user)) #pylint:disable=too-many-function-args
    assert result == 'User "fake-display-name-123" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_list_basic_return():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234567', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])

    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_list(cog, context) #pylint:disable=too-many-function-args
    assert context.messages_sent == ['```Role Name\n------------------------------\n@fake role\n@fake role dos```']
    assert result is True

@pytest.mark.asyncio
async def test_role_list_with_rejected_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234567', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])

    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'rejected_roles_list': [
                    fake_role2.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_list(cog, context) #pylint:disable=too-many-function-args
    assert context.messages_sent == ['```Role Name\n------------------------------\n@fake role```']
    assert result is True

@pytest.mark.asyncio
async def test_role_list_users_invalid_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_list_users(cog, FakeContext(author=fake_user), role_input='foo bar') #pylint:disable=too-many-function-args
    assert result == 'User "fake-display-name-123" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_list_users_role_name_invalid():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_list_users(cog, FakeContext(author=fake_user, fake_guild=fake_guild), role_input='foo bar') #pylint:disable=too-many-function-args
    assert result == 'Unable to find role "foo bar"'

@pytest.mark.asyncio
async def test_role_list_users_with_no_users():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_list_users(cog, context, role_input=f'{fake_role.name}') #pylint:disable=too-many-function-args
    assert result == 'No users found for role "fake role"'

@pytest.mark.asyncio
async def test_role_list_users_in_reject_roles():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='98765432', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'rejected_roles_list': [
                    fake_role2.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_list_users(cog, context, role_input=f'{fake_role2.name}') #pylint:disable=too-many-function-args
    assert result == 'Unable to list users for role "fake role dos", in reject list'

@pytest.mark.asyncio
async def test_role_list_users_with_users():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_role.members = [fake_user]
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_list_users(cog, context, role_input=f'{fake_role.name}') #pylint:disable=too-many-function-args
    assert context.messages_sent == ['```User Name\n------------------------------\n@fake-display-name-123```']
    assert result is True

def test_managed_roles_no_results():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild(roles=[fake_role])
    fake_user = FakeAuthor(roles=[fake_role])
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, VALID_BASIC_CONFIG, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context)
    assert not result

def test_managed_roles_basic_config():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                fake_role.id: {
                    'manages_roles': [
                        fake_role2.id,
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context)
    assert result[fake_role2] is False

def test_managed_roles_reject_list():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'rejected_roles_list': [
                    fake_role2.id
                ],
                fake_role.id: {
                    'manages_roles': [
                        fake_role2.id,
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context)
    assert not result

def test_managed_roles_with_self_service():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                fake_role.id: {
                    'manages_roles': [
                        fake_role2.id,
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context)
    assert result[fake_role2] is True

def test_managed_roles_with_self_service_but_turned_off():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='2343', name='fake role tres')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                fake_role.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context, exclude_self_service=True)
    assert fake_role2 not in result

def test_managed_roles_with_self_service_rejected_list():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='3456', name='fake role tres')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2, fake_role3])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                'rejected_roles_list': [
                    fake_role2.id,
                ],
                fake_role.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context)
    assert result[fake_role3] is False
    assert fake_role2 not in result

def test_managed_roles_basic_config_with_fake_ids():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    'fake-id3'
                ],
                fake_role.id: {
                    'manages_roles': [
                        fake_role2.id,
                        'fake-id2'
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = cog.get_managed_roles(context)
    assert len(list(result.keys())) == 1

@pytest.mark.asyncio
async def test_list_managed_no_required_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_managed(cog, FakeContext(author=fake_user)) #pylint:disable=too-many-function-args
    assert result == 'User "fake-display-name-123" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_list_managed_no_roles_to_list():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_managed(cog, FakeContext(author=fake_user)) #pylint:disable=too-many-function-args
    assert result == 'No roles found'

@pytest.mark.asyncio
async def test_list_managed_with_multiple_options():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='3456', name='fake role tres')
    fake_guild = FakeGuild(roles=[fake_role, fake_role2, fake_role3])
    fake_user = FakeAuthor(roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                fake_role.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }

    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(fake_guild=fake_guild, author=fake_user)
    result = await cog.role_managed(cog, context) #pylint:disable=too-many-function-args
    assert context.messages_sent == ['```Role Name                     || Control\n-------------------------------------------\n@fake role dos                || Self-Serve\n@fake role tres               || Full```']
    assert result is True

@pytest.mark.asyncio
async def test_role_add_no_required_perms():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_guild = FakeGuild()
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    result = await cog.role_add(cog, FakeContext(author=fake_user), inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == 'User "fake-display-name-123" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_add():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[fake_role])
    fake_user3 = FakeAuthor(id='2343', roles=[fake_role])
    fake_guild = FakeGuild(members=[fake_user, fake_user2, fake_user3], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    await cog.role_add(cog, FakeContext(author=fake_user, fake_guild=fake_guild), inputs=f'<@{fake_user2.id}> <@{fake_user3.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles
    assert fake_role3 in fake_user3.roles

@pytest.mark.asyncio
async def test_role_add_already_exists():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[fake_role, fake_role3])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_add(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles
    assert 'User "fake-display-name-123" already has role "fake role tres", skipping' in context.messages_sent

@pytest.mark.asyncio
async def test_role_no_required_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_add(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles
    assert 'User "fake-display-name-123" does not have required roles, skipping' in context.messages_sent


@pytest.mark.asyncio
async def test_role_non_required_role_butadmin_override():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'admin_override_role_list': [
                    fake_role2.id
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_add(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles

@pytest.mark.asyncio
async def test_role_add_in_reject_list():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'rejected_roles_list': [
                    fake_role3.id
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_add(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert 'Cannot add users to role "fake role tres", you do not manage role. Use `!role available` to see a list of roles you manage' in context.messages_sent

@pytest.mark.asyncio
async def test_role_add_admin_override():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'admin_override_role_list': [
                    fake_role2.id
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_add(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles

@pytest.mark.asyncio
async def test_role_add_invalid_inputs():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_guild = FakeGuild(members=[fake_user], roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_add(cog, context, inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == 'Unable to find users or role from input'

@pytest.mark.asyncio
async def test_role_add_cant_add_self_service_only_for_another_user_no_admin():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')
    fake_role3 = FakeRole(id='234', name='fake role tres')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='1234', roles=[fake_role])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_add(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role2.id}>') #pylint:disable=too-many-function-args
    assert result == 'Cannot add users to role "fake role dos", you do not manage role. Use `!role available` to see a list of roles you manage'

@pytest.mark.asyncio
async def test_role_add_with_valid_self_service():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='9876', name='fake role dos')

    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_guild = FakeGuild(members=[fake_user], roles=[fake_role, fake_role2])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_add(cog, context, inputs=f'<@{fake_user.id}> <@{fake_role2.id}>') #pylint:disable=too-many-function-args
    assert fake_role2 in fake_user.roles

@pytest.mark.asyncio
async def test_role_remove_no_required_role():
    fake_role = FakeRole(id='123456789', name='fake role')

    fake_user = FakeAuthor(id='987654321', roles=[])
    fake_guild = FakeGuild(members=[fake_user], roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_remove(cog, context, inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == 'User "fake-display-name-123" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_remove_invalid_input():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role])
    fake_guild = FakeGuild(members=[fake_user], roles=[fake_role])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    result = await cog.role_remove(cog, context, inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == 'Unable to find users or role from input'

@pytest.mark.asyncio
async def test_remove_user_valid():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_role3 = FakeRole(id='345', name='fake role tres')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='345', roles=[fake_role3])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles

@pytest.mark.asyncio
async def test_remove_user_doesnt_have_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_role3 = FakeRole(id='345', name='fake role tres')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='913813', roles=[])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles
    assert 'User "fake-display-name-123" does not have role "fake role tres", skipping' in context.messages_sent

@pytest.mark.asyncio
async def test_remove_user_no_perms():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_role3 = FakeRole(id='345', name='fake role tres')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='913813', roles=[fake_role3])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert 'Cannot remove users from role "fake role tres", you do not manage role. Use `!role available` to see a list of roles you manage' in context.messages_sent

@pytest.mark.asyncio
async def test_remove_user_no_perms_but_admin_override():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_role3 = FakeRole(id='345', name='fake role tres')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='913813', roles=[])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'admin_override_role_list': [
                    fake_role2.id
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles

@pytest.mark.asyncio
async def test_remove_self_from_role():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_guild = FakeGuild(members=[fake_user], roles=[fake_role, fake_role2])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user.id}> <@{fake_role2.id}>') #pylint:disable=too-many-function-args
    assert fake_role2 not in fake_user.roles

@pytest.mark.asyncio
async def test_remove_user_where_another_has_self_service():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_role3 = FakeRole(id='345', name='fake role tres')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='913813', roles=[fake_role3])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'required_roles_list': [
                    fake_role.id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3  in fake_user2.roles
    assert 'Cannot remove users from role "fake role tres", you do not manage role. Use `!role available` to see a list of roles you manage' in context.messages_sent

@pytest.mark.asyncio
async def test_remove_user_where_another_has_self_service_but_admin_override():
    fake_role = FakeRole(id='123456789', name='fake role')
    fake_role2 = FakeRole(id='234', name='fake role dos')
    fake_role3 = FakeRole(id='345', name='fake role tres')
    fake_user = FakeAuthor(id='987654321', roles=[fake_role, fake_role2])
    fake_user2 = FakeAuthor(id='913813', roles=[fake_role3])
    fake_guild = FakeGuild(members=[fake_user, fake_user2], roles=[fake_role, fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_guild.id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                'admin_override_role_list': [
                    fake_role.id,
                ]
            }
        }
    }
    fake_bot = fake_bot_yielder(guilds=[fake_guild])()
    cog = RoleAssignment(fake_bot, logging, config, None)
    context = FakeContext(author=fake_user, fake_guild=fake_guild)
    await cog.role_remove(cog, context, inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles
