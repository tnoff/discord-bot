import pytest

from discord_bot.cogs.role import RoleAssignment
from discord_bot.exceptions import CogMissingRequiredArg

from tests.helpers import fake_context #pylint:disable=unused-import
from tests.helpers import FakeAuthor, FakeRole


BASE_GENERAL_CONFIG = {
    'general': {
        'include': {
            'role': True
        }
    },
}

VALID_BASIC_CONFIG = {
    'role': {
        'foo_bar': {
            'bar_foo': []
        }
    }
} |  BASE_GENERAL_CONFIG

def test_role_no_enabled(fake_context):  #pylint:disable=redefined-outer-name
    with pytest.raises(CogMissingRequiredArg) as exc:
        RoleAssignment(fake_context['bot'], {}, None)
    assert 'Role not enabled' in str(exc.value)

def test_clean_string(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    assert 'foo' == cog.clean_input('foo')
    assert cog.clean_input('“foo“') == 'foo'

def test_rejected_roles_no_data(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    assert [] == cog.get_rejected_roles_list(fake_context['context'])

def test_required_roles_no_data(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    assert [] == cog.get_required_roles(fake_context['context'])

def test_override_roles_no_data(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    assert [] == cog.get_override_role(fake_context['context'])

def test_self_service_roles_no_data(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    assert [] == cog.get_self_service_roles(fake_context['context'])

def test_rejected_roles_with_data(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'rejected_roles_list': [
                    'reject-role-1234',
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    assert ['reject-role-1234'] == cog.get_rejected_roles_list(fake_context['context'])

def test_required_roles_with_data(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    'required-role-1234',
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    assert ['required-role-1234'] == cog.get_required_roles(fake_context['context'])

def test_override_roles_with_data(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'admin_override_role_list': [
                    'admin-role-1234',
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    assert ['admin-role-1234'] == cog.get_override_role(fake_context['context'])

def test_self_service_roles_with_data(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    'self-service-role-1234',
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    assert ['self-service-role-1234'] == cog.get_self_service_roles(fake_context['context'])

@pytest.mark.asyncio
async def test_get_user_invalid_input(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = await cog.get_user(fake_context['context'], 'foo bar')
    assert result is None

@pytest.mark.asyncio
async def test_get_user_valid_user(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = await cog.get_user(fake_context['context'], f'<@{fake_context["author"].id}>')
    assert result.id == fake_context['author'].id

@pytest.mark.asyncio
async def test_get_user_not_found(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    # Add one to current user id
    result = await cog.get_user(fake_context['context'], f'<@{fake_context["author"].id + 123}>')
    assert result is None

def test_get_role_valid_id(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.get_role(fake_context['context'], fake_context['author'].roles[0].id)
    assert result.id == fake_context['author'].roles[0].id

def test_get_role_valid_id_but_no_roles(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.get_role(fake_context['context'], fake_context['author'].roles[0].id + 123)
    assert result is None

def test_get_role_valid_name(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.get_role(fake_context['context'], fake_context['author'].roles[0].name)
    assert result.id == fake_context['author'].roles[0].id

def test_get_role_valid_name_with_space(fake_context):  #pylint:disable=redefined-outer-name
    new_role = FakeRole()
    fake_context['guild'].roles.append(new_role)
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.get_role(fake_context['context'], new_role.name)
    assert result.id == new_role.id

def test_get_role_invalid_name(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.get_role(fake_context['context'], 'another-fake-role')
    assert result is None

@pytest.mark.asyncio
async def test_get_user_roles_with_spaces_for_role(fake_context):  #pylint:disable=redefined-outer-name
    new_role = FakeRole()
    fake_context['guild'].roles.append(new_role)
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = await cog.get_user_or_role(fake_context['context'], f'<@{fake_context["author"].id}> {new_role.name}')
    assert result[0][0].id == fake_context['author'].id
    assert result[1].id == new_role.id

def test_required_roles_with_no_config(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.check_required_roles(fake_context['context'])
    assert result is True

def test_required_roles_just_author(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.check_required_roles(fake_context['context'])
    assert result is True

def test_required_roles_just_author_with_invalid(fake_context):  #pylint:disable=redefined-outer-name
    '''
    User which called function is valid
    '''
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    '1234'
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.check_required_roles(fake_context['context'])
    assert result is False

def test_required_roles_author_user_but_author_invalid(fake_context):  #pylint:disable=redefined-outer-name
    '''
    User which called function is valid, but user modifying is not
    '''
    # No roles
    fake_user2 = FakeAuthor()
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.check_required_roles(fake_context['context'], user=fake_user2)
    assert result is False

def test_required_roles_both(fake_context):  #pylint:disable=redefined-outer-name
    '''
    Both user making call and user being modified have required roles
    '''
    fake_user2 = FakeAuthor(roles=[fake_context['author'].roles[0]])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.check_required_roles(fake_context['context'], user=fake_user2)
    assert result is True

def test_check_override_role(fake_context):  #pylint:disable=redefined-outer-name
    '''
    User override exists
    '''
    config = {
        'role': {
            fake_context['guild'].id: {
                'admin_override_role_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.check_override_role(fake_context['context'])
    assert result is True

def test_check_override_role_with_no_role(fake_context):  #pylint:disable=redefined-outer-name
    '''
    User does not have override
    '''
    config = {
        'role': {
            fake_context['guild'].id: {
                'admin_override_role_list': [
                    fake_context['author'].roles[0].id + 1234,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.check_override_role(fake_context['context'])
    assert result is False

@pytest.mark.asyncio
async def test_role_list_with_no_roles(fake_context):  #pylint:disable=redefined-outer-name
    '''
    Role list with no active roles
    '''
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'rejected_roles_list': [
                    fake_context['author'].roles[0].id
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert result == 'No roles found'

@pytest.mark.asyncio
async def test_role_list_with_invalid_role(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id + 1234,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert result == f'User "{fake_context["author"].display_name}" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_list_basic_return(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_context['guild'].roles.append(fake_role2)
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert fake_context['context'].messages_sent == [f'```Role Name\n---------\n@{fake_context["author"].roles[0].name}\n@{fake_role2.name}```']
    assert result is True

@pytest.mark.asyncio
async def test_role_list_with_rejected_role(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'rejected_roles_list': [
                    fake_role2.id,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert fake_context['context'].messages_sent == [f'```Role Name\n---------\n@{fake_context["author"].roles[0].name}```']
    assert result is True

@pytest.mark.asyncio
async def test_role_list_users_invalid_role(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list_users(cog, fake_context['context'], role_input='foo bar') #pylint:disable=too-many-function-args
    assert result == f'User "{fake_context["author"].display_name}" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_list_users_role_name_invalid(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list_users(cog, fake_context['context'], role_input='foo bar') #pylint:disable=too-many-function-args
    assert result == 'Unable to find role "foo bar"'

@pytest.mark.asyncio
async def test_role_list_users_with_no_users(fake_context):  #pylint:disable=redefined-outer-name
    fake_role = FakeRole()
    fake_context['guild'].roles.append(fake_role)
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list_users(cog, fake_context['context'], role_input=f'{fake_role.name}') #pylint:disable=too-many-function-args
    assert result == f'No users found for role "{fake_role.name}"'

@pytest.mark.asyncio
async def test_role_list_users_in_reject_roles(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'rejected_roles_list': [
                    fake_role2.id,
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list_users(cog, fake_context['context'], role_input=f'{fake_role2.name}') #pylint:disable=too-many-function-args
    assert result == f'Unable to find role "{fake_role2.name}"'

@pytest.mark.asyncio
async def test_role_list_users_with_users(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_list_users(cog, fake_context['context'], role_input=f'{fake_context["author"].roles[0].name}') #pylint:disable=too-many-function-args
    assert fake_context['context'].messages_sent == [f'```User Name\n---------\n@{fake_context["author"].display_name}```']
    assert result is True

def test_managed_roles_no_results(fake_context):  #pylint:disable=redefined-outer-name
    cog = RoleAssignment(fake_context['bot'], VALID_BASIC_CONFIG, None)
    result = cog.get_managed_roles(fake_context['context'])
    assert not result

def test_managed_roles_basic_config(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_context['guild'].roles.append(fake_role2)
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role2.id,
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.get_managed_roles(fake_context['context'])
    assert result[fake_role2] is False

def test_managed_roles_reject_list(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_context['guild'].roles.append(fake_role2)
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'rejected_roles_list': [
                    fake_role2.id
                ],
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role2.id,
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.get_managed_roles(fake_context['context'])
    assert not result

def test_managed_roles_with_self_service(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_context['guild'].roles.append(fake_role2)
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role2.id,
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.get_managed_roles(fake_context['context'])
    assert result[fake_role2] is True

def test_managed_roles_with_self_service_but_turned_off(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.get_managed_roles(fake_context['context'], exclude_self_service=True)
    assert fake_role2 not in result

def test_managed_roles_with_self_service_rejected_list(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                'rejected_roles_list': [
                    fake_role2.id,
                ],
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.get_managed_roles(fake_context['context'])
    assert result[fake_role3] is False
    assert fake_role2 not in result

def test_managed_roles_basic_config_with_fake_ids(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    fake_role3.id
                ],
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role2.id,
                        fake_role3.id
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = cog.get_managed_roles(fake_context['context'])
    # Should have 2 roles: fake_role2 (managed) and fake_role3 (self-service)
    assert len(list(result.keys())) == 2

@pytest.mark.asyncio
async def test_list_managed_no_required_role(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_managed(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert result == f'User "{fake_context["author"].display_name}" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_list_managed_no_roles_to_list(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    }
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_managed(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert result == 'No roles found'

@pytest.mark.asyncio
async def test_list_managed_with_multiple_options(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                fake_context['author'].roles[0].id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    }

    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_managed(cog, fake_context['context']) #pylint:disable=too-many-function-args
    assert fake_context['context'].messages_sent == [f'```Role Name                     || Control\n----------------------------------------\n@{fake_role3.name}                 || Full\n@{fake_role2.name}                 || Self-Serve```']
    assert result is True

@pytest.mark.asyncio
async def test_role_add_no_required_perms(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    '9876'
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_add(cog, fake_context['context'], inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == f'User "{fake_context["author"].display_name}" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_add(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_user2 = FakeAuthor()
    fake_user3 = FakeAuthor()

    # Set up relationships
    fake_context['author'].roles.append(fake_role2)  # Main user has both roles
    fake_user2.roles = [fake_context['author'].roles[0]]  # User2 has base role
    fake_user3.roles = [fake_context['author'].roles[0]]  # User3 has base role
    fake_context['guild'].members.extend([fake_user2, fake_user3])
    fake_context['guild'].roles.extend([fake_role2, fake_role3])

    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_user3.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles
    assert fake_role3 in fake_user3.roles

@pytest.mark.asyncio
async def test_role_add_already_exists(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_user2 = FakeAuthor()

    # Set up relationships
    fake_context['author'].roles.append(fake_role2)  # Main user has both roles
    fake_user2.roles = [fake_context['author'].roles[0], fake_role3]  # User2 already has the target role
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])

    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles
    assert f'User "{fake_user2.display_name}" already has role "{fake_role3.name}", skipping' in fake_context['context'].messages_sent

@pytest.mark.asyncio
async def test_role_no_required_role(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_user2 = FakeAuthor()  # No roles

    # Set up relationships
    fake_context['author'].roles.append(fake_role2)  # Main user has both roles
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])

    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                },
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles
    assert f'User "{fake_user2.display_name}" does not have required roles, skipping' in fake_context['context'].messages_sent


@pytest.mark.asyncio
async def test_role_non_required_role_butadmin_override(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()

    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor()
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
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
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles

@pytest.mark.asyncio
async def test_role_add_in_reject_list(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor()
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
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
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert f'Cannot add users to role "{fake_role3.name}", you do not manage role. Use `!role available` to see a list of roles you manage' in fake_context['context'].messages_sent

@pytest.mark.asyncio
async def test_role_add_admin_override(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_user2 = FakeAuthor()

    fake_context['author'].roles.append(fake_role2)
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])

    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'admin_override_role_list': [
                    fake_role2.id
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 in fake_user2.roles

@pytest.mark.asyncio
async def test_role_add_invalid_inputs(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_add(cog, fake_context['context'], inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == 'Unable to find users or role from input'

@pytest.mark.asyncio
async def test_role_add_cant_add_self_service_only_for_another_user_no_admin(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()

    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor()
    fake_user2.roles.append(fake_context['author'].roles[0])
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    fake_context['guild'].members.append(fake_user2)
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role2.id}>') #pylint:disable=too-many-function-args
    assert result == f'Cannot add users to role "{fake_role2.name}", you do not manage role. Use `!role available` to see a list of roles you manage'

@pytest.mark.asyncio
async def test_role_add_with_valid_self_service(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_context['guild'].roles.append(fake_role2)
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_add(cog, fake_context['context'], inputs=f'<@{fake_context["author"].id}> <@{fake_role2.id}>') #pylint:disable=too-many-function-args
    assert fake_role2 in fake_context['author'].roles

@pytest.mark.asyncio
async def test_role_remove_no_required_role(fake_context):  #pylint:disable=redefined-outer-name
    # Clear the author's roles to simulate no required role
    fake_context['author'].roles = []
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    'some-role-id',
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_remove(cog, fake_context['context'], inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == f'User "{fake_context["author"].display_name}" does not have required roles, skipping'

@pytest.mark.asyncio
async def test_role_remove_invalid_input(fake_context):  #pylint:disable=redefined-outer-name
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    result = await cog.role_remove(cog, fake_context['context'], inputs='foo bar') #pylint:disable=too-many-function-args
    assert result == 'Unable to find users or role from input'

@pytest.mark.asyncio
async def test_remove_user_valid(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor(roles=[fake_role3])
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles

@pytest.mark.asyncio
async def test_remove_user_doesnt_have_role(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor(roles=[fake_role2])
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                fake_role2.id: {
                    'manages_roles': [
                        fake_role3.id,
                    ]
                }
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles
    assert f'User "{fake_user2.display_name}" does not have role "{fake_role3.name}", skipping' in fake_context['context'].messages_sent

@pytest.mark.asyncio
async def test_remove_user_no_perms(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor(roles=[fake_role3])
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert f'Cannot remove users from role "{fake_role3.name}", you do not manage role. Use `!role available` to see a list of roles you manage' in fake_context['context'].messages_sent

@pytest.mark.asyncio
async def test_remove_user_no_perms_but_admin_override(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor()
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'admin_override_role_list': [
                    fake_role2.id
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles

@pytest.mark.asyncio
async def test_remove_self_from_role(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_context['guild'].roles.append(fake_role2)
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_context["author"].id}> <@{fake_role2.id}>') #pylint:disable=too-many-function-args
    assert fake_role2 not in fake_context['author'].roles

@pytest.mark.asyncio
async def test_remove_user_where_another_has_self_service(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor(roles=[fake_role3])
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'role': {
            fake_context['guild'].id: {
                'required_roles_list': [
                    fake_context['author'].roles[0].id,
                ],
                'self_service_role_list': [
                    fake_role2.id
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3  in fake_user2.roles
    assert f'Cannot remove users from role "{fake_role3.name}", you do not manage role. Use `!role available` to see a list of roles you manage' in fake_context['context'].messages_sent

@pytest.mark.asyncio
async def test_remove_user_where_another_has_self_service_but_admin_override(fake_context):  #pylint:disable=redefined-outer-name
    fake_role2 = FakeRole()
    fake_role3 = FakeRole()
    fake_context['author'].roles.append(fake_role2)
    fake_user2 = FakeAuthor(roles=[fake_role3])
    fake_context['guild'].members.append(fake_user2)
    fake_context['guild'].roles.extend([fake_role2, fake_role3])
    config = {
        'general': {
            'include': {
                'role': True
            }
        },
        'role': {
            fake_context['guild'].id: {
                'self_service_role_list': [
                    fake_role2.id
                ],
                'admin_override_role_list': [
                    fake_context['author'].roles[0].id,
                ]
            }
        }
    } | BASE_GENERAL_CONFIG
    cog = RoleAssignment(fake_context['bot'], config, None)
    await cog.role_remove(cog, fake_context['context'], inputs=f'<@{fake_user2.id}> <@{fake_role3.id}>') #pylint:disable=too-many-function-args
    assert fake_role3 not in fake_user2.roles
