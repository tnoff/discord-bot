import pytest
from pydantic import ValidationError

from discord_commands.types.role_config import RoleConfig


def test_an_existing_config_validates_again():
    existing = RoleConfig.model_validate({})
    assert RoleConfig.model_validate(existing) is not None


def test_ids_come_back_as_integers():
    raw = {
        11: {
            'rejected_roles_list': [1],
            'required_roles_list': [2],
            'admin_override_role_list': [3],
            'self_service_role_list': [4],
            100: {'manages_roles': [200]},
        },
    }
    assert RoleConfig.model_validate(raw).model_dump() == raw


def test_string_ids_are_read_as_integers():
    parsed = RoleConfig.model_validate({'11': {'100': {'manages_roles': [200]}}}).model_dump()
    assert parsed == {11: {100: {'manages_roles': [200]}}}


def test_a_role_that_manages_nothing_is_refused():
    with pytest.raises(ValidationError):
        RoleConfig.model_validate({11: {100: {'manages_roles': []}}})


def test_a_nested_entry_without_manages_roles_is_refused():
    with pytest.raises(ValidationError):
        RoleConfig.model_validate({999: {12345: {'some_key': 'some_value'}}})


def test_a_server_config_that_is_not_a_dict_is_kept_as_is():
    assert RoleConfig.model_validate({999: 'not_a_dict'}).model_dump() == {999: 'not_a_dict'}


def test_a_server_list_that_is_not_a_list_does_not_stop_the_other_entries():
    '''The per-server list shape is checked loosely; only role management entries are strict.'''
    parsed = RoleConfig.model_validate({11: {'required_roles_list': 'oops', 100: {'manages_roles': [200]}}})
    assert parsed.model_dump()[11][100] == {'manages_roles': [200]}


def test_a_scalar_value_beside_the_lists_is_kept():
    assert RoleConfig.model_validate({11: {'note': 'x'}}).model_dump() == {11: {'note': 'x'}}


def test_a_config_that_is_not_a_dict_goes_to_pydantic():
    with pytest.raises(ValidationError):
        RoleConfig.model_validate('nope')
