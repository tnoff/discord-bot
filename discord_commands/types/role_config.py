'''
The `role` config the role commands read (docs/role.md), validated into integer-keyed settings.
'''
from pydantic import BaseModel, Field, ValidationError as PydanticValidationError


class RoleManagementConfig(BaseModel):
    '''Role management configuration'''
    manages_roles: list[int] = Field(min_length=1)

class RoleServerConfig(BaseModel):
    '''Per-server role configuration'''
    rejected_roles_list: list[int] = Field(default_factory=list)
    required_roles_list: list[int] = Field(default_factory=list)
    admin_override_role_list: list[int] = Field(default_factory=list)
    self_service_role_list: list[int] = Field(default_factory=list)

class RoleConfig(BaseModel):
    '''Top-level role cog configuration - validates server and role configs'''
    model_config = {"extra": "allow"}

    @classmethod
    def model_validate(cls, obj):  # pylint: disable=arguments-differ
        '''Validate config and convert keys to integers'''
        if not isinstance(obj, dict):
            return super().model_validate(obj)

        # Step 1: Convert integer keys to strings for Pydantic validation
        str_keyed = {}
        for server_id, server_config in obj.items():
            str_key = str(server_id)

            if isinstance(server_config, dict):
                # Validate nested structure using the appropriate model
                validated_config = {}
                for k, v in server_config.items():
                    str_k = str(k)

                    # If this is a dict with 'manages_roles', validate as RoleManagementConfig
                    if isinstance(v, dict) and 'manages_roles' in v:
                        validated_config[str_k] = RoleManagementConfig(**v).model_dump()
                    # Otherwise validate as a field value
                    elif str_k in ['rejected_roles_list', 'required_roles_list',
                                   'admin_override_role_list', 'self_service_role_list']:
                        validated_config[str_k] = v
                    # If it's a nested dict (role ID -> management config)
                    elif isinstance(v, dict):
                        validated_config[str_k] = RoleManagementConfig(**v).model_dump()
                    else:
                        validated_config[str_k] = v

                # Validate the full server config structure
                try:
                    RoleServerConfig(**{k: v for k, v in validated_config.items()
                                       if k in ['rejected_roles_list', 'required_roles_list',
                                               'admin_override_role_list', 'self_service_role_list']})
                except PydanticValidationError:
                    pass  # Server config might have additional role management entries

                str_keyed[str_key] = validated_config
            else:
                str_keyed[str_key] = server_config

        # Step 2: Create instance with string keys (Pydantic requirement)
        instance = super().model_validate(str_keyed)

        # Step 3: Convert back to integer keys and store
        int_keyed_config = {}
        for k, v in instance.__pydantic_extra__.items():
            # Convert server ID key to int
            int_key = int(k) if k.isdigit() else k

            # Convert nested role ID keys to int
            if isinstance(v, dict):
                int_v = {}
                for vk, vv in v.items():
                    int_vk = int(vk) if isinstance(vk, str) and vk.isdigit() else vk
                    int_v[int_vk] = vv
                int_keyed_config[int_key] = int_v
            else:
                int_keyed_config[int_key] = v

        # Store in private attribute
        object.__setattr__(instance, '_int_keyed_config', int_keyed_config)

        return instance

    def model_dump(self, **kwargs):  # pylint: disable=unused-argument
        '''Return the integer-keyed config'''
        return object.__getattribute__(self, '_int_keyed_config')
