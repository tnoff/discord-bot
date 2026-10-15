from discord.ext.commands import Bot, Context, group
from pydantic import BaseModel, Field, ValidationError as PydanticValidationError

from discord_core.exceptions import CogMissingRequiredArg

from discord_core.clients.dispatch_client_base import DispatchClientBase

from discord_gateway.cogs.common import CogHelperBase, envelope_from_ctx
from discord_gateway.cogs.role_gateway import CogRoleGateway
from discord_gateway.command_logic.role import RoleCommands
from discord_gateway.utils.otel_command import command_wrapper

# Pydantic config models
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

class RoleAssignment(CogHelperBase):
    '''
    Class that can add roles in more managed fashion
    '''
    def __init__(self, bot: Bot, settings: dict, dispatcher: DispatchClientBase,
                 _stores: object = None, redis_manager=None):
        if not settings.get('general', {}).get('include', {}).get('role', False):
            raise CogMissingRequiredArg('Role not enabled')
        if not bot.intents.members:
            raise CogMissingRequiredArg('"members" intents required to run role commands')
        super().__init__(bot, settings, dispatcher, None,
                         settings_prefix='role', config_model=RoleConfig,
                         redis_manager=redis_manager)
        # Use validated config with integer keys
        self.settings = self.config.model_dump()

    @property
    def logic(self) -> RoleCommands:
        '''The role command logic, over the in-process guild gateway.'''
        return RoleCommands(self.dispatch_message, CogRoleGateway(self), self.settings)

    @group(name='role', invoke_without_command=False)
    async def role(self, ctx: Context):
        '''
        Role functions.
        '''
        if ctx.invoked_subcommand is None:
            await self.dispatch_message(ctx.guild.id, ctx.channel.id,'Invalid sub command passed...')

    @role.command(name='list')
    @command_wrapper
    async def role_list(self, ctx: Context):
        '''
        List all roles within the server
        '''
        return await self.logic.role_list(envelope_from_ctx(ctx))

    @role.command(name='users')
    @command_wrapper
    async def role_list_users(self, ctx: Context, *, role_input: str):
        '''
        List all users with a specific role
        '''
        return await self.logic.role_list_users(envelope_from_ctx(ctx), role_input)

    @role.command(name='available')
    @command_wrapper
    async def role_managed(self, ctx: Context):
        '''
        List all roles in the server that are available to your user to manage
        '''
        return await self.logic.role_managed(envelope_from_ctx(ctx))

    @role.command(name='add')
    @command_wrapper
    async def role_add(self, ctx: Context, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
                Role input must be last entered
        '''
        return await self.logic.role_add(envelope_from_ctx(ctx), inputs)

    @role.command(name='remove')
    @command_wrapper
    async def role_remove(self, ctx: Context, *, inputs: str):
        '''
        Add user to role that is available to you

        inputs: Either @mention of user, @mention of role, or role name
            Role input must be last entered
        '''
        return await self.logic.role_remove(envelope_from_ctx(ctx), inputs)
