# Role Cog

A bot to help with discord role management. All discord roles are set in a hierarchical order, shown in a top to bottom view in the UI. Each role has a corresponding "rank" that matches with their position.

![](./images/role_order.png)


If one of these roles is given the "Manage Roles" command, then any user with that role can add/remove users to roles for all roles with a lower rank.

![](./images/manage_roles.png)

For most servers this probably isn't a real problem, but for some larger server setups this can become problematic. If you want to give certain roles permissions to *only* control certain roles "below" their rank this becomes not possible.

This cog allows you to set granular permissions for roles, so some roles can only control who is a member of certain other roles. It will require you to setup the bot with a role and certain permissions to do so.

## Intents

Note that to use all functions, this cog requires the `members` intent. You will not be allowed to start the cog unless this intent is added.

```
general:
  intents:
    - members
```


## Setup

For the cog to work as intended, you will need to setup the bot with a higher rank than all of the roles setup in its config. Its recommended that you turn off the "Manage Roles" permissions for all roles with a rank below the one the bot has.

The trick for how this all works is the bot will be the one adding/removing users from roles, but only on the request of one of the members via a command.

For example you may want the following roles set up with the following ranks:

```
# Higher in the list means a higher rank
- Administrators
- Bot
- Moderators
- GroupA-Admin
- GroupB-Admin
- GroupA
- GroupB
```

Here the Bot role (which the bot belongs to) has a role with a higher rank than all of the following roles.


## Role Commands

You can use the following commands to list/modify user roles:

```
!role list # list all roles
!role available # list roles user controls
!role add @user1 @user2 @role # add user to role
!role remove @user1 @user2 @role # remove user from role
```

## Config w/ Examples

Various config options with an example of what it allows

### Role Manages Rules

You can give one role permissions to add/remove users from another role:

```
role:
  <discord-server-id>:
    <manager-role-id>:
      manages_roles:
        - <managed-role-id>
```

Take these example roles:

```
# Higher in the list means a higher rank
- Administrators
- Bot
- Moderators
- GroupA-Admin
- GroupB-Admin
- GroupA
- GroupB
```

Say you want to give the `GroupA-Admin` permissions to add/remove users from the `GroupA` role, but not permissions to add/remove users from any of the other roles such as `GroupB` or `GroupB-Admin`. As we said before since the `GroupA-Admin` role has a higher rank than these `GroupB` roles, if you gave this role "Manage Roles" permissions it would implicitly have permissions to add/remove users from those `GroupB` rules.

But if you were to give the Bot role the "Manage Roles" permissions, and then update the config to include

```
role:
  <discord-server-id>:
    GroupA-Admin.role.id:
      manages_roles:
        - GroupA.role.id
```

A user with the `GroupA-Admin` role could then add/remove users from the `GroupA` role via the `!role add` and `!role remove` commands, where the Bot would then run the action of modifying those users roles.

You can also give users in a role permission to add/remove users from the same role. So for example you can give users with the `GroupA-Admin` role permission to add/remove users from the `GroupA-Admin` and `GroupA` rules if you wish.

```
role:
  <discord-server-id>:
    GroupA-Admin.role.id:
      manages_roles:
        - GroupA.role.id
        - GroupA-Admin.role.id
```

## Required Roles

You can set Required Roles that a user must have before they can be added to any other role. This is useful for servers where you have a role that users get onboarded to after they agree to a Code of Conduct.

With this setting you can ensure that you don't allow anyone to add users to a given role until after they are given this basic role.

To setup a required role use the following config:

```
role:
  <discord-server-id>:
    required_roles_list:
      - <required-role-id>
```

Say you have a role named `Member` given to users after they onboard to your server. You can require that before anyone adds this user to another role, say `GroupA`, that they are required to first have this role.

```
role:
  <discord-server-id>:
    required_roles_list:
      - Member.role.id
    GroupA-Admin.role.id:
      manages_roles:
        - GroupA.role.id
```

Note that the required role setting is checked on `!role add` commands but not on `!role remove` commands.

## Admin Override Rules

You can designate certain roles as admins so they can override the required role checks. This effectively gives these roles permissions to add or remove any user they want from any other roles (with a rank below the Bot role).

To setup an admin override role use this config:

```
role:
  <discord-server-id>:
    admin_override_role_list:
      - <admin-role-id>
```

Say you have a role such as `Moderator` that you want to be able to add/remove users from any role (with a rank lower than the bot role). Add this role id to the config and you can bypass any checks with the required user role.

```
role:
  <discord-server-id>:
    admin_override_role_list:
      - Moderator.role.id
```

## Self Service Roles

You may want users to be able to add themselves to a certain role. This is pretty common in scenarios where you want to add users to groups for notifications or even opt-in channels. You can designate roles as Self Service so users can add themselves with the `!role add` command or remove themselves with the `!role remove` command.

To setup self service roles use this config:

```
role:
  <discord-server-id>:
    self_service_role_list:
      - <self-service-role-id>
```

Say you have a role such as `Baseball-Fans` that you want users to be able to add themselves to. You can setup the following config

```
role:
  <discord-server-id>:
    self_service_role_list:
      - Baseball-Fans.role.id
```

Note that if Required Roles are setup, users will need to have those required roles before they can add themselves to a Self Service Role. Also users will not be able to add different users to the self service role unless given explicit permission to do so via a `manage_roles` setting.

## Rejected Roles

You can setup Rejected Roles that where the Bot will not attempt to add, remove, or list the role. This is useful for certain types of admin roles that you want to make sure no one wants to add users to on accident.

To setup rejected roles use this config:

```
role:
  <discord-server-id>:
    rejected_roles_list:
      - <rejected-role-id>
```

Note that if a user is desginated an admin via the Admin Override role list, then the admin override will overrule the reject roles setting on `!role add` and `!role remove` commands.