import argparse

import asyncio
from discord import Client

from discord_bot.defaults import CONFIG_PATH_DEFAULT
from discord_bot.database import RoleAssignmentMessage, RoleAssignmentReaction
from discord_bot.utils import get_logger, load_args, get_db_session

EMOJI_MAPPING = {
    '\u0030\ufe0f\u20e3': ':zero:',
    '\u0031\ufe0f\u20e3': ':one:',
    '\u0032\ufe0f\u20e3': ':two:',
    '\u0033\ufe0f\u20e3': ':three:',
    '\u0034\ufe0f\u20e3': ':four:',
    '\u0035\ufe0f\u20e3': ':five:',
    '\u0036\ufe0f\u20e3': ':six:',
    '\u0037\ufe0f\u20e3': ':seven:',
    '\u0038\ufe0f\u20e3': ':eight:',
    '\u0039\ufe0f\u20e3': ':nine:',
}

def parse_args():
    '''
    Parse command line args
    '''
    parser = argparse.ArgumentParser(description='Discord CLI')
    parser.add_argument("--config-file", "-c", default=CONFIG_PATH_DEFAULT, help="Config file")
    parser.add_argument("--log-file", "-l",
                        help="Logging file")
    sub_parser = parser.add_subparsers(dest='command', help='Command')
    sub_parser.add_parser('check-role-assignment', help='Check role assignment messages')
    return parser.parse_args()

async def check_role_assignment(client, db_session, logger): #pylint:disable=too-many-locals
    '''
    Check for role assignments
    '''
    guild_roles = {}
    guild_cache = {}
    channel_cache = {}

    member_cache = {}

    for assignment_message in db_session.query(RoleAssignmentMessage).all():
        # Use cache for guild
        try:
            guild = guild_cache[assignment_message.guild_id]
        except KeyError:
            guild = await client.fetch_guild(assignment_message.guild_id)
            guild_cache[assignment_message.guild_id] = guild
        # Use cache for roles
        try:
            role_dict = guild_roles[guild.id]
        except KeyError:
            role_dict = {}
            for role in await guild.fetch_roles():
                role_dict[role.id] = role
            guild_roles[guild.id] = role_dict
        # Use cache for channel
        try:
            channel = channel_cache[assignment_message.channel_id]
        except KeyError:
            channel = await client.fetch_channel(assignment_message.channel_id)
            channel_cache[assignment_message.channel_id] = channel

        message = await channel.fetch_message(assignment_message.message_id)

        reaction_dict = {}
        for role_reaction in db_session.query(RoleAssignmentReaction).\
            filter(RoleAssignmentReaction.role_assignment_message_id == assignment_message.id):
            reaction_dict[role_reaction.emoji_name] = role_reaction.role_id

        for reaction in message.reactions:
            role = role_dict[reaction_dict[EMOJI_MAPPING[reaction.emoji]]]
            async for user in reaction.users():
                try:
                    member = member_cache[guild.id][user.id]
                except KeyError:
                    member = await guild.fetch_member(user.id)
                    member_cache.setdefault(guild.id, {})
                    member_cache[guild.id][user.id] = member
                if role.id not in [r.id for r in member.roles]:
                    await member.add_roles(role)
                    logger.info(f'Adding role {role.name} to user {user.name}')
                else:
                    logger.debug(f'User {user.name} already has role {role.name}')


async def real_main():
    '''
    Actual Main method
    '''
    settings = load_args(vars(parse_args()))

    # Setup vars
    logger = get_logger(__name__, settings['log_file'])
    # Setup database
    db_session = get_db_session(settings)
    client = Client()
    logger.debug(f'Logging in with token {settings["discord_token"]}')
    await client.login(token=settings['discord_token'])

    if settings['command'] == 'check-role-assignment':
        await check_role_assignment(client, db_session, logger)

    await client.close()

def main():
    '''
    Main loop
    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(real_main())
    loop.close()
