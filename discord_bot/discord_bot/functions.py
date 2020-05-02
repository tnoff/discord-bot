import random
import re

from discord_bot.database import Server, User

ROLL_REGEX = '^(?P<rolls>\d+)?[dD](?P<sides>\d+) *(?P<operator>[+-])? *(?P<modifier>\d+)?'

def _log_message(ctx, logger, message):
    logger.info('Server "%s", invoked with command "%s", by user "%s", sending message "%s"',
                ctx.guild.name, ctx.command.name, ctx.author.name, message)

def hello(ctx, logger):
    message = 'Waddup %s' % ctx.author.name
    _log_message(ctx, logger, message)
    return True, message

def roll(ctx, logger, number):
    matcher = re.match(ROLL_REGEX, number)
    # First check if matches regex
    if not matcher:
        message = 'Invalid number given %s' % number
        _log_message(ctx, logger, message)
        return False, message
    try:
        sides = int(matcher.group('sides'))
        rolls = matcher.group('rolls')
        modifier = matcher.group('modifier')
        if rolls is None:
            rolls = 1
        else:
            rolls = int(rolls)
        if modifier is None:
            modifier = 0
        else:
            modifier = int(modifier)
    except ValueError:
        message = 'Non integer value given'
        _log_message(ctx, logger, message)
        return False, message

    total = 0
    for _ in range(rolls):
        total += random.randint(1, sides)
    if modifier:
        if matcher.group('operator') == '-':
            total = total - modifier
        elif matcher.group('operator') == '+':
            total = total + modifier

    message = f'{ctx.author.name} rolled a {total}'
    _log_message(ctx, logger, message)
    return True, message
    
def windows(ctx, logger):
    message = 'Install linux coward'
    _log_message(ctx, logger, message)
    return True, message

def planner_register(ctx, logger, db_session):
    # First create server entry
    server = db_session.query(Server).get(ctx.guild.id)
    if server:
        logger.info(f'Found server matching id {server.id}')
    else:
        server_args = {
            'id' : ctx.guild.id,
            'name' : ctx.guild.name,
        }
        logger.debug(f'Attempting to create server with args {server_args}')
        server_entry = Server(**server_args)
        db_session.add(server_entry)
        db_session.commit()
        logger.info(f'Created server with id {server_entry.id}')
    # Then check for user
    user = db_session.query(User).get(ctx.author.id)
    if user:
        logger.info(f'Found user matching id {user.id}')
    else:
        user_args = {
            'id' : ctx.author.id,
            'name' : ctx.author.name,
        }
        logger.debug(f'Attempting to create user with args {user_args}')
        user_entry = User(**user_args)
        db_session.add(user_entry)
        db_session.commit()
        logger.info(f'Created user with id {user_entry.id}')
    message = 'Successfully registered!'
    _log_message(ctx, logger, message)
    return True, message
