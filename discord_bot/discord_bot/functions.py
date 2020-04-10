import random
import re

from discord_bot.database import Server, User

ROLL_REGEX = '^d?(?P<number>[0-9]+)$'

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
    # Then check if valid number
    # If passes assume its a number
    number = matcher.group('number')
    number = int(number)
    if number < 2:
        message = 'Invalid number given %s' % number
        _log_message(ctx, logger, message)
        return False, message
    logger.debug("Getting random number between 1 and %s", number)
    random_num = random.randint(1, number)
    message = '%s rolled a %s' % (ctx.author.name, random_num)
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
