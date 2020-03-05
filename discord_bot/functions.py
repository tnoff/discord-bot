import random
import re

ROLL_REGEX = '^d?(?P<number>[0-9]+)$'

def _log_message(ctx, logger, message):
    logger.info('Server "%s", invoked with command "%s", by user "%s", sending message "%s"',
                ctx.guild.name, ctx.command.name, ctx.author.name, message)

def help(ctx, logger):
    message = '''Possible commands
    !hello - Say hello to the bot
    !roll [d]?[number] - Get a random number between 1 and the number given
    !windows - Get an inspirational note about your operating system
    '''
    _log_message(ctx, logger, message)
    return True, message

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
