import random
import re

ROLL_REGEX = '^((?P<rolls>\d+)[dD])?(?P<sides>\d+) *(?P<operator>[+-])? *(?P<modifier>\d+)?'

def _log_message(ctx, logger, message):
    logger.info('Server "%s", invoked with command "%s", by user "%s", sending message "%s"',
                ctx.guild.name, ctx.command.name, ctx.author.name, message)

def hello(ctx, logger):
    '''
    Say wassup
    '''
    message = 'Waddup %s' % ctx.author.name
    _log_message(ctx, logger, message)
    return True, message

def roll(ctx, logger, number):
    '''
    Roll some dice
    '''
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
    '''
    Inspirational note about your os
    '''
    message = 'Install linux coward'
    _log_message(ctx, logger, message)
    return True, message
