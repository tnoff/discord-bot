ROLL_REGEX = '^d?(?P<number>[0-9]+)$'

def _log_message(ctx, logger, message):
    logger.info('Server %s, invoked with command %s, sending message "%s"',
                ctx.guild.name, ctx.command.name, message)

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
    random_num = random.randint(1, number)
    message = '%s rolled a %s' % (ctx.author.name, random_num)
    _log_message(ctx, logger, message)
    return True, message
    
def windows(ctx):
    message = 'Install linux coward'
    _log_message(ctx, logger, message)
    return True, message
