class DiscordBotException(Exception):
    '''
    Generic discord exception
    '''

class CogMissingRequiredArg(DiscordBotException):
    '''
    Cog Missing Required Arg
    '''

class ExitEarlyException(Exception):
    '''
    Exit early from tasks
    '''
