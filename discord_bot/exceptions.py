class DiscordBotException(Exception):
    '''
    Generic discord exception
    '''

class CogMissingRequiredArg(DiscordBotException):
    '''
    Cog Missing Required Arg
    '''

class UnhandledColumnType(DiscordBotException):
    '''
    JSON encoding does not cover column type
    '''

class ExitEarlyException(Exception):
    '''
    Exit early from tasks
    '''
