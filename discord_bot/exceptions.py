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

class YoutubeMusicRetryException(Exception):
    '''
    Retry youtube music

    Lives here, not next to YoutubeMusicClient, because this module imports
    nothing: utils/integrations/youtube_music.py imports ytmusicapi at module
    scope, so every module that only wanted to catch this exception was dragging
    the ytmusicapi dependency into its process. Same split, and the same reason,
    as ClearGuildResult and CheckoutResult moving to types/. It re-exports from
    its old home, so existing imports keep working.
    '''
