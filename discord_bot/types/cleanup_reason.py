from enum import Enum


class CleanupReason(str, Enum):
    '''Reason a guild cleanup was triggered, controls cleanup behaviour.'''
    USER_STOP      = 'user_stop'      # !stop command — user explicitly ended playback
    QUEUE_TIMEOUT  = 'queue_timeout'  # player timed out waiting for the next item
    VOICE_INACTIVE = 'voice_inactive' # channel emptied while music was playing
    BOT_SHUTDOWN   = 'bot_shutdown'   # cog_unload — full process teardown
