from enum import Enum

# We only care about the following data in the yt-dlp dict
YT_DLP_KEYS = ['id', 'title', 'webpage_url', 'uploader', 'duration', 'extractor']

# RIP Twitter
TWITTER_VIDEO_PREFIX = 'https://x.com'
FXTWITTER_VIDEO_PREFIX = 'https://fxtwitter.com'

# Common Youtube Prefixes
YOUTUBE_SHORT_PREFIX = 'https://www.youtube.com/shorts/'
YOUTUBE_VIDEO_PREFIX = 'https://www.youtube.com/watch?v='

class SearchType(Enum):
    '''
    Search Types Supported
    '''
    SPOTIFY = 'spotify' # Spotify url was passed, these go to youtube eventually
    YOUTUBE = 'youtube' # Youtube url was passed
    DIRECT = 'direct' # Direct url for non-youtube passed
    SEARCH = 'search' # Search passed, goes to youtube
    OTHER = 'other' # Grouped searches usually

class StorageOptions(Enum):
    '''
    Storage options
    '''
    S3 = 's3'

class MediaRequestLifecycleStage(Enum):
    '''
    Lifecycle of a media request through the system
    '''
    QUEUED = 'queued'
    IN_PROGRESS = 'in_progress'
    FAILED = 'failed'
    COMPLETED = 'completed'
    DISCARDED = 'discarded'

class MessageType(Enum):
    '''
    Types of messages queue returns
    '''
    MULTIPLE_MUTABLE = 'multiple_mutable'
    SINGLE_IMMUTABLE = 'single_immutable'

class MultipleMutableType(Enum):
    '''
    Message Multiple Types
    '''
    PLAY_ORDER = 'play_order'
    REQUEST_BUNDLE = 'request_bundle'
    SEARCH = 'search'
