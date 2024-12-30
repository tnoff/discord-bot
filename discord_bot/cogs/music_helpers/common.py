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
    SPOTIFY = 'spotify'
    DIRECT = 'direct'
    SEARCH = 'search'
    OTHER = 'other'
