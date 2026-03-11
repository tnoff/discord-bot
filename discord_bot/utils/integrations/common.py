from dataclasses import dataclass, field

# RIP Twitter
TWITTER_VIDEO_PREFIX = 'https://x.com'
FXTWITTER_VIDEO_PREFIX = 'https://fxtwitter.com'

# Common Youtube Prefixes
YOUTUBE_SHORT_PREFIX = 'https://www.youtube.com/shorts/'
YOUTUBE_VIDEO_PREFIX = 'https://www.youtube.com/watch?v='

@dataclass
class CatalogItem:
    '''Individual Item'''
    search_string: str
    title: str = None

@dataclass
class CatalogResponse:
    '''Response from 3rd Party Catalog'''
    items: list[CatalogItem] = field(default_factory=list)
    collection_name: str = None
