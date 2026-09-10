'''
The media-search seam: the two provider-expansion routes the search pod serves
and the bot calls.

Imported by `servers/media_search_server.py` and
`clients/http_media_search_client.py` — fanout 2 (bot, search), the smallest of
the five seams.

**Two servers share the `/search` space and they are different seams.** This one
serves `/search/spotify` and `/search/youtube`; `YoutubeMusicSearchHttpServer`
serves `/search/ytmusic` from the queue-worker registry. Both run in the SAME
search pod behind a `CompositeHttpServer`, so the pod's advertisement lists
routes from two registries. Adding a route here that starts `/search/ytmusic`
would collide at startup rather than quietly shadow, which is the behaviour worth
having.
'''
from discord_bot.routes.route import Route, collect

# Kept as a named constant because both sides derive route names from it, and the
# client's public methods are named after the suffixes.
ROUTE_PREFIX = '/search'

SPOTIFY = Route('POST', f'{ROUTE_PREFIX}/spotify')
YOUTUBE = Route('POST', f'{ROUTE_PREFIX}/youtube')

ALL = collect(globals())
