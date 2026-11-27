'''
The media-search seam's contract: expanding a Spotify or YouTube URL into a list of searches.

A provider that says "no such playlist" is an answer, so it comes back as a 200 with ``error``
set; only a malformed request is a 4xx.

The request bodies and the response are `discord_core` types, which the client builds from and the server reads,
so this table only adds what core does not hold: the summary and which statuses a route may answer.

Both ends read this: the search pod's server is proved against it (tests/routes/specs/media_search.py runs
every reply), and the commands pod's client is tested against a stub that serves it
(discord_commands/tests/clients/test_http_media_search_client.py), so neither needs the other pod.
'''
from discord_core.routes import media_search as media_search_routes
from discord_core.types.media_search import MediaSearchResponse, SpotifySourceRequest, YoutubeSourceRequest

from testing.contracts.model import Contract, reply


CONTRACTS = {
    media_search_routes.SPOTIFY: Contract(
        'Expand a Spotify playlist, album or track.', request=SpotifySourceRequest,
        replies=(reply(200, MediaSearchResponse, doc='A `catalog`, or an `error` from the provider.'),
                 reply(422, doc='Not exactly one id.'))),
    media_search_routes.YOUTUBE: Contract(
        'Expand a YouTube playlist.', request=YoutubeSourceRequest,
        replies=(reply(200, MediaSearchResponse, doc='A `catalog`, or an `error` from the provider.'),
                 reply(422, doc='No playlist_id.'))),
}
