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


class SearchException(Exception):
    '''
    For issues with Search

    Lives here rather than in cogs/music_helpers/search_client.py, where it was
    defined, because clients/ and interfaces/ now raise and catch it and must not
    import a cogs module to do so. This module imports nothing, which is the same
    reason YoutubeMusicRetryException lives here. search_client re-exports all
    three, so existing imports keep working.
    '''
    def __init__(self, message, user_message=None):
        self.message = message
        super().__init__(self.message)
        self.user_message = user_message


class ThirdPartyException(SearchException):
    '''
    Issue with 3rd Party Library
    '''


class InvalidSearchURL(SearchException):
    '''
    Invalid URL to give bot
    '''


class MediaSearchError(Exception):
    '''
    A media-search provider call failed, described without naming the provider SDK.

    This is the seam's error type: InMemoryMediaSearchClient translates spotipy
    and googleapiclient failures into one of these, and the cog-side SearchClient
    renders it into the user-facing SearchException. That split is the point --
    the provider libraries stay behind the client, and the Discord copy stays in
    the cog, so when media_search moves to the search pod the pod returns
    (provider, reason, http_status) over HTTP and the bot still writes the
    message. A pod deciding what a Discord user reads would put user-facing copy
    in a process that has no other reason to know about Discord.

    provider : 'spotify' or 'youtube'
    reason   : MISSING_CREDENTIALS / NOT_FOUND / AUTH_ERROR / API_ERROR
    http_status : provider HTTP status where there was one, else None
    '''
    SPOTIFY = 'spotify'
    YOUTUBE = 'youtube'

    MISSING_CREDENTIALS = 'missing_credentials'
    NOT_FOUND = 'not_found'
    AUTH_ERROR = 'auth_error'
    API_ERROR = 'api_error'

    def __init__(self, provider: str, reason: str, message: str,
                 http_status: int | None = None):
        self.provider = provider
        self.reason = reason
        self.http_status = http_status
        super().__init__(message)


class DatabaseUnavailable(Exception):
    '''
    The persistence tier could not answer -- not that the answer was "nothing".

    The seam's error type, and the line it draws is the whole retry story. "No
    such playlist" is an **answer**: it comes back as a normal return value, and
    nothing retries it, because re-running the query cannot change it. This
    exception is for the other case, where the database itself would not serve
    the request.

    It is raised only after the db pod has already exhausted
    `async_retry_database_commands` against its local engine. The pod is the
    layer nearest that failure and the only one that can roll back a session and
    try again, so it owns retrying it; by the time this crosses the wire the
    retrying is done. The HTTP client re-raises it rather than retrying, which is
    what keeps the two ladders from multiplying into the 1+2+4-inside-a-retry
    shape from the 2026-08-26 incident.

    detail : What the pod reported, for logs -- not user-facing copy
    '''

    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(f'database tier unavailable: {detail}')


class SeamResponseInvalid(Exception):
    '''
    A peer answered on a seam, and this build could not parse what it sent.

    The counterpart to the route check in clients/seam_contract. That check asks
    "does my peer still serve the routes I call" and is **predictive** -- it
    fires before any traffic, because the 2026-07-31 incident was a 404 on first
    use killing a consumer permanently. This one is **reactive** by necessity:
    a route that exists but whose body has gained a required field cannot be
    seen by a route-set comparison, only by trying to parse a real response.

    Raised in place of a bare pydantic ValidationError so the failure carries
    the peer's identity. An unwrapped ValidationError says a MarkovChannelEntry
    was malformed; it does not say which peer sent it or on which seam, and a
    client that calls three stores over one envelope type cannot tell them apart
    at all. See docs/projects/http-seam-contract.md, "Payload-shape drift".

    **This does not change control flow.** It propagates exactly as the
    ValidationError did, into the same broad catch in return_loop_runner, which
    retries with backoff and records the error against LoopHealth. The class was
    always alerting; what it lacked was attribution, and attribution is all this
    adds.

    seam : Seam name, e.g. 'database' -- the same label the breach gauge carries
    prefix : Route prefix distinguishing peers that share a seam, e.g.
             '/database/markov'. Empty for seams served by one pod at the root.
    model : The model that rejected the body
    error : The ValidationError being wrapped, kept for the full field report
    '''

    def __init__(self, seam: str, prefix: str, model: type, error: Exception):
        self.seam = seam
        self.prefix = prefix
        self.model = model
        self.validation_error = error
        peer = f'{seam}{prefix}' if prefix else seam
        super().__init__(f'peer on seam {peer} sent a body this build cannot parse '
                         f'as {model.__name__}: {error}')
