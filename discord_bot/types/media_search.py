'''
Wire types for the media-search HTTP surface.

One response model for both provider routes: either a catalog or an error, never
both. The error half is a flattened MediaSearchError -- provider, reason and the
provider's HTTP status -- and deliberately carries no user-facing text. The pod
reports what the provider said; the cog decides what a Discord user reads. See
MediaSearchError's docstring for why that line is drawn there.

Lives in types/ rather than next to the server because the cog-side client parses
these and must not import a servers module, and because the search pod must be
able to build one without importing anything the bot owns.
'''
from pydantic import BaseModel

from discord_bot.exceptions import MediaSearchError
from discord_bot.types.catalog import CatalogResponse


class MediaSearchErrorBody(BaseModel):
    '''A provider failure, described without naming the provider SDK.'''
    provider: str
    reason: str
    http_status: int | None = None

    @classmethod
    def from_exception(cls, error: MediaSearchError) -> 'MediaSearchErrorBody':
        '''Flatten a MediaSearchError for the wire.'''
        return cls(provider=error.provider, reason=error.reason,
                   http_status=error.http_status)

    def to_exception(self) -> MediaSearchError:
        '''Rebuild the MediaSearchError the caller would have seen in-process.'''
        return MediaSearchError(self.provider, self.reason,
                                f'{self.provider} media search failed: {self.reason}',
                                http_status=self.http_status)


class MediaSearchResponse(BaseModel):
    '''
    Result of one provider expansion: a catalog, or an error, never both.

    Both are returned over HTTP 200, which is the deliberate part. A provider
    saying "no such playlist" is not a failure of the pod -- the pod did its job
    and is reporting the answer. Encoding it as a 4xx/5xx would mean the client's
    retry wrapper re-runs a lookup whose answer will not change, and would also
    lose this body, since HttpClientMixin._http calls raise_for_status(). Non-2xx
    stays reserved for the pod itself being broken, which IS worth a retry.
    '''
    catalog: CatalogResponse | None = None
    error: MediaSearchErrorBody | None = None
