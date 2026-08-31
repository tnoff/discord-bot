'''
Wire envelope for the persistence-tier HTTP surface.

One response model for every route on the db pod: a result, or an error, never
both -- and **both over HTTP 200**. That is the media_search error-envelope
lesson applied to a second seam, and here it is also the answer to "which layer
owns retrying".

`HttpClientMixin._http` calls `raise_for_status()` inside
`async_retry_broker_command`, so any non-2xx is retried three times with 1, 2 and
4 second backoff *and* its body is discarded. Two things follow:

  A "no such row" encoded as 404 would be re-run three times to be told the same
  thing, and the caller would never see the typed body that said so.

  A database fault encoded as 5xx would be retried by the client on top of the
  three retries the pod already ran against its own engine -- nine attempts and
  up to ~30 seconds of wall clock for one `get_playlist`, which is exactly the
  compounding the spec's open question was about.

So non-2xx stays reserved for the pod itself being broken or absent -- a
connection error, or a genuine 500 out of aiohttp -- which is the one failure
class the client is nearest to and the only one worth a client-side ladder.

`result` is deliberately untyped here. Thirty-odd routes returning thirty-odd
shapes would mean thirty-odd response models whose only job is to name a field;
the client validates the payload into the DTO its Protocol method already
promised, and that DTO is the contract.
'''
from typing import Any

from pydantic import BaseModel

from discord_bot.exceptions import DatabaseUnavailable


class DatabaseErrorBody(BaseModel):
    '''A persistence-tier failure, flattened for the wire.'''
    detail: str

    @classmethod
    def from_exception(cls, error: Exception) -> 'DatabaseErrorBody':
        '''
        Flatten whatever the store raised into a reportable detail string.

        The exception type is deliberately not carried across. The caller cannot
        do anything different for an OperationalError than for a
        PendingRollbackError -- both mean the pod tried and could not -- and
        naming SQLAlchemy's exception hierarchy on the wire would make the bot
        need SQLAlchemy to read the response, which is what this seam exists to
        stop.

        error : The exception the store raised after its own retries
        '''
        return cls(detail=f'{error.__class__.__name__}: {error}')

    def to_exception(self) -> DatabaseUnavailable:
        '''Rebuild the exception the caller sees on the bot side.'''
        return DatabaseUnavailable(self.detail)


class DatabaseResponse(BaseModel):
    '''
    Result of one store call: a result, or an error, never both.

    `result` is None for both "the method returns None" and "the method returned
    nothing" because those are the same thing -- every Protocol method that can
    answer "no such row" already does it by returning None, so there is nothing
    for the envelope to disambiguate.
    '''
    result: Any = None
    error: DatabaseErrorBody | None = None
