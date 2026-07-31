'''
Payload for the bot-ready search-result queue.

A SearchResolution is what rides the broker's search-result queue: the resolved
MediaRequest (its SearchResult already carries any YouTube-Music videoId the
search worker found) plus the search worker's span context for trace linking.
The search worker produces one per resolved request; the cog's
process_search_results loop consumes it and runs the bot-side tail (cache check
then download submit).

media_request is typed as AnyMediaRequest — exactly like DownloadResult — so it
round-trips over HTTP (model_dump/model_validate) when a standalone search pod
hands resolutions back to the bot through the broker.
'''
from pydantic import BaseModel

from discord_bot.types.playlist_add_request import AnyMediaRequest


class SearchResolution(BaseModel):
    '''A resolved search request, ready for the bot-side download hand-off.'''
    media_request: AnyMediaRequest
    span_context: dict | None = None
