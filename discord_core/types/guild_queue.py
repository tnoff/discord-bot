'''
Client-side views of a guild's player queue, as the broker seam returns them.

Plain dataclasses of the real domain objects (MediaDownload, CheckoutResult), the way
CheckoutResult is: the wire models in broker_responses are the contract, these are what a
caller works with once the response is parsed.  Kept out of broker_responses so a caller that
only wants the values does not import every response model.

Named *Snapshot / *Claimed rather than reusing the broker engine's own dataclasses: those hold
BrokerEntry objects and live in the broker pod, these hold what a client can rebuild from the
wire.
'''
from dataclasses import dataclass, field
from typing import List

from discord_core.types.checkout_result import CheckoutResult
from discord_core.types.media_download import MediaDownload


@dataclass
class PlayingSnapshot:
    '''
    The track a guild's player is playing, as the broker last heard from the gateway.

    download is None if the entry has expired since the track started.
    '''
    uuid: str
    started_at: float
    gateway_id: str
    download: MediaDownload | None = None


@dataclass
class GuildQueueSnapshot:
    '''
    A guild's queue in play order, with its playing track and the markers a poller watches.

    version moves on every queue mutation, so a holder of an older version knows its view is
    stale without comparing contents.  skip_for is the uuid of the playing track a skip was
    requested for.
    '''
    version: int
    items: List[MediaDownload] = field(default_factory=list)
    playing: PlayingSnapshot | None = None
    skip_for: str | None = None
    closed: bool = False


@dataclass
class ClaimedDownload:
    '''A track taken off the queue and marked as playing, with the S3 location to fetch it from.'''
    download: MediaDownload
    checkout: CheckoutResult
