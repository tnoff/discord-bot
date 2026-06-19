from typing import Annotated, Literal, Union

from pydantic import Field, TypeAdapter

from discord_bot.types.media_request import MediaRequest


class PlaylistAddRequest(MediaRequest):
    '''MediaRequest variant for adding a track to a playlist without playing it.'''
    download_file: Literal[False] = False
    playlist_id: int


# Discriminated union over download_file — use this in any Pydantic field
# that holds a media request loaded from JSON / Redis so the right subclass
# (and its extra fields like PlaylistAddRequest.playlist_id) survives the
# round-trip.  Plain MediaRequest pins download_file: Literal[True] and
# would otherwise reject a serialised PlaylistAddRequest with 422.
AnyMediaRequest = Annotated[
    Union[MediaRequest, PlaylistAddRequest],
    Field(discriminator='download_file'),
]

_MEDIA_REQUEST_ADAPTER: TypeAdapter[AnyMediaRequest] = TypeAdapter(AnyMediaRequest)


def parse_media_request(data: dict) -> MediaRequest:
    '''Validate a wire-format dict into the right MediaRequest subclass.

    Returns either MediaRequest or PlaylistAddRequest based on download_file.
    '''
    return _MEDIA_REQUEST_ADAPTER.validate_python(data)
