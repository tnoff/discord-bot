'''The media-search request bodies: what the client may send and the pod will accept.'''
import pytest
from pydantic import ValidationError

from discord_core.types.media_search import SpotifySourceRequest, YoutubeSourceRequest


@pytest.mark.parametrize('field', ['playlist_id', 'album_id', 'track_id'])
def test_spotify_accepts_exactly_one_id_and_reports_which(field):
    '''The one id given is the one `ids()` returns, under the keyword the provider call takes.'''
    request = SpotifySourceRequest(**{field: 'abc'})
    assert request.ids() == {field: 'abc'}


@pytest.mark.parametrize('body', [
    {},
    {'playlist_id': None},
    {'playlist_id': ''},
    {'album_id': 'a', 'track_id': 'b'},
    {'playlist_id': 'a', 'album_id': 'b', 'track_id': 'c'},
    {'playlist_id': 'a', 'album_id': ''},
])
def test_spotify_refuses_none_empty_and_several(body):
    '''None, an empty id, and more than one are all a caller bug rather than a precedence question.'''
    with pytest.raises(ValidationError):
        SpotifySourceRequest(**body)


def test_a_refused_spotify_body_is_a_value_error():
    '''Callers that guard with `except ValueError` keep working when the client builds the model itself.'''
    with pytest.raises(ValueError):
        SpotifySourceRequest()


def test_youtube_needs_a_non_empty_playlist_id():
    '''The playlist id is required and may not be empty.'''
    assert YoutubeSourceRequest(playlist_id='PL1').playlist_id == 'PL1'
    for body in ({}, {'playlist_id': ''}, {'playlist_id': None}):
        with pytest.raises(ValidationError):
            YoutubeSourceRequest(**body)
