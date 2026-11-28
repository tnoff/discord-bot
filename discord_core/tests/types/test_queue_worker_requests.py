'''The queue-worker request bodies: what the client may send and the pod will accept.'''
import pytest
from pydantic import ValidationError

from discord_core.types.playlist_add_request import PlaylistAddRequest
from discord_core.types.queue_worker_requests import BlockRequestBody, ClearRequestBody, SubmitRequestBody

from testing.fakes.worker_requests import queue_request


def test_a_submit_carries_the_request_back_as_the_same_kind():
    '''A plain request stays a MediaRequest and a playlist add stays a PlaylistAddRequest, which is what the
    `download_file` discriminator is for: the worker must not receive a different class than was sent.'''
    plain = queue_request(guild_id=7)
    playlist_add = PlaylistAddRequest(**{**plain.model_dump(), 'download_file': False, 'playlist_id': 3})
    for sent in (plain, playlist_add):
        body = SubmitRequestBody(guild_id=7, media_request=sent, priority=2)
        wire = body.model_dump(mode='json')
        received = SubmitRequestBody.model_validate(wire)
        assert type(received.media_request) is type(sent)
        assert received.media_request.uuid == sent.uuid
        assert received.priority == 2


def test_a_submit_priority_is_optional():
    '''Absent and null both mean no priority.'''
    wire = {'guild_id': 7, 'media_request': queue_request(guild_id=7).model_dump(mode='json')}
    assert SubmitRequestBody.model_validate(wire).priority is None
    assert SubmitRequestBody.model_validate({**wire, 'priority': None}).priority is None


@pytest.mark.parametrize('body', [
    {},
    {'guild_id': 7},
    {'media_request': {}},
    {'guild_id': 'x', 'media_request': {}},
    {'guild_id': 7, 'media_request': {'not': 'a request'}},
])
def test_a_submit_missing_or_malformed_is_refused(body):
    '''Both fields are required and the request must be one of the two kinds.'''
    with pytest.raises(ValidationError):
        SubmitRequestBody.model_validate(body)


def test_clear_keeps_nothing_unless_asked():
    '''`preserve_playlist_adds` defaults to dropping everything, and guild_id is required.'''
    assert ClearRequestBody(guild_id=7).preserve_playlist_adds is False
    assert ClearRequestBody(guild_id=7, preserve_playlist_adds=True).preserve_playlist_adds is True
    with pytest.raises(ValidationError):
        ClearRequestBody()


def test_block_needs_a_guild():
    '''The one field is required.'''
    assert BlockRequestBody(guild_id=7).guild_id == 7
    with pytest.raises(ValidationError):
        BlockRequestBody()
