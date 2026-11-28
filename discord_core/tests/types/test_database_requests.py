'''The database seam's request bodies: every route has one, each says what its Protocol method takes, and
a malformed body is refused rather than read loosely.'''
import inspect
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from discord_core.routes import database as db_routes
from discord_core.types import database_requests as requests
from discord_core.types.database_requests import REQUESTS
from discord_core.types.media_download import MediaDownload, media_download_from_dict, media_download_to_dict

from discord_core.tests.routes.test_database_route_models import store_protocol
from testing.fakes.worker_requests import queue_request

ROUTES = [(group.name, route_name) for group in db_routes.GROUPS for route_name in group.routes]


def test_every_route_has_an_entry_and_nothing_else_does():
    '''The map is total over the registry, in both directions.'''
    assert sorted(REQUESTS) == sorted(ROUTES)


@pytest.mark.parametrize('group, route', ROUTES)
def test_a_request_model_is_the_arguments_of_the_method_the_route_is_named_for(group, route):
    '''Field names and which are optional come off the Protocol signature, so the wire cannot drift from it.'''
    parameters = {name: parameter for name, parameter in
                  inspect.signature(getattr(store_protocol(group), route)).parameters.items() if name != 'self'}
    model = REQUESTS[(group, route)]
    if model is None:
        assert not parameters
        return
    assert set(model.model_fields) == set(parameters)
    for name, field in model.model_fields.items():
        assert field.is_required() == (parameters[name].default is inspect.Parameter.empty), name


def test_a_markov_cutoff_is_read_as_a_datetime():
    '''The one route that sends a timestamp: ISO text in, a datetime out.'''
    cutoff = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    body = requests.PruneRelationsRequest(cutoff=cutoff).model_dump(mode='json')
    assert requests.PruneRelationsRequest.model_validate(body).cutoff == cutoff


def test_generate_words_takes_no_seed_as_none():
    '''`!markov speak` with no seed is absent or null, never an error.'''
    assert requests.GenerateWordsRequest(guild_id=1, count=5).first_word is None
    assert requests.GenerateWordsRequest.model_validate({'guild_id': 1, 'count': 5, 'first_word': None}).first_word is None


def test_delete_by_index_accepts_the_first_position():
    '''Index 0 is the first item; a model that read it as missing would delete the wrong track.'''
    assert requests.DeleteItemByIndexRequest(playlist_id=1, index=0).index == 0


def test_ready_remove_defaults_to_the_eviction_policy():
    '''An empty body applies the policy; evict_all flags every row.'''
    assert requests.ReadyRemoveRequest().evict_all is False
    assert requests.ReadyRemoveRequest(evict_all=True).evict_all is True


def test_remove_video_cache_base_paths_are_optional():
    '''Rows by id alone is the common call.'''
    assert not requests.RemoveVideoCacheRequest(video_cache_ids=[1]).base_paths


@pytest.mark.parametrize('model, body', [
    (requests.GuildRequest, {}),
    (requests.GuildRequest, {'guild_id': None}),
    (requests.GuildRequest, {'guild_id': 'x'}),
    (requests.RecordPlayRequest, {'guild_id': 1, 'duration_seconds': 3, 'cache_hit': 'maybe'}),
    (requests.GuildNameRequest, {'guild_id': 1}),
    (requests.SaveMessagesRequest, {'guild_id': 1, 'channel_id': 2, 'messages': [{'not': 'a message'}]}),
    (requests.AddItemsRequest, {'playlist_id': 1, 'items': [{'title': 'no url'}], 'max_size': 5}),
    (requests.AddItemsRequest, {'playlist_id': 1, 'items': 'text', 'max_size': 5}),
    (requests.RecordHistoryItemRequest, {'playlist_id': 1, 'item': {}, 'max_size': 5}),
    (requests.PruneRelationsRequest, {'cutoff': 'yesterday'}),
    (requests.IterateFileRequest, {'media_download': {}}),
    (requests.IterateFileRequest, {'media_download': {'request': {'not': 'a request'}}}),
    (requests.GetWebpageUrlItemRequest, {'media_request': None}),
    (requests.RemoveVideoCacheRequest, {'video_cache_ids': ['a']}),
])
def test_a_missing_or_malformed_body_is_refused(model, body):
    '''Each is a 422 on the server: the request never reached the database.'''
    with pytest.raises(ValidationError):
        model.model_validate(body)


def test_a_download_survives_the_payload_the_client_writes():
    '''`media_download_to_dict` is what the clients send and `media_download_from_dict` what the server reads, and
    the payload model sits between them: the download comes out as it went in.'''
    request = queue_request(guild_id=7)
    sent = MediaDownload(Path('/cache/a.mp3'), {'id': 'a', 'title': 'A', 'webpage_url': 'https://x/a'}, request,
                         cache_hit=True)
    sent.file_size_bytes = 99
    wire = requests.IterateFileRequest(
        media_download=media_download_to_dict(sent)).model_dump(mode='json')
    payload = requests.IterateFileRequest.model_validate(wire).media_download
    received = media_download_from_dict(payload.model_dump(exclude={'request'}), payload.request)
    assert (received.file_path, received.file_size_bytes, received.cache_hit) == (Path('/cache/a.mp3'), 99, True)
    assert (received.id, received.title, received.webpage_url) == ('a', 'A', 'https://x/a')
    assert received.media_request.uuid == request.uuid
