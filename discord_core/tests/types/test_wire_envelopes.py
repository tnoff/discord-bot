'''Tests for the wire envelopes: catalog, media search, database and queue-worker bodies.'''
import pytest
from pydantic import ValidationError

from discord_core.exceptions import DatabaseUnavailable, MediaSearchError
from discord_core.types.catalog import CatalogItem, CatalogResponse
from discord_core.types.clear_guild_result import ClearGuildResult
from discord_core.types.database_wire import DatabaseErrorBody, DatabaseResponse
from discord_core.types.media_search import MediaSearchErrorBody, MediaSearchResponse
from discord_core.types.queue_worker_responses import (
    BlockResponse,
    ClearGuildResponse,
    StatusSnapshotResponse,
    SubmitAcceptedResponse,
    SubmitRejectedResponse,
)

from testing.helpers import fake_source_dict, generate_fake_context


def test_catalog_response_defaults_and_round_trip():
    '''An empty catalog has no items; a populated one survives a JSON round trip'''
    assert not CatalogResponse().items
    assert CatalogResponse().collection_name is None
    catalog = CatalogResponse(items=[CatalogItem(search_string='a - b', title='B')], collection_name='mix')
    assert CatalogResponse.model_validate_json(catalog.model_dump_json()) == catalog


def test_catalog_item_requires_a_search_string():
    '''Validation rejects an item with no search string'''
    with pytest.raises(ValidationError):
        CatalogItem.model_validate({'title': 'only a title'})


def test_clear_guild_result_defaults_to_no_preserved_bundles():
    '''preserved_bundle_uuids is a fresh set per instance and the result is frozen'''
    first = ClearGuildResult(dropped=[])
    second = ClearGuildResult(dropped=[])
    assert first.preserved_bundle_uuids == set()
    assert first.preserved_bundle_uuids is not second.preserved_bundle_uuids
    with pytest.raises(AttributeError):
        first.dropped = [1]


def test_clear_guild_result_carries_dropped_requests_and_bundles():
    '''The dropped requests and kept bundle uuids are exposed as given'''
    request = fake_source_dict(generate_fake_context())
    result = ClearGuildResult(dropped=[request], preserved_bundle_uuids={'b1'})
    assert result.dropped == [request]
    assert result.preserved_bundle_uuids == {'b1'}


def test_database_error_body_flattens_the_exception_class_and_message():
    '''from_exception names the class so the bot needs no SQLAlchemy to read it'''
    body = DatabaseErrorBody.from_exception(KeyError('missing'))
    assert body.detail == "KeyError: 'missing'"


def test_database_error_body_rebuilds_database_unavailable():
    '''to_exception yields the typed error with the detail preserved'''
    error = DatabaseErrorBody(detail='OperationalError: down').to_exception()
    assert isinstance(error, DatabaseUnavailable)
    assert error.detail == 'OperationalError: down'
    assert str(error) == 'database tier unavailable: OperationalError: down'


def test_database_response_holds_any_result_and_defaults_to_none():
    '''result is untyped, error is optional, and the envelope round-trips'''
    assert DatabaseResponse().result is None
    assert DatabaseResponse().error is None
    ok = DatabaseResponse(result={'id': 3, 'names': ['a']})
    assert DatabaseResponse.model_validate(ok.model_dump()).result == {'id': 3, 'names': ['a']}
    failed = DatabaseResponse(error=DatabaseErrorBody(detail='x'))
    assert DatabaseResponse.model_validate_json(failed.model_dump_json()).error.detail == 'x'


def test_media_search_error_body_round_trips_a_media_search_error():
    '''Flattening and rebuilding keeps provider, reason, status and the message text'''
    original = MediaSearchError(MediaSearchError.SPOTIFY, MediaSearchError.NOT_FOUND, 'ignored', http_status=404)
    body = MediaSearchErrorBody.from_exception(original)
    assert (body.provider, body.reason, body.http_status) == ('spotify', 'not_found', 404)
    rebuilt = body.to_exception()
    assert isinstance(rebuilt, MediaSearchError)
    assert (rebuilt.provider, rebuilt.reason, rebuilt.http_status) == ('spotify', 'not_found', 404)
    assert str(rebuilt) == 'spotify media search failed: not_found'


def test_media_search_error_body_status_is_optional():
    '''A provider failure without an HTTP status leaves http_status None'''
    body = MediaSearchErrorBody(provider='youtube', reason='api_error')
    assert body.to_exception().http_status is None


def test_media_search_response_carries_a_catalog_or_an_error():
    '''Both halves default to None and parse from nested JSON'''
    assert MediaSearchResponse().catalog is None
    parsed = MediaSearchResponse.model_validate({'catalog': {'items': [{'search_string': 'x'}]}})
    assert parsed.catalog.items[0].search_string == 'x'
    assert parsed.error is None
    failed = MediaSearchResponse.model_validate({'error': {'provider': 'spotify', 'reason': 'auth_error'}})
    assert failed.error.reason == 'auth_error'


def test_submit_bodies_have_fixed_status_literals():
    '''Accepted and block say ok, rejected says rejected, and wrong literals fail'''
    assert SubmitAcceptedResponse().model_dump() == {'status': 'ok'}
    assert BlockResponse().model_dump() == {'status': 'ok'}
    rejected = SubmitRejectedResponse(reason='GuildBlocked', detail='guild is blocked')
    assert rejected.model_dump() == {'status': 'rejected', 'reason': 'GuildBlocked', 'detail': 'guild is blocked'}
    with pytest.raises(ValidationError):
        SubmitAcceptedResponse(status='rejected')
    with pytest.raises(ValidationError):
        SubmitRejectedResponse(reason='x')


def test_clear_guild_response_uses_a_list_of_bundle_uuids_on_the_wire():
    '''The wire form keeps a list; a bare string is not accepted as one'''
    body = ClearGuildResponse(dropped=[{'uuid': 'u'}], preserved_bundle_uuids=['a', 'b'])
    assert body.model_dump()['preserved_bundle_uuids'] == ['a', 'b']
    with pytest.raises(ValidationError):
        ClearGuildResponse(dropped=[], preserved_bundle_uuids='abc')


def test_status_snapshot_keeps_a_missing_backoff_as_none():
    '''backoff_seconds_remaining may be absent and is reported as None, not 0'''
    snapshot = StatusSnapshotResponse(failure_summary='none', failure_count=0, queue_sizes={'1': 2})
    assert snapshot.backoff_seconds_remaining is None
    with pytest.raises(ValidationError):
        StatusSnapshotResponse(failure_summary='x', failure_count=0, queue_sizes={'1': 'many'})
