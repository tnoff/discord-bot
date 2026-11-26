'''Tests for exceptions, shared constants, seam-contract settings and otel branches.'''
import functools

import pytest
from opentelemetry.trace.status import StatusCode

from discord_core import common
from discord_core.exceptions import (
    DatabaseUnavailable,
    InvalidSearchURL,
    MediaSearchError,
    SearchException,
    ThirdPartyException,
)
from discord_core.utils import otel
from discord_core.utils.common import SeamContractConfig, seam_contract_from_settings
from discord_core.utils.integrations import common as integration_common
from discord_core.utils.loop_health import LOOP_HEALTH


def test_search_exception_keeps_message_and_user_message():
    '''The subclasses inherit a message, a str form, and an optional user message'''
    error = ThirdPartyException('spotify down', user_message='Try later')
    assert isinstance(error, SearchException)
    assert (error.message, error.user_message, str(error)) == ('spotify down', 'Try later', 'spotify down')
    assert InvalidSearchURL('bad').user_message is None


def test_media_search_error_exposes_its_fields_and_constants():
    '''provider, reason and http_status are attributes; the message is the str form'''
    error = MediaSearchError(MediaSearchError.YOUTUBE, MediaSearchError.API_ERROR, 'boom', http_status=503)
    assert (error.provider, error.reason, error.http_status) == ('youtube', 'api_error', 503)
    assert str(error) == 'boom'
    assert MediaSearchError('spotify', MediaSearchError.MISSING_CREDENTIALS, 'x').http_status is None
    assert MediaSearchError.AUTH_ERROR == 'auth_error'


def test_database_unavailable_formats_the_detail():
    '''The detail is kept raw and prefixed in the message'''
    error = DatabaseUnavailable('pool exhausted')
    assert error.detail == 'pool exhausted'
    assert str(error) == 'database tier unavailable: pool exhausted'


def test_shared_constants_describe_discord_and_youtube_limits():
    '''The constant modules hold the documented values'''
    assert common.DISCORD_MAX_MESSAGE_LENGTH == 2000
    assert integration_common.YOUTUBE_VIDEO_PREFIX == 'https://www.youtube.com/watch?v='
    assert integration_common.YOUTUBE_SHORT_PREFIX == 'https://www.youtube.com/shorts/'


def test_seam_contract_settings_default_when_the_block_is_absent():
    '''No settings, no general block and a null block all give the defaults'''
    for settings in (None, {}, {'general': {}}, {'general': {'seam_contract': None}}):
        config = seam_contract_from_settings(settings)
        assert (config.grace_seconds, config.interval_seconds) == (300.0, 60.0)


def test_seam_contract_settings_read_the_block_and_reject_bad_values():
    '''A present block overrides fields; a wrongly typed field fails validation'''
    config = seam_contract_from_settings({'general': {'seam_contract': {'grace_seconds': 12}}})
    assert config == SeamContractConfig(grace_seconds=12.0, interval_seconds=60.0)
    with pytest.raises(ValueError):
        seam_contract_from_settings({'general': {'seam_contract': {'grace_seconds': 'soon'}}})


def test_sync_span_wrapper_records_and_reraises_the_exception(recording_tracer, monkeypatch):
    '''A body error marks the span ERROR, records it, and propagates unchanged'''
    tracer, exporter = recording_tracer
    monkeypatch.setattr(otel, 'TRACER', tracer)
    with pytest.raises(RuntimeError, match='kaput'):
        with otel.otel_span_wrapper('sync.failure', attributes={'k': 'v'}):
            raise RuntimeError('kaput')
    span = exporter.get_finished_spans()[0]
    assert span.status.status_code is StatusCode.ERROR
    assert span.attributes['k'] == 'v'
    assert 'exception' in [event.name for event in span.events]


def test_loop_heartbeat_observations_report_registered_loops_only():
    '''An unregistered loop emits nothing; a registered one emits 1 with its job name'''
    name = 'small-core-modules-heartbeat'
    assert not otel.loop_heartbeat_observations(name)
    try:
        LOOP_HEALTH.register(name).record_success()
        observations = otel.loop_heartbeat_observations(name)
        assert len(observations) == 1
        assert observations[0].value == 1
        assert observations[0].attributes == {otel.AttributeNaming.BACKGROUND_JOB.value: name}
        bound = functools.partial(otel.loop_heartbeat_observations, name)
        assert bound()[0].value == 1
    finally:
        LOOP_HEALTH.unregister(name)
