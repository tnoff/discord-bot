import pytest
from aiohttp import ClientConnectionError

from tests.fakes.pipeline_fakes import fake_envelope, yield_search_client_check_source
from tests.helpers import fake_source_dict
from tests.helpers import fake_engine, fake_context, fake_stores  #pylint:disable=unused-import
from tests.results_pod import ResultsPod


async def search_for(fake_context, fake_stores, *requests):  #pylint:disable=redefined-outer-name
    '''A pod whose source search answers with ``requests``, after one pass of its search driver.'''
    pod = ResultsPod(fake_context, fake_stores, with_search=True,
                     source_search_client=yield_search_client_check_source(list(requests))())
    await pod.enqueuer.generate_media_requests_from_search(fake_envelope(fake_context), 'foo bar')
    await pod.search_driver.run_once(pod.stop_event)
    return pod


@pytest.mark.asyncio()
async def test_search_results_reach_the_download_queue(fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    s = fake_source_dict(fake_context)
    s1 = fake_source_dict(fake_context)
    pod = await search_for(fake_context, fake_stores, s, s1)
    await pod.loops.process_search_results()
    await pod.search_driver.run_once(pod.stop_event)
    await pod.loops.process_search_results()
    item0 = await pod.download_client.local_worker.get_input_nowait()
    item1 = await pod.download_client.local_worker.get_input_nowait()
    # Compare key properties since SearchClient refactoring creates new MediaRequest objects
    assert item0.search_result.raw_search_string == s.search_result.raw_search_string
    assert item0.search_result.search_type == s.search_result.search_type
    assert item1.search_result.raw_search_string == s1.search_result.raw_search_string
    assert item1.search_result.search_type == s1.search_result.search_type


@pytest.mark.asyncio()
async def test_search_result_is_requeued_when_the_downloader_is_unreachable(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    '''
    next_search_result is a destructive pop, so a failed submit must put it back.

    The downloader deploys under Recreate -- one Mullvad key means no rolling
    overlap is possible -- so a hard gap on its Service is routine, not exotic.
    Every resolution popped during that gap used to be destroyed outright.
    '''
    s = fake_source_dict(fake_context)
    pod = await search_for(fake_context, fake_stores, s)

    mocker.patch.object(pod.download_client, 'submit',
                        side_effect=ClientConnectionError('downloader is not accepting connections'))
    with pytest.raises(ClientConnectionError):
        await pod.loops.process_search_results()

    # Still on the queue: a later pass, once the new downloader pod is up, finds
    # it and can hand it off. This is the assertion the whole fix exists for.
    requeued = await pod.broker_client.next_search_result()
    assert requeued is not None
    assert requeued.media_request.search_result.raw_search_string == s.search_result.raw_search_string


@pytest.mark.asyncio()
async def test_requeue_failure_does_not_mask_the_original(mocker, fake_context, fake_stores):  #pylint:disable=redefined-outer-name
    '''
    When the requeue also fails the request really is lost -- report why honestly.

    The broker being down too is the one case with nowhere to put the resolution.
    It is logged as a loss, and the *submit* failure is what propagates, because
    a traceback naming the requeue would point at the second-order symptom.
    '''
    pod = await search_for(fake_context, fake_stores, fake_source_dict(fake_context))

    mocker.patch.object(pod.download_client, 'submit',
                        side_effect=ClientConnectionError('downloader is not accepting connections'))
    requeue = mocker.patch.object(pod.broker_client, 'register_search_result',
                                  side_effect=ClientConnectionError('broker is not accepting connections'))
    with pytest.raises(ClientConnectionError) as exc_info:
        await pod.loops.process_search_results()

    # Asserted, not implied: without it this test passes on the unfixed code,
    # where submit's exception propagates because nothing tried to requeue at all.
    requeue.assert_called_once()
    assert 'downloader is not accepting connections' in str(exc_info.value)
