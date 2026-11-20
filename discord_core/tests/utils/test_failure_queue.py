'''Tests for FailureQueue, the shared failure-rate tracker.'''
from asyncio import QueueFull
from datetime import datetime, timedelta, timezone

from discord_core.utils.failure_queue import FailureQueue, FailureStatus


def _failure(age_seconds: int = 0) -> FailureStatus:
    return FailureStatus(success=False,
                         created_at=datetime.now(timezone.utc) - timedelta(seconds=age_seconds))


def test_failures_accumulate():
    queue = FailureQueue(max_size=10)
    for _ in range(3):
        assert queue.add_item(_failure()) is True
    assert queue.size == 3


def test_success_pops_one_failure():
    queue = FailureQueue(max_size=10)
    queue.add_item(_failure())
    queue.add_item(_failure())
    assert queue.add_item(FailureStatus(success=True)) is True
    assert queue.size == 1


def test_success_on_empty_queue_is_a_no_op():
    queue = FailureQueue(max_size=10)
    assert queue.add_item(FailureStatus(success=True)) is True
    assert queue.size == 0


def test_full_queue_drops_oldest_to_make_room():
    queue = FailureQueue(max_size=2, max_age_seconds=0)
    first, second, third = _failure(), _failure(), _failure()
    for item in (first, second, third):
        queue.add_item(item)
    assert queue.size == 2
    assert queue.queue.items() == [second, third]


def test_old_failures_are_cleaned_on_add():
    queue = FailureQueue(max_size=10, max_age_seconds=60)
    queue.queue.put_nowait(_failure(age_seconds=120))
    queue.queue.put_nowait(_failure(age_seconds=10))
    queue.add_item(_failure())
    assert queue.size == 2


def test_max_age_zero_keeps_old_failures():
    '''max_age_seconds <= 0 disables age-based cleanup entirely.'''
    queue = FailureQueue(max_size=10, max_age_seconds=0)
    queue.queue.put_nowait(_failure(age_seconds=10_000))
    queue.add_item(_failure())
    assert queue.size == 2


def test_clean_stops_refilling_when_queue_is_full(mocker):
    '''A refill that hits QueueFull stops instead of raising out of add_item.'''
    queue = FailureQueue(max_size=10, max_age_seconds=60)
    queue.queue.put_nowait(_failure())
    queue.queue.put_nowait(_failure())
    put = mocker.patch.object(queue.queue, 'put_nowait', side_effect=QueueFull)
    queue._clean_old_items()  # pylint: disable=protected-access
    assert put.call_count == 1
    assert queue.size == 0


def test_status_summary_empty():
    assert FailureQueue().get_status_summary() == '0 failures in queue'


def test_status_summary_seconds():
    queue = FailureQueue(max_age_seconds=0)
    queue.queue.put_nowait(_failure(age_seconds=5))
    queue.queue.put_nowait(_failure())
    assert queue.get_status_summary() == '2 failures in queue, oldest: 5s ago'


def test_status_summary_minutes():
    queue = FailureQueue(max_age_seconds=0)
    queue.queue.put_nowait(_failure(age_seconds=125))
    assert queue.get_status_summary() == '1 failures in queue, oldest: 2m 5s ago'
