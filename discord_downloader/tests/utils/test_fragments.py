'''Tests for the yt-dlp fragment-skip logger proxy.'''
import threading
from unittest.mock import MagicMock

from discord_downloader.utils.fragments import FragmentSkipLogger


def narrate(tracker, *lines):
    '''Feed (level, message) pairs to the tracker the way yt-dlp's downloader would.'''
    for level, message in lines:
        getattr(tracker, level)(message)


TOTAL_47 = ('debug', '[hlsnative] Total fragments: 47')
EMPTY_BODY = ('error', 'ERROR: Did not get any data blocks')


def make_tracker():
    '''A tracker wrapping a mock logger, reset like the downloader does before a download.'''
    logger = MagicMock()
    tracker = FragmentSkipLogger(logger)
    tracker.reset()
    return tracker, logger


def test_nothing_skipped_loses_nothing():
    '''Ordinary progress narration records nothing.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, ('debug', '[download]  50.0% of ~  25.42MiB at    2.28MiB/s ETA 00:01 (frag 24/47)'))
    assert not tracker.lost_fragments()


def test_empty_final_fragment_is_not_a_loss():
    '''The 0-byte trailer YouTube's HLS manifests end with: skipped for an empty body, last fragment.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, EMPTY_BODY, ('debug', '[download] fragment not found; Skipping fragment 47 ...'))
    assert not tracker.lost_fragments()


def test_empty_mid_stream_fragment_is_a_loss():
    '''An empty body is only excusable on the final fragment.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, EMPTY_BODY, ('debug', '[download] fragment not found; Skipping fragment 12 ...'))
    assert tracker.lost_fragments() == [12]


def test_final_fragment_lost_to_another_error_is_a_loss():
    '''The final fragment failing for a reason other than an empty body is missing audio.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, ('error', 'ERROR: HTTP Error 403: Forbidden'),
            ('debug', '[download] fragment not found; Skipping fragment 47 ...'))
    assert tracker.lost_fragments() == [47]


def test_final_fragment_skipped_with_no_error_is_a_loss():
    '''No preceding error at all: nothing explains the skip, so it counts.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, ('debug', '[download] Skipping fragment 47 ...'))
    assert tracker.lost_fragments() == [47]


def test_an_empty_body_error_explains_only_the_next_skip():
    '''The explanation is consumed by the skip it precedes.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, EMPTY_BODY, ('debug', 'Skipping fragment 47 ...'),
            ('debug', 'Skipping fragment 47 ...'))
    assert tracker.lost_fragments() == [47]


def test_skip_without_a_total_is_a_loss():
    '''If yt-dlp never said how many fragments there are, no skip can be called the last.'''
    tracker, _ = make_tracker()
    narrate(tracker, EMPTY_BODY, ('debug', '[download] Skipping fragment 3 ...'))
    assert tracker.lost_fragments() == [3]


def test_unknown_live_total_is_a_loss():
    '''A live stream reports no numeric total, so there is no final fragment to excuse.'''
    tracker, _ = make_tracker()
    narrate(tracker, ('debug', '[hlsnative] Total fragments: unknown (live)'), EMPTY_BODY,
            ('debug', 'Skipping fragment 3 ...'))
    assert tracker.lost_fragments() == [3]


def test_each_fragmented_download_has_its_own_final_fragment():
    '''A video+audio fetch has two fragmented downloads; the last one belongs to the latest.'''
    tracker, _ = make_tracker()
    narrate(tracker, ('debug', '[hlsnative] Total fragments: 10'), ('debug', 'Skipping fragment 10 ...'),
            ('debug', '[hlsnative] Total fragments: 47'), EMPTY_BODY, ('debug', 'Skipping fragment 47 ...'))
    assert tracker.lost_fragments() == [10]


def test_reset_forgets_the_previous_download():
    '''A loss recorded for one download must not carry into the next.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, ('debug', 'Skipping fragment 5 ...'))
    assert tracker.lost_fragments() == [5]
    tracker.reset()
    assert not tracker.lost_fragments()


def test_a_thread_that_never_reset_still_logs_cleanly():
    '''Metadata-only yt-dlp calls on a fresh executor thread log through the proxy too.'''
    logger = MagicMock()
    tracker = FragmentSkipLogger(logger)
    seen = []
    thread = threading.Thread(target=lambda: (tracker.debug('[youtube] Extracting URL'), seen.append(tracker.lost_fragments())))
    thread.start()
    thread.join()
    assert seen == [[]]
    logger.debug.assert_called_once_with('[youtube] Extracting URL')


def test_records_are_kept_per_thread():
    '''Pool-mode downloads share one logger on different threads and must not see each other's skips.'''
    tracker, _ = make_tracker()
    narrate(tracker, TOTAL_47, ('debug', 'Skipping fragment 5 ...'))
    other = []

    def other_download():
        tracker.reset()
        narrate(tracker, TOTAL_47, EMPTY_BODY, ('debug', 'Skipping fragment 47 ...'))
        other.append(tracker.lost_fragments())

    thread = threading.Thread(target=other_download)
    thread.start()
    thread.join()
    assert other == [[]]
    assert tracker.lost_fragments() == [5]


def test_messages_are_forwarded_unchanged():
    '''The proxy observes; the wrapped logger still gets every call, with its arguments.'''
    tracker, logger = make_tracker()
    tracker.debug('[download] %s', 'x', stacklevel=2)
    tracker.error('ERROR: boom')
    tracker.warning('WARNING: careful')
    tracker.info('hello')
    logger.debug.assert_called_once_with('[download] %s', 'x', stacklevel=2)
    logger.error.assert_called_once_with('ERROR: boom')
    logger.warning.assert_called_once_with('WARNING: careful')
    logger.info.assert_called_once_with('hello')
