'''
The tests the background profiler threads share.

GcCensusProfiler and ProcessMetricsProfiler both run a daemon thread that logs a summary every
interval. A profiler's test module subclasses ``ProfilerThreadSuite`` on its test class and names
the summary method, the logger and the marker its log line carries.
'''
# White-box tests: they drive the profiler's thread flag and loop directly.
# pylint: disable=protected-access
import time
from unittest.mock import Mock, patch


class ProfilerThreadSuite:
    '''Cases every profiler thread must pass. Not collected itself.'''
    # The profiler class under test; called with ``interval_seconds=``.
    profiler_cls: type
    # The method the loop calls for the text it logs.
    summary_method = ''
    # The module attribute holding the profiler's logger.
    logger_path = ''
    # Text the logged summary line contains.
    logged_marker = ''

    def test_start_stop(self):
        """Starting the profiler runs its thread; stopping it clears the flag"""
        profiler = self.profiler_cls(interval_seconds=1)

        assert profiler._running is False

        profiler.start()
        assert profiler._running is True
        assert profiler._thread is not None
        assert profiler._thread.is_alive()

        profiler.stop()
        assert profiler._running is False

    def test_start_already_running(self):
        """Starting a running profiler does not crash"""
        profiler = self.profiler_cls(interval_seconds=1)

        profiler.start()
        assert profiler._running is True

        profiler.start()
        assert profiler._running is True

        profiler.stop()

    def test_summary_is_logged_by_the_loop(self):
        """The background loop logs a summary"""
        mock_logger = Mock()
        profiler = self.profiler_cls(interval_seconds=1)
        with patch(self.logger_path, mock_logger):
            profiler.start()
            time.sleep(1.5)  # wait for at least one snapshot
            profiler.stop()

        assert mock_logger.info.called
        calls = mock_logger.info.call_args_list
        assert any(self.logged_marker in str(call) for call in calls), self.logged_marker

    def test_profiling_loop_logs_exception_and_continues(self):
        """Exceptions inside the loop are caught, logged, and do not kill the thread"""
        profiler = self.profiler_cls(interval_seconds=0)
        call_count = 0

        def flaky_summary():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('transient error')
            profiler._running = False  # stop after the second call
            return "ok"

        setattr(profiler, self.summary_method, flaky_summary)
        profiler._running = True
        profiler._profiling_loop()

        assert call_count == 2
