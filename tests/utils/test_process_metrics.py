"""
Tests for process metrics profiler
"""
# pylint: disable=redefined-outer-name,protected-access
import time
from unittest.mock import Mock, patch

import psutil

from discord_bot.utils.process_metrics import ProcessMetricsProfiler


class TestProcessMetricsProfiler:
    """Tests for ProcessMetricsProfiler class"""

    def test_init(self):
        """Test ProcessMetricsProfiler initialization"""
        profiler = ProcessMetricsProfiler(interval_seconds=15)
        assert profiler.interval_seconds == 15
        assert profiler._running is False
        assert profiler._thread is None

    def test_start_stop(self):
        """Test starting and stopping the profiler"""
        profiler = ProcessMetricsProfiler(interval_seconds=1)

        # Should not be running initially
        assert profiler._running is False

        # Start the profiler
        profiler.start()
        assert profiler._running is True
        assert profiler._thread is not None
        assert profiler._thread.is_alive()

        # Stop the profiler
        profiler.stop()
        assert profiler._running is False

    def test_start_already_running(self):
        """Test starting profiler when already running"""
        profiler = ProcessMetricsProfiler(interval_seconds=1)

        profiler.start()
        assert profiler._running is True

        # Starting again should not crash
        profiler.start()
        assert profiler._running is True

        profiler.stop()

    def test_get_process_metrics(self):
        """Test getting process metrics"""
        profiler = ProcessMetricsProfiler()

        metrics = profiler.get_process_metrics()

        # Should return a dict with expected keys
        assert isinstance(metrics, dict)
        assert 'rss' in metrics
        assert 'vms' in metrics
        assert 'uss' in metrics
        assert 'cpu_percent' in metrics
        assert 'num_threads' in metrics

        # Memory values should be positive
        assert metrics['rss'] > 0
        assert metrics['vms'] > 0
        assert metrics['uss'] > 0

    def test_get_metrics_summary(self):
        """Test getting human-readable metrics summary"""
        profiler = ProcessMetricsProfiler()

        summary = profiler.get_metrics_summary()

        # Should be a string with expected content
        assert isinstance(summary, str)
        assert len(summary) > 0
        assert "Process Metrics" in summary
        assert "Main Process Memory" in summary
        assert "RSS (Physical)" in summary
        assert "USS (Unique)" in summary
        assert "Resources" in summary

    def test_metrics_with_logging(self):
        """Test that metrics are logged"""
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(interval_seconds=1)
        with patch('discord_bot.utils.process_metrics.logger', mock_logger):
            # Start profiler and wait for snapshot
            profiler.start()
            time.sleep(1.5)  # Wait for one snapshot
            profiler.stop()

        # Verify logger was called
        assert mock_logger.info.called

        # Check that metrics were logged
        calls = mock_logger.info.call_args_list
        metrics_logged = any('Process metrics' in str(call) for call in calls)
        assert metrics_logged, "Process metrics should be logged"

    def test_memory_deltas(self):
        """Test that memory deltas are calculated"""
        profiler = ProcessMetricsProfiler()

        # Get first summary (no deltas yet)
        summary1 = profiler.get_metrics_summary()
        assert "Memory Changes" not in summary1

        # Get second summary (should have deltas)
        time.sleep(0.1)  # Small delay
        summary2 = profiler.get_metrics_summary()
        assert "Memory Changes" in summary2
        assert "RSS Delta" in summary2
        assert "USS Delta" in summary2

    # ------------------------------------------------------------------
    # num_fds fallback
    # ------------------------------------------------------------------

    def test_get_process_metrics_num_fds_not_available(self):
        """num_fds is None when the platform does not support it"""
        profiler = ProcessMetricsProfiler()
        profiler.process.num_fds = Mock(side_effect=AttributeError('not supported'))

        metrics = profiler.get_process_metrics()

        assert metrics is not None
        assert metrics['num_fds'] is None

    # ------------------------------------------------------------------
    # Child process paths
    # ------------------------------------------------------------------

    def _make_child_mock(self, pid=1234, name='ffmpeg', cmdline=None, rss=1024*1024):
        """Build a mock psutil child process."""
        child = Mock()
        child.pid = pid
        child.name.return_value = name
        child.cmdline.return_value = cmdline or ['ffmpeg', '-i', 'input.mp4']
        mem = Mock()
        mem.rss = rss
        child.memory_info.return_value = mem
        child.cpu_percent.return_value = 5.0
        return child

    def test_get_process_metrics_with_children(self):
        """Children are collected and included in totals"""
        profiler = ProcessMetricsProfiler()
        child = self._make_child_mock()
        profiler.process.children = Mock(return_value=[child])

        metrics = profiler.get_process_metrics()

        assert metrics is not None
        assert len(metrics['children']) == 1
        assert metrics['children'][0]['name'] == 'ffmpeg'
        assert metrics['total_rss'] == metrics['rss'] + child.memory_info().rss

    def test_get_process_metrics_child_terminates_during_collection(self):
        """A child that raises NoSuchProcess mid-collection is silently skipped"""
        profiler = ProcessMetricsProfiler()
        child = Mock()
        child.pid = 9999
        child.name.side_effect = psutil.NoSuchProcess(pid=9999)
        profiler.process.children = Mock(return_value=[child])

        metrics = profiler.get_process_metrics()

        assert metrics is not None
        assert metrics['children'] == []

    def test_get_process_metrics_children_access_denied(self):
        """AccessDenied on children() is caught; children list stays empty"""
        profiler = ProcessMetricsProfiler()
        profiler.process.children = Mock(side_effect=psutil.AccessDenied(pid=0))

        metrics = profiler.get_process_metrics()

        assert metrics is not None
        assert metrics['children'] == []

    # ------------------------------------------------------------------
    # Top-level process error
    # ------------------------------------------------------------------

    def test_get_process_metrics_returns_none_on_process_error(self):
        """Returns None and logs a warning when the process itself disappears"""
        profiler = ProcessMetricsProfiler()
        profiler.process.memory_info = Mock(side_effect=psutil.NoSuchProcess(pid=0))

        metrics = profiler.get_process_metrics()

        assert metrics is None

    # ------------------------------------------------------------------
    # get_metrics_summary edge cases
    # ------------------------------------------------------------------

    def test_get_metrics_summary_when_metrics_unavailable(self):
        """Returns a fallback string when get_process_metrics returns None"""
        profiler = ProcessMetricsProfiler()
        profiler.get_process_metrics = Mock(return_value=None)

        summary = profiler.get_metrics_summary()

        assert summary == "Unable to retrieve process metrics"

    def test_get_metrics_summary_with_children(self):
        """Summary includes child process section and totals when children present"""
        profiler = ProcessMetricsProfiler()
        child = self._make_child_mock(pid=42, name='ffmpeg',
                                      cmdline=['ffmpeg', '-i', 'x'], rss=2*1024*1024)
        profiler.process.children = Mock(return_value=[child])

        summary = profiler.get_metrics_summary()

        assert "Child Processes" in summary
        assert "ffmpeg" in summary
        assert "Total (with children)" in summary
        assert "Total RSS" in summary

    def test_get_metrics_summary_total_rss_delta_with_previous_children(self):
        """Total RSS Delta line appears when either current or previous snapshot has children"""
        profiler = ProcessMetricsProfiler()
        child = self._make_child_mock()
        profiler.process.children = Mock(return_value=[child])

        profiler.get_metrics_summary()   # first call — sets _last_metrics with children
        summary = profiler.get_metrics_summary()  # second call — delta section visible

        assert "Total RSS Delta" in summary

    def test_get_metrics_summary_num_fds_omitted_when_none(self):
        """File Descriptors line is omitted when num_fds is None"""
        profiler = ProcessMetricsProfiler()
        profiler.process.num_fds = Mock(side_effect=AttributeError)

        summary = profiler.get_metrics_summary()

        assert "File Descriptors" not in summary

    # ------------------------------------------------------------------
    # _profiling_loop exception handling
    # ------------------------------------------------------------------

    def test_profiling_loop_logs_exception_and_continues(self):
        """Exceptions inside the loop are caught, logged, and do not kill the thread"""
        profiler = ProcessMetricsProfiler(interval_seconds=0)
        call_count = 0

        def flaky_summary():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('transient error')
            profiler._running = False  # stop after second call
            return "ok"

        profiler.get_metrics_summary = flaky_summary
        profiler._running = True
        profiler._profiling_loop()

        assert call_count == 2
