"""
Tests for process metrics profiler
"""
# pylint: disable=redefined-outer-name,protected-access
import time
from unittest.mock import Mock

from discord_bot.utils.process_metrics import ProcessMetricsProfiler


class TestProcessMetricsProfiler:
    """Tests for ProcessMetricsProfiler class"""

    def test_init(self):
        """Test ProcessMetricsProfiler initialization"""
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(mock_logger, interval_seconds=15)
        assert profiler.interval_seconds == 15
        assert profiler._running is False
        assert profiler._thread is None

    def test_start_stop(self):
        """Test starting and stopping the profiler"""
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(mock_logger, interval_seconds=1)

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
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(mock_logger, interval_seconds=1)

        profiler.start()
        assert profiler._running is True

        # Starting again should not crash
        profiler.start()
        assert profiler._running is True

        profiler.stop()

    def test_get_process_metrics(self):
        """Test getting process metrics"""
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(mock_logger)

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
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(mock_logger)

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
        profiler = ProcessMetricsProfiler(mock_logger, interval_seconds=1)

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
        mock_logger = Mock()
        profiler = ProcessMetricsProfiler(mock_logger)

        # Get first summary (no deltas yet)
        summary1 = profiler.get_metrics_summary()
        assert "Memory Changes" not in summary1

        # Get second summary (should have deltas)
        time.sleep(0.1)  # Small delay
        summary2 = profiler.get_metrics_summary()
        assert "Memory Changes" in summary2
        assert "RSS Delta" in summary2
        assert "USS Delta" in summary2
