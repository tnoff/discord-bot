"""
Tests for memory profiling utilities
"""
  # pylint: disable=protected-access
import time
from unittest.mock import Mock, patch

from discord_core.utils.memory_profiler import MemoryProfiler


class TestMemoryProfiler:
    """Tests for MemoryProfiler class"""

    def test_init(self):
        """Test MemoryProfiler initialization"""
        tracker = MemoryProfiler(interval_seconds=30, top_n_lines=25)
        assert tracker.interval_seconds == 30
        assert tracker.top_n_lines == 25
        assert tracker._running is False
        assert tracker._thread is None

    def test_start_stop(self):
        """Test starting and stopping the tracker"""
        tracker = MemoryProfiler(interval_seconds=1)

        # Should not be running initially
        assert tracker._running is False

        # Start the tracker
        tracker.start()
        assert tracker._running is True
        assert tracker._thread is not None
        assert tracker._thread.is_alive()

        # Stop the tracker
        tracker.stop()
        assert tracker._running is False

    def test_start_already_running(self):
        """Test starting tracker when already running"""
        tracker = MemoryProfiler(interval_seconds=1)

        tracker.start()
        assert tracker._running is True

        # Starting again should not crash
        tracker.start()
        assert tracker._running is True

        tracker.stop()

    def test_get_snapshot_summary(self):
        """Test getting a human-readable snapshot summary"""
        tracker = MemoryProfiler(top_n_lines=10)

        # Start tracemalloc
        tracker.start()

        try:
            summary = tracker.get_snapshot_summary()

            # Should be a string with snapshot content
            assert isinstance(summary, str)
            assert len(summary) > 0
            assert "Memory Snapshot" in summary
            assert "allocation sites" in summary.lower()
        finally:
            tracker.stop()

    def test_get_top_allocations(self):
        """Test getting top allocation sites"""
        tracker = MemoryProfiler(top_n_lines=5)

        # Start tracemalloc
        tracker.start()

        try:
            # Allocate some memory
            _data = [list(range(1000)) for _ in range(10)]

            allocations = tracker.get_top_allocations(n=5)

            # Should return a list of tuples
            assert isinstance(allocations, list)
            assert len(allocations) <= 5

            # Each item should be (filename, lineno, size, line_text)
            if allocations:  # May be empty in some test environments
                filename, lineno, size, line_text = allocations[0]
                assert isinstance(filename, str)
                assert isinstance(lineno, int)
                assert isinstance(size, int)
                assert isinstance(line_text, str)
                assert size >= 0
                assert lineno > 0
        finally:
            tracker.stop()

    def test_get_allocation_diff(self):
        """Test getting allocation differences between snapshots"""
        tracker = MemoryProfiler(top_n_lines=5)

        tracker.start()

        try:
            # First call returns empty (establishes baseline)
            diff1 = tracker.get_allocation_diff()
            assert isinstance(diff1, list)

            # Allocate more memory
            _data = [list(range(1000)) for _ in range(10)]

            # Second call should show differences
            diff2 = tracker.get_allocation_diff()
            assert isinstance(diff2, list)

            # Each item should be (filename, lineno, size_diff, line_text)
            if diff2:
                filename, lineno, size_diff, line_text = diff2[0]
                assert isinstance(filename, str)
                assert isinstance(lineno, int)
                assert isinstance(size_diff, int)  # Can be positive or negative
                assert isinstance(line_text, str)
        finally:
            tracker.stop()

    def test_memory_profiling_with_logging(self):
        """Test that memory snapshots are logged"""
        mock_logger = Mock()
        tracker = MemoryProfiler(interval_seconds=1, top_n_lines=10)
        with patch('discord_core.utils.memory_profiler.logger', mock_logger):
            # Start tracker and wait for snapshot
            tracker.start()
            time.sleep(1.5)  # Wait for one snapshot
            tracker.stop()

        # Verify logger was called
        assert mock_logger.info.called

        # Check that snapshot summary was logged
        calls = mock_logger.info.call_args_list
        snapshot_logged = any('Memory snapshot' in str(call) or 'Memory Snapshot' in str(call) for call in calls)
        assert snapshot_logged, "Memory snapshot should be logged"

    def test_get_top_allocations_defaults_to_top_n_lines(self):
        """Without n, the configured top_n_lines caps the result"""
        tracker = MemoryProfiler(top_n_lines=3)
        tracker.start()
        try:
            _data = [list(range(1000)) for _ in range(10)]
            assert len(tracker.get_top_allocations()) <= 3
        finally:
            tracker.stop()

    def test_not_tracing_returns_empty_results(self):
        """Every reader degrades gracefully when tracemalloc is off"""
        tracker = MemoryProfiler()
        with patch('discord_core.utils.memory_profiler.tracemalloc.is_tracing', return_value=False):
            assert not tracker.get_top_allocations()
            assert not tracker.get_allocation_diff()
            assert tracker.get_snapshot_summary() == "Tracemalloc not running"

    def test_get_snapshot_summary_without_allocations(self):
        """An empty allocation list says so instead of printing an empty header"""
        tracker = MemoryProfiler()
        with patch('discord_core.utils.memory_profiler.tracemalloc.is_tracing', return_value=True), \
                patch.object(tracker, 'get_top_allocations', return_value=[]), \
                patch.object(tracker, 'get_allocation_diff', return_value=[]):
            summary = tracker.get_snapshot_summary()
        assert "No allocation data available" in summary
        assert "allocation changes" not in summary

    def test_profiling_loop_survives_summary_error(self):
        """An error building a snapshot is logged and the loop keeps going"""
        tracker = MemoryProfiler(interval_seconds=0)
        tracker._running = True
        mock_logger = Mock()

        def stop_after_sleep(_seconds):
            tracker._running = False

        with patch('discord_core.utils.memory_profiler.logger', mock_logger), \
                patch.object(tracker, 'get_snapshot_summary', side_effect=RuntimeError('boom')), \
                patch('discord_core.utils.memory_profiler.time.sleep', side_effect=stop_after_sleep):
            tracker._profiling_loop()

        mock_logger.warning.assert_called_once()
        assert 'boom' in mock_logger.warning.call_args[0][0]
