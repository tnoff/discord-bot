"""
Tests for the GC object-census profiler
"""
# pylint: disable=redefined-outer-name,protected-access
import time
from unittest.mock import Mock, patch

from discord_bot.utils.gc_census import GcCensusProfiler


class TestGcCensusProfiler:
    """Tests for GcCensusProfiler class"""

    def test_init(self):
        """Test GcCensusProfiler initialization"""
        profiler = GcCensusProfiler(interval_seconds=300, top_n=10)
        assert profiler.interval_seconds == 300
        assert profiler.top_n == 10
        assert profiler._running is False
        assert profiler._thread is None
        assert profiler._last_census is None

    def test_start_stop(self):
        """Test starting and stopping the profiler"""
        profiler = GcCensusProfiler(interval_seconds=1)

        assert profiler._running is False

        profiler.start()
        assert profiler._running is True
        assert profiler._thread is not None
        assert profiler._thread.is_alive()

        profiler.stop()
        assert profiler._running is False

    def test_start_already_running(self):
        """Test starting profiler when already running does not crash"""
        profiler = GcCensusProfiler(interval_seconds=1)

        profiler.start()
        assert profiler._running is True

        # Starting again should be a no-op, not a crash
        profiler.start()
        assert profiler._running is True

        profiler.stop()

    def test_stop_without_start(self):
        """Stopping a never-started profiler is safe (no thread to join)"""
        profiler = GcCensusProfiler()
        profiler.stop()
        assert profiler._running is False

    def test_get_census(self):
        """Census returns the total and a per-type Counter"""
        profiler = GcCensusProfiler()
        fake_objects = [1, 2, 'a', 'b', 'c', {}, {}]
        with patch('discord_bot.utils.gc_census.gc.get_objects',
                   return_value=fake_objects):
            census = profiler.get_census()

        assert census['total'] == 7
        assert census['counts']['int'] == 2
        assert census['counts']['str'] == 3
        assert census['counts']['dict'] == 2

    def test_summary_first_call_has_no_deltas(self):
        """First summary shows totals + top types but no delta/grower sections"""
        profiler = GcCensusProfiler(top_n=5)
        with patch('discord_bot.utils.gc_census.gc.get_objects',
                   return_value=['a', 'b', {}]):
            summary = profiler.get_census_summary()

        assert "GC Object Census" in summary
        assert "Objects (gc-tracked):" in summary
        assert "Total:" in summary
        assert "Top 5 types by count:" in summary
        assert "str" in summary
        assert "Total Delta" not in summary
        assert "Top growers" not in summary
        # A prior snapshot is now stored for the next call.
        assert profiler._last_census is not None

    def test_summary_second_call_shows_deltas_and_growers(self):
        """Second summary shows total delta, per-type deltas, and a growers section"""
        profiler = GcCensusProfiler(top_n=5)
        # Second snapshot has one extra 'dict' -> a positive grower.
        with patch('discord_bot.utils.gc_census.gc.get_objects',
                   side_effect=[['a', 'b', {}], ['a', 'b', {}, {}]]):
            profiler.get_census_summary()          # seeds _last_census
            summary = profiler.get_census_summary()  # deltas now visible

        assert "Total Delta" in summary
        assert "+" in summary  # a positive delta is rendered
        assert "Top growers (Delta count since last snapshot):" in summary
        assert "dict" in summary

    def test_summary_second_call_no_growth_omits_growers(self):
        """When nothing grows, the growers section is omitted (deltas still shown)"""
        profiler = GcCensusProfiler(top_n=5)
        # Second snapshot is smaller: every delta is <= 0, so no growers.
        with patch('discord_bot.utils.gc_census.gc.get_objects',
                   side_effect=[['a', 'b', {}], ['a']]):
            profiler.get_census_summary()
            summary = profiler.get_census_summary()

        assert "Total Delta" in summary
        assert "Top growers" not in summary

    def test_census_with_logging(self):
        """The background loop logs a census summary"""
        mock_logger = Mock()
        profiler = GcCensusProfiler(interval_seconds=1)
        with patch('discord_bot.utils.gc_census.logger', mock_logger):
            profiler.start()
            time.sleep(1.5)  # wait for at least one census
            profiler.stop()

        assert mock_logger.info.called
        calls = mock_logger.info.call_args_list
        census_logged = any('GC census' in str(call) for call in calls)
        assert census_logged, "GC census should be logged"

    def test_profiling_loop_logs_exception_and_continues(self):
        """Exceptions inside the loop are caught, logged, and don't kill the thread"""
        profiler = GcCensusProfiler(interval_seconds=0)
        call_count = 0

        def flaky_summary():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError('transient error')
            profiler._running = False  # stop after the second call
            return "ok"

        profiler.get_census_summary = flaky_summary
        profiler._running = True
        profiler._profiling_loop()

        assert call_count == 2
