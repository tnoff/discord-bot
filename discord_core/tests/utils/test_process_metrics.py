"""
Tests for process metrics profiler
"""
  # pylint: disable=protected-access
import time
from unittest.mock import Mock, patch

import psutil

from discord_core.tests.utils.profiler_suite import ProfilerThreadSuite
from discord_core.utils.process_metrics import (
    ProcessMetricsProfiler,
    get_glibc_malloc_stats,
)


def _fake_mallinfo2(arena=1, hblkhd=2, uordblks=3, fordblks=4, keepcost=5):
    """A stand-in for the object glibc's mallinfo2() returns."""
    return Mock(arena=arena, hblkhd=hblkhd, uordblks=uordblks,
                fordblks=fordblks, keepcost=keepcost)


class TestProcessMetricsProfiler(ProfilerThreadSuite):
    """Tests for ProcessMetricsProfiler class"""
    profiler_cls = ProcessMetricsProfiler
    summary_method = 'get_metrics_summary'
    logger_path = 'discord_core.utils.process_metrics.logger'
    logged_marker = 'Process metrics'

    def test_init(self):
        """Test ProcessMetricsProfiler initialization"""
        profiler = ProcessMetricsProfiler(interval_seconds=15)
        assert profiler.interval_seconds == 15
        assert profiler._running is False
        assert profiler._thread is None

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

class TestGlibcMallocStats:
    """Tests for the glibc mallinfo2 allocator read"""

    def test_returns_mapped_fields(self):
        """mallinfo2 fields are mapped to the surfaced dict keys"""
        with patch('discord_core.utils.process_metrics.ctypes.CDLL') as cdll:
            cdll.return_value.mallinfo2.return_value = _fake_mallinfo2(
                arena=100, hblkhd=200, uordblks=300, fordblks=400, keepcost=50)

            stats = get_glibc_malloc_stats()

        assert stats == {
            'arena': 100,
            'hblkhd': 200,
            'in_use': 300,
            'free_retained': 400,
            'trimmable': 50,
        }

    def test_returns_none_when_libc_missing(self):
        """A platform without libc.so.6 yields None instead of raising"""
        with patch('discord_core.utils.process_metrics.ctypes.CDLL',
                   side_effect=OSError('no libc')):
            assert get_glibc_malloc_stats() is None

    def test_returns_none_when_symbol_absent(self):
        """An older glibc without the mallinfo2 symbol yields None"""
        with patch('discord_core.utils.process_metrics.ctypes.CDLL') as cdll:
            # Deleting the auto-created attr makes any later access raise
            # AttributeError, mimicking a libc that lacks the mallinfo2 symbol.
            del cdll.return_value.mallinfo2

            assert get_glibc_malloc_stats() is None


class TestGlibcSummarySection:
    """Tests for the glibc allocator section in the metrics summary"""

    def test_summary_includes_glibc_section(self):
        """Summary renders the allocator section when glibc stats are available"""
        profiler = ProcessMetricsProfiler()
        with patch('discord_core.utils.process_metrics.get_glibc_malloc_stats',
                   return_value={'arena': 1, 'hblkhd': 2, 'in_use': 3,
                                 'free_retained': 4, 'trimmable': 5}):
            summary = profiler.get_metrics_summary()

        assert "Glibc Allocator (mallinfo2)" in summary
        assert "In-use:" in summary
        assert "Free (retained):" in summary

    def test_summary_glibc_deltas_on_second_call(self):
        """In-use / free-retained deltas appear once a prior snapshot exists"""
        profiler = ProcessMetricsProfiler()
        with patch('discord_core.utils.process_metrics.get_glibc_malloc_stats',
                   side_effect=[
                       {'arena': 1, 'hblkhd': 2, 'in_use': 3,
                        'free_retained': 4, 'trimmable': 5},
                       {'arena': 1, 'hblkhd': 2, 'in_use': 9,
                        'free_retained': 7, 'trimmable': 5},
                   ]):
            profiler.get_metrics_summary()          # seeds _last_metrics
            summary = profiler.get_metrics_summary()  # deltas now visible

        assert "In-use Delta:" in summary
        assert "Free-retained Delta:" in summary

    def test_summary_omits_glibc_section_when_unavailable(self):
        """No allocator section on a platform where mallinfo2 returns None"""
        profiler = ProcessMetricsProfiler()
        with patch('discord_core.utils.process_metrics.get_glibc_malloc_stats',
                   return_value=None):
            summary = profiler.get_metrics_summary()

        assert "Glibc Allocator" not in summary
