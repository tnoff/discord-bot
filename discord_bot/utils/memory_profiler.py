"""
Memory profiling utilities
Tracks memory allocations by location using tracemalloc and reports via logging
"""
import linecache
import tracemalloc
from threading import Thread
import time

class MemoryProfiler:
    """
    Profile Python memory usage by tracking allocation locations using tracemalloc
    Reports via periodic logging showing top allocation sites by file and line number
    """
    def __init__(self, logger, interval_seconds=60, top_n_lines=25):
        self.logger = logger
        self.interval_seconds = interval_seconds
        self.top_n_lines = top_n_lines
        self._running = False
        self._thread = None
        self._snapshot = None  # Store previous snapshot for comparison

    def get_top_allocations(self, n=None):
        """
        Get top N memory allocation sites
        Returns list of (filename, lineno, size, line_text) tuples
        """
        if n is None:
            n = self.top_n_lines

        if not tracemalloc.is_tracing():
            return []

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')

        results = []
        for stat in top_stats[:n]:
            frame = stat.traceback[0]
            # Get the actual line of code
            line = linecache.getline(frame.filename, frame.lineno).strip()
            results.append((frame.filename, frame.lineno, stat.size, line))

        return results

    def get_allocation_diff(self):
        """
        Get memory allocation differences since last snapshot
        Returns list of (filename, lineno, size_diff, line_text) tuples
        """
        if not tracemalloc.is_tracing():
            return []

        snapshot = tracemalloc.take_snapshot()

        if self._snapshot is None:
            # First snapshot, just show current allocations
            self._snapshot = snapshot
            return []

        # Compare with previous snapshot
        top_stats = snapshot.compare_to(self._snapshot, 'lineno')
        self._snapshot = snapshot

        results = []
        for stat in top_stats[:self.top_n_lines]:
            frame = stat.traceback[0]
            line = linecache.getline(frame.filename, frame.lineno).strip()
            results.append((frame.filename, frame.lineno, stat.size_diff, line))

        return results

    def get_snapshot_summary(self):
        """
        Get a human-readable summary of current memory state using tracemalloc
        """
        if not tracemalloc.is_tracing():
            return "Tracemalloc not running"

        allocations = self.get_top_allocations(self.top_n_lines)
        diffs = self.get_allocation_diff()

        lines = ["**Memory Snapshot (tracemalloc)**", ""]

        # Show top current allocations
        if allocations:
            lines.append(f"Top {len(allocations)} allocation sites by current size:")
            for i, (filename, lineno, size, line_text) in enumerate(allocations, 1):
                size_mb = size / (1024 * 1024)
                lines.append(f"#{i}: {filename}:{lineno}: {size_mb:.2f} MB")
                if line_text:
                    lines.append(f"    {line_text}")
        else:
            lines.append("No allocation data available")

        # Show memory growth/shrinkage since last snapshot
        if diffs:
            lines.append("")
            lines.append(f"Top {len(diffs)} allocation changes since last snapshot:")
            for i, (filename, lineno, size_diff, line_text) in enumerate(diffs, 1):
                size_diff_mb = size_diff / (1024 * 1024)
                sign = "+" if size_diff > 0 else ""
                lines.append(f"#{i}: {filename}:{lineno}: {sign}{size_diff_mb:.2f} MB")
                if line_text:
                    lines.append(f"    {line_text}")

        return "\n".join(lines)

    def _profiling_loop(self):
        """Background thread that collects and reports memory snapshots"""
        self.logger.info("Memory profiler started (using tracemalloc)")

        while self._running:
            try:
                # Get snapshot summary and log it
                summary = self.get_snapshot_summary()
                self.logger.info(f"Memory snapshot:\n{summary}")

            except Exception as e:
                self.logger.error(f"Error in memory profiling loop: {e}", exc_info=True)

            time.sleep(self.interval_seconds)

    def start(self):
        """Start the background profiling thread and tracemalloc"""
        if self._running:
            self.logger.warning("Memory profiler already running")
            return

        # Start tracemalloc if not already running
        if not tracemalloc.is_tracing():
            tracemalloc.start()

        self._running = True
        self._thread = Thread(target=self._profiling_loop, daemon=True, name="MemoryProfiler")
        self._thread.start()
        self.logger.info(f"Memory profiler started (interval: {self.interval_seconds}s, top {self.top_n_lines} lines)")

    def stop(self):
        """Stop the background profiling thread and tracemalloc"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

        # Stop tracemalloc
        if tracemalloc.is_tracing():
            tracemalloc.stop()

        self.logger.info("Memory profiler stopped")
