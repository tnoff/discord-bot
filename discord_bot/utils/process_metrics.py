"""
Process metrics utilities
Tracks process-level metrics (memory, CPU, etc.) using psutil and reports via logging
"""
import ctypes
import logging
import time
from threading import Thread

import psutil

logger = logging.getLogger(__name__)


class _MallInfo2(ctypes.Structure):
    """
    ctypes mirror of glibc's ``struct mallinfo2`` (glibc >= 2.33), whose fields
    are ``size_t`` so they don't overflow past 2 GiB like the legacy ``mallinfo``.
    Only a few fields are surfaced; the rest are declared so the layout matches.
    """
    _fields_ = [
        ('arena', ctypes.c_size_t),     # non-mmapped heap bytes obtained via sbrk
        ('ordblks', ctypes.c_size_t),
        ('smblks', ctypes.c_size_t),
        ('hblks', ctypes.c_size_t),
        ('hblkhd', ctypes.c_size_t),    # bytes obtained via mmap
        ('usmblks', ctypes.c_size_t),
        ('fsmblks', ctypes.c_size_t),
        ('uordblks', ctypes.c_size_t),  # total in-use (allocated) bytes
        ('fordblks', ctypes.c_size_t),  # free bytes held by the allocator, not the OS
        ('keepcost', ctypes.c_size_t),  # releasable free space at the top of the heap
    ]


def get_glibc_malloc_stats():
    """
    Read glibc allocator internals via ``mallinfo2`` so we can tell apart
    *fragmentation* (bytes freed by the app but retained by the allocator,
    ``fordblks``) from a genuine *native leak* (in-use bytes, ``uordblks`` +
    mmapped ``hblkhd``, that keep growing). This is exactly the split tracemalloc
    cannot make, since it only sees Python-level allocations.

    Returns a dict of the interesting fields (bytes), or ``None`` on a non-glibc
    platform or a glibc older than 2.33 where the symbol is absent.
    """
    try:
        libc = ctypes.CDLL('libc.so.6')
        mallinfo2 = libc.mallinfo2
    except (OSError, AttributeError):
        return None
    mallinfo2.restype = _MallInfo2
    mallinfo2.argtypes = []
    info = mallinfo2()
    return {
        'arena': info.arena,
        'hblkhd': info.hblkhd,
        'in_use': info.uordblks,
        'free_retained': info.fordblks,
        'trimmable': info.keepcost,
    }

class ProcessMetricsProfiler:
    """
    Profile process-level metrics using psutil
    Reports via periodic logging showing memory usage, CPU, threads, etc.
    """
    def __init__(self, interval_seconds=15):
        self.interval_seconds = interval_seconds
        self._running = False
        self._thread = None
        self.process = psutil.Process()
        self._last_metrics = None

    def get_process_metrics(self):
        """
        Get current process metrics
        Returns dict with memory, CPU, and other process stats
        """
        try:
            # Get memory info
            mem_info = self.process.memory_info()
            mem_full = self.process.memory_full_info()

            # Get CPU info
            cpu_percent = self.process.cpu_percent(interval=0.1)

            # Get thread/file descriptor counts
            num_threads = self.process.num_threads()

            # Get open file descriptors (if available on platform)
            try:
                num_fds = self.process.num_fds()
            except (AttributeError, NotImplementedError):
                num_fds = None

            # Get child processes (like FFmpeg)
            children = []
            try:
                for child in self.process.children(recursive=False):
                    try:
                        child_info = {
                            'pid': child.pid,
                            'name': child.name(),
                            'cmdline': ' '.join(child.cmdline()[:3]),  # First 3 args
                            'rss': child.memory_info().rss,
                            'cpu_percent': child.cpu_percent(),
                        }
                        children.append(child_info)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # Child may have terminated
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            # Calculate totals including children
            total_rss = mem_info.rss + sum(c['rss'] for c in children)
            total_cpu = cpu_percent + sum(c['cpu_percent'] for c in children)

            metrics = {
                'rss': mem_info.rss,  # Resident Set Size (physical memory)
                'vms': mem_info.vms,  # Virtual Memory Size
                'uss': mem_full.uss,  # Unique Set Size (memory unique to process)
                'cpu_percent': cpu_percent,
                'num_threads': num_threads,
                'num_fds': num_fds,
                'children': children,
                'total_rss': total_rss,  # Including children
                'total_cpu': total_cpu,  # Including children
                'glibc': get_glibc_malloc_stats(),  # None on non-glibc platforms
            }

            return metrics
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            logger.warning(f"Error getting process metrics: {e}")
            return None

    def get_metrics_summary(self):
        """
        Get a human-readable summary of current process metrics
        """
        metrics = self.get_process_metrics()
        if not metrics:
            return "Unable to retrieve process metrics"

        lines = ["**Process Metrics**", ""]

        # Memory section - Main process
        lines.append("Main Process Memory:")
        lines.append(f"  RSS (Physical):     {metrics['rss'] / (1024**2):>8.2f} MB")
        lines.append(f"  VMS (Virtual):      {metrics['vms'] / (1024**2):>8.2f} MB")
        lines.append(f"  USS (Unique):       {metrics['uss'] / (1024**2):>8.2f} MB")

        # Child processes section
        if metrics['children']:
            lines.append("")
            lines.append(f"Child Processes ({len(metrics['children'])}):")
            for child in metrics['children']:
                child_rss_mb = child['rss'] / (1024**2)
                lines.append(f"  [{child['pid']}] {child['name']:<12} RSS: {child_rss_mb:>7.2f} MB  CPU: {child['cpu_percent']:>5.1f}%")
                # Show command line if it's FFmpeg or other interesting process
                if child['cmdline']:
                    lines.append(f"      {child['cmdline'][:80]}")

        # Total including children
        if metrics['children']:
            lines.append("")
            lines.append("Total (with children):")
            lines.append(f"  Total RSS:          {metrics['total_rss'] / (1024**2):>8.2f} MB")
            lines.append(f"  Total CPU:          {metrics['total_cpu']:>8.1f}%")

        # Calculate changes if we have previous metrics
        if self._last_metrics:
            rss_delta = metrics['rss'] - self._last_metrics['rss']
            uss_delta = metrics['uss'] - self._last_metrics['uss']
            total_rss_delta = metrics['total_rss'] - self._last_metrics.get('total_rss', metrics['rss'])

            lines.append("")
            lines.append("Memory Changes (since last snapshot):")
            lines.append(f"  Main RSS Delta:     {rss_delta / (1024**2):>+8.2f} MB")
            lines.append(f"  Main USS Delta:     {uss_delta / (1024**2):>+8.2f} MB")
            if metrics['children'] or self._last_metrics.get('children'):
                lines.append(f"  Total RSS Delta:    {total_rss_delta / (1024**2):>+8.2f} MB")

        # glibc allocator section — the fragmentation-vs-native-leak discriminator.
        # in_use climbing => genuine native retention; free_retained climbing (with
        # in_use flat) => arena fragmentation the OS never gets back (fix: malloc_trim).
        glibc = metrics.get('glibc')
        if glibc:
            lines.append("")
            lines.append("Glibc Allocator (mallinfo2):")
            lines.append(f"  In-use:             {glibc['in_use'] / (1024**2):>8.2f} MB")
            lines.append(f"  Free (retained):    {glibc['free_retained'] / (1024**2):>8.2f} MB")
            lines.append(f"  Heap (arena):       {glibc['arena'] / (1024**2):>8.2f} MB")
            lines.append(f"  Mmapped:            {glibc['hblkhd'] / (1024**2):>8.2f} MB")
            lines.append(f"  Trimmable:          {glibc['trimmable'] / (1024**2):>8.2f} MB")
            last_glibc = self._last_metrics.get('glibc') if self._last_metrics else None
            if last_glibc:
                in_use_delta = glibc['in_use'] - last_glibc['in_use']
                free_delta = glibc['free_retained'] - last_glibc['free_retained']
                lines.append(f"  In-use Delta:       {in_use_delta / (1024**2):>+8.2f} MB")
                lines.append(f"  Free-retained Delta:{free_delta / (1024**2):>+8.2f} MB")

        # CPU and resource section
        lines.append("")
        lines.append("Resources:")
        lines.append(f"  Main CPU Usage:     {metrics['cpu_percent']:>8.1f}%")
        lines.append(f"  Threads:            {metrics['num_threads']:>8d}")
        if metrics['num_fds'] is not None:
            lines.append(f"  File Descriptors:   {metrics['num_fds']:>8d}")

        # Store for next comparison
        self._last_metrics = metrics

        return "\n".join(lines)

    def _profiling_loop(self):
        """Background thread that collects and reports process metrics"""
        logger.info("Process metrics profiler started (using psutil)")

        while self._running:
            try:
                # Get metrics summary and log it
                summary = self.get_metrics_summary()
                logger.info(f"Process metrics:\n{summary}")

            except Exception as e:
                logger.warning(f"Error in process metrics loop: {e}", exc_info=True)

            time.sleep(self.interval_seconds)

    def start(self):
        """Start the background profiling thread"""
        if self._running:
            logger.debug("Process metrics profiler already running")
            return

        self._running = True
        self._thread = Thread(target=self._profiling_loop, daemon=True, name="ProcessMetrics")
        self._thread.start()
        logger.info(f"Process metrics profiler started (interval: {self.interval_seconds}s)")

    def stop(self):
        """Stop the background profiling thread"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

        logger.info("Process metrics profiler stopped")
