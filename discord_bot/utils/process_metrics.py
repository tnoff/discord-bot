"""
Process metrics utilities
Tracks process-level metrics (memory, CPU, etc.) using psutil and reports via logging
"""
import time
from threading import Thread

import psutil


class ProcessMetricsProfiler:
    """
    Profile process-level metrics using psutil
    Reports via periodic logging showing memory usage, CPU, threads, etc.
    """
    def __init__(self, logger, interval_seconds=15):
        self.logger = logger
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
            }

            return metrics
        except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
            self.logger.error(f"Error getting process metrics: {e}")
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
        self.logger.info("Process metrics profiler started (using psutil)")

        while self._running:
            try:
                # Get metrics summary and log it
                summary = self.get_metrics_summary()
                self.logger.info(f"Process metrics:\n{summary}")

            except Exception as e:
                self.logger.error(f"Error in process metrics loop: {e}", exc_info=True)

            time.sleep(self.interval_seconds)

    def start(self):
        """Start the background profiling thread"""
        if self._running:
            self.logger.warning("Process metrics profiler already running")
            return

        self._running = True
        self._thread = Thread(target=self._profiling_loop, daemon=True, name="ProcessMetrics")
        self._thread.start()
        self.logger.info(f"Process metrics profiler started (interval: {self.interval_seconds}s)")

    def stop(self):
        """Stop the background profiling thread"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

        self.logger.info("Process metrics profiler stopped")
