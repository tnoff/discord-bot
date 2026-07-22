"""
GC object-census utilities
Periodically censuses the live Python object graph via ``gc.get_objects()`` and
reports, each interval, the total object count and the top object types by count
with per-type deltas.

This names a slow *object-retention* leak that the other probes can't: tracemalloc
misses it when the retention is in CPython raw-domain allocations (the Python heap
reads flat), and heaptrack can't attribute it because it can't unwind Python frames
(every allocation collapses to the eval loop). A type whose count marches up
snapshot-over-snapshot is the leak.

Note: ``gc.get_objects()`` returns only gc-tracked objects (containers, instances —
anything that can participate in a reference cycle). Atomic leaked objects (bare
int/str/bytes) are not tracked and won't appear here; retention leaks are almost
always containers/instances accumulating, which this does catch.
"""
import gc
import logging
import time
from collections import Counter
from threading import Thread

logger = logging.getLogger(__name__)


class GcCensusProfiler:
    """
    Census the live Python object graph on a background thread and log, each
    interval, the total object count plus the top types by count with deltas.
    Opt-in — ``gc.get_objects()`` over a large heap is not free — so it runs only
    when ``monitoring.gc_census.enabled`` is set.
    """
    def __init__(self, interval_seconds=300, top_n=25):
        self.interval_seconds = interval_seconds
        self.top_n = top_n
        self._running = False
        self._thread = None
        self._last_census = None

    def get_census(self):
        """
        Census the live object graph.
        Returns a dict: ``total`` (gc-tracked object count) and ``counts`` (a
        ``Counter`` of type qualname -> count, full — not just the top rows — so
        deltas stay correct for a type that later enters or leaves the top list).
        """
        objects = gc.get_objects()
        counts = Counter(type(obj).__qualname__ for obj in objects)
        return {
            'total': len(objects),
            'counts': counts,
        }

    def get_census_summary(self):
        """
        Human-readable census summary; tracks deltas versus the previous snapshot.
        """
        census = self.get_census()
        counts = census['counts']
        last = self._last_census

        lines = ["**GC Object Census**", ""]
        lines.append("Objects (gc-tracked):")
        lines.append(f"  Total:              {census['total']:>12,d}")
        if last is not None:
            lines.append(f"  Total Delta:        {census['total'] - last['total']:>+12,d}")

        # Top types by absolute count, with a delta versus the previous snapshot.
        lines.append("")
        lines.append(f"Top {self.top_n} types by count:")
        for type_name, count in counts.most_common(self.top_n):
            if last is not None:
                delta = count - last['counts'].get(type_name, 0)
                lines.append(f"  {type_name:<28} {count:>12,d}  ({delta:>+9,d})")
            else:
                lines.append(f"  {type_name:<28} {count:>12,d}")

        # Top growers by delta — the leak signal. A leaking type can grow steadily
        # without ever being large enough to make the top-by-count list, so surface
        # the biggest positive deltas separately. Only meaningful with a prior snapshot.
        if last is not None:
            growers = sorted(
                ((name, count - last['counts'].get(name, 0)) for name, count in counts.items()),
                key=lambda kv: kv[1], reverse=True,
            )
            growers = [(name, delta) for name, delta in growers[:self.top_n] if delta > 0]
            if growers:
                lines.append("")
                lines.append("Top growers (Delta count since last snapshot):")
                for type_name, delta in growers:
                    lines.append(f"  {type_name:<28} {delta:>+12,d}")

        self._last_census = census
        return "\n".join(lines)

    def _profiling_loop(self):
        """Background thread that censuses the object graph and logs it."""
        logger.info("GC census profiler loop started")

        while self._running:
            try:
                summary = self.get_census_summary()
                logger.info(f"GC census:\n{summary}")

            except Exception as e:
                logger.warning(f"Error in GC census loop: {e}", exc_info=True)

            time.sleep(self.interval_seconds)

    def start(self):
        """Start the background census thread"""
        if self._running:
            logger.debug("GC census profiler already running")
            return

        self._running = True
        self._thread = Thread(target=self._profiling_loop, daemon=True, name="GcCensus")
        self._thread.start()
        logger.info(f"GC census profiler started (interval: {self.interval_seconds}s)")

    def stop(self):
        """Stop the background census thread"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

        logger.info("GC census profiler stopped")
