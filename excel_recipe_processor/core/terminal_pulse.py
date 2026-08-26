"""
Terminal-only liveness indication for long-running phases.

excel_recipe_processor/core/terminal_pulse.py

Silence during legitimate long work is indistinguishable from a hang,
and a timer-driven spinner is worse than silence: it keeps spinning
over a wedged process. Both tools here are HONEST by construction:

- TerminalPulse.tick() is called FROM the working loop, so its counter
  advances only because the work advances. A frozen loop freezes the
  display, which is exactly the truth.
- ByteGrowthPulse watches bytes accumulating during opaque calls
  (openpyxl's save into a buffer) where no loop is reachable. Bytes
  growing cannot be faked by a live thread over dead work; the elapsed
  clock alongside is labeled for what it is - process liveness, not
  progress.

Nothing here touches the logging module: output is stderr with
carriage-return rewrites, enabled only when stderr is a TTY, so
file-based logs and redirected runs stay byte-identical to today.
Set ERP_PULSE=off to silence even on a TTY; ERP_PULSE=force enables
without a TTY (testing).
"""

import os
import sys
import time
import threading


def _enabled() -> bool:
    mode = os.environ.get('ERP_PULSE', '').lower()
    if mode == 'off':
        return False
    if mode == 'force':
        return True
    stream = sys.stderr
    return hasattr(stream, 'isatty') and stream.isatty()


class TerminalPulse:
    """Work-driven progress line: tick() from the loop that does the work."""

    def __init__(self, label: str, min_interval: float = 0.25):
        self.label = label
        self.min_interval = min_interval
        self.enabled = _enabled()
        self.started = time.perf_counter()
        self._last_write = 0.0
        self._wrote_anything = False

    def tick(self, detail: str = ''):
        if not self.enabled:
            return
        now = time.perf_counter()
        if now - self._last_write < self.min_interval:
            return
        self._last_write = now
        elapsed = now - self.started
        sys.stderr.write(f"\r\033[K⏳ {self.label} {detail} ({elapsed:.0f}s)")
        sys.stderr.flush()
        self._wrote_anything = True

    def done(self):
        if self.enabled and self._wrote_anything:
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()


class ByteGrowthPulse:
    """Byte-watcher for opaque calls that write somewhere measurable.

    size_fn returns the bytes written so far - a BytesIO's tell during
    an in-memory save, or a file's on-disk size via for_file(). Bytes
    growing are unfakeable evidence the opaque call is really working;
    the elapsed clock beside them is labeled as liveness only. Use as a
    context manager around the opaque call.
    """

    def __init__(self, label: str, size_fn, interval: float = 0.5):
        self.label = label
        self.size_fn = size_fn
        self.interval = interval
        self.enabled = _enabled()
        self._stop = threading.Event()
        self._thread = None
        self._wrote_anything = False

    @classmethod
    def for_file(cls, label: str, path, interval: float = 0.5):
        def size_fn():
            return os.path.getsize(str(path))
        return cls(label, size_fn, interval)

    def _watch(self):
        started = time.perf_counter()
        while not self._stop.wait(self.interval):
            try:
                shown = f"{self.size_fn() / 1_048_576:.1f} MB written"
            except OSError:
                shown = "waiting for first bytes"
            elapsed = time.perf_counter() - started
            sys.stderr.write(
                f"\r\033[K⏳ {self.label}: {shown}, {elapsed:.0f}s elapsed")
            sys.stderr.flush()
            self._wrote_anything = True

    def __enter__(self):
        if self.enabled:
            self._thread = threading.Thread(target=self._watch, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if self.enabled and self._wrote_anything:
            sys.stderr.write("\r\033[K")
            sys.stderr.flush()
        return False

# End of file #
