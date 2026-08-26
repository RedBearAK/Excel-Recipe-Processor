"""
Terminal-only liveness indication for long-running phases.

excel_recipe_processor/core/terminal_pulse.py

Silence during legitimate long work is indistinguishable from a hang,
and a timer-driven spinner is worse than silence: it keeps spinning
over a wedged process. The display here is HONEST by construction and
RIDES BELOW the log stream:

- pulse_tick() is called FROM working loops, so the counter advances
  only because the work advances.
- A refresher thread re-renders the LAST frame's elapsed clock every
  second: a frozen counter with a running clock is the truthful
  picture of "process alive, stuck inside one work unit".
- ByteGrowthPulse watches bytes accumulating during opaque calls
  (openpyxl's buffer save): growing bytes cannot be faked.
- A logging bridge wraps stream handlers: each log record first clears
  the pulse line, prints normally, then the pulse redraws beneath it -
  log lines stay pristine, the pulse rides the bottom.

Nothing touches the log FILES: pulse output is stderr with
carriage-return rewrites, enabled only on a TTY. ERP_PULSE=off
silences even on a TTY; ERP_PULSE=force enables without one (tests).
"""

import os
import sys
import time
import logging
import threading

_LOCK = threading.RLock()
_ACTIVE = None          # the pulse whose frame rides the bottom
_BRIDGE_INSTALLED = False


def _enabled() -> bool:
    mode = os.environ.get('ERP_PULSE', '').lower()
    if mode == 'off':
        return False
    if mode == 'force':
        return True
    stream = sys.stderr
    return hasattr(stream, 'isatty') and stream.isatty()


def _clear_line():
    sys.stderr.write("\r\033[K")
    sys.stderr.flush()


def _install_logging_bridge():
    """Wrap stream handlers so log records clear and redraw the pulse."""
    global _BRIDGE_INSTALLED
    if _BRIDGE_INSTALLED:
        return
    _BRIDGE_INSTALLED = True
    for handler in logging.getLogger().handlers:
        if not isinstance(handler, logging.StreamHandler):
            continue
        if getattr(handler.stream, 'name', '') not in ('<stderr>', '<stdout>'):
            continue  # file handlers stay untouched
        original_emit = handler.emit

        def bridged_emit(record, _original=original_emit):
            with _LOCK:
                active = _ACTIVE
                if active is not None:
                    _clear_line()
                _original(record)
                if active is not None:
                    active.redraw()
        handler.emit = bridged_emit


class TerminalPulse:
    """Work-driven bottom line with a liveness clock between ticks."""

    def __init__(self, label: str, min_interval: float = 0.2):
        self.label = label
        self.min_interval = min_interval
        self.enabled = _enabled()
        self.started = time.perf_counter()
        self._last_write = 0.0
        self._detail = ''
        self._refresher = None
        self._stop = threading.Event()
        if self.enabled:
            global _ACTIVE
            with _LOCK:
                _install_logging_bridge()
                _ACTIVE = self
            self._refresher = threading.Thread(
                target=self._refresh_loop, daemon=True)
            self._refresher.start()

    def _frame(self) -> str:
        elapsed = time.perf_counter() - self.started
        return f"⏳ {self.label} {self._detail} ({elapsed:.0f}s)"

    def redraw(self):
        sys.stderr.write("\r\033[K" + self._frame())
        sys.stderr.flush()

    def _refresh_loop(self):
        # The clock alone advances between ticks - liveness, labeled by
        # the unchanged counter beside it
        while not self._stop.wait(1.0):
            with _LOCK:
                if _ACTIVE is self:
                    self.redraw()

    def tick(self, detail: str = ''):
        if not self.enabled:
            return
        now = time.perf_counter()
        if now - self._last_write < self.min_interval:
            self._detail = detail
            return
        self._last_write = now
        self._detail = detail
        with _LOCK:
            if _ACTIVE is self:
                self.redraw()

    def done(self):
        if not self.enabled:
            return
        global _ACTIVE
        self._stop.set()
        with _LOCK:
            if _ACTIVE is self:
                _ACTIVE = None
                elapsed = time.perf_counter() - self.started
                # The final frame PERSISTS as a normal line - the record
                # of what ran and how long, not a vanishing act
                sys.stderr.write(
                    f"\r\033[K✅ {self.label} {self._detail} "
                    f"- {elapsed:.0f}s total\n")
                sys.stderr.flush()


def pulse_tick(detail: str = ''):
    """Tick the active pulse from any depth, no plumbing required."""
    with _LOCK:
        active = _ACTIVE
    if active is not None:
        active.tick(detail)


class ByteGrowthPulse:
    """Byte-watcher for opaque calls; rides the bottom like TerminalPulse."""

    def __init__(self, label: str, size_fn, interval: float = 0.25):
        self.label = label
        self.size_fn = size_fn
        self.interval = interval
        self.enabled = _enabled()
        self.started = time.perf_counter()
        self._shown = 'starting'
        self._stop = threading.Event()
        self._thread = None

    def _frame(self) -> str:
        elapsed = time.perf_counter() - self.started
        return f"⏳ {self.label}: {self._shown}, {elapsed:.0f}s elapsed"

    def redraw(self):
        sys.stderr.write("\r\033[K" + self._frame())
        sys.stderr.flush()

    _GLYPHS = '|/-\\'

    def _watch(self):
        beat = 0
        while not self._stop.wait(self.interval):
            beat += 1
            glyph = self._GLYPHS[beat % 4]
            try:
                size = self.size_fn()
                if size < 1024:
                    # openpyxl serializes worksheet XML in memory before
                    # zip bytes land - a true phase, labeled truthfully,
                    # with the beating glyph as the liveness signal
                    self._shown = f"{glyph} serializing (no bytes yet)"
                else:
                    self._shown = f"{glyph} {size / 1_048_576:.1f} MB written"
            except OSError:
                self._shown = f"{glyph} waiting for first bytes"
            with _LOCK:
                if _ACTIVE is self:
                    self.redraw()

    def __enter__(self):
        if self.enabled:
            global _ACTIVE
            with _LOCK:
                _install_logging_bridge()
                _ACTIVE = self
            self._thread = threading.Thread(target=self._watch, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        if self.enabled:
            global _ACTIVE
            with _LOCK:
                if _ACTIVE is self:
                    _ACTIVE = None
                    elapsed = time.perf_counter() - self.started
                    sys.stderr.write(
                        f"\r\033[K✅ {self.label}: {self._shown.lstrip('|/-\\ ')}"
                        f" - {elapsed:.0f}s total\n")
                    sys.stderr.flush()
        return False

# End of file #
