"""
Run log plumbing: mirror terminal lines to an attached log file, and hold
startup records until the log-file decision is made.

excel_recipe_processor/core/run_log.py

Moved here from core/main.py on 2026-11-02: the pipeline (core) imported
these from the CLI entry point (main), so the library depended on its own
front end - the dependency arrow pointed backwards. main.py now imports
them from here like any other caller; nothing about the behaviour moved.
"""

import logging

from pathlib import Path

from excel_recipe_processor.core.log_format import q


_attached_log_files = {'cli': None, 'recipe': None}
_attached_log_streams = []


def mirror_print(text='') -> None:
    """print() that also lands in any attached log file, verbatim.

    The run summary and the blank separator lines are deliberately
    UNPREFIXED terminal output (no 'INFO:'), so they live outside the
    logging system and no FileHandler ever sees them - the tail of the
    log file was silently missing the summary block until 2026-08-16.
    This writes the same bytes to both places: the terminal via print,
    and each attached log stream raw (no formatter, so the file matches
    the terminal exactly). Before any attachment it is just print.
    """
    print(text)
    if _attached_log_streams:
        for stream in _attached_log_streams:
            try:
                stream.write(str(text) + '\n')
                stream.flush()
            except Exception:
                pass  # a dead stream must not kill the run for a log line
    elif _early_buffer is not None:
        # No file yet: hold the line in emission order beside the log
        # records, so a later attach replays the head exactly
        _early_buffer.add_text(str(text))


class _EarlyLogBuffer(logging.Handler):
    """Holds startup log records until the log-file decision is made.

    A recipe's log_file directive cannot attach until external variables
    resolve, but the loading lines are worth keeping - so this buffer
    captures everything from startup, attach_log_file() flushes it as
    the FILE'S OPENING LINES, and the pipeline discards it at the seam
    when the recipe declines and no --log-file was given. Capped so a
    pathological pre-seam flood cannot grow memory unbounded.
    """

    MAX_RECORDS = 1000

    def __init__(self):
        super().__init__()
        # Ordered mixed items: ('record', LogRecord) from the logging
        # system, ('text', str) from mirror_print - so the head of the
        # file replays in TRUE emission order, blanks included.
        self.items = []

    def emit(self, record):
        if len(self.items) < self.MAX_RECORDS:
            self.items.append(('record', record))

    def add_text(self, text: str) -> None:
        if len(self.items) < self.MAX_RECORDS:
            self.items.append(('text', text))


_early_buffer = None


def install_early_log_buffer() -> None:
    """Start capturing startup records (called right after basicConfig)."""
    global _early_buffer
    if _early_buffer is None:
        _early_buffer = _EarlyLogBuffer()
        logging.getLogger().addHandler(_early_buffer)


def discard_early_log_buffer() -> None:
    """The decision point passed with no log file wanted: drop the buffer."""
    global _early_buffer
    if _early_buffer is not None:
        logging.getLogger().removeHandler(_early_buffer)
        _early_buffer.items.clear()
        _early_buffer = None


def attach_log_file(file_path, source='cli') -> bool:
    """
    Mirror the log stream to a file - same format, emoji and all.

    The stream carries no ANSI codes (plain basicConfig), so the file is
    byte-identical to the terminal's content. UTF-8 is explicit so the
    Unicode symbols survive on every platform. Overwrites per run (each
    run's log pairs with that run's output file). A recipe-settings
    attachment is skipped when the CLI already attached one - the person
    at the command line outranks the recipe.

    EXTENSION POLICY (2026-08-16, decided): the path is taken VERBATIM -
    no extension appended, corrected, or warned about. Rewriting an
    extensionless path would be the framework overriding stated intent,
    and the 🪵 announcement (terminal + first line of the file itself)
    already makes any path mistake immediately visible. House default is
    '_log.txt': the NAME carries the semantics, the txt EXTENSION the
    ergonomics (double-click opens a text editor everywhere; macOS
    routes .log to Console.app, which reads worse for this content).
    Anyone preferring .log simply writes it.

    Returns True if attached, False if skipped.
    """
    if source == 'recipe' and _attached_log_files['cli']:
        logging.getLogger(__name__).info(
            f"🪵 Recipe log_file skipped; --log-file already active: "
            f"{_attached_log_files['cli']}")
        return False
    if _attached_log_files.get(source):
        return False

    resolved = Path(file_path).expanduser()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(resolved, mode='w', encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

    # The buffered startup lines become the FILE'S OPENING LINES - they
    # already reached the terminal, so they flush to the file handler
    # only, before it joins the root logger for live records.
    global _early_buffer
    if _early_buffer is not None:
        for kind, payload in _early_buffer.items:
            if kind == 'record':
                handler.handle(payload)
            else:
                handler.stream.write(payload + '\n')
        handler.stream.flush()
        discard_early_log_buffer()

    logging.getLogger().addHandler(handler)
    _attached_log_files[source] = str(resolved)
    _attached_log_streams.append(handler.stream)
    logging.getLogger(__name__).info(f"🪵 Logging to file: {q(resolved)}")
    return True


# End of file #
