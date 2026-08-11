"""
Shared openpyxl workbook session for Excel Recipe Processor.

excel_recipe_processor/core/workbook_session.py

Consecutive file-operation steps (named ranges, formula seeding, formatting)
each targeted the same output file, and each paid a full openpyxl
load-and-save round trip to do it - on a large workbook, most of their
runtime. This session keeps one live Workbook object per file path: the
first step's load is the only load, every later step mutates the same
in-memory object, and one save at the end of the run writes everything.

Correctness falls out of identity: the named ranges written by one step are
simply PRESENT in the object the next step seeds, because it is the same
object. There is no merge, no reconciliation, no rereading.

Failure semantics: on any step failure the pipeline DISCARDS the session
without saving. The exported file on disk stays exactly as the export step
wrote it, and a rerun regenerates the file operations from scratch - better
than resuming from a half-modified workbook.

Recipes may open any number of files; the session tracks them all by
absolute path and flushes every dirty one at pipeline end. A recipe can
force an earlier write with the flush_workbooks processor.
"""

import time
import logging

import openpyxl

from pathlib import Path


logger = logging.getLogger(__name__)


class WorkbookSessionError(Exception):
    """Raised when the session is used inconsistently."""
    pass


class WorkbookSession:
    """
    Class-level cache of open workbooks, keyed by resolved absolute path.

    Deliberately parallel to StageManager: class methods, explicit
    lifecycle calls from the pipeline, loud failures over silent surprises.
    """

    _open_workbooks: dict    = {}       # dict[str, openpyxl Workbook]
    _dirty_paths: set        = set()

    # Deferred mode is OPT-IN by the pipeline. Standalone callers (tests,
    # ad-hoc scripts) keep the legacy semantics: mark_dirty saves right
    # away, so a processor used outside a pipeline still writes its file.
    # The pipeline turns deferral on at run start and flushes at run end.
    _deferred: bool          = False

    @classmethod
    def _key(cls, file_path) -> str:
        """Resolve to an absolute path so aliases share one workbook."""
        return str(Path(file_path).resolve())

    @classmethod
    def get_workbook(cls, file_path):
        """
        Return the live workbook for this path, loading it on first request.

        Later calls return the SAME object, so mutations accumulate across
        steps. data_only and read_only views are deliberately not offered:
        a values-only or frozen copy of a cached workbook would fork
        reality between steps.

        Args:
            file_path: Path to an existing .xlsx file

        Returns:
            The (possibly already-mutated) openpyxl Workbook
        """
        key = cls._key(file_path)

        if key not in cls._open_workbooks:
            started = time.perf_counter()
            cls._open_workbooks[key] = openpyxl.load_workbook(key, data_only=False)
            logger.info(f"⏱️  Workbook loaded in {time.perf_counter() - started:.1f}s (session)")

        return cls._open_workbooks[key]

    @classmethod
    def mark_dirty(cls, file_path) -> None:
        """
        Record that this workbook owes a save.

        No write happens here; the pipeline flushes at run end (or a
        flush_workbooks step does it sooner).
        """
        key = cls._key(file_path)

        if key not in cls._open_workbooks:
            raise WorkbookSessionError(
                f"mark_dirty for a path the session never loaded: {key}"
            )

        if not cls._deferred:
            started = time.perf_counter()
            cls._open_workbooks[key].save(key)
            logger.info(
                f"💾 Workbook saved in {time.perf_counter() - started:.1f}s "
                f"(immediate; no pipeline session active)"
            )
            return

        cls._dirty_paths.add(key)

    @classmethod
    def set_deferred(cls, deferred: bool) -> None:
        """Pipeline lifecycle hook: batch saves (True) or save-per-step (False)."""
        cls._deferred = bool(deferred)

    @classmethod
    def is_open(cls, file_path) -> bool:
        """Whether this path currently has a live session workbook."""
        return cls._key(file_path) in cls._open_workbooks

    @classmethod
    def flush_all(cls) -> int:
        """
        Save every dirty workbook and close the session.

        Returns:
            Number of files written
        """
        written = 0

        for key in sorted(cls._dirty_paths):
            started = time.perf_counter()
            cls._open_workbooks[key].save(key)
            logger.info(
                f"💾 Workbook saved in {time.perf_counter() - started:.1f}s (session): "
                f"{Path(key).name}"
            )
            written += 1

        cls.reset()
        return written

    @classmethod
    def discard_all(cls) -> int:
        """
        Close the session WITHOUT saving - the failure path.

        Files on disk stay exactly as the last explicit write (normally the
        export step) left them.

        Returns:
            Number of unsaved dirty workbooks discarded
        """
        discarded = len(cls._dirty_paths)

        if discarded:
            logger.warning(
                f"🗑️  Discarding {discarded} unsaved workbook(s) after failure; "
                f"files on disk are untouched since their last explicit write"
            )

        cls.reset()
        return discarded

    @classmethod
    def reset(cls) -> None:
        """Forget everything (start of each pipeline run, end of flush)."""
        cls._open_workbooks = {}
        cls._dirty_paths = set()
        cls._deferred = False

# End of file #
