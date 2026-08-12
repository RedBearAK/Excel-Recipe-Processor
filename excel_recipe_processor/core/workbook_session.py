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
without saving. With the export bridge active (the normal pipeline case),
the export itself lives in the session, so a crash before flush leaves NO
output file at all - the same outcome as crashing anywhere earlier while
stages exist only in memory. The run failed; rerun it. A recipe that wants
a post-export checkpoint on disk can place a flush_workbooks step right
after its export step, at the cost of the save the bridge would have
skipped.

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
    def adopt_workbook(cls, file_path, workbook) -> None:
        """
        Register an already-populated workbook as the live session copy.

        The export bridge: instead of the export step saving to disk only
        for the next file operation to read the identical bytes back, the
        export hands its in-memory workbook straight to the session. The
        adopted workbook is immediately dirty - it exists nowhere on disk
        yet, so it MUST be written at flush.

        Failure semantics follow from that: a crash between adoption and
        flush leaves NO output file at all - the same outcome as crashing
        anywhere earlier in the pipeline while stages are only in memory.
        A recipe that wants a post-export checkpoint on disk can place a
        flush_workbooks step right after the export.

        Args:
            file_path: Destination path the workbook will save to at flush
            workbook:  Fully populated openpyxl Workbook

        Raises:
            WorkbookSessionError: If the path already has a session workbook
        """
        key = cls._key(file_path)

        if key in cls._open_workbooks:
            raise WorkbookSessionError(
                f"adopt_workbook for a path the session already holds: {key}. "
                f"Two steps exported to the same file without a flush between."
            )

        if not Path(key).parent.is_dir():
            # Fail at the EXPORT step, the way a direct save would - not at
            # the end-of-run flush, where a missing directory would surface
            # long after its cause and could interrupt the batch of saves.
            raise WorkbookSessionError(
                f"Export destination directory does not exist: {Path(key).parent}"
            )

        if not cls._deferred:
            # Standalone semantics: no session lifecycle exists to flush
            # later, so adoption degenerates to an immediate save.
            started = time.perf_counter()
            workbook.save(key)
            logger.info(
                f"💾 Workbook saved in {time.perf_counter() - started:.1f}s "
                f"(immediate; no pipeline session active)"
            )
            return

        cls._open_workbooks[key] = workbook
        cls._dirty_paths.add(key)
        logger.info(f"🔗 Export bridged to the session; the file writes once, at run end")

    @classmethod
    def set_deferred(cls, deferred: bool) -> None:
        """Pipeline lifecycle hook: batch saves (True) or save-per-step (False)."""
        cls._deferred = bool(deferred)

    @classmethod
    def is_deferred(cls) -> bool:
        """Whether a pipeline session is batching saves right now."""
        return cls._deferred

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
