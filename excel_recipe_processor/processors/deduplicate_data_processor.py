"""
Deduplication processor for Excel Recipe Processor.

excel_recipe_processor/processors/deduplicate_data_processor.py

Collapse rows to one per key, keeping every column and reporting any group
whose surviving values were actually in dispute.

The motivating case: a production export at line-item grain (orders joined to
their products) where dropping the item columns SHOULD leave identical rows
per order - but occasionally does not, because a product-less header line
carries different order attributes than the real lines. Plain full-row
drop_duplicates keeps every distinct variant, silently emitting two rows for
one key; blind keyed dedupe silently picks a winner. This processor picks the
winner AND says so, so a resolution is something chosen rather than something
that happened.
"""

import logging

import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.file_writer import FileWriter
from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class DeduplicateDataProcessor(BaseStepProcessor):
    """
    Collapse rows to unique key values, detecting and reporting conflicts.

    All columns are kept. That inverts the failure mode of dedupe-via-
    aggregation, where only enumerated columns survive and a column added
    upstream silently vanishes; here, forgetting a column changes nothing.

    A "conflict" is a key group whose non-key values genuinely differ. Pure
    repetition - the normal residue of a join - collapses without comment.
    """

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'key_columns': ['test_key_column'],
            'save_to_stage': 'stg_deduplicated'
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)

        self.key_columns = self.get_config_value('key_columns', None)

        # Which duplicate row survives: the first seen (default) or the last.
        self.keep = self.get_config_value('keep', 'first')

        # Optional stage receiving every row of every CONFLICTED key group -
        # kept and discarded alike - plus two explanatory columns:
        # 'Dedupe Status' (kept/discarded) and 'Conflicting Columns'.
        self.save_conflicts_to_stage = self.get_config_value('save_conflicts_to_stage', None)

        # Optional report file with the same contents, written ONLY when
        # conflicts exist: a clean run leaves no file, so the file's presence
        # is itself the signal. Meant to sit beside the source download as
        # evidence for cleaning up the source database.
        self.conflicts_file = self.get_config_value('conflicts_file', None)

        if not self.key_columns or not isinstance(self.key_columns, list):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'key_columns': a list of column names"
            )

        if self.keep not in ('first', 'last'):
            raise StepProcessorError(
                f"Invalid keep '{self.keep}'. Supported: first, last"
            )

    def execute(self, data):
        """Deduplicate by key, detect conflicts, and report them."""
        self.log_step_start()

        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires a pandas DataFrame"
            )

        missing = [col for col in self.key_columns if col not in data.columns]
        if missing:
            raise StepProcessorError(
                f"Step '{self.step_name}': key column(s) not found: {missing}. "
                f"Available: {list(data.columns)}"
            )

        # Empty in, empty out: a dedupe of nothing is nothing, not an error
        if data.empty:
            logger.warning(f"⚠️  '{self.step_name}': input is empty; nothing to deduplicate")
            self._emit_conflicts(data.head(0))
            self.log_step_complete("0 rows in, 0 rows out")
            return data.copy()

        result = data.drop_duplicates(subset=self.key_columns, keep=self.keep)

        conflicts = self._find_conflicts(data)
        self._emit_conflicts(conflicts)

        removed = len(data) - len(result)
        conflicted_keys = conflicts[self.key_columns].drop_duplicates() if len(conflicts) else conflicts

        logger.info(
            f"🔑 Deduplicated on {self.key_columns}: {len(data)} rows -> "
            f"{len(result)} unique key(s), {removed} duplicate row(s) removed"
        )

        if len(conflicts):
            logger.warning(
                f"⚠️  {len(conflicted_keys)} key(s) had CONFLICTING values across their "
                f"duplicate rows; '{self.keep}' won. Details follow."
            )
            self._log_conflict_details(conflicts)
        elif removed:
            logger.info("✓ All removed duplicates were pure repetition - no conflicts")

        self.log_step_complete(f"{len(result)} unique rows, {len(conflicted_keys)} conflicted key(s)")

        return result

    def _find_conflicts(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Return every row of every key group whose non-key values differ.

        The frame gains 'Dedupe Status' (kept/discarded) and 'Conflicting
        Columns' (comma-joined names of the columns that disagreed), so the
        report explains itself without the reader re-deriving anything.
        """
        value_columns = [col for col in data.columns if col not in self.key_columns]

        duplicated_mask = data.duplicated(subset=self.key_columns, keep=False)
        candidates = data[duplicated_mask]

        if candidates.empty or not value_columns:
            return data.head(0)

        conflict_frames = []
        kept_index = data.drop_duplicates(subset=self.key_columns, keep=self.keep).index

        for _, group in candidates.groupby(self.key_columns, dropna=False, sort=False):
            disputed = [
                col for col in value_columns
                if group[col].nunique(dropna=False) > 1
            ]
            if not disputed:
                continue

            annotated = group.copy()
            annotated['Dedupe Status'] = [
                'kept' if idx in kept_index else 'discarded' for idx in group.index
            ]
            annotated['Conflicting Columns'] = ', '.join(disputed)
            conflict_frames.append(annotated)

        if not conflict_frames:
            return data.head(0)

        return pd.concat(conflict_frames, ignore_index=True)

    def _log_conflict_details(self, conflicts: pd.DataFrame, max_keys: int = 10) -> None:
        """Log each conflicted key with its disputed columns and values."""
        shown = 0

        for key_vals, group in conflicts.groupby(self.key_columns, dropna=False, sort=False):
            if shown >= max_keys:
                remaining = conflicts[self.key_columns].drop_duplicates().shape[0] - shown
                logger.warning(f"   ... and {remaining} more conflicted key(s)")
                break

            key_text = key_vals if isinstance(key_vals, str) else ', '.join(str(v) for v in key_vals) if isinstance(key_vals, tuple) else str(key_vals)

            for col in str(group['Conflicting Columns'].iloc[0]).split(', '):
                values = group[col].astype(str).tolist()
                logger.warning(f"   🔀 [{key_text}] '{col}': {values} -> kept '{values[0 if self.keep == 'first' else -1]}'")

            shown += 1

    def _emit_conflicts(self, conflicts: pd.DataFrame) -> None:
        """Send conflicts to their stage and/or file, as configured."""
        if self.save_conflicts_to_stage:
            StageManager.save_stage(
                self.save_conflicts_to_stage, conflicts,
                description=f"Conflicted duplicate rows from '{self.step_name}'",
                step_name=self.step_name, overwrite=True
            )

        if not self.conflicts_file:
            return

        if conflicts.empty:
            logger.info(f"✓ No conflicts; not writing {Path(str(self.conflicts_file)).name}")
            return

        FileWriter.write_file(conflicts, self.conflicts_file, sheet_name='Collapse_Conflicts')
        logger.warning(f"📄 Conflict report written: {self.conflicts_file}")

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Collapse rows to one per key, keeping all columns and reporting groups whose values conflicted',
            'keep_options': ['first', 'last'],
            'conflict_outputs': [
                'per-key warning log with disputed columns and values',
                'optional stage of conflicted rows (kept and discarded, annotated)',
                'optional report file, written only when conflicts exist'
            ],
            'column_handling': 'all columns kept - nothing to enumerate, nothing to silently lose',
            'empty_input': 'empty output, not an error',
        }

# End of file #
