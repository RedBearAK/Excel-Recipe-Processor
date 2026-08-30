"""
Stage deletion processor for Excel Recipe Processor.

excel_recipe_processor/processors/free_stages_processor.py

Delete named stages mid-run to reclaim memory.

Stages normally live until the run ends, which is what keeps the whole
pipeline history addressable. For ordinary data sizes that history is cheap;
for large merged inputs the retained full-size frames become the bulk of the
process footprint, and the biggest single spike - the openpyxl phases at the
end - lands on top of them. Freeing stages after their last consumer trims
the plateau and, placed just before the file operations, cuts the peak.

Deletion never conflicts with --dump-stage: dumps are written the moment a
stage is saved, so by the time a free_stages step runs, any requested dump of
that stage already happened.
"""

import logging
from excel_recipe_processor.core.log_format import qblock

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class FreeStagesProcessor(FileOpsBaseProcessor):
    """
    Delete listed stages, logging how much memory each release returns.

    Protected stages refuse deletion - protection means "survives the run".
    A stage a later step still needs will fail loudly at that step's load,
    naming the missing stage, so a misplaced deletion cannot corrupt data,
    only halt the run.
    """

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'stages': ['stg_no_longer_needed']
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)

        self.stages = self.get_config_value('stages', None)

        # "error" (default) halts on a stage that does not exist - a typo in
        # a deletion list should be loud. "skip" tolerates absence, for
        # recipes with conditional branches where a stage may never have
        # been created.
        self.on_missing = self.get_config_value('on_missing', 'error')

        if not self.stages or not isinstance(self.stages, list):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'stages': a list of stage names to delete"
            )

        if self.on_missing not in ('error', 'skip'):
            raise StepProcessorError(
                f"Invalid on_missing '{self.on_missing}'. Supported: error, skip"
            )

    def _validate_file_operation_config(self):
        """No file target needed: the operation is on in-memory stages."""
        return

    def perform_file_operation(self):
        """Delete each listed stage, tallying the memory returned."""
        freed_bytes = 0
        deleted = []
        skipped = []

        for stage_name in self.stages:
            if not StageManager.stage_exists(stage_name):
                if self.on_missing == 'skip':
                    skipped.append(stage_name)
                    continue
                raise StepProcessorError(
                    f"Step '{self.step_name}': cannot free stage '{stage_name}': not found. "
                    f"If this stage is legitimately absent in some runs, set on_missing: skip"
                )

            frame = StageManager.load_stage(stage_name)
            if isinstance(frame, pd.DataFrame):
                freed_bytes += int(frame.memory_usage(deep=True).sum())

            try:
                StageManager.delete_stage(stage_name)
            except StageError as error:
                raise StepProcessorError(f"Step '{self.step_name}': {error}")

            deleted.append(stage_name)

        freed_mb = freed_bytes / (1024 * 1024)
        alive = len(StageManager._current_stages)
        logger.info(
            f"🧹 Freed {len(deleted)} stage(s), ~{freed_mb:.0f} MB returned; "
            f"{alive} stage(s) remain in memory:{qblock(deleted)}")

        if skipped:
            logger.info(f"   (skipped {len(skipped)} absent stage(s): {skipped})")

        return f"freed {len(deleted)} stage(s), ~{freed_mb:.0f} MB"

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Delete named stages mid-run to reclaim memory once their consumers have run',
            'safety': [
                'protected stages refuse deletion',
                'a missing stage halts by default (on_missing: skip to tolerate)',
                'a stage a later step needs fails loudly at that step, naming it',
                'never conflicts with --dump-stage: dumps write at save time',
            ],
            'reporting': 'logs stage count and approximate MB returned',
        }

# End of file #
