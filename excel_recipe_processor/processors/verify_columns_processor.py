"""
Column verification processor for Excel Recipe Processor.

excel_recipe_processor/processors/verify_columns_processor.py

Compare a stage's columns to a declared expectation, so the log can announce
that the source system changed its export before the change surfaces as a
confusing failure - or worse, a silent one - somewhere downstream.

The two directions of mismatch differ in severity, and get separate knobs:

- a MISSING expected column will break steps downstream anyway; failing here
  fails faster with a better message (default: error)
- a NEW unexpected column is usually survivable - shaping steps ignore
  strangers - but it is exactly the event worth a loud line in the log
  (default: warn)

Reads its stage without re-saving anything, so a full-size frame is not
duplicated just to be inspected.
"""

import logging

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class VerifyColumnsProcessor(FileOpsBaseProcessor):
    """
    Check a stage's columns against an expected list.

    Order is deliberately NOT checked: downstream steps address columns by
    name, so a reordered export is harmless, while a check that failed on
    reorder would train people to ignore it.
    """

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'stage': 'stg_to_verify',
            'expected_columns': ['some_column']
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)

        self.stage = self.get_config_value('stage', None)
        self.expected_columns = self.get_config_value('expected_columns', None)

        # What a column present in the data but absent from the expectation
        # does: "warn" (default) logs and proceeds; "error" halts.
        self.on_unexpected = self.get_config_value('on_unexpected', 'warn')

        # What an expected column absent from the data does: "error"
        # (default) halts - it would break downstream steps anyway, and this
        # failure names the actual cause; "warn" logs and proceeds.
        self.on_missing_expected = self.get_config_value('on_missing_expected', 'error')

        if not self.stage:
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'stage': the stage to verify"
            )

        if not self.expected_columns or not isinstance(self.expected_columns, list):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'expected_columns': a list of column names"
            )

        for knob, value in (('on_unexpected', self.on_unexpected),
                            ('on_missing_expected', self.on_missing_expected)):
            if value not in ('warn', 'error'):
                raise StepProcessorError(
                    f"Invalid {knob} '{value}'. Supported: warn, error"
                )

    def _validate_file_operation_config(self):
        """No file target: the operation inspects an in-memory stage."""
        return

    def perform_file_operation(self):
        """Compare actual columns to expected and react per the knobs."""
        if not StageManager.stage_exists(self.stage):
            raise StepProcessorError(
                f"Step '{self.step_name}': stage '{self.stage}' does not exist"
            )

        actual = list(StageManager.load_stage(self.stage).columns)
        expected = list(self.expected_columns)

        unexpected = [col for col in actual if col not in expected]
        missing = [col for col in expected if col not in actual]

        if not unexpected and not missing:
            logger.info(
                f"✅ Columns verified: all {len(expected)} expected columns "
                f"present, nothing extra"
            )
            return f"{len(expected)} columns verified"

        if missing:
            message = (
                f"Expected column(s) MISSING from '{self.stage}': {missing}. "
                f"The source system's export has changed shape."
            )
            if self.on_missing_expected == 'error':
                raise StepProcessorError(f"Step '{self.step_name}': {message}")
            logger.warning(f"⚠️  {message}")

        if unexpected:
            message = (
                f"NEW column(s) in '{self.stage}' not in the expected list: "
                f"{unexpected}. The source system added something - review "
                f"whether the recipe should use it, then add it to the "
                f"expected list either way so this notice retires."
            )
            if self.on_unexpected == 'error':
                raise StepProcessorError(f"Step '{self.step_name}': {message}")
            logger.warning(f"⚠️  {message}")

        return (
            f"column drift: {len(missing)} missing, {len(unexpected)} new"
        )

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Verify a stage\'s columns against an expected list, announcing new or missing columns',
            'directions': {
                'missing expected column': 'on_missing_expected: error (default) / warn',
                'new unexpected column': 'on_unexpected: warn (default) / error',
            },
            'order_checking': 'deliberately none - downstream steps address columns by name',
            'memory': 'reads the stage without re-saving it',
        }

# End of file #
