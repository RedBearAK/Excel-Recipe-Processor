"""
Explicit workbook flush processor for Excel Recipe Processor.

excel_recipe_processor/processors/flush_workbooks_processor.py

Write every session-held workbook to disk NOW, instead of at run end.

The workbook session normally saves once, after the last step succeeds. A
recipe that needs the file on disk mid-run - handing it to an external tool,
or checkpointing before a risky operation - can force the write here. After
a flush the session is empty; a later file operation on the same path
reloads it fresh, which is correct because the disk copy is now the truth.
"""

import logging

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor
from excel_recipe_processor.core.config_schema import Schema
from excel_recipe_processor.core.workbook_session import WorkbookSession


logger = logging.getLogger(__name__)


class FlushWorkbooksProcessor(FileOpsBaseProcessor):
    """Save all dirty session workbooks immediately."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-04): none beyond the step keys - it flushes the workbook session."""
        return Schema([])

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {}

    def _validate_file_operation_config(self):
        """Nothing to validate: the session knows what it holds."""
        return

    def perform_file_operation(self):
        """Flush the session and report what was written."""
        written = WorkbookSession.flush_all()

        if written == 0:
            logger.info("💾 No unsaved workbooks in the session; nothing to flush")

        return f"flushed {written} workbook(s)"

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Write every session-held workbook to disk now, instead of at run end',
            'when_to_use': 'an external tool or risky operation needs the file on disk mid-run',
            'after_effects': 'the session empties; later file operations reload from disk',
        }

# End of file #
