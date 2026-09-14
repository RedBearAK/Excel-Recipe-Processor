"""
Explicit workbook flush processor for Excel Recipe Processor.

excel_recipe_processor/processors/flush_workbooks_processor.py

Write session-held workbooks to disk NOW, instead of at run end.

The workbook session normally saves once, after the last step succeeds. A
recipe that needs a file on disk mid-run - handing it to an external tool,
checkpointing before a risky operation, or simply being DONE with a file it
will not touch again - can force the write here.

Two forms (2026-09-13):

    processor_type: "flush_workbooks"
    target_files: ["{output_dir}/vms_glossary.xlsx"]

  writes and closes the NAMED workbooks only. This is the form to reach for
  when a file is finished: it says which file, and it cannot be changed by a
  file operation someone adds earlier in the recipe. A named file that is
  not open in the session is an error - the step is in the wrong place.

    processor_type: "flush_workbooks"

  with no target_files writes and closes EVERY dirty workbook. That is a
  recipe-wide checkpoint, and it flushes whatever happens to be open at the
  time, so it belongs where that is the intent.

After either, a later file operation on a flushed path reloads it fresh,
which is correct because the disk copy is now the truth.
"""

import logging

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.core.log_format import q
from excel_recipe_processor.core.config_schema import Key, Schema
from excel_recipe_processor.core.workbook_session import WorkbookSession, WorkbookSessionError


logger = logging.getLogger(__name__)


class FlushWorkbooksProcessor(FileOpsBaseProcessor):
    """Save session workbooks immediately - the named ones, or all of them."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys: target_files names the workbooks to write and close (2026-09-13); absent, every dirty one is flushed."""
        return Schema([
            Key('target_files', 'list', item_kind='str',
                description='Workbooks to write and close now; the rest of the session is untouched. Absent: flush every dirty workbook.'),
        ])

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {}

    def _validate_file_operation_config(self):
        """Nothing to validate: the session knows what it holds."""
        return

    def perform_file_operation(self):
        """Flush the named workbooks, or the whole session, and report what was written."""
        target_files = self.get_config_value('target_files', None)
        if target_files:
            if not isinstance(target_files, list) or not all(isinstance(path, str) and path.strip() for path in target_files):
                raise StepProcessorError(
                    f"Flush step '{self.step_name}': 'target_files' must be a list of file paths")
            try:
                written = WorkbookSession.flush_paths(target_files)
            except WorkbookSessionError as error:
                raise StepProcessorError(f"Flush step '{self.step_name}': {error}")
            for key in written:
                logger.info(f"💾 Written and closed: {q(key)}")
            unchanged = len(target_files) - len(written)
            if unchanged:
                logger.info(f"💾 {unchanged} named workbook(s) had no unsaved changes; closed")
            return f"flushed {len(written)} of {len(target_files)} named workbook(s)"

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
            'description': 'Write session-held workbooks now: the named ones (target_files) or all',
            'when_to_use': 'a file is finished and will not be touched again; an external tool or risky operation needs it on disk mid-run',
            'named_form': 'target_files writes and closes only those; a named file not open in the session is an error',
            'after_effects': 'flushed workbooks leave the session; a later file operation on one reloads it from disk',
        }

# End of file #
