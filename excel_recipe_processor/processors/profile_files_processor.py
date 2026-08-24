"""
File metadata import processor for Excel Recipe Processor.

excel_recipe_processor/processors/profile_files_processor.py

Produces a stage describing files rather than reading their contents: name,
last-modified time, and size. The motivating case is a provenance sheet in a
generated workbook - which vintage of each lookup file built this output -
where the answer lives in the filesystem, not in any cell.
"""

import logging

import pandas as pd

from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.base_processor import ImportBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class ProfileFilesProcessor(ImportBaseProcessor):
    """
    Profile files on disk: File, Modified, Size (KB), one row each.

    Part of the profile_* family (profile_files, profile_sheets, planned
    profile_workbooks) - discovery processors storing metadata as plain
    stages for other steps to consume. Renamed from file_metadata
    2026-08-15 when the family standardized; breaking change, per house
    preference.

    Rows appear in the order the files are listed, so the sheet reads the way
    the recipe author arranged it rather than alphabetically.
    """

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'files': ['some_file.xlsx'],
            'save_to_stage': 'stg_profiled_files'
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)

        self.files = self.get_config_value('files', None)

        # What to do about a listed file that does not exist.
        #
        #   "error"  stop and name it (the default): a provenance sheet that
        #            silently omits a missing input would hide exactly the
        #            problem it exists to surface
        #   "note"   include the row with MISSING in the Modified column
        #   "skip"   leave the row out
        self.on_missing = self.get_config_value('on_missing', 'error')

        # Adds a Path column with the absolute resolved path of each file.
        # Built for the Sources-tab file:// hyperlink pattern: the recipe
        # points a make_hyperlinks column rule at Path and every source
        # file becomes one click away. Opt-in so existing Sources tabs
        # keep their shape.
        self.include_full_paths = self.get_config_value('include_full_paths', False)

        if not self.files or not isinstance(self.files, list):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'files': a list of paths"
            )

        if self.on_missing not in ('error', 'note', 'skip'):
            raise StepProcessorError(
                f"Invalid on_missing '{self.on_missing}'. Supported: error, note, skip"
            )

        if not isinstance(self.include_full_paths, bool):
            raise StepProcessorError(
                f"Step '{self.step_name}': 'include_full_paths' must be "
                f"true or false, got {self.include_full_paths!r}"
            )

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Per-file metadata discovery (e.g., modification times, sizes)',
            'file_columns': ['File', 'Modified', 'Size (KB)'],
            'optional_columns': {'Path': 'include_full_paths: true'},
            'missing_file_handling': ['error', 'note', 'skip'],
            'preserves_listed_order': True,
        }

    def load_data(self) -> pd.DataFrame:
        """Stat each listed file and assemble the metadata frame."""
        rows = []

        for entry in self.files:
            path = Path(str(entry))

            if not path.exists():
                if self.on_missing == 'error':
                    raise StepProcessorError(
                        f"Step '{self.step_name}': file not found: {path}"
                    )
                if self.on_missing == 'skip':
                    logger.warning(f"⚠️  Skipping missing file: {path}")
                    continue
                rows.append({
                    'File': path.name,
                    'Modified': 'MISSING',
                    'Size (KB)': None,
                    # resolve() works on nonexistent paths too; a MISSING
                    # row pointing at where the file SHOULD be is exactly
                    # the diagnostic a provenance sheet is for
                    'Path': str(path.resolve()),
                })
                continue

            stat = path.stat()

            rows.append({
                'File': path.name,
                # Naive local time, matching what Finder and ls show. Kept as
                # a real datetime so Excel can sort and format it.
                'Modified': datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0),
                'Size (KB)': round(stat.st_size / 1024, 1),
                'Path': str(path.resolve()),
            })

        columns_to_emit = ['File', 'Modified', 'Size (KB)']
        if self.include_full_paths:
            columns_to_emit.append('Path')

        frame = pd.DataFrame(rows, columns=columns_to_emit)

        logger.info(f"📋 Collected metadata for {len(frame)} file(s)")

        return frame

# End of file #
