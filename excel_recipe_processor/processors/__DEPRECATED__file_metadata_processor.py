"""
File metadata import processor for Excel Recipe Processor.

excel_recipe_processor/processors/file_metadata_processor.py

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


class FileMetadataProcessor(ImportBaseProcessor):
    """
    Build a DataFrame of file metadata: File, Modified, Size (KB).

    Rows appear in the order the files are listed, so the sheet reads the way
    the recipe author arranged it rather than alphabetically.
    """

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'files': ['some_file.xlsx'],
            'save_to_stage': 'stg_file_metadata'
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

        if not self.files or not isinstance(self.files, list):
            raise StepProcessorError(
                f"Step '{self.step_name}' requires 'files': a list of paths"
            )

        if self.on_missing not in ('error', 'note', 'skip'):
            raise StepProcessorError(
                f"Invalid on_missing '{self.on_missing}'. Supported: error, note, skip"
            )

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'List file names, modification times, and sizes for provenance tabs',
            'file_columns': ['File', 'Modified', 'Size (KB)'],
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
                })
                continue

            stat = path.stat()

            rows.append({
                'File': path.name,
                # Naive local time, matching what Finder and ls show. Kept as
                # a real datetime so Excel can sort and format it.
                'Modified': datetime.fromtimestamp(stat.st_mtime).replace(microsecond=0),
                'Size (KB)': round(stat.st_size / 1024, 1),
            })

        frame = pd.DataFrame(rows, columns=['File', 'Modified', 'Size (KB)'])

        logger.info(f"📋 Collected metadata for {len(frame)} file(s)")

        return frame

# End of file #
