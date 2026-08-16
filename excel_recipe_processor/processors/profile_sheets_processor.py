"""
Profile sheets: per-column metadata as an ordinary stage.

excel_recipe_processor/processors/profile_sheets_processor.py

Part of the profile_* family (profile_files, profile_sheets, and the
planned profile_workbooks): discovery processors that store metadata
as plain stages for OTHER steps to consume. There are deliberately no
"apply" siblings - consumers are existing processors with directives
that read the profile by column name (the first: format_excel's
column_widths_from_stage, which lets FILTER-spill view tabs inherit
the seed sheet's calculated widths instead of accumulating hand-set
overrides).

Inputs are plural: each entry profiles either a stage (source_stage)
or an on-disk sheet (input_file + optional sheet_name). The
memory-vs-disk distinction is a pointer choice, not a mode - both
kinds land in the same output with the same facts, identified by the
Source column ("stg_name" or "file.xlsx!Sheet").

OUTPUT CONTRACT (columns by name; future facts APPEND, never rename):
    Source          Where the sheet came from
    Column          Header name
    Position        1-based column position
    Width           Auto-fit width via the SHARED clamp - the same
                    arithmetic format_excel's auto-fit uses, so a view
                    inheriting these matches what the seed sheet's own
                    auto-fit would compute. Exact for plain data;
                    workbook-side font/bold factors are approximated
                    as none (stages have no fonts).
    Dtype           pandas dtype string
    Blank_Count     NA values plus empty-string values
    Distinct_Count  Distinct NON-BLANK values (NA and empty string both
                    excluded). An empty string does not survive an Excel
                    write/read round trip - it becomes an empty cell - so
                    counting it would make stage-side and disk-side
                    profiles of the SAME data disagree.
    Row_Count       Data rows profiled

PLANNED (needs openpyxl-side reading, not yet implemented): a
number-format census and header-style survey for disk sheets, for
mirroring seed formatting onto view tabs the same way widths are
inherited now.
"""

import logging

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import ImportBaseProcessor, StepProcessorError
from excel_recipe_processor.processors._helpers.column_width_scan import (
    BASE_PADDING,
    DEFAULT_MIN_WIDTH,
    DEFAULT_MAX_WIDTH,
    scan_frame_column_widths,
)


logger = logging.getLogger(__name__)


class ProfileSheetsProcessor(ImportBaseProcessor):
    """Profile one or more sheets/stages into a per-column metadata stage."""

    @classmethod
    def get_minimal_config(cls):
        return {
            'sheets': [{'source_stage': 'stg_example_data'}],
            'save_to_stage': 'stg_sheet_profiles',
        }

    def __init__(self, step_config: dict):
        super().__init__(step_config)

        self.sheet_entries = self.get_config_value('sheets', None)
        self.min_width = self.get_config_value('min_width', DEFAULT_MIN_WIDTH)
        self.max_width = self.get_config_value('max_width', DEFAULT_MAX_WIDTH)
        self.padding = self.get_config_value('padding', BASE_PADDING)
        self.scan_rows = self.get_config_value('scan_rows', None)

        self._validate_sheet_entries(self.sheet_entries)

    def get_capabilities(self) -> dict:
        """Processor capabilities information."""
        return {
            'description': 'Per-column sheet metadata (widths, dtypes, blanks, distincts) stored as an ordinary stage for other steps to consume',
            'profile_columns': ['Source', 'Column', 'Position', 'Width',
                                'Dtype', 'Blank_Count', 'Distinct_Count',
                                'Row_Count'],
            'input_kinds': ['source_stage', 'input_file (+ sheet_name)'],
            'width_math': 'shared with format_excel auto-fit (column_width_scan helper)',
            'consumers': ['format_excel column_widths_from_stage'],
        }

    def load_data(self) -> pd.DataFrame:
        """Build the profile frame from every configured input."""
        rows = []
        for entry in self.sheet_entries:
            source_label, frame = self._resolve_entry(entry)
            widths = scan_frame_column_widths(
                frame, min_width=self.min_width, max_width=self.max_width,
                padding=self.padding, scan_rows=self.scan_rows)
            for position, (column_name, width, _length) in enumerate(widths, 1):
                series = frame[column_name]
                non_null = series.dropna()
                if len(non_null) > 0:
                    empty_mask = non_null.astype(str) == ''
                else:
                    empty_mask = non_null == non_null  # empty boolean series
                blank_count = int(series.isna().sum()) + int(empty_mask.sum())
                non_blank = non_null[~empty_mask] if len(non_null) > 0 else non_null
                rows.append({
                    'Source': source_label,
                    'Column': str(column_name),
                    'Position': position,
                    'Width': width,
                    'Dtype': str(series.dtype),
                    'Blank_Count': blank_count,
                    'Distinct_Count': int(non_blank.nunique()),
                    'Row_Count': len(frame),
                })

        profile = pd.DataFrame(rows, columns=[
            'Source', 'Column', 'Position', 'Width', 'Dtype',
            'Blank_Count', 'Distinct_Count', 'Row_Count'])
        logger.info(
            f"📊 Profiled {len(self.sheet_entries)} sheet(s): "
            f"{len(profile)} column rows")
        return profile

    def _validate_sheet_entries(self, sheet_entries) -> None:
        """Guided errors for the sheets list shape."""
        if not isinstance(sheet_entries, list) or len(sheet_entries) == 0:
            raise StepProcessorError(
                f"Step '{self.step_name}': 'sheets' must be a non-empty "
                f"list of entries, each with either source_stage OR "
                f"input_file (+ optional sheet_name)"
            )
        for index, entry in enumerate(sheet_entries, 1):
            if not isinstance(entry, dict):
                raise StepProcessorError(
                    f"Step '{self.step_name}': sheets entry {index} must "
                    f"be a mapping, got {type(entry).__name__}"
                )
            has_stage = 'source_stage' in entry
            has_file = 'input_file' in entry
            if has_stage == has_file:
                raise StepProcessorError(
                    f"Step '{self.step_name}': sheets entry {index} needs "
                    f"exactly ONE of source_stage or input_file, got "
                    f"{'both' if has_stage else 'neither'}"
                )

    def _resolve_entry(self, entry: dict):
        """(source label, DataFrame) for one sheets entry.

        Paths arrive already variable-substituted - the pipeline
        substitutes the whole step config before construction.
        """
        if 'source_stage' in entry:
            stage_name = entry['source_stage']
            return stage_name, StageManager.load_stage(stage_name)
        input_path = entry['input_file']
        sheet_name = entry.get('sheet_name', 0)
        frame = pd.read_excel(input_path, sheet_name=sheet_name)
        label = sheet_name if isinstance(sheet_name, str) else 'first sheet'
        return f"{input_path}!{label}", frame

# End of file #
