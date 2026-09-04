"""
Import file step processor for Excel automation recipes.

excel_recipe_processor/processors/import_file_processor.py

Pure stage-based file import - no pipeline data concept.
"""

import logging

import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.processors._helpers.sheet_addressing import resolve_sheet_ref
from excel_recipe_processor.core.base_processor import ImportBaseProcessor, StepProcessorError
from excel_recipe_processor.core.config_schema import Key, Schema, name_list

logger = logging.getLogger(__name__)


class ImportFileProcessor(ImportBaseProcessor):
    """
    Processor for importing data from external files into stages.
    
    Supports Excel, CSV, and TSV files with automatic format detection
    and variable substitution. Always saves to a declared stage.
    """
    
    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-03); see core/config_schema.py."""
        return Schema([
            Key('input_file', 'str', required=True),
            Key('sheet_name', 'any', default='?sheet_001?', description='Tab name, 1-based number, or ?sheet_NNN? token'),
            Key('header_row', 'int', default=1),
            Key('encoding', 'str', default='utf-8'), Key('separator', 'str', default=','),
            Key('format', 'str', choices=['xlsx', 'xls', 'csv', 'tsv']),
            name_list('verbatim_text_columns'),
            Key('on_missing_file', 'str', default='error', choices=['error', 'create_empty']),
        ], variants={'on_missing_file': {
            'error': Schema([]),
            'create_empty': Schema([name_list('create_empty_columns', required=True)]),
        }})

    @classmethod
    def get_minimal_config(cls):
        return {
            'input_file': 'test_data.xlsx',
            'save_to_stage': 'imported_data'  # Required for import processors
        }
    
    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Import Excel, CSV, or TSV files into stages, with sheet selection',
            'fast_excel_reader': 'when the optional python-calamine wheel is installed, '
                                 'Excel imports use it automatically (several times faster '
                                 'on large files, values identical); without it the '
                                 'openpyxl path serves unchanged - no configuration',
            'file_formats': ['xlsx', 'xls', 'xlsm', 'xlsb', 'csv', 'tsv', 'txt (as tsv)'],
            'excel_options': ['sheet selection by name or index'],
            'path_features': ['recipe variable substitution'],
            'header_row': "1-based row holding the column headers (default 1); "
                          "rows above it are discarded",
            'missing_file_policy': "on_missing_file: 'error' (default) or "
                                   "'create_empty' with declared "
                                   "create_empty_columns for fail-safe "
                                   "imports of files that may not exist yet",
        }

    def load_data(self):
        """Load data from file (implements ImportBaseProcessor abstract method)."""
        input_file = self.get_config_value('input_file')
        if not input_file:
            # Without this guard a missing key reaches Path(None) and dies
            # with a bare TypeError instead of a guided error.
            raise StepProcessorError(
                f"Import step '{self.step_name}' requires 'input_file'"
            )

        if 'sheet' in self.step_config:
            raise StepProcessorError(
                f"Import step '{self.step_name}': 'sheet' was replaced by "
                f"'sheet_name' (2026-08-14 sheet-addressing doctrine). A tab "
                f"name, or the '?sheet_001?' pseudo-name to address by "
                f"position."
            )
        sheet = self.get_config_value('sheet_name', '?sheet_001?')
        encoding = self.get_config_value('encoding', 'utf-8')
        separator = self.get_config_value('separator', ',')
        explicit_format = self.get_config_value('format', None)

        # Fail-safe for files that legitimately may not exist yet (a lookup
        # produced by a sibling recipe that has not run, a first-run
        # baseline). 'error' preserves the historical loud failure;
        # 'create_empty' stands up an empty stage with the DECLARED columns
        # so every downstream step (keys, filters, exports) stays valid.
        on_missing_file = self.get_config_value('on_missing_file', 'error')
        create_empty_columns = self.get_config_value('create_empty_columns', None)

        # OPT header_row: 1-based row holding the headers (default 1). Report
        # exports that lead with title lines import directly, without the
        # import -> slice -> promote dance, and a create_empty fallback can
        # declare the REAL header names because no promotion step follows.
        header_row = self.get_config_value('header_row', 1)
        if not isinstance(header_row, int) or isinstance(header_row, bool) or header_row < 1:
            raise StepProcessorError(
                f"Import step '{self.step_name}': header_row must be a positive "
                f"integer (1-based row of the header line), got {header_row!r}"
            )

        if on_missing_file not in ('error', 'create_empty'):
            raise StepProcessorError(
                f"Import step '{self.step_name}': invalid on_missing_file "
                f"'{on_missing_file}'. Supported: 'error' (default), "
                f"'create_empty' (requires 'create_empty_columns')."
            )
        if on_missing_file == 'create_empty':
            if (not create_empty_columns or
                    not isinstance(create_empty_columns, list) or
                    not all(isinstance(col, str) for col in create_empty_columns)):
                raise StepProcessorError(
                    f"Import step '{self.step_name}': on_missing_file "
                    f"'create_empty' requires 'create_empty_columns': a list "
                    f"of column names, so downstream steps that address "
                    f"columns still find them when the file is absent."
                )
        elif create_empty_columns is not None:
            raise StepProcessorError(
                f"Import step '{self.step_name}': 'create_empty_columns' "
                f"only applies with on_missing_file: 'create_empty'. Remove "
                f"the key or set the policy explicitly."
            )

        # Check if sheet was explicitly specified in the recipe step
        sheet_was_specified = 'sheet_name' in self.step_config
        
        # Apply variable substitution BEFORE calling FileReader
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            resolved_file = self.variable_substitution.substitute(input_file)
        else:
            resolved_file = input_file

        if on_missing_file == 'create_empty' and not Path(resolved_file).is_file():
            logger.warning(
                f"⚠️  '{self.step_name}': input file not found: {resolved_file} - "
                f"continuing with an EMPTY stage carrying declared columns "
                f"{create_empty_columns} (on_missing_file: create_empty)"
            )
            return pd.DataFrame(columns=create_empty_columns)

        
        # Determine if this is an Excel file for sheet-specific logging
        try:
            file_format = FileReader._determine_format(resolved_file, explicit_format)
            is_excel_file = file_format in FileReader.EXCEL_FORMATS
        except FileReaderError:
            is_excel_file = False
        
        # Resolve token / numeric / name forms to the actual tab name, so
        # FileReader only ever receives a real name (the doctrine's single
        # resolution point for imports; file_reader's isdigit hack is gone).
        if is_excel_file:
            try:
                available_for_resolution = FileReader.get_excel_sheets(resolved_file)
            except Exception:
                available_for_resolution = None
            if available_for_resolution is not None:
                # Variable substitution may have produced the value
                if hasattr(self, 'variable_substitution') and self.variable_substitution and isinstance(sheet, str):
                    sheet = self.variable_substitution.substitute(sheet)
                try:
                    sheet = resolve_sheet_ref(
                        sheet, available_for_resolution,
                        f"Import step '{self.step_name}'"
                    )
                except ValueError as error:
                    raise StepProcessorError(str(error))

        # For Excel files, prepare enhanced sheet information for final logging
        sheet_info_str = ""
        if is_excel_file:
            try:
                available_sheets = FileReader.get_excel_sheets(resolved_file)
                
                if isinstance(sheet, str):
                    # Sheet specified by name
                    sheet_info_str = f" (sheet: '{sheet}' - specified)"
                elif isinstance(sheet, int):
                    # Sheet specified by index, get actual name
                    if 1 <= sheet <= len(available_sheets):
                        actual_sheet_name = available_sheets[sheet - 1]  # Convert to 0-based
                        if sheet_was_specified:
                            sheet_info_str = f" (sheet: {sheet} - specified, actual name: '{actual_sheet_name}')"
                        else:
                            sheet_info_str = f" (sheet: {sheet} - default first sheet, actual name: '{actual_sheet_name}')"
                    else:
                        # Invalid sheet index
                        sheet_info_str = f" (sheet: {sheet} - ERROR: only {len(available_sheets)} sheets available)"
                
            except Exception as e:
                # Fallback if we can't get sheet names
                if isinstance(sheet, str):
                    sheet_info_str = f" (sheet: '{sheet}' - specified)"
                elif sheet_was_specified:
                    sheet_info_str = f" (sheet: {sheet} - specified)"
                else:
                    sheet_info_str = f" (sheet: {sheet} - default)"
        
        # FileReader gets the fully resolved filename
        try:
            # OPT verbatim_text_columns: columns whose literal text must
            # survive import untouched. pandas normally coerces strings like
            # "N/A", "NA", "NULL" to missing values; for a designated column
            # they stay the characters someone typed, while a genuinely
            # empty cell still imports as missing. Required for any column
            # that carries literal "N/A" entries a filter needs to match.
            verbatim_text_columns = self.get_config_value('verbatim_text_columns', None)

            data = FileReader.read_file(
                resolved_file,  # No variables parameter needed
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format,
                verbatim_text_columns=verbatim_text_columns,
                header_row=header_row
            )
            
            # Final import summary with comprehensive sheet information
            if is_excel_file:
                logger.info(f"Imported {len(data)} rows, {len(data.columns)} columns from '{resolved_file}'{sheet_info_str}")
            else:
                logger.info(f"Imported {len(data)} rows, {len(data.columns)} columns from '{resolved_file}'")
            
            return data
            
        except FileReaderError as e:
            raise StepProcessorError(f"Failed to import file '{input_file}': {e}")


# End of file #
