"""
Import file step processor for Excel automation recipes.

excel_recipe_processor/processors/import_file_processor.py

Pure stage-based file import - no pipeline data concept.
"""

import logging

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.processors._helpers.sheet_addressing import resolve_sheet_ref
from excel_recipe_processor.core.base_processor import ImportBaseProcessor, StepProcessorError

logger = logging.getLogger(__name__)


class ImportFileProcessor(ImportBaseProcessor):
    """
    Processor for importing data from external files into stages.
    
    Supports Excel, CSV, and TSV files with automatic format detection
    and variable substitution. Always saves to a declared stage.
    """
    
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
            'description': 'Import Excel, CSV, or TSV files into stages, with sheet selection and variable-substituted paths',
            'fast_excel_reader': 'when the optional python-calamine wheel is installed, '
                                 'Excel imports use it automatically (several times faster '
                                 'on large files, values identical); without it the '
                                 'openpyxl path serves unchanged - no configuration',
            'file_formats': ['xlsx', 'xls', 'xlsm', 'xlsb', 'csv', 'tsv', 'txt (as tsv)'],
            'excel_options': ['sheet selection by name or index'],
            'path_features': ['recipe variable substitution'],
        }

    def load_data(self):
        """Load data from file (implements ImportBaseProcessor abstract method)."""
        input_file = self.get_config_value('input_file')

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
        
        # Check if sheet was explicitly specified in the recipe step
        sheet_was_specified = 'sheet_name' in self.step_config
        
        # Apply variable substitution BEFORE calling FileReader
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            resolved_file = self.variable_substitution.substitute(input_file)
        else:
            resolved_file = input_file
        
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
                verbatim_text_columns=verbatim_text_columns
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
