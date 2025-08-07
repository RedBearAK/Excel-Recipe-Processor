"""
Import file step processor for Excel automation recipes.

excel_recipe_processor/processors/import_file_processor.py

Pure stage-based file import - no pipeline data concept.
"""

import logging

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
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
    
    def load_data(self):
        """Load data from file (implements ImportBaseProcessor abstract method)."""
        input_file = self.get_config_value('input_file')
        sheet = self.get_config_value('sheet', 1)
        encoding = self.get_config_value('encoding', 'utf-8')
        separator = self.get_config_value('separator', ',')
        explicit_format = self.get_config_value('format', None)
        
        # Check if sheet was explicitly specified in the recipe step
        sheet_was_specified = 'sheet' in self.step_config
        
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
            data = FileReader.read_file(
                resolved_file,  # No variables parameter needed
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format
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
