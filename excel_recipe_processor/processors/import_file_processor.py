
"""
Import file step processor for Excel automation recipes.

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
        sheet = self.get_config_value('sheet', 0)
        encoding = self.get_config_value('encoding', 'utf-8')
        separator = self.get_config_value('separator', ',')
        explicit_format = self.get_config_value('format', None)
        
        # Apply variable substitution if available
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            substituted_path = self.variable_substitution.substitute(input_file)
        else:
            substituted_path = input_file
        
        # Use FileReader for consistent file handling
        try:
            data = FileReader.read_file(
                substituted_path,
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format
            )
            
            logger.info(f"Imported {len(data)} rows, {len(data.columns)} columns from {substituted_path}")
            return data
            
        except FileReaderError as e:
            raise StepProcessorError(f"Failed to import file '{input_file}': {e}")