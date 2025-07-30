"""
Import file step processor for Excel automation recipes.

Handles importing data from various file formats with automatic format detection,
variable substitution, and optional stage saving via StageManager.
"""

import logging

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class ImportFileProcessor(BaseStepProcessor):
    """
    Processor for importing data from external files.
    
    Supports Excel, CSV, and TSV files with automatic format detection,
    variable substitution, and flexible configuration options.
    Can replace current pipeline data and/or save to named stages.
    """
    
    @classmethod
    def get_minimal_config(cls):
        return {
            'input_file': 'test_data.xlsx',
            'replace_current_data': True  # Required parameter - must be explicit
            # Optional: 'save_to_stage': 'Imported Customer Data'
        }
    
    def execute(self, data):
        """
        Execute the import operation, optionally saving to a named stage.
        
        Args:
            data: Input pandas DataFrame (will be replaced with imported data if replace_current_data=True)
            
        Returns:
            DataFrame with imported file contents or original data
            
        Raises:
            StepProcessorError: If import operation fails
        """
        self.log_step_start()
        
        # Validate required configuration
        self.validate_required_fields(['input_file', 'replace_current_data'])
        
        input_file = self.get_config_value('input_file')
        replace_current_data = self.get_config_value('replace_current_data')
        sheet = self.get_config_value('sheet', 0)
        encoding = self.get_config_value('encoding', 'utf-8')
        separator = self.get_config_value('separator', ',')
        explicit_format = self.get_config_value('format', None)
        save_to_stage = self.get_config_value('save_to_stage', None)
        stage_overwrite = self.get_config_value('stage_overwrite', False)
        stage_description = self.get_config_value('stage_description', '')
        
        # Safety check - require explicit confirmation to replace current data
        if not replace_current_data:
            raise StepProcessorError(
                "'replace_current_data' must be set to true to acknowledge that current pipeline data will be replaced"
            )
        
        # Get custom variables from pipeline if available
        variables = self._get_pipeline_variables()
        
        # Validate configuration
        self._validate_import_config(input_file, sheet, encoding, separator, explicit_format, save_to_stage)
        
        try:
            # Use FileReader to import the file
            imported_data = FileReader.read_file(
                filename=input_file,
                variables=variables,
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format
            )
            
            # Save to stage if requested
            if save_to_stage:
                try:
                    StageManager.save_stage(
                        stage_name=save_to_stage,
                        data=imported_data,
                        overwrite=stage_overwrite,
                        description=stage_description or f"Imported from {input_file}",
                        step_name=self.step_name
                    )
                    logger.info(f"Saved imported data to stage '{save_to_stage}'")
                except StageError as e:
                    raise StepProcessorError(f"Error saving to stage '{save_to_stage}': {e}")
            
            # Log the replacement warning (same as load_stage does)
            logger.warning(
                f"Replacing current data ({len(data)} rows) "
                f"with imported data from '{input_file}' ({len(imported_data)} rows)"
            )
            
            # Log import results
            imported_rows = len(imported_data)
            imported_columns = len(imported_data.columns)
            
            stage_info = f" and saved to stage '{save_to_stage}'" if save_to_stage else ""
            result_info = f"imported {imported_rows} rows, {imported_columns} columns from {input_file}{stage_info}"
            self.log_step_complete(result_info)
            
            return imported_data
            
        except FileReaderError as e:
            raise StepProcessorError(f"Error importing file in step '{self.step_name}': {e}")
        except Exception as e:
            raise StepProcessorError(f"Unexpected error importing file in step '{self.step_name}': {e}")
    
    def _validate_import_config(self, input_file, sheet, encoding, separator, explicit_format, save_to_stage):
        """
        Validate import configuration parameters.
        
        Args:
            input_file: Input file path
            sheet: Sheet specification for Excel files
            encoding: Text encoding
            separator: CSV separator
            explicit_format: Explicit format override
            save_to_stage: Optional stage name to save to
        """
        # Validate input file
        if not isinstance(input_file, str) or not input_file.strip():
            raise StepProcessorError("'input_file' must be a non-empty string")
        
        # Validate sheet parameter
        if sheet is not None and not isinstance(sheet, (str, int)):
            raise StepProcessorError("'sheet' must be a string (sheet name) or integer (sheet index)")
        
        # Validate encoding
        if not isinstance(encoding, str) or not encoding.strip():
            raise StepProcessorError("'encoding' must be a non-empty string")
        
        # Validate separator
        if not isinstance(separator, str):
            raise StepProcessorError("'separator' must be a string")
        
        # Validate explicit format if provided
        if explicit_format is not None:
            if not isinstance(explicit_format, str) or not explicit_format.strip():
                raise StepProcessorError("'format' must be a non-empty string")
            
            supported_formats = FileReader.get_supported_formats()['all_formats']
            if explicit_format.lower() not in supported_formats:
                raise StepProcessorError(
                    f"Unsupported format '{explicit_format}'. "
                    f"Supported formats: {supported_formats}"
                )
        
        # Validate stage name if provided
        if save_to_stage is not None:
            if not isinstance(save_to_stage, str) or not save_to_stage.strip():
                raise StepProcessorError("'save_to_stage' must be a non-empty string")
            
            # Let StageManager handle detailed validation during execution
            # This gives better error messages with naming suggestions
    
    def _get_pipeline_variables(self):
        """
        Get variables from the pipeline for filename substitution.
        
        Returns:
            Dictionary of variables for substitution
        """
        # TODO: Access pipeline variables when available
        # For now, return empty dict - FileReader will use built-in variables
        return {}
    
    def get_supported_formats(self):
        """
        Get list of supported import formats.
        
        Returns:
            List of supported format strings
        """
        return FileReader.get_supported_formats()['all_formats']
    
    def get_supported_extensions(self):
        """
        Get list of supported file extensions.
        
        Returns:
            List of supported extension strings
        """
        return FileReader.get_supported_formats()['supported_extensions']
    
    def get_capabilities(self):
        """Get processor capabilities information."""
        supported_info = FileReader.get_supported_formats()
        
        return {
            'description': 'Import data from external files with automatic format detection and optional stage saving',
            'import_formats': supported_info['all_formats'],
            'supported_extensions': supported_info['supported_extensions'],
            'import_features': [
                'automatic_format_detection', 'variable_substitution', 'multi_sheet_excel',
                'custom_encoding', 'custom_separators', 'sheet_selection', 'stage_saving'
            ],
            'stage_features': [
                'save_to_named_stage', 'stage_overwrite_control', 'stage_descriptions',
                'stage_name_validation', 'automatic_metadata_tracking'
            ],
            'variable_substitution': [
                'built_in_date_variables', 'custom_variables', 'pipeline_variables'
            ],
            'file_validation': [
                'existence_checking', 'format_validation', 'error_handling'
            ],
            'examples': {
                'basic_import': "Import data.xlsx to replace current data",
                'stage_import': "Import monthly_data.csv and save to 'Monthly Sales Data' stage",
                'variable_import': "Import report_{date}.xlsx with date substitution",
                'sheet_specific': "Import 'Summary' sheet from quarterly_report.xlsx",
                'csv_custom': "Import CSV with semicolon separator and UTF-8 encoding",
                'overwrite_stage': "Import and overwrite existing 'Customer Master' stage"
            },
            'configuration_options': {
                'input_file': 'File path with optional variable substitution',
                'save_to_stage': 'Optional stage name to save imported data',
                'stage_overwrite': 'Whether to overwrite existing stage (default: false)',
                'stage_description': 'Optional description for the saved stage',
                'sheet': 'Sheet name or index for Excel files',
                'encoding': 'Text encoding for CSV/TSV files',
                'separator': 'Column separator for CSV files',
                'format': 'Explicit format override (auto-detected by default)'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the import_file processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('import_file')


# Note: Other processors should use FileReader directly instead of ImportFileProcessor
# Example: data = FileReader.read_file(filename, variables, sheet, encoding, separator)
