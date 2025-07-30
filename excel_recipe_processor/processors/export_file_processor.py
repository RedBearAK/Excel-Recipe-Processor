"""
Export file step processor for Excel automation recipes.

Handles exporting data to various file formats using FileWriter infrastructure
and StageManager for accessing multiple data sources.
"""

import pandas as pd
import logging

from excel_recipe_processor.core.file_writer import FileWriter, FileWriterError
from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class ExportFileProcessor(BaseStepProcessor):
    """
    Processor for exporting data to files in various formats.
    
    Supports single and multi-sheet Excel files, CSV, TSV formats with 
    variable substitution and access to saved stages via StageManager.
    """
    
    @classmethod
    def get_minimal_config(cls):
        return {
            'output_file': 'output.xlsx'
        }
    
    def execute(self, data):
        """
        Execute the file export operation while passing data through unchanged.
        
        Args:
            data: Input pandas DataFrame to export (and pass through)
            
        Returns:
            Original DataFrame unchanged (export is a side effect)
            
        Raises:
            StepProcessorError: If export operation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Export file step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['output_file'])
        
        output_file = self.get_config_value('output_file')
        sheets = self.get_config_value('sheets', None)
        sheet_name = self.get_config_value('sheet_name', 'Data')
        explicit_format = self.get_config_value('format', None)
        create_backup = self.get_config_value('create_backup', False)
        encoding = self.get_config_value('encoding', 'utf-8')
        separator = self.get_config_value('separator', ',')
        
        # Get custom variables from pipeline if available
        variables = self._get_pipeline_variables()
        
        # Validate configuration
        self._validate_export_config(output_file, sheets, explicit_format)
        
        try:
            if sheets:
                # Multi-sheet export
                sheets_data = self._build_sheets_data(data, sheets)
                active_sheet = self._get_active_sheet(sheets)
                
                final_output_file = FileWriter.write_multi_sheet_excel(
                    sheets_data=sheets_data,
                    filename=output_file,
                    variables=variables,
                    create_backup=create_backup,
                    active_sheet=active_sheet
                )
                
                result_info = f"exported {len(sheets)} sheets to {final_output_file}"
            else:
                # Single file export
                final_output_file = FileWriter.write_file(
                    data=data,
                    filename=output_file,
                    variables=variables,
                    sheet_name=sheet_name,
                    create_backup=create_backup,
                    explicit_format=explicit_format,
                    encoding=encoding,
                    separator=separator
                )
                
                result_info = f"exported {len(data)} rows to {final_output_file}"
            
            self.log_step_complete(result_info)
            
            # Return original data unchanged
            return data
            
        except FileWriterError as e:
            raise StepProcessorError(f"Error exporting file in step '{self.step_name}': {e}")
        except StageError as e:
            raise StepProcessorError(f"Error accessing stage data in step '{self.step_name}': {e}")
        except Exception as e:
            raise StepProcessorError(f"Unexpected error exporting file in step '{self.step_name}': {e}")
    
    def _validate_export_config(self, output_file, sheets, explicit_format):
        """
        Validate export configuration parameters.
        
        Args:
            output_file: Output file path
            sheets: Sheet configuration list
            explicit_format: Explicit format type
        """
        # Validate output file
        if not isinstance(output_file, str) or not output_file.strip():
            raise StepProcessorError("'output_file' must be a non-empty string")
        
        # Validate sheets configuration if provided
        if sheets is not None:
            if not isinstance(sheets, list):
                raise StepProcessorError("'sheets' must be a list of sheet configurations")
            
            if len(sheets) == 0:
                raise StepProcessorError("'sheets' list cannot be empty")
            
            sheet_names = []
            for i, sheet in enumerate(sheets):
                if not isinstance(sheet, dict):
                    raise StepProcessorError(f"Sheet {i+1} must be a dictionary")
                
                if 'sheet_name' not in sheet:
                    raise StepProcessorError(f"Sheet {i+1} missing required 'sheet_name'")
                
                sheet_name = sheet['sheet_name']
                if not isinstance(sheet_name, str) or not sheet_name.strip():
                    raise StepProcessorError(f"Sheet {i+1} 'sheet_name' must be a non-empty string")
                
                if sheet_name in sheet_names:
                    raise StepProcessorError(f"Duplicate sheet name: '{sheet_name}'")
                sheet_names.append(sheet_name)
                
                # Validate data_source if provided
                data_source = sheet.get('data_source', 'current')
                if not isinstance(data_source, str) or not data_source.strip():
                    raise StepProcessorError(f"Sheet {i+1} 'data_source' must be a non-empty string")
        
        # Validate explicit format if provided
        if explicit_format is not None:
            if not isinstance(explicit_format, str) or not explicit_format.strip():
                raise StepProcessorError("'format' must be a non-empty string")
            
            supported_formats = FileWriter.get_supported_formats()['all_formats']
            if explicit_format.lower() not in supported_formats:
                raise StepProcessorError(
                    f"Unsupported format '{explicit_format}'. "
                    f"Supported formats: {supported_formats}"
                )
    
    def _build_sheets_data(self, current_data, sheets):
        """
        Build dictionary of sheet data for multi-sheet export.
        
        Args:
            current_data: Current pipeline data
            sheets: List of sheet configurations
            
        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        sheets_data = {}
        
        for sheet_config in sheets:
            sheet_name = sheet_config['sheet_name']
            data_source = sheet_config.get('data_source', 'current')
            
            # Get data for this sheet
            sheet_data = self._get_sheet_data(current_data, data_source)
            sheets_data[sheet_name] = sheet_data
        
        return sheets_data
    
    def _get_sheet_data(self, current_data, data_source):
        """
        Get data for a specific sheet based on data source.
        
        Args:
            current_data: Current pipeline data
            data_source: Source identifier ('current', stage name, etc.)
            
        Returns:
            DataFrame for the sheet
        """
        if data_source == 'current':
            return current_data
        elif data_source == 'input':
            # TODO: Access original input data from pipeline when available
            logger.warning("Input data source not yet implemented, using current data")
            return current_data
        else:
            # Assume it's a stage name
            try:
                stage_data = StageManager.load_stage(data_source)
                logger.debug(f"Retrieved data from stage '{data_source}': {stage_data.shape}")
                return stage_data
            except StageError as e:
                # Provide helpful error with available stages
                available_stages = list(StageManager.list_stages().keys())
                raise StepProcessorError(
                    f"Cannot access data source '{data_source}': {e}. "
                    f"Available stages: {available_stages}"
                )
    
    def _get_active_sheet(self, sheets):
        """
        Get the name of the sheet that should be active.
        
        Args:
            sheets: List of sheet configurations
            
        Returns:
            Name of active sheet or None
        """
        for sheet_config in sheets:
            if sheet_config.get('active', False):
                return sheet_config['sheet_name']
        
        # Default to first sheet if none specified as active
        return sheets[0]['sheet_name'] if sheets else None
    
    def _get_pipeline_variables(self):
        """
        Get variables from the pipeline for filename substitution.
        
        Returns:
            Dictionary of variables for substitution
        """
        # TODO: Access pipeline variables when available
        # For now, return empty dict - FileWriter will use built-in variables
        return {}
    
    def get_supported_formats(self):
        """
        Get list of supported export formats.
        
        Returns:
            List of supported format strings
        """
        return FileWriter.get_supported_formats()['all_formats']
    
    def get_supported_data_sources(self):
        """
        Get list of supported data sources for sheets.
        
        Returns:
            List of supported data source strings
        """
        stage_names = list(StageManager.list_stages().keys())
        return ['current', 'input'] + stage_names
    
    def get_capabilities(self):
        """Get processor capabilities information."""
        supported_info = FileWriter.get_supported_formats()
        
        return {
            'description': 'Export data to files in various formats with multi-sheet and stage support',
            'export_formats': supported_info['all_formats'],
            'supported_extensions': supported_info['supported_extensions'],
            'export_features': [
                'multi_sheet_excel', 'single_sheet_excel', 'csv_export', 'tsv_export',
                'variable_substitution', 'backup_creation', 'stage_data_access'
            ],
            'data_sources': [
                'current_pipeline_data', 'saved_stages', 'input_data_planned'
            ],
            'file_features': [
                'automatic_format_detection', 'explicit_format_override', 
                'custom_sheet_naming', 'active_sheet_control', 'custom_encoding'
            ],
            'stage_integration': [
                'export_from_multiple_stages', 'combine_current_and_stage_data',
                'stage_validation_and_error_handling'
            ],
            'examples': {
                'single_file': "Export current data to report.xlsx",
                'multi_sheet_current': "Export current data to multiple sheets",
                'multi_sheet_stages': "Export from different stages to combined report",
                'csv_export': "Export data as comma-separated values with custom encoding",
                'variable_filename': "Export to report_{date}.xlsx with date substitution",
                'backup_export': "Export with automatic backup of existing file"
            },
            'configuration_options': {
                'output_file': 'File path with optional variable substitution',
                'sheets': 'List of sheet configurations for multi-sheet export',
                'sheet_name': 'Sheet name for single-sheet Excel export',
                'format': 'Explicit format override (auto-detected by default)',
                'create_backup': 'Whether to backup existing file before overwrite',
                'encoding': 'Text encoding for CSV/TSV files',
                'separator': 'Column separator for CSV files'
            }
        }
    
    # export_file_processor.py
    def get_usage_examples(self) -> dict:
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('export_file')

# Note: Other processors should use FileWriter directly for simple exports
# Example: FileWriter.write_file(data, filename, variables, sheet_name, format)
