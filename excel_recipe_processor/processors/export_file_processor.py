"""
Export file step processor for Excel automation recipes - REFACTORED VERSION.

Handles exporting data to various file formats using existing ExcelWriter infrastructure.
"""

import pandas as pd
import logging

from pathlib import Path
from datetime import datetime

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError
from excel_recipe_processor.writers.excel_writer import ExcelWriter, ExcelWriterError


logger = logging.getLogger(__name__)


class ExportFileProcessor(BaseStepProcessor):
    """
    Processor for exporting data to files in various formats.
    
    Supports single and multi-sheet Excel files, CSV, TSV formats with 
    variable substitution and uses existing ExcelWriter infrastructure.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'output_file': 'output.xlsx'
        }
    
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
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
        format_type = self.get_config_value('format', None)
        create_backup = self.get_config_value('create_backup', False)
        
        # Validate configuration
        self._validate_export_config(output_file, sheets, format_type)
        
        try:
            # Apply variable substitution to output filename
            final_output_file = self._apply_variable_substitution(output_file)
            
            # Determine export format
            export_format = self._determine_format(final_output_file, format_type)
            
            # Initialize ExcelWriter for backup and Excel operations
            excel_writer = ExcelWriter()
            
            # Create backup if requested and file exists
            if create_backup and Path(final_output_file).exists():
                excel_writer.create_backup(final_output_file)
            
            # Export the file using appropriate method
            if export_format == 'csv':
                self._export_csv(data, final_output_file)
            elif export_format == 'tsv':
                self._export_tsv(data, final_output_file)
            elif export_format in ['xlsx', 'xls']:
                if sheets:
                    self._export_multi_sheet_excel(data, final_output_file, sheets, excel_writer)
                else:
                    self._export_single_sheet_excel(data, final_output_file, excel_writer)
            else:
                raise StepProcessorError(f"Unsupported export format: {export_format}")
            
            # Log success
            rows_exported = len(data)
            sheet_info = f", {len(sheets)} sheets" if sheets else ""
            result_info = f"exported {rows_exported} rows to {final_output_file} ({export_format}{sheet_info})"
            self.log_step_complete(result_info)
            
            # Return original data unchanged
            return data
            
        except ExcelWriterError as e:
            raise StepProcessorError(f"Excel export error in step '{self.step_name}': {e}")
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error exporting file in step '{self.step_name}': {e}")
    
    def _validate_export_config(self, output_file: str, sheets: list, format_type: str) -> None:
        """
        Validate export configuration parameters.
        
        Args:
            output_file: Output file path
            sheets: Sheet configuration list
            format_type: Explicit format type
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
                if sheet_name in sheet_names:
                    raise StepProcessorError(f"Duplicate sheet name: '{sheet_name}'")
                sheet_names.append(sheet_name)
                
                # Validate data_source if provided
                data_source = sheet.get('data_source', 'current')
                valid_sources = ['current', 'input']  # Will expand when save_stage is implemented
                if data_source not in valid_sources and not isinstance(data_source, str):
                    logger.warning(f"Data source '{data_source}' may not be available. Valid sources: {valid_sources}")
        
        # Validate format if provided
        if format_type is not None:
            valid_formats = ['xlsx', 'xls', 'csv', 'tsv']
            if format_type not in valid_formats:
                raise StepProcessorError(f"Invalid format '{format_type}'. Valid formats: {valid_formats}")
    
    def _apply_variable_substitution(self, filename: str) -> str:
        """
        Apply variable substitution to filename using pipeline variables.
        
        Args:
            filename: Original filename template
            
        Returns:
            Filename with variables substituted
        """
        # TODO: Access pipeline's variable substitution system
        # For now, do basic timestamp substitution
        now = datetime.now()
        
        # Basic variable substitutions
        substitutions = {
            '{timestamp}': now.strftime('%Y%m%d_%H%M%S'),
            '{date}': now.strftime('%Y%m%d'),
            '{YY}': now.strftime('%y'),
            '{MMDD}': now.strftime('%m%d'),
            '{year}': now.strftime('%Y'),
            '{month}': now.strftime('%m'),
            '{day}': now.strftime('%d')
        }
        
        result = filename
        for variable, value in substitutions.items():
            result = result.replace(variable, value)
        
        return result
    
    def _determine_format(self, filename: str, explicit_format: str) -> str:
        """
        Determine export format from filename extension or explicit format.
        
        Args:
            filename: Output filename
            explicit_format: Explicitly specified format
            
        Returns:
            Format string ('xlsx', 'csv', etc.)
        """
        if explicit_format:
            return explicit_format.lower()
        
        # Determine from file extension
        file_path = Path(filename)
        extension = file_path.suffix.lower()
        
        format_map = {
            '.xlsx': 'xlsx',
            '.xls': 'xls', 
            '.csv': 'csv',
            '.tsv': 'tsv',
            '.txt': 'tsv'  # Assume tab-separated for .txt
        }
        
        return format_map.get(extension, 'xlsx')  # Default to xlsx
    
    def _export_csv(self, data: pd.DataFrame, filename: str) -> None:
        """Export data as CSV file."""
        data.to_csv(filename, index=False)
        logger.debug(f"Exported CSV: {filename}")
    
    def _export_tsv(self, data: pd.DataFrame, filename: str) -> None:
        """Export data as TSV file."""
        data.to_csv(filename, sep='\t', index=False)
        logger.debug(f"Exported TSV: {filename}")
    
    def _export_single_sheet_excel(self, data: pd.DataFrame, filename: str, excel_writer: ExcelWriter) -> None:
        """Export data as single-sheet Excel file using ExcelWriter."""
        sheet_name = self.get_config_value('sheet_name', 'Data')
        
        try:
            excel_writer.write_file(
                df=data,
                output_path=filename,
                sheet_name=sheet_name,
                index=False
            )
            logger.debug(f"Exported single-sheet Excel: {filename}")
        except ExcelWriterError as e:
            raise StepProcessorError(f"Failed to export single-sheet Excel: {e}")
    
    def _export_multi_sheet_excel(self, data: pd.DataFrame, filename: str, sheets: list, excel_writer: ExcelWriter) -> None:
        """
        Export data as multi-sheet Excel file using ExcelWriter.
        
        Args:
            data: Current pipeline data
            filename: Output filename
            sheets: List of sheet configurations
            excel_writer: ExcelWriter instance
        """
        # Build data dictionary for multi-sheet export
        data_dict = {}
        
        for sheet_config in sheets:
            sheet_name = sheet_config['sheet_name']
            data_source = sheet_config.get('data_source', 'current')
            
            # Get data for this sheet
            sheet_data = self._get_sheet_data(data, data_source)
            data_dict[sheet_name] = sheet_data
        
        try:
            excel_writer.write_multiple_sheets(data_dict, filename)
            logger.debug(f"Exported multi-sheet Excel: {filename} ({len(sheets)} sheets)")
        except ExcelWriterError as e:
            raise StepProcessorError(f"Failed to export multi-sheet Excel: {e}")
    
    def _get_sheet_data(self, current_data: pd.DataFrame, data_source: str) -> pd.DataFrame:
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
            # TODO: Access original input data from pipeline
            logger.warning("Input data source not yet implemented, using current data")
            return current_data
        else:
            # TODO: Access saved stages from pipeline
            logger.warning(f"Stage data source '{data_source}' not yet implemented, using current data")
            return current_data
    
    def get_supported_formats(self) -> list:
        """
        Get list of supported export formats.
        
        Returns:
            List of supported format strings
        """
        return ['xlsx', 'xls', 'csv', 'tsv']
    
    def get_supported_data_sources(self) -> list:
        """
        Get list of supported data sources for sheets.
        
        Returns:
            List of supported data source strings
        """
        return ['current', 'input']  # Will expand with save_stage integration
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Export data to files in various formats with multi-sheet support',
            'export_formats': self.get_supported_formats(),
            'data_sources': self.get_supported_data_sources(),
            'export_features': [
                'multi_sheet_excel', 'single_sheet_excel', 'csv_export', 'tsv_export',
                'variable_substitution', 'backup_creation', 'uses_excel_writer_infrastructure'
            ],
            'file_features': [
                'automatic_format_detection', 'explicit_format_override', 
                'custom_sheet_naming', 'sheet_ordering'
            ],
            'examples': {
                'single_file': "Export current data to report.xlsx", 
                'multi_sheet': "Export summary and details to combined report",
                'csv_export': "Export data as comma-separated values",
                'intermediate': "Export intermediate results while continuing processing"
            }
        }
