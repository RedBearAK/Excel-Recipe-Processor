
"""
Export file step processor for Excel automation recipes.

Pure stage-based file export - consumes stages, saves to files.
"""

import logging

from excel_recipe_processor.core.file_writer import FileWriter, FileWriterError
from excel_recipe_processor.core.base_processor import ExportBaseProcessor, StepProcessorError

logger = logging.getLogger(__name__)


class ExportFileProcessor(ExportBaseProcessor):
    """
    Processor for exporting data from stages to files.
    
    Supports Excel, CSV, and TSV output with variable substitution
    and multi-sheet Excel export capabilities.
    """
    
    @classmethod
    def get_minimal_config(cls):
        return {
            'source_stage': 'final_data',
            'output_file': 'output.xlsx'
        }
    
    # def save_data(self, data):
    #     """Save data to file (implements ExportBaseProcessor abstract method)."""
    #     output_file = self.get_config_value('output_file')
    #     sheet_name = self.get_config_value('sheet_name', 'Data')
    #     explicit_format = self.get_config_value('format', None)
    #     sheets = self.get_config_value('sheets', None)
        
    #     # Apply variable substitution if available
    #     if hasattr(self, 'variable_substitution') and self.variable_substitution:
    #         substituted_path = self.variable_substitution.substitute(output_file)
    #     else:
    #         substituted_path = output_file
        
    #     try:
    #         if sheets:
    #             # Multi-sheet export
    #             sheets_data = self._build_sheets_data(sheets)
    #             FileWriter.write_multi_sheet_excel(sheets_data, substituted_path)
    #         else:
    #             # Single file export
    #             FileWriter.write_file(
    #                 data,
    #                 substituted_path,
    #                 sheet_name=sheet_name,
    #                 explicit_format=explicit_format
    #             )
            
    #         logger.info(f"Exported {len(data)} rows to {substituted_path}")
            
    #     except FileWriterError as e:
    #         raise StepProcessorError(f"Failed to export to '{output_file}': {e}")


    def save_data(self, data):
        """Save data to file (implements ExportBaseProcessor abstract method)."""
        output_file = self.get_config_value('output_file')
        sheet_name = self.get_config_value('sheet_name', 'Data')
        explicit_format = self.get_config_value('format', None)
        sheets = self.get_config_value('sheets', None)
        # See if user wants to disable the creation of a backup file to avoid clobbering same name
        create_backup = self.get_config_value('create_backup', True)
        
        # Apply variable substitution BEFORE calling FileWriter
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            resolved_file = self.variable_substitution.substitute(output_file)
        else:
            resolved_file = output_file
        
        try:

            if sheets:
                # Multi-sheet export
                sheets_data = self._build_sheets_data(sheets)
                FileWriter.write_multi_sheet_excel(
                    sheets_data,
                    resolved_file,
                    create_backup=create_backup
                )  # No variables parameter
            else:
                # Single file export
                FileWriter.write_file(
                    data,
                    resolved_file,  # No variables parameter needed
                    sheet_name=sheet_name,
                    explicit_format=explicit_format,
                    create_backup=create_backup
                )
            
            logger.info(f"Exported {len(data)} rows to '{resolved_file}'")
            
        except FileWriterError as e:
            raise StepProcessorError(f"Failed to export to '{output_file}': {e}")



    def _build_sheets_data(self, sheets):
        """Build dictionary of sheet data for multi-sheet export."""
        from excel_recipe_processor.core.stage_manager import StageManager
        
        sheets_data = {}
        
        for sheet_config in sheets:
            sheet_name = sheet_config['sheet_name']
            data_source = sheet_config.get('data_source')
            
            if not data_source:
                raise StepProcessorError(f"Sheet '{sheet_name}' missing data_source")
            
            try:
                sheet_data = StageManager.load_stage(data_source)
                sheets_data[sheet_name] = sheet_data
            except Exception as e:
                available_stages = list(StageManager.list_stages().keys())
                raise StepProcessorError(
                    f"Cannot load data_source '{data_source}' for sheet '{sheet_name}': {e}. "
                    f"Available stages: {available_stages}"
                )
        
        return sheets_data
