"""
Format Excel step processor for Excel automation recipes.

Handles formatting existing Excel files with auto-fit columns, header styling, and other presentation features.
"""

# import pandas as pd
import logging

from pathlib import Path

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class FormatExcelProcessor(FileOpsBaseProcessor):
    """
    Processor for formatting existing Excel files.
    
    Applies formatting like auto-fit columns, header styling, freeze panes,
    and other presentation features to make Excel files look professional.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'target_file': 'output.xlsx'
        }
    
    # def perform_file_operation(self) -> str:
    #     """Format the target Excel file."""
    #     # Check openpyxl availability
    #     if not OPENPYXL_AVAILABLE:
    #         raise StepProcessorError("openpyxl is required for Excel formatting but not installed")
        
    #     target_file = self.get_config_value('target_file')
    #     formatting = self.get_config_value('formatting', {})
        
    #     # Validate configuration
    #     self._validate_format_config(target_file, formatting)
        
    #     # Apply variable substitution to target filename
    #     final_target_file = self._apply_variable_substitution(target_file)
        
    #     # Check file exists
    #     if not Path(final_target_file).exists():
    #         raise StepProcessorError(f"Target file not found: {final_target_file}")
        
    #     # Load and format the workbook
    #     formatted_sheets = self._format_excel_file(final_target_file, formatting)
        
    #     return f"formatted {final_target_file} ({formatted_sheets} sheets processed)"



    def perform_file_operation(self) -> str:
        """Format the target Excel file."""
        # Check openpyxl availability
        if not OPENPYXL_AVAILABLE:
            raise StepProcessorError("openpyxl is required for Excel formatting but not installed")
        
        target_file = self.get_config_value('target_file')
        formatting = self.get_config_value('formatting', {})
        
        # Validate configuration
        self._validate_format_config(target_file, formatting)
        
        # Apply variable substitution BEFORE file operations
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            resolved_file = self.variable_substitution.substitute(target_file)
        else:
            resolved_file = target_file
        
        # Check file exists
        if not Path(resolved_file).exists():
            raise StepProcessorError(f"Target file not found: {resolved_file}")
        
        # Load and format the workbook
        formatted_sheets = self._format_excel_file(resolved_file, formatting)
        
        return f"formatted {resolved_file} ({formatted_sheets} sheets processed)"



    # ADD THIS METHOD (optional override for better validation):
    def _validate_file_operation_config(self):
        """Validate format_excel specific configuration."""
        if not self.get_config_value('target_file'):
            raise StepProcessorError(f"Format Excel step '{self.step_name}' requires 'target_file'")
    
    def _validate_format_config(self, target_file: str, formatting: dict) -> None:
        """
        Validate formatting configuration parameters.
        
        Args:
            target_file: Target Excel file path
            formatting: Formatting configuration dictionary
        """
        # Validate target file
        if not isinstance(target_file, str) or not target_file.strip():
            raise StepProcessorError("'target_file' must be a non-empty string")
        
        # Check file extension
        file_path = Path(target_file)
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            raise StepProcessorError(f"Target file must be Excel format (.xlsx or .xls), got: {file_path.suffix}")
        
        # Validate formatting config
        if not isinstance(formatting, dict):
            raise StepProcessorError("'formatting' must be a dictionary")
        
        # Validate specific formatting options
        if 'max_column_width' in formatting:
            max_width = formatting['max_column_width']
            if not isinstance(max_width, (int, float)) or max_width <= 0:
                raise StepProcessorError("'max_column_width' must be a positive number")
        
        if 'min_column_width' in formatting:
            min_width = formatting['min_column_width']
            if not isinstance(min_width, (int, float)) or min_width <= 0:
                raise StepProcessorError("'min_column_width' must be a positive number")
    
    # def _apply_variable_substitution(self, filename: str) -> str:
    #     """
    #     Apply variable substitution to filename.
        
    #     Args:
    #         filename: Original filename template
            
    #     Returns:
    #         Filename with variables substituted
    #     """
    #     # Get custom variables from processor config
    #     custom_variables = self.get_config_value('variables', {})
        
    #     # Create variable substitution instance
    #     # Note: We don't have input_path/recipe_path context, but we can still
    #     # handle custom variables and date/time substitution
    #     variable_sub = VariableSubstitution(
    #         input_path=None,
    #         recipe_path=None, 
    #         custom_variables=custom_variables
    #     )
        
    #     # Apply substitution
    #     try:
    #         substituted = variable_sub.substitute(filename)
    #         if substituted != filename:
    #             logger.debug(f"Variable substitution: {filename} → {substituted}")
    #         return substituted
    #     except Exception as e:
    #         logger.warning(f"Variable substitution failed for '{filename}': {e}")
    #         return filename
    
    def _format_excel_file(self, filename: str, formatting: dict) -> int:
        """
        Apply formatting to an Excel file.
        
        Args:
            filename: Excel file to format
            formatting: Formatting configuration
            
        Returns:
            Number of sheets processed
        """
        workbook = openpyxl.load_workbook(filename)
        sheets_processed = 0
        
        # Get sheet-specific formatting
        sheet_specific = formatting.get('sheet_specific', {})
        
        # Process each sheet
        for sheet_name in workbook.sheetnames:
            worksheet = workbook[sheet_name]
            
            # Apply general formatting to all sheets
            self._apply_sheet_formatting(worksheet, formatting)
            
            # Apply sheet-specific formatting
            if sheet_name in sheet_specific:
                self._apply_sheet_formatting(worksheet, sheet_specific[sheet_name])
            
            sheets_processed += 1
        
        # Set active sheet if specified
        active_sheet = formatting.get('active_sheet')
        if active_sheet and active_sheet in workbook.sheetnames:
            workbook.active = workbook[active_sheet]
        
        # Save the formatted workbook
        workbook.save(filename)
        workbook.close()
        
        return sheets_processed
    
    def _apply_sheet_formatting(self, worksheet, formatting: dict) -> None:
        """
        Apply formatting to a single worksheet.
        
        Args:
            worksheet: openpyxl worksheet object
            formatting: Formatting configuration for this sheet
        """
        # Auto-fit columns
        if formatting.get('auto_fit_columns', False):
            self._auto_fit_columns(worksheet, formatting)
        
        # Set specific column widths
        column_widths = formatting.get('column_widths', {})
        if column_widths:
            self._set_column_widths(worksheet, column_widths)
        
        # Format header row
        if formatting.get('header_bold', False):
            self._make_headers_bold(worksheet)
        
        if formatting.get('header_background', False):
            header_color = formatting.get('header_background_color', 'D3D3D3')
            self._set_header_background(worksheet, header_color)
        
        # Freeze panes
        freeze_panes = formatting.get('freeze_panes')
        if freeze_panes:
            self._freeze_panes(worksheet, freeze_panes)
        
        # Freeze top row (shortcut)
        if formatting.get('freeze_top_row', False):
            worksheet.freeze_panes = 'A2'
        
        # Set row heights
        row_heights = formatting.get('row_heights', {})
        if row_heights:
            self._set_row_heights(worksheet, row_heights)
        
        # Auto-filter
        if formatting.get('auto_filter', False):
            self._add_auto_filter(worksheet)
    
    def _auto_fit_columns(self, worksheet, formatting: dict) -> None:
        """Auto-fit column widths based on content."""
        max_width = formatting.get('max_column_width', 50)
        min_width = formatting.get('min_column_width', 8)
        
        for column in worksheet.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            
            for cell in column:
                try:
                    if cell.value:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            
            # Calculate adjusted width with padding
            adjusted_width = max_length + 2
            adjusted_width = max(min_width, min(adjusted_width, max_width))
            
            worksheet.column_dimensions[column_letter].width = adjusted_width
        
        logger.debug(f"Auto-fitted columns with width range {min_width}-{max_width}")
    
    def _set_column_widths(self, worksheet, column_widths: dict) -> None:
        """Set specific column widths."""
        for column_ref, width in column_widths.items():
            # Handle both letter (A) and number (1) references
            if isinstance(column_ref, int):
                column_letter = get_column_letter(column_ref)
            else:
                column_letter = column_ref.upper()
            
            worksheet.column_dimensions[column_letter].width = width
        
        logger.debug(f"Set specific widths for {len(column_widths)} columns")
    
    def _make_headers_bold(self, worksheet) -> None:
        """Make the first row (headers) bold."""
        bold_font = Font(bold=True)
        
        for cell in worksheet[1]:  # First row
            cell.font = bold_font
        
        logger.debug("Applied bold formatting to header row")
    
    def _set_header_background(self, worksheet, color: str) -> None:
        """Set background color for header row."""
        # Remove # if present
        color = color.lstrip('#')
        fill = PatternFill(start_color=color, end_color=color, fill_type='solid')
        
        for cell in worksheet[1]:  # First row
            cell.fill = fill
        
        logger.debug(f"Applied background color {color} to header row")
    
    def _freeze_panes(self, worksheet, freeze_ref: str) -> None:
        """Freeze panes at specified cell reference."""
        try:
            worksheet.freeze_panes = freeze_ref
            logger.debug(f"Froze panes at {freeze_ref}")
        except Exception as e:
            logger.error(f"Failed to freeze panes at {freeze_ref}: {e}")
    
    def _set_row_heights(self, worksheet, row_heights: dict) -> None:
        """Set specific row heights."""
        for row_num, height in row_heights.items():
            worksheet.row_dimensions[row_num].height = height
        
        logger.debug(f"Set heights for {len(row_heights)} rows")
    
    def _add_auto_filter(self, worksheet) -> None:
        """Add auto-filter to the data range."""
        if worksheet.max_row > 1:  # Only if there's data beyond headers
            data_range = f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"
            worksheet.auto_filter.ref = data_range
            logger.debug(f"Added auto-filter to range {data_range}")
    
    def get_supported_features(self) -> list:
        """
        Get list of supported formatting features.
        
        Returns:
            List of supported feature strings
        """
        return [
            'auto_fit_columns', 'column_widths', 'header_bold', 'header_background',
            'freeze_panes', 'freeze_top_row', 'row_heights', 'auto_filter', 'active_sheet'
        ]
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Format existing Excel files with professional presentation features',
            'formatting_features': self.get_supported_features(),
            'formatting_categories': [
                'column_sizing', 'header_styling', 'pane_freezing', 
                'row_sizing', 'data_filtering', 'sheet_activation'
            ],
            'file_requirements': ['xlsx', 'xls'],
            'dependencies': ['openpyxl'],
            'examples': {
                'auto_fit': "Automatically size columns to fit content",
                'header_styling': "Bold headers with background color",
                'freeze_panes': "Freeze top row for easier navigation",
                'professional': "Apply comprehensive formatting for business reports"
            }
        }
