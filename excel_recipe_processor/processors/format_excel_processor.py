"""
Format Excel step processor for Excel automation recipes - Enhanced Version.

excel_recipe_processor/processors/format_excel_processor.py

Handles formatting existing Excel files with auto-fit columns, header styling, and other presentation features.
Enhanced with comprehensive header formatting including text colors and font sizes.
"""

import logging
import openpyxl
import webcolors

from pathlib import Path

from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

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

    def perform_file_operation(self) -> str:
        """Format the target Excel file."""
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

    def _validate_file_operation_config(self):
        """Validate format_excel specific configuration."""
        if not self.get_config_value('target_file'):
            raise StepProcessorError(f"Format Excel step '{self.step_name}' requires 'target_file'")
    
    def _validate_format_config(self, target_file: str, formatting: dict) -> None:
        """
        Validate formatting configuration parameters.
        
        Args:
            target_file: Target Excel file path
            formatting: Formatting configuration dict
        """
        # Validate color formats for new header options
        header_text_color = formatting.get('header_text_color')
        if header_text_color is not None:  # Validate any provided string, including empty ones
            self._validate_color_format(header_text_color, 'header_text_color')
        
        # Validate font size
        header_font_size = formatting.get('header_font_size')
        if header_font_size is not None:
            if not isinstance(header_font_size, (int, float)) or header_font_size <= 0:
                raise StepProcessorError(f"header_font_size must be a positive number, got: {header_font_size}")
        
        # Validate existing background color format
        header_bg_color = formatting.get('header_background_color')
        if header_bg_color is not None:  # Same fix for background color
            self._validate_color_format(header_bg_color, 'header_background_color')
        
        # Phase 2 Enhancement: Validate general formatting options
        general_text_color = formatting.get('general_text_color')
        if general_text_color is not None:
            self._validate_color_format(general_text_color, 'general_text_color')
        
        general_font_size = formatting.get('general_font_size')
        if general_font_size is not None:
            if not isinstance(general_font_size, (int, float)) or general_font_size <= 0:
                raise StepProcessorError(f"general_font_size must be a positive number, got: {general_font_size}")
        
        # Validate alignment options
        general_h_align = formatting.get('general_alignment_horizontal')
        if general_h_align is not None:
            valid_h_alignments = ['general', 'left', 'center', 'right', 'fill', 'justify', 'centerContinuous', 'distributed']
            if general_h_align not in valid_h_alignments:
                raise StepProcessorError(f"general_alignment_horizontal must be one of {valid_h_alignments}, got: {general_h_align}")
        
        general_v_align = formatting.get('general_alignment_vertical')
        if general_v_align is not None:
            valid_v_alignments = ['top', 'center', 'bottom', 'justify', 'distributed']
            if general_v_align not in valid_v_alignments:
                raise StepProcessorError(f"general_alignment_vertical must be one of {valid_v_alignments}, got: {general_v_align}")
        
        # Phase 3 Enhancement: Validate cell range formatting
        cell_ranges = formatting.get('cell_ranges', {})
        if cell_ranges:
            self._validate_cell_ranges(cell_ranges)

    def _is_valid_range_format(self, range_spec: str) -> bool:
        """
        Check if range specification follows valid Excel range format.
        
        Args:
            range_spec: Range specification to validate
            
        Returns:
            True if valid, False otherwise
        """
        import re
        
        # Patterns for valid Excel ranges
        patterns = [
            r'^[A-Z]+[0-9]+$',              # Single cell: A1, AB10
            r'^[A-Z]+[0-9]+:[A-Z]+[0-9]+$', # Cell range: A1:B2, AA1:AB10
            r'^[A-Z]+:[A-Z]+$',             # Column range: A:A, A:C
            r'^[0-9]+:[0-9]+$'              # Row range: 1:1, 1:5
        ]
        
        for pattern in patterns:
            if re.match(pattern, range_spec.upper()):
                return True
        
        return False

    def _validate_range_formatting_options(self, range_spec: str, formatting: dict) -> None:
        """
        Validate formatting options for a specific cell range.
        
        Args:
            range_spec: Range specification being validated
            formatting: Formatting options dict for this range
        """
        # Validate colors
        for color_field in ['text_color', 'background_color']:
            color_value = formatting.get(color_field)
            if color_value is not None:
                self._validate_color_format(color_value, f"{range_spec}.{color_field}")
        
        # Validate font size
        font_size = formatting.get('font_size')
        if font_size is not None:
            if not isinstance(font_size, (int, float)) or font_size <= 0:
                raise StepProcessorError(f"{range_spec}.font_size must be a positive number, got: {font_size}")
        
        # Validate font name
        font_name = formatting.get('font_name')
        if font_name is not None:
            if not isinstance(font_name, str) or not font_name.strip():
                raise StepProcessorError(f"{range_spec}.font_name must be a non-empty string, got: {font_name}")
        
        # Validate boolean options
        for bool_field in ['bold', 'italic']:
            bool_value = formatting.get(bool_field)
            if bool_value is not None:
                if not isinstance(bool_value, bool):
                    raise StepProcessorError(f"{range_spec}.{bool_field} must be a boolean (true/false), got: {bool_value}")
        
        # Validate alignment options
        h_align = formatting.get('alignment_horizontal')
        if h_align is not None:
            valid_h_alignments = ['general', 'left', 'center', 'right', 'fill', 'justify', 'centerContinuous', 'distributed']
            if h_align not in valid_h_alignments:
                raise StepProcessorError(f"{range_spec}.alignment_horizontal must be one of {valid_h_alignments}, got: {h_align}")
        
        v_align = formatting.get('alignment_vertical')
        if v_align is not None:
            valid_v_alignments = ['top', 'center', 'bottom', 'justify', 'distributed']
            if v_align not in valid_v_alignments:
                raise StepProcessorError(f"{range_spec}.alignment_vertical must be one of {valid_v_alignments}, got: {v_align}")
        
        # Validate border options (basic validation for now)
        border = formatting.get('border')
        if border is not None:
            self._validate_border_specification(range_spec, border)

    def _validate_border_specification(self, range_spec: str, border) -> None:
        """
        Validate border specification format.
        
        Args:
            range_spec: Range specification being validated  
            border: Border specification (string or dict)
        """
        if isinstance(border, str):
            # Simple border style
            valid_styles = ['thin', 'thick', 'medium', 'dashed', 'dotted', 'double', 'hair']
            if border not in valid_styles:
                raise StepProcessorError(f"{range_spec}.border style must be one of {valid_styles}, got: {border}")
        
        elif isinstance(border, dict):
            # Complex border specification
            valid_sides = ['top', 'bottom', 'left', 'right', 'all']
            for side, side_spec in border.items():
                if side not in valid_sides:
                    raise StepProcessorError(f"{range_spec}.border side must be one of {valid_sides}, got: {side}")
                
                if isinstance(side_spec, str):
                    # Simple side style
                    valid_styles = ['thin', 'thick', 'medium', 'dashed', 'dotted', 'double', 'hair']
                    if side_spec not in valid_styles:
                        raise StepProcessorError(f"{range_spec}.border.{side} style must be one of {valid_styles}, got: {side_spec}")
                
                elif isinstance(side_spec, dict):
                    # Detailed side specification with style and color
                    style = side_spec.get('style')
                    if style is not None:
                        valid_styles = ['thin', 'thick', 'medium', 'dashed', 'dotted', 'double', 'hair']
                        if style not in valid_styles:
                            raise StepProcessorError(f"{range_spec}.border.{side}.style must be one of {valid_styles}, got: {style}")
                    
                    color = side_spec.get('color')
                    if color is not None:
                        self._validate_color_format(color, f"{range_spec}.border.{side}.color")
                
                else:
                    raise StepProcessorError(f"{range_spec}.border.{side} must be a string or dict, got: {type(side_spec).__name__}")
        
        else:
            raise StepProcessorError(f"{range_spec}.border must be a string or dict, got: {type(border).__name__}")

    def _validate_cell_ranges(self, cell_ranges: dict) -> None:
        """
        Validate cell range formatting configuration (Phase 3 enhancement).
        
        Args:
            cell_ranges: Dict mapping range specs to formatting options
        """
        if not isinstance(cell_ranges, dict):
            raise StepProcessorError(f"cell_ranges must be a dictionary, got: {type(cell_ranges).__name__}")
        
        for range_spec, range_formatting in cell_ranges.items():
            # Validate range specification format
            if not isinstance(range_spec, str):
                raise StepProcessorError(f"Range specification must be a string, got: {type(range_spec).__name__}")
            
            range_spec_clean = range_spec.strip()
            if not range_spec_clean:
                raise StepProcessorError("Range specification cannot be empty")
            
            # Basic range format validation (A1, A1:B2, A:A, 1:1)
            if not self._is_valid_range_format(range_spec_clean):
                raise StepProcessorError(f"Invalid range format: '{range_spec}'. Use formats like 'A1', 'A1:B2', 'A:A', or '1:1'")
            
            # Validate formatting options for this range
            if not isinstance(range_formatting, dict):
                raise StepProcessorError(f"Range formatting for '{range_spec}' must be a dictionary, got: {type(range_formatting).__name__}")
            
            self._validate_range_formatting_options(range_spec, range_formatting)

    def _validate_color_format(self, color_value: str, field_name: str) -> None:
        """
        Validate color format - enhanced to support webcolors (Phase 3).
        
        Args:
            color_value: Color value to validate
            field_name: Name of the field being validated (for error messages)
        """
        if not isinstance(color_value, str):
            raise StepProcessorError(f"{field_name} must be a string, got: {type(color_value).__name__}")
        
        color_clean = color_value.strip()
        
        # Check for empty string after stripping
        if not color_clean:
            raise StepProcessorError(f"{field_name} cannot be empty")
        
        # Try to normalize the color - this will validate all supported formats
        try:
            self._normalize_color(color_clean)
        except Exception as e:
            raise StepProcessorError(f"{field_name} must be a valid color format (hex, CSS name, or RGB), got: {color_value}. Error: {e}")

    def _normalize_color(self, color_value: str) -> str:
        """
        Normalize color to 6-digit uppercase hex format for openpyxl (Phase 3 enhanced).
        
        Args:
            color_value: Color in various formats (hex, CSS names, RGB, etc.)
            
        Returns:
            6-digit uppercase hex color (e.g., 'FF0000')
        """
        color_clean = color_value.strip()
        
        try:
            
            # 1. RGB format: rgb(255, 0, 0)
            if color_clean.startswith('rgb(') and color_clean.endswith(')'):
                rgb_str = color_clean[4:-1]  # Remove 'rgb(' and ')'
                rgb_parts = [int(x.strip()) for x in rgb_str.split(',')]
                if len(rgb_parts) == 3:
                    hex_color = webcolors.rgb_to_hex(tuple(rgb_parts))
                    return hex_color[1:].upper()  # Remove # and uppercase
                else:
                    raise ValueError("RGB format must have exactly 3 values")
            
            # 2. Hex format with hash: #FF0000, #F00
            elif color_clean.startswith('#'):
                hex_color = webcolors.normalize_hex(color_clean)
                return hex_color[1:].upper()  # Remove # and uppercase
            
            # 3. Check if it contains any non-hex characters
            elif self._contains_non_hex_chars(color_clean):
                # Must be a color name: white, red, forestgreen, etc.
                hex_color = webcolors.name_to_hex(color_clean.lower())
                return hex_color[1:].upper()  # Remove # and uppercase
            
            # 4. Only hex characters - treat as plain hex: FF0000, F00
            else:
                if len(color_clean) == 3:
                    # Expand 3-digit to 6-digit hex: F00 -> FF0000
                    return ''.join([c*2 for c in color_clean]).upper()
                elif len(color_clean) == 6:
                    return color_clean.upper()
                else:
                    raise ValueError(f"Hex color must be 3 or 6 digits, got {len(color_clean)}")
                
        except ImportError:
            # Fallback to basic hex parsing if webcolors not available
            logger.warning("webcolors not available, using basic hex color parsing")
            return self._normalize_color_basic(color_clean)
        except Exception as e:
            # If webcolors fails, try basic parsing as fallback
            try:
                return self._normalize_color_basic(color_clean)
            except Exception:
                raise ValueError(f"Invalid color format '{color_value}': {e}")

    def _contains_non_hex_chars(self, text: str) -> bool:
        """
        Check if text contains any characters that are NOT valid hex digits.
        
        Args:
            text: Text to check
            
        Returns:
            True if contains non-hex characters, False if only hex characters
        """
        hex_chars = set('0123456789ABCDEFabcdef')
        return any(c not in hex_chars for c in text)

    def _normalize_color_basic(self, color_clean: str) -> str:
        """
        Basic color normalization (fallback when webcolors unavailable).
        
        Args:
            color_clean: Cleaned color string
            
        Returns:
            6-digit uppercase hex color
        """
        # Remove # if present
        if color_clean.startswith('#'):
            color_clean = color_clean[1:]
        
        # Check for empty string after removing hash
        if not color_clean:
            raise ValueError("Color cannot be just a hash symbol")
        
        if not all(c in '0123456789ABCDEFabcdef' for c in color_clean):
            raise ValueError("Must be a valid hex color (e.g., 'FF0000', '#FF0000')")
        
        if len(color_clean) == 3:
            # Expand 3-digit to 6-digit hex
            color_clean = ''.join([c*2 for c in color_clean])
        elif len(color_clean) != 6:
            raise ValueError("Hex color must be 3 or 6 digits")
        
        return color_clean.upper()
    
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
        
        # Phase 2 Enhancement: Apply general formatting to all cells
        self._apply_general_formatting(worksheet, formatting)
        
        # Enhanced header formatting (Phase 1 enhancement)
        self._apply_header_formatting(worksheet, formatting)
        
        # Phase 3 Enhancement: Apply cell range specific formatting
        cell_ranges = formatting.get('cell_ranges', {})
        if cell_ranges:
            self._apply_cell_range_formatting(worksheet, cell_ranges)
        
        # Freeze panes
        freeze_panes = formatting.get('freeze_panes')
        if freeze_panes:
            self._freeze_panes(worksheet, freeze_panes)
        
        # Freeze top row (shortcut)
        if formatting.get('freeze_top_row', False):
            self._freeze_panes(worksheet, 'A2')
        
        # Set row heights
        row_heights = formatting.get('row_heights', {})
        if row_heights:
            self._set_row_heights(worksheet, row_heights)
        
        # Auto-filter
        if formatting.get('auto_filter', False):
            self._add_auto_filter(worksheet)
    
    def _apply_header_formatting(self, worksheet, formatting: dict) -> None:
        """
        Apply comprehensive header formatting including new Phase 1 enhancements.
        
        Args:
            worksheet: openpyxl worksheet object
            formatting: Formatting configuration
        """
        # Check if any header formatting is needed
        needs_header_formatting = any([
            formatting.get('header_bold', False),
            formatting.get('header_text_color') is not None,  # Check if explicitly provided
            formatting.get('header_font_size') is not None,   # Check if explicitly provided
            formatting.get('header_background', False)
        ])
        
        if not needs_header_formatting:
            return
        
        # Build comprehensive font for headers
        font_kwargs = {}
        
        # Bold setting
        if formatting.get('header_bold', False):
            font_kwargs['bold'] = True
        
        # Text color (Phase 1 enhancement)
        header_text_color = formatting.get('header_text_color')
        if header_text_color:
            normalized_color = self._normalize_color(header_text_color)
            font_kwargs['color'] = normalized_color
            logger.debug(f"Applied header text color: {header_text_color} → {normalized_color}")
        
        # Font size (Phase 1 enhancement)
        header_font_size = formatting.get('header_font_size')
        if header_font_size:
            font_kwargs['size'] = header_font_size
            logger.debug(f"Applied header font size: {header_font_size}")
        
        # Apply font formatting to header row
        if font_kwargs:
            header_font = Font(**font_kwargs)
            for cell in worksheet[1]:  # First row
                cell.font = header_font
            
            logger.debug(f"Applied header font formatting: {font_kwargs}")
        
        # Background color (existing functionality)
        if formatting.get('header_background', False):
            header_color = formatting.get('header_background_color', 'D3D3D3')
            self._set_header_background(worksheet, header_color)
    
    def _apply_general_formatting(self, worksheet, formatting: dict) -> None:
        """
        Apply general formatting to all cells (Phase 2 enhancement).
        
        Args:
            worksheet: openpyxl worksheet object
            formatting: Formatting configuration
        """
        # Check if any general formatting is needed
        needs_general_formatting = any([
            formatting.get('general_text_color') is not None,
            formatting.get('general_font_size') is not None,
            formatting.get('general_font_name') is not None,
            formatting.get('general_alignment_horizontal') is not None,
            formatting.get('general_alignment_vertical') is not None
        ])
        
        if not needs_general_formatting:
            return
        
        # Build font formatting if needed
        font_kwargs = {}
        general_text_color = formatting.get('general_text_color')
        if general_text_color:
            normalized_color = self._normalize_color(general_text_color)
            font_kwargs['color'] = normalized_color
        
        general_font_size = formatting.get('general_font_size')
        if general_font_size:
            font_kwargs['size'] = general_font_size
        
        general_font_name = formatting.get('general_font_name')
        if general_font_name:
            font_kwargs['name'] = general_font_name
        
        # Build alignment formatting if needed
        alignment_kwargs = {}
        general_h_align = formatting.get('general_alignment_horizontal')
        if general_h_align:
            alignment_kwargs['horizontal'] = general_h_align
        
        general_v_align = formatting.get('general_alignment_vertical')
        if general_v_align:
            alignment_kwargs['vertical'] = general_v_align
        
        # Apply formatting to all data cells (excluding headers which are handled separately)
        start_row = 2 if worksheet.max_row > 1 else 1  # Skip header row if present
        
        for row in range(start_row, worksheet.max_row + 1):
            for col in range(1, worksheet.max_column + 1):
                cell = worksheet.cell(row=row, column=col)
                
                # Apply font formatting
                if font_kwargs:
                    # Merge with existing font properties to avoid overwriting header formatting
                    existing_font = cell.font
                    merged_font_kwargs = {
                        'name': existing_font.name,
                        'size': existing_font.size,
                        'bold': existing_font.bold,
                        'italic': existing_font.italic,
                        'color': existing_font.color
                    }
                    # Override with general formatting
                    merged_font_kwargs.update(font_kwargs)
                    cell.font = Font(**merged_font_kwargs)
                
                # Apply alignment formatting
                if alignment_kwargs:
                    # Merge with existing alignment properties
                    existing_alignment = cell.alignment
                    merged_alignment_kwargs = {
                        'horizontal': existing_alignment.horizontal,
                        'vertical': existing_alignment.vertical,
                        'text_rotation': existing_alignment.text_rotation,
                        'wrap_text': existing_alignment.wrap_text,
                        'shrink_to_fit': existing_alignment.shrink_to_fit,
                        'indent': existing_alignment.indent
                    }
                    # Override with general formatting
                    merged_alignment_kwargs.update(alignment_kwargs)
                    cell.alignment = Alignment(**merged_alignment_kwargs)
        
        applied_features = []
        if font_kwargs:
            applied_features.append(f"font: {font_kwargs}")
        if alignment_kwargs:
            applied_features.append(f"alignment: {alignment_kwargs}")
        
        logger.debug(f"Applied general formatting to rows {start_row}-{worksheet.max_row}: {', '.join(applied_features)}")
    
    def _apply_cell_range_formatting(self, worksheet, cell_ranges: dict) -> None:
        """
        Apply formatting to specific cell ranges (Phase 3 enhancement).
        
        Args:
            worksheet: openpyxl worksheet object
            cell_ranges: Dict mapping range specs to formatting options
        """
        ranges_processed = 0
        
        for range_spec, range_formatting in cell_ranges.items():
            try:
                # Parse the range specification
                cells = self._parse_range_spec(worksheet, range_spec)
                
                # Apply formatting to each cell in the range
                self._apply_cell_formatting(cells, range_formatting)
                
                ranges_processed += 1
                logger.debug(f"Applied formatting to range '{range_spec}': {len(cells)} cells")
                
            except Exception as e:
                logger.error(f"Failed to format range '{range_spec}': {e}")
                raise StepProcessorError(f"Error formatting cell range '{range_spec}': {e}")
        
        logger.info(f"Applied cell range formatting to {ranges_processed} ranges")

    def _parse_range_spec(self, worksheet, range_spec: str):
        """
        Parse a range specification and return list of cells.
        
        Args:
            worksheet: openpyxl worksheet object
            range_spec: Range like "A1:C10", "B2", etc.
            
        Returns:
            List of openpyxl cell objects
        """
        range_spec_clean = range_spec.strip().upper()
        
        if ':' in range_spec_clean:
            # Range specification: A1:C10
            cell_range = worksheet[range_spec_clean]
            # Flatten the range into a list of cells
            cells = []
            for row in cell_range:
                if hasattr(row, '__iter__'):
                    cells.extend(row)
                else:
                    cells.append(row)
            return cells
        else:
            # Single cell specification: B2
            return [worksheet[range_spec_clean]]

    def _apply_cell_formatting(self, cells, formatting: dict) -> None:
        """
        Apply formatting to a list of cells.
        
        Args:
            cells: List of openpyxl cell objects
            formatting: Formatting options to apply
        """
        for cell in cells:
            # Build font formatting
            font_kwargs = {}
            existing_font = cell.font
            
            # Start with existing font properties
            font_kwargs = {
                'name': existing_font.name,
                'size': existing_font.size,
                'bold': existing_font.bold,
                'italic': existing_font.italic,
                'color': existing_font.color
            }
            
            # Apply range-specific font formatting
            if 'text_color' in formatting and formatting['text_color']:
                font_kwargs['color'] = self._normalize_color(formatting['text_color'])
            
            if 'font_size' in formatting and formatting['font_size']:
                font_kwargs['size'] = formatting['font_size']
            
            if 'font_name' in formatting and formatting['font_name']:
                font_kwargs['name'] = formatting['font_name']
            
            if 'bold' in formatting and formatting['bold'] is not None:
                font_kwargs['bold'] = formatting['bold']
            
            if 'italic' in formatting and formatting['italic'] is not None:
                font_kwargs['italic'] = formatting['italic']
            
            # Apply font formatting
            cell.font = Font(**font_kwargs)
            
            # Build alignment formatting
            alignment_kwargs = {}
            existing_alignment = cell.alignment
            
            # Start with existing alignment properties
            alignment_kwargs = {
                'horizontal': existing_alignment.horizontal,
                'vertical': existing_alignment.vertical,
                'text_rotation': existing_alignment.text_rotation,
                'wrap_text': existing_alignment.wrap_text,
                'shrink_to_fit': existing_alignment.shrink_to_fit,
                'indent': existing_alignment.indent
            }
            
            # Apply range-specific alignment formatting
            if 'alignment_horizontal' in formatting and formatting['alignment_horizontal']:
                alignment_kwargs['horizontal'] = formatting['alignment_horizontal']
            
            if 'alignment_vertical' in formatting and formatting['alignment_vertical']:
                alignment_kwargs['vertical'] = formatting['alignment_vertical']
            
            # Apply alignment formatting
            cell.alignment = Alignment(**alignment_kwargs)
            
            # Apply background color
            if 'background_color' in formatting and formatting['background_color']:
                bg_color = self._normalize_color(formatting['background_color'])
                fill = PatternFill(start_color=bg_color, end_color=bg_color, fill_type='solid')
                cell.fill = fill
            
            # Apply border formatting
            if 'border' in formatting and formatting['border']:
                border = self._create_border(formatting['border'])
                cell.border = border

    def _create_border(self, border_spec):
        """
        Create an openpyxl Border object from specification.
        
        Args:
            border_spec: Border specification (string or dict)
            
        Returns:
            openpyxl Border object
        """
        if isinstance(border_spec, str):
            # Simple border: apply same style to all sides
            side = Side(style=border_spec)
            return Border(left=side, right=side, top=side, bottom=side)
        
        elif isinstance(border_spec, dict):
            # Detailed border specification
            sides = {}
            
            for side_name, side_spec in border_spec.items():
                if side_name == 'all':
                    # Apply to all sides
                    if isinstance(side_spec, str):
                        side_obj = Side(style=side_spec)
                    else:
                        style = side_spec.get('style', 'thin')
                        color = side_spec.get('color')
                        side_obj = Side(style=style, color=self._normalize_color(color) if color else None)
                    
                    sides['left'] = sides['right'] = sides['top'] = sides['bottom'] = side_obj
                else:
                    # Specific side
                    if isinstance(side_spec, str):
                        sides[side_name] = Side(style=side_spec)
                    else:
                        style = side_spec.get('style', 'thin')
                        color = side_spec.get('color')
                        sides[side_name] = Side(style=style, color=self._normalize_color(color) if color else None)
            
            return Border(**sides)
        
        else:
            raise ValueError(f"Invalid border specification: {border_spec}")

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
    
    def _set_header_background(self, worksheet, color: str) -> None:
        """Set background color for header row."""
        # Remove # if present and normalize
        normalized_color = self._normalize_color(color)
        fill = PatternFill(start_color=normalized_color, end_color=normalized_color, fill_type='solid')
        
        for cell in worksheet[1]:  # First row
            cell.fill = fill
        
        logger.debug(f"Applied background color {normalized_color} to header row")
    
    def _freeze_panes(self, worksheet, freeze_ref: str) -> None:
        """
        Freeze panes at specified cell reference with comprehensive validation.
        
        Args:
            worksheet: openpyxl worksheet object
            freeze_ref: Cell reference like 'A2', 'B3', 'D10', etc.
                        Special case: 'A1' unfreezes all panes
        """
        import re
        from openpyxl.utils import column_index_from_string, get_column_letter
        
        # Get sheet name for detailed logging
        sheet_name = getattr(worksheet, 'title', 'Unknown')
        
        # Validate input type and clean up
        if not isinstance(freeze_ref, str):
            error_msg = f"freeze_panes must be a string cell reference, got {type(freeze_ref).__name__}: {freeze_ref}"
            logger.error(f"❌ [{sheet_name}] {error_msg}")
            raise StepProcessorError(error_msg)
        
        freeze_ref_clean = freeze_ref.strip().upper()
        if not freeze_ref_clean:
            error_msg = "freeze_panes cannot be empty string"
            logger.error(f"❌ [{sheet_name}] {error_msg}")
            raise StepProcessorError(error_msg)
        
        # Handle special case: A1 means unfreeze everything
        if freeze_ref_clean == 'A1':
            worksheet.freeze_panes = None
            logger.info(f"🔓 [{sheet_name}] Unfroze all panes (A1 removes all freezing)")
            return
        
        # Validate cell reference format using regex
        cell_pattern = r'^([A-Z]+)(\d+)$'
        match = re.match(cell_pattern, freeze_ref_clean)
        
        if not match:
            error_msg = f"Invalid freeze_panes format '{freeze_ref}'. Use format like 'A2', 'B3', etc."
            logger.error(f"❌ [{sheet_name}] {error_msg}")
            raise StepProcessorError(error_msg)
        
        column_letters, row_number = match.groups()
        row_number = int(row_number)
        
        # Validate row number
        if row_number < 1:
            error_msg = f"freeze_panes row number must be >= 1, got: {row_number}"
            logger.error(f"❌ [{sheet_name}] {error_msg}")
            raise StepProcessorError(error_msg)
        
        # Apply freeze panes
        try:
            worksheet.freeze_panes = freeze_ref_clean
            logger.info(f"❄️ [{sheet_name}] Froze panes at {freeze_ref_clean}")
        except Exception as e:
            error_msg = f"Failed to freeze panes at '{freeze_ref_clean}': {e}"
            logger.error(f"❌ [{sheet_name}] {error_msg}")
            raise StepProcessorError(error_msg)
    
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
            # Column and row sizing
            'auto_fit_columns', 'column_widths', 'row_heights',
            # Header formatting (Phase 1)
            'header_bold', 'header_background', 'header_text_color', 'header_font_size',
            # General formatting (Phase 2)
            'general_text_color', 'general_font_size', 'general_font_name',
            'general_alignment_horizontal', 'general_alignment_vertical',
            # Cell range formatting (Phase 3)
            'cell_ranges',
            # Panes and filtering
            'freeze_panes', 'freeze_top_row', 'auto_filter', 'active_sheet'
        ]
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Format existing Excel files with professional presentation features',
            'formatting_features': self.get_supported_features(),
            'formatting_categories': [
                'column_sizing', 'enhanced_header_styling', 'general_cell_formatting',
                'cell_range_targeting', 'advanced_color_support', 'border_formatting',  # Added Phase 3 categories
                'pane_freezing', 'row_sizing', 'data_filtering', 'sheet_activation'
            ],
            'file_requirements': ['xlsx', 'xls'],
            'dependencies': ['openpyxl'],
            'optional_dependencies': ['webcolors'],  # Enhanced color support
            'phase_1_enhancements': ['header_text_color', 'header_font_size'],
            'phase_2_enhancements': ['general_text_color', 'general_font_size', 'general_font_name', 'general_alignment_horizontal', 'general_alignment_vertical'],
            'phase_3_enhancements': ['cell_ranges', 'webcolors_integration', 'border_formatting', 'css_color_names', 'rgb_color_support'],
            'color_formats_supported': [
                'hex_with_hash (#FF0000)', 'hex_without_hash (FF0000)', 'short_hex (#F00)', 
                'css_color_names (red, blue, forestgreen)', 'rgb_format (rgb(255, 0, 0))'
            ],
            'examples': {
                'auto_fit': "Automatically size columns to fit content",
                'enhanced_headers': "Bold headers with custom text color and font size",
                'general_formatting': "Apply font and alignment to all data cells",
                'cell_range_targeting': "Format specific ranges with custom styles",
                'css_colors': "Use CSS color names like 'red', 'forestgreen', 'navy'",
                'rgb_colors': "Use RGB format like 'rgb(255, 0, 0)' for red",
                'borders': "Add professional borders to specific cell ranges",
                'dark_theme_fix': "White text on dark backgrounds for readability",
                'freeze_panes': "Freeze top row for easier navigation",
                'professional': "Apply comprehensive formatting for business reports"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get usage examples for this processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('format_excel')


# End of file #
