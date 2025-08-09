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
        """Format the target Excel file with new explicit sheet targeting."""
        target_file = self.get_config_value('target_file')
        sheet_configs = self.get_config_value('formatting', [])
        active_sheet = self.get_config_value('active_sheet')
        
        # Validate configuration
        self._validate_format_config(target_file, sheet_configs, active_sheet)
        
        # Apply variable substitution BEFORE file operations
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            resolved_file = self.variable_substitution.substitute(target_file)
        else:
            resolved_file = target_file
        
        # Check file exists
        if not Path(resolved_file).exists():
            raise StepProcessorError(f"Target file not found: {resolved_file}")
        
        # Load and format the workbook
        formatted_sheets = self._format_excel_file(resolved_file, sheet_configs, active_sheet)
        
        return f"formatted {resolved_file} ({formatted_sheets} sheets processed)"

    def _validate_file_operation_config(self):
        """Validate format_excel specific configuration (updated for new format)."""
        if not self.get_config_value('target_file'):
            raise StepProcessorError(f"Format Excel step '{self.step_name}' requires 'target_file'")
        
        # Get and validate the formatting configuration
        sheet_configs = self.get_config_value('formatting', [])
        active_sheet = self.get_config_value('active_sheet')
        target_file = self.get_config_value('target_file')
        
        # Validate using the new validation method
        self._validate_format_config(target_file, sheet_configs, active_sheet)

    def _validate_format_config(self, target_file: str, sheet_configs: list, active_sheet=None) -> None:
        """
        Validate the new explicit sheet targeting configuration format.
        
        Args:
            target_file: Target Excel file path
            sheet_configs: List of sheet configuration dicts
            active_sheet: Active sheet specification (optional)
        """
        # Require sheet_configs to be a list
        if not isinstance(sheet_configs, list):
            raise StepProcessorError("'formatting' must be a list of sheet configurations, each with a 'sheet' key")
        
        if not sheet_configs:
            logger.warning("Empty formatting list - no sheets will be formatted")
            return
        
        # Validate each sheet configuration
        for i, sheet_config in enumerate(sheet_configs):
            if not isinstance(sheet_config, dict):
                raise StepProcessorError(f"Formatting entry {i+1} must be a dictionary")
            
            if 'sheet' not in sheet_config:
                raise StepProcessorError(f"Formatting entry {i+1} must have a 'sheet' key")
            
            sheet_spec = sheet_config['sheet']
            if not isinstance(sheet_spec, (str, int)):
                raise StepProcessorError(f"Sheet specification must be string (name) or integer (1-based index), got: {type(sheet_spec).__name__}")
            
            if isinstance(sheet_spec, int) and sheet_spec < 1:
                raise StepProcessorError(f"Sheet index must be >= 1, got: {sheet_spec}")
            
            if isinstance(sheet_spec, str) and not sheet_spec.strip():
                raise StepProcessorError("Sheet name cannot be empty")
            
            # Validate the formatting options for this sheet
            self._validate_sheet_formatting_options(sheet_config, f"sheet {sheet_spec}")
        
        # Validate active_sheet specification
        if active_sheet is not None:
            if not isinstance(active_sheet, (str, int)):
                raise StepProcessorError(f"active_sheet must be string (name) or integer (1-based index), got: {type(active_sheet).__name__}")
            
            if isinstance(active_sheet, int) and active_sheet < 1:
                raise StepProcessorError(f"active_sheet index must be >= 1, got: {active_sheet}")
            
            if isinstance(active_sheet, str) and not active_sheet.strip():
                raise StepProcessorError("active_sheet name cannot be empty")

    def _validate_sheet_formatting_options(self, sheet_config: dict, context: str) -> None:
        """
        Complete validation for sheet formatting options with header alignment support.
        
        Args:
            sheet_config: Sheet formatting configuration
            context: Context string for error messages (e.g., "sheet Summary")
        """
        # Define all known/valid field names
        valid_fields = {
            # Header formatting
            'header_text_color', 'header_background_color', 'header_font_size', 'header_bold', 'header_background',
            'header_alignment_horizontal', 'header_alignment_vertical',  # NEW: Header alignment
            # General formatting  
            'general_text_color', 'general_font_size', 'general_font_name', 
            'general_alignment_horizontal', 'general_alignment_vertical',
            # Column and row sizing
            'auto_fit_columns', 'max_column_width', 'min_column_width', 'column_widths', 'row_heights',
            # Panes and features
            'freeze_panes', 'freeze_top_row', 'auto_filter',
            # Cell ranges and borders
            'cell_ranges',
            # Required field
            'sheet'
        }
        
        # Check for unknown fields first with helpful suggestions
        for field_name in sheet_config.keys():
            if field_name not in valid_fields:
                # Common mistakes with helpful suggestions
                suggestions = {
                    'header_font_color': 'header_text_color',
                    'header_color': 'header_text_color', 
                    'font_color': 'header_text_color or general_text_color',
                    'background_color': 'header_background_color',
                    'text_color': 'header_text_color or general_text_color',
                    'alignment_vertical': 'header_alignment_vertical (for headers) or use cell_ranges for specific cells',
                    'alignment_horizontal': 'header_alignment_horizontal (for headers) or use cell_ranges for specific cells'
                }
                
                if field_name in suggestions:
                    suggestion = suggestions[field_name]
                    raise StepProcessorError(f"Unknown field '{field_name}' in {context}. Did you mean '{suggestion}'?")
                else:
                    valid_list = ', '.join(sorted(valid_fields))
                    raise StepProcessorError(f"Unknown field '{field_name}' in {context}. Valid fields: {valid_list}")
        
        # Validate colors
        for color_field in ['header_text_color', 'header_background_color', 'general_text_color']:
            color_value = sheet_config.get(color_field)
            if color_value is not None:
                try:
                    self._validate_color_format(color_value, f"{context}.{color_field}")
                except Exception as e:
                    raise StepProcessorError(f"Invalid {color_field} for {context}: {e}")
        
        # Validate font sizes
        for size_field in ['header_font_size', 'general_font_size']:
            size_value = sheet_config.get(size_field)
            if size_value is not None:
                if not isinstance(size_value, (int, float)) or size_value <= 0:
                    raise StepProcessorError(f"{context}.{size_field} must be a positive number, got: {size_value}")
        
        # Validate font names
        general_font_name = sheet_config.get('general_font_name')
        if general_font_name is not None:
            if not isinstance(general_font_name, str) or not general_font_name.strip():
                raise StepProcessorError(f"{context}.general_font_name must be a non-empty string, got: {general_font_name}")
        
        # Validate header alignment options
        h_align = sheet_config.get('header_alignment_horizontal')
        if h_align is not None:
            valid_h_alignments = ['general', 'left', 'center', 'right', 'fill', 'justify', 'centerContinuous', 'distributed']
            if h_align not in valid_h_alignments:
                raise StepProcessorError(f"{context}.header_alignment_horizontal must be one of {valid_h_alignments}, got: {h_align}")
        
        v_align = sheet_config.get('header_alignment_vertical')
        if v_align is not None:
            valid_v_alignments = ['top', 'center', 'bottom', 'justify', 'distributed']
            if v_align not in valid_v_alignments:
                raise StepProcessorError(f"{context}.header_alignment_vertical must be one of {valid_v_alignments}, got: {v_align}")
        
        # Validate general alignment options
        general_h_align = sheet_config.get('general_alignment_horizontal')
        if general_h_align is not None:
            valid_h_alignments = ['general', 'left', 'center', 'right', 'fill', 'justify', 'centerContinuous', 'distributed']
            if general_h_align not in valid_h_alignments:
                raise StepProcessorError(f"{context}.general_alignment_horizontal must be one of {valid_h_alignments}, got: {general_h_align}")
        
        general_v_align = sheet_config.get('general_alignment_vertical')
        if general_v_align is not None:
            valid_v_alignments = ['top', 'center', 'bottom', 'justify', 'distributed']
            if general_v_align not in valid_v_alignments:
                raise StepProcessorError(f"{context}.general_alignment_vertical must be one of {valid_v_alignments}, got: {general_v_align}")
        
        # Validate boolean options
        for bool_field in ['header_bold', 'header_background', 'auto_fit_columns', 'freeze_top_row', 'auto_filter']:
            bool_value = sheet_config.get(bool_field)
            if bool_value is not None:
                if not isinstance(bool_value, bool):
                    raise StepProcessorError(f"{context}.{bool_field} must be a boolean (true/false), got: {bool_value}")
        
        # Validate numeric constraints
        for constraint_field in ['max_column_width', 'min_column_width']:
            constraint_value = sheet_config.get(constraint_field)
            if constraint_value is not None:
                if not isinstance(constraint_value, (int, float)) or constraint_value <= 0:
                    raise StepProcessorError(f"{context}.{constraint_field} must be a positive number, got: {constraint_value}")
        
        # Validate min <= max for column widths
        min_width = sheet_config.get('min_column_width')
        max_width = sheet_config.get('max_column_width')
        if min_width is not None and max_width is not None:
            if min_width > max_width:
                raise StepProcessorError(f"{context}: min_column_width ({min_width}) cannot be greater than max_column_width ({max_width})")
        
        # Validate row heights
        row_heights = sheet_config.get('row_heights', {})
        if row_heights:
            if not isinstance(row_heights, dict):
                raise StepProcessorError(f"{context}.row_heights must be a dictionary")
            
            for row_num, height in row_heights.items():
                if not isinstance(row_num, int) or row_num < 1:
                    raise StepProcessorError(f"{context}.row_heights key must be positive integer (row number), got: {row_num}")
                if not isinstance(height, (int, float)) or height <= 0:
                    raise StepProcessorError(f"{context}.row_heights[{row_num}] must be positive number, got: {height}")
        
        # Validate column widths
        column_widths = sheet_config.get('column_widths', {})
        if column_widths:
            if not isinstance(column_widths, dict):
                raise StepProcessorError(f"{context}.column_widths must be a dictionary")
            
            for col_spec, width in column_widths.items():
                # Column can be specified as letter (A, B) or number (1, 2)
                if not isinstance(col_spec, (str, int)):
                    raise StepProcessorError(f"{context}.column_widths key must be string (letter) or integer (number), got: {type(col_spec).__name__}")
                if isinstance(col_spec, str) and not col_spec.strip():
                    raise StepProcessorError(f"{context}.column_widths key cannot be empty string")
                if isinstance(col_spec, int) and col_spec < 1:
                    raise StepProcessorError(f"{context}.column_widths key must be positive integer, got: {col_spec}")
                if not isinstance(width, (int, float)) or width <= 0:
                    raise StepProcessorError(f"{context}.column_widths[{col_spec}] must be positive number, got: {width}")
        
        # Validate freeze_panes format if specified
        freeze_panes = sheet_config.get('freeze_panes')
        if freeze_panes is not None:
            if not isinstance(freeze_panes, str) or not freeze_panes.strip():
                raise StepProcessorError(f"{context}.freeze_panes must be a non-empty string (e.g., 'A2', 'B3')")
            
            # Basic format validation (A1, B2, etc.)
            import re
            if not re.match(r'^[A-Z]+\d+$', freeze_panes.strip().upper()):
                raise StepProcessorError(f"{context}.freeze_panes must be in format like 'A2', 'B3', etc., got: {freeze_panes}")
        
        # Validate cell ranges
        cell_ranges = sheet_config.get('cell_ranges', {})
        if cell_ranges:
            if not isinstance(cell_ranges, dict):
                raise StepProcessorError(f"{context}.cell_ranges must be a dictionary")
            # Delegate detailed cell range validation to existing method
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

    def _format_excel_file(self, filename: str, sheet_configs: list, active_sheet=None) -> int:
        """
        Apply formatting to an Excel file with explicit sheet targeting.
        
        Args:
            filename: Path to Excel file
            sheet_configs: List of sheet configuration dicts
            active_sheet: Sheet to set as active (name or number), optional
            
        Returns:
            Number of sheets processed
        """
        logger.info(f"📋 Loading Excel file: {Path(filename).name}")
        
        # Load workbook
        workbook = openpyxl.load_workbook(filename)
        sheets_processed = 0
        total_sheets = len(workbook.worksheets)
        
        logger.info(f"📊 Found {total_sheets} worksheet(s): {', '.join(workbook.sheetnames)}")
        
        # Check if we have sheet configurations
        if not sheet_configs:
            logger.warning("⚠️ No sheet formatting configurations found - no formatting applied")
            workbook.close()
            return 0
            
        logger.info(f"🎯 Processing {len(sheet_configs)} explicit sheet configuration(s)")
        
        # Process each sheet configuration
        for i, sheet_config in enumerate(sheet_configs):
            if 'sheet' not in sheet_config:
                workbook.close()
                raise StepProcessorError(f"Formatting entry {i+1} must have a 'sheet' key")
            
            sheet_spec = sheet_config['sheet']
            sheet_name = self._resolve_sheet_name(workbook, sheet_spec)
            
            if sheet_name:
                worksheet = workbook[sheet_name]
                logger.info(f"🔧 Processing sheet: '{sheet_name}' (specified as: {sheet_spec})")
                self._apply_sheet_formatting(worksheet, sheet_config)
                sheets_processed += 1
            else:
                logger.warning(f"⚠️ Sheet '{sheet_spec}' not found, skipping")
        
        # Set active sheet if specified (supports both names and numbers)
        if active_sheet is not None:
            active_sheet_name = self._resolve_sheet_name(workbook, active_sheet)
            if active_sheet_name:
                workbook.active = workbook[active_sheet_name]
                logger.info(f"📌 Set active sheet to '{active_sheet_name}' (specified as: {active_sheet})")
            else:
                logger.warning(f"⚠️ Active sheet '{active_sheet}' not found")
        
        # Save workbook
        logger.info(f"💾 Saving formatted workbook...")
        workbook.save(filename)
        workbook.close()
        
        logger.info(f"✅ Excel formatting completed: {sheets_processed}/{total_sheets} sheets processed")
        return sheets_processed

    def _resolve_sheet_name(self, workbook, sheet_spec) -> str:
        """
        Resolve a sheet specification (name or number) to actual sheet name.
        
        Args:
            workbook: openpyxl workbook object
            sheet_spec: Sheet name (string) or 1-based index (integer)
            
        Returns:
            Actual sheet name if found, None if not found
        """
        if isinstance(sheet_spec, str):
            # Sheet specified by name
            if sheet_spec in workbook.sheetnames:
                return sheet_spec
            else:
                logger.debug(f"Sheet name '{sheet_spec}' not found in {workbook.sheetnames}")
                return None
                
        elif isinstance(sheet_spec, int):
            # Sheet specified by 1-based index
            if 1 <= sheet_spec <= len(workbook.worksheets):
                # Convert to 0-based index and get sheet name
                resolved_name = workbook.worksheets[sheet_spec - 1].title
                logger.debug(f"Sheet index {sheet_spec} resolved to '{resolved_name}'")
                return resolved_name
            else:
                logger.debug(f"Sheet index {sheet_spec} out of range (1-{len(workbook.worksheets)})")
                return None
                
        else:
            # Invalid sheet specification type
            logger.debug(f"Invalid sheet specification type: {type(sheet_spec)}")
            return None

    def _apply_sheet_formatting(self, worksheet, formatting: dict) -> None:
        """
        Apply formatting to a specific worksheet with correct operation order.
        
        Args:
            worksheet: openpyxl worksheet object
            formatting: Formatting configuration for this sheet
        """
        sheet_name = worksheet.title
        applied_operations = []
        
        # STEP 1: Apply text formatting FIRST so auto-fit can measure correctly
        
        # Apply general formatting to all cells
        if self._has_general_formatting(formatting):
            logger.info(f"🎨 [{sheet_name}] Applying general cell formatting")
            self._apply_general_formatting(worksheet, formatting)
            applied_operations.append("general formatting")
        
        # Enhanced header formatting (BEFORE auto-fit so it can measure bold/larger text)
        if self._has_header_formatting(formatting):
            header_details = self._get_header_formatting_details(formatting)
            logger.info(f"👑 [{sheet_name}] Header formatting: {header_details}")
            self._apply_header_formatting(worksheet, formatting)
            applied_operations.append("header formatting")
        
        # Apply cell range specific formatting (BEFORE auto-fit)
        cell_ranges = formatting.get('cell_ranges', {})
        if cell_ranges:
            logger.info(f"🎯 [{sheet_name}] Applying cell range formatting to {len(cell_ranges)} ranges")
            self._apply_cell_range_formatting(worksheet, cell_ranges)
            applied_operations.append(f"range formatting ({len(cell_ranges)} ranges)")
        
        # STEP 2: Set row heights (might affect auto-fit calculations)
        row_heights = formatting.get('row_heights', {})
        if row_heights:
            logger.info(f"📊 [{sheet_name}] Setting row heights for {len(row_heights)} rows")
            self._set_row_heights(worksheet, row_heights)
            applied_operations.append(f"row heights ({len(row_heights)} rows)")
        
        # STEP 3: Add AutoFilter BEFORE auto-fit so auto-fit knows dropdowns will be there
        if formatting.get('auto_filter', False):
            if worksheet.max_row > 1:
                data_range = f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"
                logger.info(f"🔍 [{sheet_name}] Adding auto-filter to range {data_range}")
                self._add_auto_filter(worksheet)
                applied_operations.append(f"auto-filter ({data_range})")
            else:
                logger.warning(f"⚠️ [{sheet_name}] Skipping auto-filter - no data rows found")
        
        # STEP 4: NOW auto-fit can measure the final formatted text + account for AutoFilter
        if formatting.get('auto_fit_columns', False):
            max_width = formatting.get('max_column_width', 50)
            min_width = formatting.get('min_column_width', 8)
            has_auto_filter = formatting.get('auto_filter', False)
            filter_note = " (with AutoFilter compensation)" if has_auto_filter else ""
            logger.info(f"📏 [{sheet_name}] Auto-fitting columns (width: {min_width}-{max_width}){filter_note}")
            self._auto_fit_columns(worksheet, formatting)
            applied_operations.append("auto-fit columns")
        
        # STEP 5: Manual column widths override auto-fit
        column_widths = formatting.get('column_widths', {})
        if column_widths:
            logger.info(f"📐 [{sheet_name}] Setting custom column widths for {len(column_widths)} columns")
            self._set_column_widths(worksheet, column_widths)
            applied_operations.append(f"custom widths ({len(column_widths)} columns)")
        
        # STEP 6: Freeze panes (purely visual, doesn't affect sizing)
        freeze_panes = formatting.get('freeze_panes')
        if freeze_panes:
            logger.info(f"❄️ [{sheet_name}] Freezing panes at {freeze_panes}")
            self._freeze_panes(worksheet, freeze_panes)
            applied_operations.append(f"freeze panes ({freeze_panes})")
        
        # Freeze top row (shortcut)
        if formatting.get('freeze_top_row', False):
            logger.info(f"❄️ [{sheet_name}] Freezing top row")
            self._freeze_panes(worksheet, 'A2')
            applied_operations.append("freeze top row")
        
        # Summary log for this sheet
        if applied_operations:
            operations_str = ", ".join(applied_operations)
            logger.info(f"✅ [{sheet_name}] Applied: {operations_str}")
        else:
            logger.info(f"ℹ️ [{sheet_name}] No formatting operations applied")

    def _has_header_formatting(self, formatting: dict) -> bool:
        """Check if any header formatting options are configured."""
        header_options = [
            'header_bold', 'header_background', 'header_text_color', 
            'header_font_size', 'header_background_color'
        ]
        return any(formatting.get(option) for option in header_options)

    def _has_general_formatting(self, formatting: dict) -> bool:
        """Check if any general formatting options are configured."""
        general_options = [
            'general_text_color', 'general_font_size', 'general_font_name',
            'general_alignment_horizontal', 'general_alignment_vertical'
        ]
        return any(formatting.get(option) for option in general_options)

    def _get_header_formatting_details(self, formatting: dict) -> str:
        """Get a human-readable description of header formatting being applied."""
        details = []
        
        if formatting.get('header_bold'):
            details.append("bold")
        if formatting.get('header_background') and formatting.get('header_background_color'):
            bg_color = formatting.get('header_background_color')
            details.append(f"background({bg_color})")
        if formatting.get('header_text_color'):
            text_color = formatting.get('header_text_color')
            details.append(f"text-color({text_color})")
        if formatting.get('header_font_size'):
            font_size = formatting.get('header_font_size')
            details.append(f"font-size({font_size})")
        
        return ", ".join(details) if details else "default"

    def _apply_header_formatting(self, worksheet, formatting: dict) -> None:
        """
        Apply comprehensive header formatting with vertical alignment support.
        
        Args:
            worksheet: openpyxl worksheet object
            formatting: Formatting configuration dict
        """
        sheet_name = worksheet.title
        
        if worksheet.max_row < 1:
            logger.warning(f"⚠️ [{sheet_name}] No header row found - skipping header formatting")
            return
        
        # Build font properties
        font_kwargs = {}
        if formatting.get('header_bold'):
            font_kwargs['bold'] = True
        if formatting.get('header_text_color'):
            font_kwargs['color'] = self._normalize_color(formatting['header_text_color'])
        if formatting.get('header_font_size'):
            font_kwargs['size'] = formatting['header_font_size']
        
        # Build alignment properties (including vertical centering)
        alignment_kwargs = {}
        # Default to center alignment for headers (looks more professional)
        alignment_kwargs['vertical'] = 'center'
        alignment_kwargs['horizontal'] = 'center'  # Center headers by default
        
        # Build background fill
        fill = None
        if formatting.get('header_background') and formatting.get('header_background_color'):
            bg_color = self._normalize_color(formatting['header_background_color'])
            fill = PatternFill(start_color=bg_color, end_color=bg_color, fill_type='solid')
        
        # Apply formatting to header row (row 1)
        for cell in worksheet[1]:  # First row
            if font_kwargs:
                # Merge with existing font properties
                existing_font = cell.font
                merged_font_kwargs = {
                    'name': existing_font.name,
                    'size': existing_font.size,
                    'bold': existing_font.bold,
                    'italic': existing_font.italic,
                    'color': existing_font.color
                }
                merged_font_kwargs.update(font_kwargs)
                cell.font = Font(**merged_font_kwargs)
            
            if alignment_kwargs:
                # Merge with existing alignment
                existing_alignment = cell.alignment
                merged_alignment_kwargs = {
                    'horizontal': existing_alignment.horizontal,
                    'vertical': existing_alignment.vertical,
                    'text_rotation': existing_alignment.text_rotation,
                    'wrap_text': existing_alignment.wrap_text,
                    'shrink_to_fit': existing_alignment.shrink_to_fit,
                    'indent': existing_alignment.indent
                }
                merged_alignment_kwargs.update(alignment_kwargs)
                cell.alignment = Alignment(**merged_alignment_kwargs)
            
            if fill:
                cell.fill = fill
        
        logger.debug(f"Applied header formatting: font={font_kwargs}, alignment={alignment_kwargs}, fill={fill is not None}")

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
        """Auto-fit column widths accounting for actual font properties."""
        max_width = formatting.get('max_column_width', 50)
        min_width = formatting.get('min_column_width', 8)
        has_auto_filter = formatting.get('auto_filter', False)
        
        # AutoFilter dropdown arrows need extra space
        autofilter_padding = 5 if has_auto_filter else 2
        base_padding = 2
        
        logger.info(f"🔍 DEBUG: AutoFilter detected: {has_auto_filter}")
        logger.info(f"🔍 DEBUG: Padding - base: {base_padding}, autofilter: {autofilter_padding}")
        
        columns_processed = 0
        for column in worksheet.columns:
            max_width_needed = 0
            column_letter = get_column_letter(column[0].column)
            
            for row_idx, cell in enumerate(column):
                try:
                    if cell.value:
                        # Get the actual font properties of this cell
                        cell_font = cell.font
                        is_bold = cell_font.bold if cell_font.bold is not None else False
                        font_size = cell_font.size if cell_font.size is not None else 11
                        
                        # Calculate character count
                        char_count = len(str(cell.value))
                        
                        # Apply font-based multipliers for visual width
                        width_multiplier = 1.0
                        
                        # Bold text takes more space
                        if is_bold:
                            width_multiplier *= 1.2
                        
                        # Larger fonts take more space (relative to 11pt baseline)
                        if font_size != 11:
                            width_multiplier *= (font_size / 11.0)
                        
                        # Convert to visual width
                        visual_width = char_count * width_multiplier
                        
                        if visual_width > max_width_needed:
                            max_width_needed = visual_width
                        
                        # Debug first few cells
                        if columns_processed < 3 and row_idx < 2:
                            logger.info(f"🔍 DEBUG: {column_letter}{row_idx+1} '{cell.value}': chars={char_count}, bold={is_bold}, size={font_size}, multiplier={width_multiplier:.2f}, visual_width={visual_width:.1f}")
                        
                except Exception as e:
                    logger.debug(f"Error measuring cell {column_letter}{row_idx+1}: {e}")
                    pass
            
            # Add padding to the calculated width
            total_width = max_width_needed + base_padding + autofilter_padding
            
            # Apply min/max constraints  
            final_width = max(min_width, min(total_width, max_width))
            
            worksheet.column_dimensions[column_letter].width = final_width
            
            if columns_processed < 3:
                logger.info(f"🔍 DEBUG: Column {column_letter}: max_needed={max_width_needed:.1f}, total_width={total_width:.1f}, final_width={final_width:.1f}")
            
            columns_processed += 1
        
        logger.info(f"📏 Auto-fitted {columns_processed} columns with font-aware sizing")

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
