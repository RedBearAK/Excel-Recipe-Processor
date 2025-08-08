"""
Inject formulas step processor for Excel automation recipes.

excel_recipe_processor/processors/inject_formulas_processor.py

Handles injecting formulas into Excel files with support for both "live" (dynamic) 
and "dead" (text) formulas. Can work at the cell, range, or auto-scan level.
Supports both stage-to-stage operations (dead formulas) and file operations (live/dead/awaken).
"""

import re
import pandas as pd
import logging
import openpyxl

from pathlib import Path

# try:
#     OPENPYXL_AVAILABLE = True
# except ImportError:
#     OPENPYXL_AVAILABLE = False

from openpyxl.utils import get_column_letter, column_index_from_string
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class InjectFormulasProcessor(FileOpsBaseProcessor):
    """
    Processor for injecting formulas into existing Excel files.
    
    Supports both "live" formulas (dynamic calculations) and "dead" formulas 
    (text documentation). Can target specific cells, ranges, or auto-scan 
    entire sheets for formula-like text to awaken.
    """
    
class InjectFormulasProcessor(FileOpsBaseProcessor):
    """
    Processor for injecting formulas into Excel files or DataFrame stages.
    
    Supports both "live" formulas (dynamic calculations) and "dead" formulas 
    (text documentation). Can target specific cells, ranges, or auto-scan 
    entire sheets for formula-like text to awaken.
    
    Operating Modes:
    - dead: Stage-to-stage operation injecting formula text into DataFrames
    - live/awaken: File operations manipulating existing Excel files
    """
    
    def __init__(self, step_config: dict):
        # Initialize stage directives to None to detect what gets set
        self.source_stage = None
        self.save_to_stage = None
        
        # Let BaseStepProcessor read config and set stage directives if present
        BaseStepProcessor.__init__(self, step_config)
        
        # Validate configuration based on mode
        self._validate_configuration()
    
    def _validate_configuration(self):
        """Validate processor configuration based on operating mode."""
        mode = self.get_config_value('mode', 'live')
        has_source = bool(self.source_stage)
        has_save = bool(self.save_to_stage)
        has_target = bool(self.get_config_value('target_file'))
        
        # Validate mode
        if mode not in ['live', 'dead', 'awaken']:
            raise StepProcessorError(f"Invalid mode '{mode}'. Must be 'live', 'dead', or 'awaken'")
        
        # Mode-specific validation
        if mode == 'dead':
            # Dead formulas: must be stage-to-stage
            if not (has_source and has_save):
                raise StepProcessorError("Dead mode requires 'source_stage' and 'save_to_stage'")
            if has_target:
                raise StepProcessorError("Dead mode cannot use 'target_file' - it operates on stages")
        
        elif mode in ['live', 'awaken']:
            # Live/awaken: must have target_file, cannot use save_to_stage
            if not has_target:
                raise StepProcessorError(f"'{mode}' mode requires 'target_file'")
            if has_save:
                raise StepProcessorError(f"'{mode}' mode cannot use 'save_to_stage' - it operates on files")
    
    @classmethod
    def get_minimal_config(cls):
        return {
            'mode': 'live',
            'target_file': 'output.xlsx'
        }
    
    def execute(self, data=None):
        """Execute the appropriate operation based on mode."""
        mode = self.get_config_value('mode', 'live')
        
        if mode == 'dead':
            return self._execute_stage_operations()
        else:
            return self._execute_file_operations()
    
    def _execute_stage_operations(self):
        """Execute stage-to-stage operations for dead formulas."""
        self.log_step_start()
        
        # Load input data
        data = self._load_input_data()
        
        # Inject dead formulas into DataFrame
        modified_data = self._inject_dead_formulas_to_dataframe(data)
        
        # Save to output stage
        self._save_output_data(modified_data)
        
        formulas = self.get_config_value('formulas', [])
        self.log_step_complete(f"injected {len(formulas)} dead formulas into stage")
        return modified_data
    
    def _execute_file_operations(self):
        """Execute file operations for live/awaken formulas."""
        result = self.perform_file_operation()
        return pd.DataFrame()  # File operations return empty DataFrame
    
    def _load_input_data(self):
        """Load data from source_stage."""
        from excel_recipe_processor.core.stage_manager import StageManager
        return StageManager.load_stage(self.source_stage)
    
    def _save_output_data(self, data):
        """Save data to save_to_stage."""
        from excel_recipe_processor.core.stage_manager import StageManager
        StageManager.save_stage(
            stage_name=self.save_to_stage,
            data=data,
            description=f"Data with injected formulas from step: '{self.step_name}'",
            step_name=self.step_name,
            confirm_replacement=self.confirm_stage_replacement
        )
    
    def _inject_dead_formulas_to_dataframe(self, df):
        """Inject dead formulas as text into DataFrame cells."""
        import pandas as pd
        
        # Create a copy and convert to object dtype to allow mixed types
        result_df = df.copy().astype('object')
        formulas = self.get_config_value('formulas', [])
        
        for formula_def in formulas:
            if 'cell' in formula_def:
                # Handle cell reference like 'A1' -> row 0, col 0
                cell_ref = formula_def['cell']
                formula = formula_def['formula']
                
                # Convert Excel cell reference to pandas coordinates
                row_idx, col_idx = self._excel_ref_to_pandas(cell_ref, result_df)
                
                # Inject as text with single quote prefix
                formula_text = f"'{formula}" if not formula.startswith("'") else formula
                result_df.iloc[row_idx, col_idx] = formula_text
                
                logger.debug(f"Injected dead formula in {cell_ref}: {formula}")
        
        return result_df
    
    def _excel_ref_to_pandas(self, cell_ref, df):
        """Convert Excel cell reference like 'A1' to pandas row/col indices."""
        try:
            col_letter, row_num = coordinate_from_string(cell_ref)
            
            # Convert to 0-based indices (Excel is 1-based)
            row_idx = row_num - 1  # -1 for 0-based
            col_idx = column_index_from_string(col_letter) - 1  # -1 for 0-based
            
            # Validate indices are within DataFrame bounds
            if row_idx < 0 or row_idx >= len(df):
                raise StepProcessorError(f"Row {row_num} in cell '{cell_ref}' is outside DataFrame bounds")
            if col_idx < 0 or col_idx >= len(df.columns):
                raise StepProcessorError(f"Column {col_letter} in cell '{cell_ref}' is outside DataFrame bounds")
            
            return row_idx, col_idx
            
        except Exception as e:
            raise StepProcessorError(f"Invalid cell reference '{cell_ref}': {e}")
    
    def perform_file_operation(self) -> str:
        """Inject formulas into the target Excel file."""
        # # Check openpyxl availability
        # if not OPENPYXL_AVAILABLE:
        #     raise StepProcessorError("openpyxl is required for formula injection but not installed")
        
        target_file = self.get_config_value('target_file')
        mode = self.get_config_value('mode', 'live')
        formulas = self.get_config_value('formulas', [])
        auto_scan = self.get_config_value('auto_scan', False)
        sheets = self.get_config_value('sheets', None)  # None = active sheet, 'all' = all sheets
        
        # Apply variable substitution to target filename
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            resolved_file = self.variable_substitution.substitute(target_file)
        else:
            resolved_file = target_file
        
        # Check file exists
        if not Path(resolved_file).exists():
            raise StepProcessorError(f"Target file not found: '{resolved_file}'")
        
        # Process the file based on mode
        if mode == 'awaken':
            result = self._awaken_formulas(resolved_file, sheets, auto_scan)
        else:
            result = self._inject_formulas(resolved_file, mode, formulas, sheets)
        
        return result
    
    def _inject_formulas(self, filename: str, mode: str, formulas: list, sheets) -> str:
        """
        Inject specific formulas into the Excel file.
        
        Args:
            filename: Excel file to modify
            mode: 'live' or 'dead'
            formulas: List of formula definitions
            sheets: Sheet selection (None, sheet name, or 'all')
            
        Returns:
            Description of operation performed
        """
        workbook = openpyxl.load_workbook(filename)
        formulas_injected = 0
        sheets_processed = 0
        
        # Determine which sheets to process
        target_sheets = self._get_target_sheets(workbook, sheets)
        
        for sheet_name in target_sheets:
            worksheet = workbook[sheet_name]
            sheet_formulas = 0
            
            for formula_def in formulas:
                sheet_formulas += self._apply_formula_to_sheet(worksheet, formula_def, mode)
            
            formulas_injected += sheet_formulas
            sheets_processed += 1
            
            logger.debug(f"Injected {sheet_formulas} formulas in sheet '{sheet_name}'")
        
        # Save the modified workbook
        workbook.save(filename)
        workbook.close()
        
        mode_desc = "live" if mode == "live" else "dead"
        return f"injected {formulas_injected} {mode_desc} formulas across {sheets_processed} sheets in {filename}"
    
    def _awaken_formulas(self, filename: str, sheets, auto_scan: bool) -> str:
        """
        Awaken dead formulas in the Excel file.
        
        Args:
            filename: Excel file to modify
            sheets: Sheet selection
            auto_scan: Whether to scan entire sheets for dead formulas
            
        Returns:
            Description of operation performed
        """
        workbook = openpyxl.load_workbook(filename)
        formulas_awakened = 0
        sheets_processed = 0
        
        # Determine which sheets to process
        target_sheets = self._get_target_sheets(workbook, sheets)
        
        for sheet_name in target_sheets:
            worksheet = workbook[sheet_name]
            sheet_awakened = self._awaken_sheet_formulas(worksheet)
            
            formulas_awakened += sheet_awakened
            sheets_processed += 1
            
            logger.debug(f"Awakened {sheet_awakened} formulas in sheet '{sheet_name}'")
        
        # Save the modified workbook
        workbook.save(filename)
        workbook.close()
        
        return f"awakened {formulas_awakened} dead formulas across {sheets_processed} sheets in {filename}"
    
    def _get_target_sheets(self, workbook, sheets) -> list:
        """
        Determine which sheets to process.
        
        Args:
            workbook: openpyxl workbook
            sheets: Sheet specification (None, sheet name, or 'all')
            
        Returns:
            List of sheet names to process
        """
        if sheets is None:
            # Use active sheet
            return [workbook.active.title]
        elif sheets == 'all':
            # All sheets
            return workbook.sheetnames
        elif isinstance(sheets, str):
            # Single sheet by name
            if sheets not in workbook.sheetnames:
                raise StepProcessorError(f"Sheet '{sheets}' not found in workbook")
            return [sheets]
        elif isinstance(sheets, list):
            # Multiple specific sheets
            for sheet in sheets:
                if sheet not in workbook.sheetnames:
                    raise StepProcessorError(f"Sheet '{sheet}' not found in workbook")
            return sheets
        else:
            raise StepProcessorError(f"Invalid 'sheets' specification: '{sheets}'")
    
    def _apply_formula_to_sheet(self, worksheet, formula_def: dict, mode: str) -> int:
        """
        Apply a single formula definition to a worksheet.
        
        Args:
            worksheet: openpyxl worksheet
            formula_def: Dictionary with 'cell'/'range' and 'formula' keys
            mode: 'live' or 'dead'
            
        Returns:
            Number of cells modified
        """
        if not isinstance(formula_def, dict):
            raise StepProcessorError("Formula definition must be a dictionary")
        
        if 'formula' not in formula_def:
            raise StepProcessorError("Formula definition must include 'formula' key")
        
        formula = formula_def['formula']
        
        # Ensure formula starts with = for live mode
        if mode == 'live' and not formula.startswith('='):
            formula = '=' + formula
        
        # Handle cell vs range specification
        if 'cell' in formula_def:
            return self._apply_formula_to_cell(worksheet, formula_def['cell'], formula, mode)
        elif 'range' in formula_def:
            return self._apply_formula_to_range(worksheet, formula_def['range'], formula, mode)
        else:
            raise StepProcessorError("Formula definition must include either 'cell' or 'range' key")
    
    def _apply_formula_to_cell(self, worksheet, cell_ref: str, formula: str, mode: str) -> int:
        """
        Apply formula to a single cell.
        
        Args:
            worksheet: openpyxl worksheet
            cell_ref: Cell reference like 'A1', 'B5', etc.
            formula: Formula to inject
            mode: 'live' or 'dead'
            
        Returns:
            Number of cells modified (always 1 for single cell)
        """
        # Validate cell reference
        if not self._is_valid_cell_reference(cell_ref):
            raise StepProcessorError(f"Invalid cell reference: {cell_ref}")
        
        cell = worksheet[cell_ref]
        
        if mode == 'live':
            # Set as live formula (openpyxl auto-detects = prefix)
            cell.value = formula
        else:  # dead mode
            # Set as text (prefix with single quote to force text)
            cell.value = f"'{formula}"
        
        logger.debug(f"Set {mode} formula in {cell_ref}: {formula}")
        return 1
    
    def _apply_formula_to_range(self, worksheet, range_ref: str, formula: str, mode: str) -> int:
        """
        Apply formula to a range of cells.
        
        Args:
            worksheet: openpyxl worksheet
            range_ref: Range reference like 'A1:A10', 'B2:D5', etc.
            formula: Base formula to inject (will be adjusted for each cell)
            mode: 'live' or 'dead'
            
        Returns:
            Number of cells modified
        """
        # Validate range reference
        if not self._is_valid_range_reference(range_ref):
            raise StepProcessorError(f"Invalid range reference: {range_ref}")
        
        cells_modified = 0
        
        # Get the range
        cell_range = worksheet[range_ref]
        
        # Handle both single row/column and multi-dimensional ranges
        if hasattr(cell_range, '__iter__') and not hasattr(cell_range, 'value'):
            # Multi-dimensional range
            for row in cell_range:
                if hasattr(row, '__iter__'):
                    # Row is iterable (multiple cells)
                    for cell in row:
                        adjusted_formula = self._adjust_formula_for_cell(formula, cell.coordinate)
                        if mode == 'live':
                            cell.value = adjusted_formula
                        else:
                            cell.value = f"'{adjusted_formula}"
                        cells_modified += 1
                else:
                    # Single cell in row
                    adjusted_formula = self._adjust_formula_for_cell(formula, row.coordinate)
                    if mode == 'live':
                        row.value = adjusted_formula
                    else:
                        row.value = f"'{adjusted_formula}"
                    cells_modified += 1
        else:
            # Single cell range
            adjusted_formula = self._adjust_formula_for_cell(formula, cell_range.coordinate)
            if mode == 'live':
                cell_range.value = adjusted_formula
            else:
                cell_range.value = f"'{adjusted_formula}"
            cells_modified += 1
        
        logger.debug(f"Applied {mode} formula to range {range_ref}: {cells_modified} cells")
        return cells_modified
    
    def _awaken_sheet_formulas(self, worksheet) -> int:
        """
        Scan a worksheet for dead formulas and awaken them.
        
        Args:
            worksheet: openpyxl worksheet
            
        Returns:
            Number of formulas awakened
        """
        formulas_awakened = 0
        
        # Scan all cells with data
        for row in worksheet.iter_rows():
            for cell in row:
                if cell.value and isinstance(cell.value, str):
                    # Check if it looks like a dead formula
                    cell_value = cell.value.strip()
                    if self._looks_like_formula(cell_value):
                        # Remove single quote prefix if present
                        if cell_value.startswith("'="):
                            formula = cell_value[1:]  # Remove leading quote
                        elif cell_value.startswith("="):
                            formula = cell_value
                        else:
                            continue
                        
                        # Awaken the formula
                        cell.value = formula
                        formulas_awakened += 1
                        logger.debug(f"Awakened formula in {cell.coordinate}: {formula}")
        
        return formulas_awakened
    
    def _adjust_formula_for_cell(self, base_formula: str, cell_coord: str) -> str:
        """
        Adjust a base formula for a specific cell location.
        
        For now, this is a simple implementation. Could be enhanced
        to automatically adjust relative references.
        
        Args:
            base_formula: Base formula template
            cell_coord: Target cell coordinate like 'A1', 'B5'
            
        Returns:
            Adjusted formula for the specific cell
        """
        # Simple implementation - just return the base formula
        # Future enhancement: parse and adjust relative references
        
        # Ensure formula starts with = if not already
        if not base_formula.startswith('='):
            return '=' + base_formula
        
        return base_formula
    
    def _is_valid_cell_reference(self, cell_ref: str) -> bool:
        """Check if a string is a valid Excel cell reference."""
        try:
            coordinate_from_string(cell_ref)
            return True
        except:
            return False
    
    def _is_valid_range_reference(self, range_ref: str) -> bool:
        """Check if a string is a valid Excel range reference."""
        if ':' not in range_ref:
            # Single cell reference
            return self._is_valid_cell_reference(range_ref)
        
        try:
            start_cell, end_cell = range_ref.split(':')
            return (self._is_valid_cell_reference(start_cell) and 
                   self._is_valid_cell_reference(end_cell))
        except:
            return False
    
    def _looks_like_formula(self, text: str) -> bool:
        """
        Check if text looks like it could be a formula.
        
        Args:
            text: Text to check
            
        Returns:
            True if text looks like a formula
        """
        if not isinstance(text, str):
            return False
        
        text = text.strip()
        
        # Check for formula patterns
        formula_patterns = [
            r"^'?=",  # Starts with = or '=
            r"=\s*[A-Z]+\d+",  # Contains cell references
            r"=\s*[A-Z]+\(",  # Contains function calls
            r"=\s*(SUM|AVERAGE|COUNT|MIN|MAX|IF|VLOOKUP|INDEX|MATCH)",  # Common functions
        ]
        
        for pattern in formula_patterns:
            if re.match(pattern, text, re.IGNORECASE):
                return True
        
        return False
    
    def get_operation_type(self) -> str:
        """Get the type of file operation this processor performs."""
        return "formula_injection"
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Inject formulas into existing Excel files with live/dead modes',
            'operation_type': 'formula_injection',
            'supported_modes': ['live', 'dead', 'awaken'],
            'targeting_options': ['single_cell', 'cell_range', 'auto_scan'],
            'sheet_support': ['single_sheet', 'multiple_sheets', 'all_sheets'],
            'file_requirements': ['xlsx', 'xlsm'],
            'dependencies': ['openpyxl'],
            'stage_requirements': 'none',
            'examples': {
                'live_formula': 'Creates dynamic formulas that recalculate automatically',
                'dead_formula': 'Inserts formula text for documentation/templates',
                'auto_awaken': 'Scans and awakens all dead formulas in file'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get usage examples for this processor."""
        # For now, return inline examples - could be moved to YAML file later
        return {
            'description': 'Inject formulas into Excel files with flexible targeting and live/dead modes',
            
            'basic_example': {
                'description': 'Inject live formulas into specific cells',
                'yaml': '''
  - step_description: "Add calculation formulas to report"
    processor_type: "inject_formulas"
    target_file: "sales_report.xlsx"
    mode: "live"
    formulas:
      - cell: "D2"
        formula: "=B2*C2"
      - cell: "D3"
        formula: "=B3*C3"
      - cell: "E4"
        formula: "=SUM(D2:D3)"
'''
            },
            
            'range_example': {
                'description': 'Apply formulas to ranges and multiple sheets',
                'yaml': '''
  - step_description: "Add formulas to entire columns"
    processor_type: "inject_formulas"
    target_file: "financial_model.xlsx"
    mode: "live"
    sheets: ["Revenue", "Expenses"]
    formulas:
      - range: "D2:D50"
        formula: "=B2*C2"
      - range: "E2:E50"
        formula: "=D2*0.1"
      - cell: "D51"
        formula: "=SUM(D2:D50)"
'''
            },
            
            'awaken_example': {
                'description': 'Awaken dead formulas across entire workbook',
                'yaml': '''
  - step_description: "Convert template to live calculations"
    processor_type: "inject_formulas"
    target_file: "budget_template.xlsx"
    mode: "awaken"
    sheets: "all"
    auto_scan: true
'''
            },
            
            'documentation_example': {
                'description': 'Create dead formulas for documentation',
                'yaml': '''
  - step_description: "Add formula documentation"
    processor_type: "inject_formulas"
    target_file: "model_documentation.xlsx"
    mode: "dead"
    formulas:
      - cell: "F1"
        formula: "=VLOOKUP(E1,RefTable,2,FALSE)"
      - cell: "F2"
        formula: "This cell should contain: =E2*Rate"
'''
            }
        }


# End of file #
