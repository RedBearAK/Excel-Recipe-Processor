"""
Enhanced column configuration processor supporting both Excel and CSV.

excel_recipe_processor/processors/generate_column_config_processor.py

Compares column names between two files and generates YAML configuration files.
Uses openpyxl for Excel files to avoid pandas auto-conversion issues, and
supports CSV files for maximum compatibility. FileOps processor that reads
files directly.
"""

import pandas as pd
import logging

from difflib import SequenceMatcher
from pathlib import Path

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.core.file_reader import FileReader
from excel_recipe_processor.readers.openpyxl_excel_reader import OpenpyxlExcelReader, OpenpyxlExcelReaderError
from excel_recipe_processor.processors._helpers.column_patterns import empty_or_whitespace_rgx


logger = logging.getLogger(__name__)


class GenerateColumnConfigProcessor(FileOpsBaseProcessor):
    """
    Enhanced processor that compares column names between two files and generates 
    YAML configuration files for column management in recipes.
    
    Supports both Excel (.xlsx, .xls, .xlsm) and CSV files:
    - Excel files: Uses openpyxl to read headers directly (no pandas conversion)
    - CSV files: Uses FileReader for text-only reading
    
    This avoids pandas auto-conversion issues while supporting both formats.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'processor_type': 'generate_column_config',
            'source_file': 'data/raw_export.csv',
            'template_file': 'templates/desired_format.csv',
            'output_file': 'configs/column_config.yaml'
        }
    
    def __init__(self, step_config: dict):
        # Call parent first (which calls _validate_file_operation_config)
        super().__init__(step_config)
        
        # THEN set instance attributes for easy access
        self.source_file = self.get_config_value('source_file')
        self.template_file = self.get_config_value('template_file')
        self.output_file = self.get_config_value('output_file')
        
        # Optional configuration
        self.include_recipe_section = self.get_config_value('include_recipe_section', False)
        self.similarity_threshold = self.get_config_value('similarity_threshold', 0.8)
        self.source_sheet = self.get_config_value('source_sheet', None)
        self.template_sheet = self.get_config_value('template_sheet', None)
        self.header_row = self.get_config_value('header_row', 1)  # 1-based for Excel
        self.check_column_data = self.get_config_value('check_column_data', True)  # Check for empty columns
        self.max_rows = self.get_config_value('max_rows', 100000)  # Max rows to scan for data
        
        # Storage for Excel analysis data
        self._last_excel_analysis = None
        self.check_column_data = self.get_config_value('check_column_data', True)  # Check for empty columns
        self.sample_rows = self.get_config_value('sample_rows', 20)  # Rows to sample for data check
    
    def _validate_file_operation_config(self):
        """Validate that files exist and are supported formats."""
        # Use get_config_value, not instance attributes which aren't set yet
        source_file = self.get_config_value('source_file')
        if not source_file:
            raise StepProcessorError(f"Step '{self.step_name}' requires source_file")
        
        template_file = self.get_config_value('template_file')
        if not template_file:
            raise StepProcessorError(f"Step '{self.step_name}' requires template_file")
        
        output_file = self.get_config_value('output_file')
        if not output_file:
            raise StepProcessorError(f"Step '{self.step_name}' requires output_file")
        
        files_to_check = [
            ('source_file', source_file),
            ('template_file', template_file)
        ]
        
        for param_name, file_path in files_to_check:
            if file_path:
                file_path_obj = Path(file_path)
                
                # Check file extension
                extension = file_path_obj.suffix.lower()
                supported_extensions = {'.csv', '.xlsx', '.xls', '.xlsm', '.xlsb'}
                
                if extension not in supported_extensions:
                    raise StepProcessorError(
                        f"Step '{self.step_name}': {param_name} must be CSV or Excel file. "
                        f"Got: {file_path} (supported: {', '.join(sorted(supported_extensions))})"
                    )
    
    def perform_file_operation(self) -> str:
        """
        Generate column configuration by comparing files.
        
        Returns:
            Description of operation performed
        """
        try:
            # Read source file headers
            logger.info(f"Reading source file: {self.source_file}")
            source_columns = self._read_file_headers(self.source_file, self.source_sheet)
            
            # Read template file headers
            logger.info(f"Reading template file: {self.template_file}")
            template_columns = self._read_file_headers(self.template_file, self.template_sheet)
            
            # Trim trailing empty columns from template
            template_columns = self._trim_trailing_empty_columns(template_columns)
            
            logger.info(f"Column analysis: Source={len(source_columns)}, Template={len(template_columns)} (after trimming)")
            
            # Generate column analysis
            analysis = self._analyze_columns(source_columns, template_columns)
            
            # Write YAML configuration file
            self._write_yaml_config(analysis)
            
            result_msg = (f"Generated column config: {len(analysis['raw'])} source columns, "
                         f"{len(analysis['desired'])} template columns, "
                         f"{len(analysis['to_create'])} to create, "
                         f"{len(analysis['rename_mapping'])} renames")
            
            logger.info(f"Wrote configuration to: {self.output_file}")
            return result_msg
            
        except Exception as e:
            raise StepProcessorError(f"Failed to generate column configuration: {e}")
    
    def _read_file_headers(self, file_path: str, sheet_name=None) -> list:
        """
        Read column headers from either Excel or CSV file.
        
        Args:
            file_path: Path to file
            sheet_name: Sheet name for Excel files (ignored for CSV)
            
        Returns:
            List of column header names as strings
        """
        # Resolve variables in file path
        variables = self._get_pipeline_variables()
        resolved_path = self._resolve_file_path(file_path, variables)
        
        file_path_obj = Path(resolved_path)
        extension = file_path_obj.suffix.lower()
        
        if extension == '.csv':
            return self._read_csv_headers(resolved_path)
        elif extension in {'.xlsx', '.xls', '.xlsm', '.xlsb'}:
            return self._read_excel_headers_openpyxl(resolved_path, sheet_name)
        else:
            raise StepProcessorError(f"Unsupported file format: {extension}")
    
    def _read_csv_headers(self, file_path: str) -> list:
        """
        Read column headers from CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            List of column header names
        """
        try:
            # Read only headers using pandas with string dtype
            df = pd.read_csv(
                file_path,
                dtype=str,  # Force string reading to prevent conversions
                nrows=0     # Only read headers
            )
            
            headers = [str(col) for col in df.columns]
            logger.debug(f"Read {len(headers)} CSV headers from {file_path}")
            return headers
            
        except Exception as e:
            raise StepProcessorError(f"Failed to read CSV headers from {file_path}: {e}")
    
    def _read_excel_headers_openpyxl(self, file_path: str, sheet_name=None) -> list:
        """
        Read column headers from Excel file using openpyxl.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or None for active sheet
            
        Returns:
            List of column header names as they appear in Excel
        """
        try:
            if self.check_column_data:
                # Use enhanced reader that checks for actual data in columns
                analysis = OpenpyxlExcelReader.read_headers_with_data_check(
                    file_path=file_path,
                    sheet_name=sheet_name,
                    header_row=self.header_row,
                    max_rows=self.max_rows
                )
                
                headers = analysis['headers']
                column_has_data = analysis['column_has_data']
                empty_columns = analysis['empty_column_indices']
                scanned_rows = analysis['scanned_data_rows']
                
                if empty_columns:
                    logger.info(f"Found {len(empty_columns)} columns with headers but no data: "
                               f"positions {[i+1 for i in empty_columns]} (scanned {scanned_rows} data rows)")
                    
                    # Store analysis for potential use in trimming
                    self._last_excel_analysis = analysis
                
                logger.debug(f"Read {len(headers)} Excel headers from {file_path} using openpyxl with full data check")
                return headers
            else:
                # Use simple header-only reader
                headers = OpenpyxlExcelReader.read_headers(
                    file_path=file_path,
                    sheet_name=sheet_name,
                    header_row=self.header_row
                )
                
                logger.debug(f"Read {len(headers)} Excel headers from {file_path} using openpyxl (headers only)")
                return headers
            
        except OpenpyxlExcelReaderError as e:
            raise StepProcessorError(f"Failed to read Excel headers from {file_path}: {e}")
    
    def _resolve_file_path(self, file_path: str, variables: dict) -> str:
        """
        Resolve variable substitutions in file path.
        
        Args:
            file_path: File path potentially containing variables
            variables: Dictionary of variables for substitution
            
        Returns:
            Resolved file path
        """
        # Simple variable substitution - could be enhanced
        resolved_path = file_path
        
        # Add built-in date variables if not provided
        if not variables:
            from datetime import datetime
            now = datetime.now()
            variables = {
                'date': now.strftime('%Y-%m-%d'),
                'YYYY': now.strftime('%Y'),
                'MM': now.strftime('%m'),
                'DD': now.strftime('%d'),
                'YYYYMMDD': now.strftime('%Y%m%d')
            }
        
        # Simple string replacement for now
        for var_name, var_value in variables.items():
            placeholder = f'{{{var_name}}}'
            resolved_path = resolved_path.replace(placeholder, str(var_value))
        
        return resolved_path
    
    def _trim_trailing_empty_columns(self, columns: list) -> list:
        """
        Remove trailing empty columns that serve no purpose.
        
        For Excel files with data checking enabled, also considers whether
        columns contain actual data, not just whether headers are empty.
        
        Args:
            columns: List of column names
            
        Returns:
            List with trailing empty columns removed
        """
        if not columns:
            return columns
        
        # Check if we have Excel analysis data to make smarter decisions
        if hasattr(self, '_last_excel_analysis') and self._last_excel_analysis:
            analysis = self._last_excel_analysis
            column_has_data = analysis['column_has_data']
            
            # Find last column that either has a meaningful header OR contains data
            last_meaningful_idx = -1
            for i, (header, has_data) in enumerate(zip(columns, column_has_data)):
                header_meaningful = header and not empty_or_whitespace_rgx.match(header)
                if header_meaningful or has_data:
                    last_meaningful_idx = i
            
            if last_meaningful_idx == -1:
                logger.warning("No meaningful columns found in template (no headers or data) - returning empty column list")
                return []
            
            trimmed = columns[:last_meaningful_idx + 1]
            
            if len(trimmed) < len(columns):
                removed_count = len(columns) - len(trimmed)
                empty_header_count = sum(1 for i in range(last_meaningful_idx + 1, len(columns)) 
                                       if not columns[i] or empty_or_whitespace_rgx.match(columns[i]))
                empty_data_count = sum(1 for i in range(last_meaningful_idx + 1, len(columns)) 
                                     if not column_has_data[i])
                scanned_rows = analysis['scanned_data_rows']
                logger.info(f"Smart trimmed {removed_count} trailing columns: "
                           f"{empty_header_count} empty headers, {empty_data_count} no data "
                           f"(scanned {scanned_rows} data rows)")
            
            # Clear analysis for next file
            self._last_excel_analysis = None
            return trimmed
        
        else:
            # Fallback to simple header-based trimming
            # Find the last non-empty column header
            last_named_idx = -1
            for i, col in enumerate(columns):
                if col and not empty_or_whitespace_rgx.match(col):
                    last_named_idx = i
            
            if last_named_idx == -1:
                # No named columns found, return empty list
                logger.warning("No named columns found in template - returning empty column list")
                return []
            
            # Return columns up to and including the last named column
            trimmed = columns[:last_named_idx + 1]
            
            if len(trimmed) < len(columns):
                removed_count = len(columns) - len(trimmed)
                logger.info(f"Basic trimmed {removed_count} trailing empty header columns")
            
            return trimmed
    
    def _analyze_columns(self, source_columns: list, template_columns: list) -> dict:
        """
        Analyze column differences and generate rename suggestions.
        
        Args:
            source_columns: Column names from source file
            template_columns: Column names from template file
            
        Returns:
            Dictionary with analysis results
        """
        source_set = set(source_columns)
        template_set = set(template_columns)
        
        # Find exact matches (including empty strings)
        exact_matches = source_set.intersection(template_set)
        
        # Find columns that need to be created (in template but not in source)
        to_create = []
        potential_renames = []
        
        for template_col in template_columns:
            if template_col not in exact_matches:
                # Check if this might be a rename by fuzzy matching
                best_match = self._find_best_fuzzy_match(template_col, source_columns, exact_matches)
                if best_match:
                    potential_renames.append((best_match, template_col))
                else:
                    to_create.append(template_col)
        
        # Build rename mapping from potential renames
        rename_mapping = {}
        for old_name, new_name in potential_renames:
            rename_mapping[old_name] = new_name
        
        return {
            'raw': source_columns,
            'desired': template_columns, 
            'to_create': to_create,
            'rename_mapping': rename_mapping,
            'exact_matches': list(exact_matches)
        }
    
    def _find_best_fuzzy_match(self, target_name: str, candidate_names: list, 
                             exclude_names: set) -> str:
        """
        Find the best fuzzy match for a target name among candidates.
        
        Args:
            target_name: Name to find a match for
            candidate_names: List of potential matching names
            exclude_names: Names to exclude from matching (already matched)
            
        Returns:
            Best matching name or None if no good match found
        """
        # Skip matching for empty target names
        if not target_name or empty_or_whitespace_rgx.match(target_name):
            return None
            
        best_match = None
        best_score = 0
        
        for candidate in candidate_names:
            if candidate in exclude_names or empty_or_whitespace_rgx.match(candidate or ""):
                continue
                
            # Calculate similarity ratio
            similarity = SequenceMatcher(None, target_name.lower(), candidate.lower()).ratio()
            
            if similarity > best_score and similarity >= self.similarity_threshold:
                best_score = similarity
                best_match = candidate
        
        if best_match:
            logger.debug(f"Fuzzy match: '{best_match}' → '{target_name}' (score: {best_score:.3f})")
        
        return best_match
    
    def _write_yaml_config(self, analysis: dict) -> None:
        """
        Write the column configuration to a YAML file.
        
        Args:
            analysis: Dictionary with column analysis results
        """
        # Create output directory if it doesn't exist
        output_path = Path(self.output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Build YAML content
        yaml_lines = [
            "# Generated column configuration",
            f"# Source: {self.source_file} ({len(analysis['raw'])} columns)",
            f"# Template: {self.template_file} ({len(analysis['desired'])} columns)", 
            f"# To create: {len(analysis['to_create'])} columns",
            f"# Renames: {len(analysis['rename_mapping'])} mappings",
            "",
            "# Raw columns found in source file (in original order)",
            "raw_columns:"
        ]
        
        # Add raw columns list
        yaml_lines.extend(self._format_yaml_list(analysis['raw'], indent=2))
        
        yaml_lines.extend([
            "",
            "# Desired columns for final output (in template order)", 
            "desired_columns:"
        ])
        
        # Add desired columns list
        yaml_lines.extend(self._format_yaml_list(analysis['desired'], indent=2))
        
        yaml_lines.extend([
            "",
            "# Columns to create (not found in source file)",
            "columns_to_create:"
        ])
        
        # Add columns to create list
        yaml_lines.extend(self._format_yaml_list(analysis['to_create'], indent=2))
        
        yaml_lines.extend([
            "",
            "# Rename mapping (source_name: desired_name)",
            "rename_mapping:"
        ])
        
        # Add rename mapping
        if analysis['rename_mapping']:
            for old_name, new_name in analysis['rename_mapping'].items():
                old_quoted = f'"{old_name}"' if old_name else '""'
                new_quoted = f'"{new_name}"' if new_name else '""'
                yaml_lines.append(f'  {old_quoted}: {new_quoted}')
        else:
            yaml_lines.append("  {}")
        
        # Add recipe section if requested
        if self.include_recipe_section:
            yaml_lines.extend(self._generate_recipe_section(analysis))
        
        # Write to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(yaml_lines))
            f.write('\n')
    
    def _format_yaml_list(self, items: list, indent: int = 2) -> list:
        """
        Format a Python list as YAML list items.
        
        Args:
            items: List of items to format
            indent: Number of spaces for indentation
            
        Returns:
            List of formatted YAML lines
        """
        if not items:
            return [" " * indent + "[]"]
        
        lines = []
        for item in items:
            # Handle empty strings and special characters
            if item == "" or item is None:
                lines.append(f'{" " * indent}- ""')
            elif isinstance(item, str) and ('"' in item or "'" in item or ':' in item or item.startswith(' ')):
                lines.append(f'{" " * indent}- "{item}"')
            else:
                lines.append(f'{" " * indent}- {item}')
        
        return lines
    
    def _generate_recipe_section(self, analysis: dict) -> list:
        """
        Generate a ready-to-use recipe section.
        
        Args:
            analysis: Dictionary with column analysis results
            
        Returns:
            List of YAML lines for recipe section
        """
        lines = [
            "",
            "# Ready-to-use recipe section",
            "recipe_section:",
            "  - step_description: 'Apply column renames and create missing columns'",
            "    processor_type: 'rename_columns'",
            "    rename_type: 'mapping'",
            "    source_stage: 'raw_data'  # Update this to your source stage name",
            "    mapping:"
        ]
        
        if analysis['rename_mapping']:
            for old_name, new_name in analysis['rename_mapping'].items():
                old_quoted = f'"{old_name}"' if old_name else '""'
                new_quoted = f'"{new_name}"' if new_name else '""'
                lines.append(f'      {old_quoted}: {new_quoted}')
        else:
            lines.append("      {}")
        
        lines.extend([
            "    save_to_stage: 'renamed_data'  # Update this to your target stage name",
            "",
            "  - step_description: 'Select and create final column structure'",
            "    processor_type: 'select_columns'", 
            "    source_stage: 'renamed_data'",
            "    columns_to_keep:"
        ])
        
        # Add the desired columns list
        for col in analysis['desired']:
            if col == "" or col is None:
                lines.append('      - ""')
            else:
                lines.append(f'      - {col}')
        
        if analysis['to_create']:
            lines.extend([
                "    columns_to_create:"
            ])
            for col in analysis['to_create']:
                if col == "" or col is None:
                    lines.append('      - ""')
                else:
                    lines.append(f'      - {col}')
        
        lines.extend([
            "    default_value: ''",
            "    save_to_stage: 'final_output'  # Update this to your final stage name"
        ])
        
        return lines
    
    def _get_pipeline_variables(self) -> dict:
        """
        Get variables from the pipeline for filename substitution.
        
        Returns:
            Dictionary of variables for substitution
        """
        # TODO: Access pipeline variables when available
        # For now, return empty dict - will use built-in date variables
        return {}
    
    def get_operation_type(self) -> str:
        """Get the type of file operation this processor performs."""
        return "column_config_generation"


# End of file #
