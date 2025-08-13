"""
Enhanced column configuration processor with super fast Excel reading.

excel_recipe_processor/processors/generate_column_config_processor.py

Compares column names between two files and generates YAML configuration files.
Uses direct Excel sampling for blazing fast header analysis while preserving
original text formatting. Much faster than CSV conversion approaches.
"""

import pandas as pd
import logging
import yaml

from difflib import SequenceMatcher
from pathlib import Path

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.processors._helpers.column_patterns import empty_or_whitespace_rgx


logger = logging.getLogger(__name__)


class GenerateColumnConfigProcessor(FileOpsBaseProcessor):
    """
    Enhanced processor that compares column names between two files and generates 
    YAML configuration files for column management in recipes.
    
    SUPER FAST: Uses direct Excel sampling instead of conversion.
    
    Supports both Excel (.xlsx, .xls, .xlsm) and CSV files:
    - Excel files: Direct sampling of first few rows (preserves "8/4/2025" format)
    - CSV files: Fast pandas reading with string preservation
    
    This approach is orders of magnitude faster than cell-by-cell or CSV conversion
    while avoiding pandas auto-conversion issues.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'processor_type': 'generate_column_config',
            'source_file': 'data/raw_export.csv',
            'template_file': 'templates/desired_format.csv',
            'output_file': 'configs/column_config.yaml'
        }
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Generate YAML column configuration by comparing source and template files',
            'operation_type': 'file_analysis',
            'file_formats': ['csv', 'xlsx', 'xls', 'xlsm'],
            'analysis_features': [
                'header_comparison',
                'fuzzy_column_matching', 
                'empty_column_detection',
                'text_format_preservation',
                'rename_mapping_generation'
            ],
            'optimization_features': [
                'fast_excel_sampling',
                'smart_column_trimming',
                'configurable_similarity_threshold'
            ],
            'output_formats': ['yaml'],
            'special_capabilities': [
                'preserves_date_headers',
                'avoids_pandas_autoconversion',
                'handles_empty_columns',
                'generates_example_recipes'
            ],
            'dependencies': ['pandas', 'openpyxl', 'yaml'],
            'stage_requirements': 'none',
            'examples': {
                'basic_comparison': 'Compare source.xlsx with template.csv to generate column mapping',
                'fuzzy_matching': 'Find similar column names using configurable similarity threshold',
                'empty_detection': 'Identify columns with headers but no data (ghost columns)'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get usage examples for this processor from external YAML file."""
        try:
            from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
            return load_processor_examples('generate_column_config')
        except Exception as e:
            return {
                'error': f'Could not load usage examples: {e}',
                'fallback_description': 'Generate column configuration by comparing files with different column structures'
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
        self.sample_rows = self.get_config_value('sample_rows', 5)  # Very small sample for speed

        # Sampling more than a few rows takes a LOOONG time. We should warn user about that.
        if self.sample_rows > 25:
            logger.warning(
                f"sample_rows={self.sample_rows} is larger than recommended (>25). "
                f"This may significantly slow down processing. Consider using sample_rows=25 or less "
                f"for optimal performance with large files."
            )

    def _validate_file_operation_config(self):
        """Validate that files exist and are supported formats."""
        required_params = ['source_file', 'template_file', 'output_file']
        
        for param in required_params:
            if not self.get_config_value(param):
                raise StepProcessorError(f"Missing required parameter: {param}")
        
        # Validate input files exist and have supported extensions
        supported_extensions = {'.csv', '.xlsx', '.xls', '.xlsm', '.xlsb'}
        
        for file_param in ['source_file', 'template_file']:
            file_path = self.get_config_value(file_param)
            
            if not Path(file_path).exists():
                raise StepProcessorError(f"File not found: {file_path}")
            
            extension = Path(file_path).suffix.lower()
            if extension not in supported_extensions:
                raise StepProcessorError(
                    f"Unsupported file format for {file_param}: {extension}. "
                    f"Got: {file_path} (supported: {', '.join(sorted(supported_extensions))})"
                    )

    def perform_file_operation(self) -> str:
        """
        Generate column configuration by comparing files.
        
        Fixed to use correct analysis data for each file's trimming.
        """
        try:
            # Read source file headers with analysis
            logger.info(f"Reading source file: '{self.source_file}'")
            source_columns, source_analysis = self._read_file_headers(self.source_file, self.source_sheet)
            
            # Read template file headers with analysis  
            logger.info(f"Reading template file: '{self.template_file}'")
            template_columns, template_analysis = self._read_file_headers(self.template_file, self.template_sheet)
            
            # Trim trailing empty columns using correct analysis for each file
            source_columns = self._trim_trailing_empty_columns(source_columns, source_analysis)
            template_columns = self._trim_trailing_empty_columns(template_columns, template_analysis)
            
            logger.info(f"Column analysis: Source={len(source_columns)}, Template={len(template_columns)} (after trimming)")
            
            # Generate column analysis
            analysis = self._analyze_columns(source_columns, template_columns)
            
            # Count actual renames (exclude identity mappings for accurate reporting)
            actual_renames = {
                source: target for source, target in analysis['rename_mapping'].items()
                if source != target
            }
            
            # Write YAML configuration file
            self._write_yaml_config(analysis)
            
            result_msg = (f"Generated column config: {len(analysis['raw'])} source columns, "
                        f"{len(analysis['desired'])} template columns, "
                        f"{len(analysis['to_create'])} to create, "
                        f"{len(actual_renames)} renames")
            
            logger.info(f"Wrote configuration to: {self.output_file}")
            return result_msg
            
        except Exception as e:
            raise StepProcessorError(f"Failed to generate column configuration: {e}")

    def _read_file_headers(self, file_path: str, sheet_name=None) -> tuple:
        """
        Read column headers from either Excel or CSV file.
        
        Returns:
            Tuple of (headers_list, analysis_data_or_none)
        """
        file_path_obj = Path(file_path)
        extension = file_path_obj.suffix.lower()
        
        if extension == '.csv':
            headers = self._read_csv_headers_fast(file_path)
            return headers, None  # No analysis data for CSV
        elif extension in {'.xlsx', '.xls', '.xlsm', '.xlsb'}:
            return self._read_excel_headers_super_fast(file_path, sheet_name)
        else:
            raise StepProcessorError(f"Unsupported file format: {extension}")

    def _read_csv_headers_fast(self, file_path: str) -> list:
        """
        Read column headers from CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            List of column header names
        """
        try:
            # Read only headers using pandas with string dtype and no inference
            df = pd.read_csv(
                file_path,
                dtype=str,  # Force string reading to prevent conversions
                nrows=0,    # Only read headers
                parse_dates=False,  # Disable date parsing completely
                infer_datetime_format=False  # Disable datetime inference
            )
            
            headers = [str(col) for col in df.columns]
            logger.debug(f"Read {len(headers)} CSV headers from {file_path}")
            return headers
            
        except Exception as e:
            raise StepProcessorError(f"Failed to read CSV headers from {file_path}: {e}")

    def _read_excel_headers_super_fast(self, file_path: str, sheet_name=None) -> list:
        """
        Read Excel headers with enhanced empty header replacement.
        
        Returns both processed headers AND stores analysis data for trimming.
        """
        try:
            # Always use enhanced reader with data analysis (pandas is fast enough)
            from excel_recipe_processor.readers.openpyxl_excel_reader import OpenpyxlExcelReader
            
            analysis = OpenpyxlExcelReader.read_headers_with_data_check(
                file_path=file_path,
                sheet_name=sheet_name,
                max_rows=self.get_config_value('max_rows', 1000)
            )
            
            # Store analysis for trimming method
            self._last_excel_analysis = analysis
            
            # Replace empty headers based on data presence
            processed_headers = self._replace_empty_headers(
                analysis['headers'], 
                analysis['column_has_data']
            )
            
            logger.debug(f"Enhanced Excel headers: {len(processed_headers)} columns, data-aware replacements applied")
            
            # Return both headers and analysis for immediate trimming
            return processed_headers, analysis
            
        except Exception as e:
            raise StepProcessorError(f"Failed to read Excel headers from {file_path}: {e}")

    def _replace_empty_headers(self, headers: list, column_has_data: list) -> list:
        """
        Replace empty headers based on data presence following pandas convention.
        
        Args:
            headers: Original headers from Excel
            column_has_data: Boolean list indicating if each column has data
            
        Returns:
            Headers with empty strings replaced appropriately
        """
        processed_headers = []
        unnamed_counter = 0
        empty_counter = 0
        
        for header, has_data in zip(headers, column_has_data):
            if not header.strip():  # Empty header
                if has_data:
                    # Column has data but no header - use pandas convention
                    processed_headers.append(f"Unnamed: {unnamed_counter}")
                    unnamed_counter += 1
                else:
                    # Column has no header and no data - mark as empty for trimming
                    processed_headers.append(f"Empty: {empty_counter}")
                    empty_counter += 1
            else:
                # Keep original header
                processed_headers.append(header)
        
        logger.debug(f"Header replacement: {unnamed_counter} unnamed, {empty_counter} empty columns")
        return processed_headers

    def _pandas_data_check_by_index(self, file_path: str, sheet_name: str, num_headers: int) -> int:
        """
        Use pandas for fast data checking BY COLUMN INDEX ONLY.
        
        Args:
            file_path: Excel file path
            sheet_name: Sheet name for pandas
            num_headers: Number of headers from openpyxl
            
        Returns:
            Number of empty columns detected
        """
        try:
            # Warn about large datasets
            if self.sample_rows > 25:
                logger.warning(
                    f"Analyzing {self.sample_rows} rows for data checking. "
                    f"Consider setting sample_rows <= 25 for faster processing."
                )
            
            # Load data using pandas - NEVER interpret headers
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=None,  # CRITICAL: Don't interpret any row as headers
                skiprows=self.header_row,  # Skip past the header row completely
                nrows=self.sample_rows,  # Limit for performance
                engine='openpyxl'
            )
            
            logger.debug(f"Pandas data check: loaded {df.shape[0]} rows × {df.shape[1]} columns for analysis")
            
            # Verify pandas used numeric column indices (not string headers)
            if any(isinstance(col, str) for col in df.columns):
                logger.warning("Pandas unexpectedly used string column names - may indicate header contamination")
            
            # Check data presence by column INDEX only
            empty_count = 0
            
            for col_index in range(num_headers):
                if col_index < df.shape[1]:
                    # Check if this column index has any data (vectorized - very fast)
                    has_data = df.iloc[:, col_index].notna().any()
                    if not has_data:
                        empty_count += 1
                else:
                    # Header exists but no corresponding data column
                    empty_count += 1
            
            logger.debug(f"Data check complete: {empty_count} empty columns out of {num_headers}")
            return empty_count
            
        except Exception as e:
            # Don't let data checking break header reading
            logger.warning(f"Pandas data check failed, skipping: {e}")
            return 0

    def _fast_pandas_data_check(self, file_path: str, sheet_name: str, headers: list) -> dict:
        """
        Use pandas for blazing fast data analysis by column position.
        
        Args:
            file_path: Excel file path
            sheet_name: Sheet name (for pandas)
            headers: Headers from openpyxl (exact text)
            
        Returns:
            Dictionary with headers and data analysis
        """
        try:
            # Determine safe row limit for analysis
            analysis_rows = self.sample_rows if self.sample_rows else 100
            
            # Warn about large datasets
            if analysis_rows > 25:
                logger.warning(
                    f"Analyzing {analysis_rows} rows for data checking. "
                    f"This may be slow for very wide files. Consider setting sample_rows <= 25 "
                    f"or check_column_data=False for faster processing."
                )
            
            # Load data via pandas (skipping header row, not interpreting headers)
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=None,  # DON'T interpret first row as headers  
                skiprows=self.header_row,  # Skip past the header row
                nrows=analysis_rows,  # Limit rows for performance
                engine='openpyxl'
            )
            
            logger.debug(f"Pandas loaded {df.shape[0]} rows × {df.shape[1]} columns for analysis")
            
            # STEP 3: Match by position and analyze data presence
            column_has_data = []
            empty_columns = []
            
            for i, header in enumerate(headers):
                if i < len(df.columns):
                    # Use pandas vectorized operation - VERY fast even on large data
                    has_data = df.iloc[:, i].notna().any()
                    column_has_data.append(has_data)
                    
                    if not has_data and header.strip():  # Non-empty header but no data
                        empty_columns.append(f"{header} (col {i+1})")
                else:
                    # Header exists but no corresponding data column (trailing empty)
                    column_has_data.append(False)
                    if header.strip():
                        empty_columns.append(f"{header} (col {i+1}, no data)")
            
            empty_count = sum(1 for x in column_has_data if not x)
            
            logger.info(f"Data analysis: {len(headers)} headers, {empty_count} empty columns detected")
            if empty_columns and len(empty_columns) <= 10:  # Don't spam if too many
                logger.debug(f"Empty columns detected: {empty_columns}")
            elif empty_columns:
                logger.debug(f"Empty columns detected: {empty_columns[:10]} ... and {len(empty_columns)-10} more")
            
            return {
                'headers': headers,  # Original pristine headers from openpyxl
                'column_has_data': column_has_data,
                'empty_column_count': empty_count,
                'empty_columns': empty_columns,
                'analyzed_rows': df.shape[0],
                'pandas_columns': df.shape[1]
            }
            
        except Exception as e:
            # Fall back gracefully - don't let data checking break header reading
            logger.warning(f"Fast pandas data check failed, continuing without data analysis: {e}")
            return {
                'headers': headers,
                'column_has_data': [True] * len(headers),  # Assume all have data
                'empty_column_count': 0,
                'empty_columns': [],
                'analyzed_rows': 0,
                'pandas_columns': 0
            }

    def _quick_excel_empty_check(self, file_path: str, sheet_name: str, headers: list) -> int:
        """
        DEPRECATED: Replaced by _fast_pandas_data_check which is much faster.
        
        This method is kept for compatibility but should not be used for new code.
        The dual openpyxl/pandas approach is orders of magnitude faster.
        """
        logger.warning("Using deprecated _quick_excel_empty_check - consider upgrading to dual approach")
        
        # Call the new fast method
        result = self._fast_pandas_data_check(file_path, sheet_name, headers)
        return result['empty_column_count']

    def _trim_trailing_empty_columns(self, headers: list, analysis_data: dict = None) -> list:
        """
        Enhanced trimming that removes trailing ghost columns intelligently.
        
        Args:
            headers: List of header strings (possibly with "Empty: X" replacements)
            analysis_data: Optional analysis data to use instead of self._last_excel_analysis
            
        Returns:
            Headers with trailing ghost columns removed
        """
        # Use provided analysis data or fall back to stored analysis
        if analysis_data:
            column_has_data = analysis_data.get('column_has_data', [])
            if len(column_has_data) == len(headers):
                return self._trim_with_data_analysis(headers, column_has_data)
        
        # Try stored analysis if no analysis_data provided
        elif hasattr(self, '_last_excel_analysis') and self._last_excel_analysis:
            analysis = self._last_excel_analysis
            column_has_data = analysis.get('column_has_data', [])
            
            if len(column_has_data) == len(headers):
                return self._trim_with_data_analysis(headers, column_has_data)
        
        # Fall back to header-only logic for non-Excel files or when data analysis unavailable
        return self._trim_header_only(headers)

    def _trim_with_data_analysis(self, headers: list, column_has_data: list) -> list:
        """
        Trim using both header and data information for precise ghost column detection.
        
        Args:
            headers: List of header strings
            column_has_data: Boolean list indicating data presence
            
        Returns:
            Trimmed headers list
        """
        logger.info(f"DEBUG TRIMMING: Starting trim analysis with {len(headers)} headers")
        logger.info(f"DEBUG TRIMMING: Headers length = {len(headers)}, Data analysis length = {len(column_has_data)}")
        
        # Show the last 20 columns for debugging
        debug_start_idx = max(0, len(headers) - 20)
        logger.info(f"DEBUG TRIMMING: Last 20 columns (starting at index {debug_start_idx}):")
        for i in range(debug_start_idx, len(headers)):
            header = headers[i]
            has_data = column_has_data[i] if i < len(column_has_data) else False
            logger.info(f"  [{i}] header='{header}' has_data={has_data}")
        
        # Find the last column that should be kept
        # Keep if: non-empty header OR has data
        last_keep_index = -1
        
        logger.info(f"DEBUG TRIMMING: Evaluating trim conditions (working backwards):")
        
        for i in range(len(headers) - 1, -1, -1):
            header = headers[i]
            has_data = column_has_data[i] if i < len(column_has_data) else False
            
            # Detailed condition evaluation
            header_stripped = header.strip()
            is_empty_pattern = header.startswith("Empty: ")
            has_meaningful_header = header_stripped and not is_empty_pattern
            
            should_keep = has_meaningful_header or has_data
            
            # Log the evaluation for debugging
            if i >= debug_start_idx:  # Only log last 20 for readability
                logger.info(f"  [{i}] '{header}': "
                        f"stripped='{header_stripped}' "
                        f"is_empty_pattern={is_empty_pattern} "
                        f"meaningful_header={has_meaningful_header} "
                        f"has_data={has_data} "
                        f"should_keep={should_keep}")
            
            # Special debugging for Empty: columns
            if header.startswith("Empty: "):
                logger.warning(f"DEBUG EMPTY COLUMN [{i}] '{header}': "
                            f"has_data={has_data} should_keep={should_keep} "
                            f"(This should normally be False/False for trailing empties)")
            
            if should_keep:
                last_keep_index = i
                logger.info(f"DEBUG TRIMMING: Found last column to keep at index {i}: '{header}'")
                break
        
        # Return trimmed list
        if last_keep_index >= 0:
            trimmed = headers[:last_keep_index + 1]
            trimmed_count = len(headers) - len(trimmed)
            logger.info(f"DEBUG TRIMMING: Trimming {trimmed_count} columns "
                    f"(keeping indices 0 through {last_keep_index})")
            
            if trimmed_count > 0:
                logger.info(f"DEBUG TRIMMING: Trimmed columns: {headers[last_keep_index + 1:]}")
            
            return trimmed
        else:
            # All columns are ghosts
            logger.warning("DEBUG TRIMMING: All columns detected as ghosts, returning empty list")
            return []

    def _trim_header_only(self, headers: list) -> list:
        """
        Fallback trimming logic using only header information.
        
        Args:
            headers: List of header strings
            
        Returns:
            Headers with trailing empty strings removed
        """
        # Find last non-empty header
        last_non_empty = -1
        for i in range(len(headers) - 1, -1, -1):
            if headers[i].strip():
                last_non_empty = i
                break
        
        # Return trimmed list
        if last_non_empty >= 0:
            trimmed = headers[:last_non_empty + 1]
            trimmed_count = len(headers) - len(trimmed)
            if trimmed_count > 0:
                logger.debug(f"Basic trimming removed {trimmed_count} trailing empty columns")
            return trimmed
        else:
            # All headers are empty
            logger.debug("All headers empty, returning empty list")
            return []

    def _analyze_columns(self, source_columns: list, template_columns: list) -> dict:
        """
        Analyze columns with datetime-aware matching.
        
        Args:
            source_columns: Column names from source file
            template_columns: Column names from template file
            
        Returns:
            Dictionary with analysis results
        """
        # Clean up empty columns first
        clean_source_columns = [col.strip() for col in source_columns if col.strip()]
        clean_template_columns = [col.strip() for col in template_columns if col.strip()]
        
        # PHASE 1: Exact matches (including datetime-aware matches)
        exact_matches = set()
        handled_source_columns = set()
        handled_template_columns = set()
        
        for template_col in clean_template_columns:
            for source_col in clean_source_columns:
                if source_col in handled_source_columns:
                    continue
                    
                if self._datetime_aware_match(source_col, template_col):
                    exact_matches.add((source_col, template_col))
                    handled_source_columns.add(source_col)
                    handled_template_columns.add(template_col)
                    break
        
        logger.debug(f"Phase 1 - Datetime-aware exact matches: {len(exact_matches)} columns")
        if exact_matches:
            logger.debug(f"Exact matches: {list(exact_matches)[:5]}...")  # Show first few
        
        # PHASE 2: Fuzzy matching for remaining unmatched columns
        rename_mapping = {}
        unhandled_template_columns = [col for col in clean_template_columns if col not in handled_template_columns]
        unhandled_source_columns = [col for col in clean_source_columns if col not in handled_source_columns]
        
        logger.debug(f"Phase 2 - Fuzzy matching: {len(unhandled_template_columns)} template columns vs {len(unhandled_source_columns)} source columns")
        
        for template_col in unhandled_template_columns:
            best_match = None
            best_ratio = 0
            
            for source_col in unhandled_source_columns:
                if source_col in rename_mapping:
                    continue
                    
                ratio = SequenceMatcher(None, source_col.lower(), template_col.lower()).ratio()
                if ratio > best_ratio and ratio >= self.similarity_threshold:
                    best_ratio = ratio
                    best_match = source_col
            
            if best_match:
                rename_mapping[best_match] = template_col
                logger.debug(f"Fuzzy match: '{best_match}' → '{template_col}' (similarity: {best_ratio:.3f})")
        
        # Calculate final results
        all_source_columns = set(clean_source_columns)
        all_template_columns = set(clean_template_columns)
        
        # Columns satisfied by exact matches or renames
        satisfied_by_exact = {template_col for source_col, template_col in exact_matches}
        satisfied_by_renames = set(rename_mapping.values())
        
        columns_to_create = [
            col for col in clean_template_columns
            if col not in satisfied_by_exact and col not in satisfied_by_renames
        ]
        
        # Track what was removed from create list
        initial_create_candidates = [
            col for col in clean_template_columns 
            if col not in all_source_columns
        ]
        removed_from_create = [
            col for col in initial_create_candidates
            if col in satisfied_by_renames
        ]
        
        # Orphaned columns
        columns_handled_by_exact = {source_col for source_col, template_col in exact_matches}
        columns_handled_by_renames = set(rename_mapping.keys())
        
        orphaned_columns = [
            col for col in clean_source_columns
            if col not in columns_handled_by_exact and col not in columns_handled_by_renames
        ]
        
        logger.info(f"Column analysis complete:")
        logger.info(f"  Exact matches (datetime-aware): {len(exact_matches)}")
        logger.info(f"  Fuzzy renames: {len(rename_mapping)}")
        logger.info(f"  Columns to create: {len(columns_to_create)}")
        logger.info(f"  Removed from create (satisfied by renames): {len(removed_from_create)}")
        logger.info(f"  Orphaned columns: {len(orphaned_columns)}")
        
        return {
            'raw': clean_source_columns,
            'desired': clean_template_columns,
            'rename_mapping': rename_mapping,
            'to_create': columns_to_create,
            'orphaned': sorted(orphaned_columns),
            'exact_matches': sorted([f"{s} → {t}" for s, t in exact_matches]),
            'removed_from_create': sorted(removed_from_create)
        }

    def _datetime_aware_match(self, col1: str, col2: str) -> bool:
        """
        Check if two column names match, with datetime awareness.
        
        Args:
            col1: First column name
            col2: Second column name
            
        Returns:
            True if columns match (including datetime variations)
        """
        # Exact string match
        if col1 == col2:
            return True
        
        # Try datetime parsing if either looks like a date
        if self._looks_like_datetime(col1) or self._looks_like_datetime(col2):
            try:
                dt1 = self._parse_datetime_flexible(col1)
                dt2 = self._parse_datetime_flexible(col2)
                
                if dt1 and dt2:
                    # Compare as datetime objects (ignoring time if both are date-only)
                    if dt1.time() == dt1.time().replace(hour=0, minute=0, second=0) and \
                        dt2.time() == dt2.time().replace(hour=0, minute=0, second=0):
                        # Both are date-only, compare dates
                        return dt1.date() == dt2.date()
                    else:
                        # At least one has time, compare full datetime
                        return dt1 == dt2
                        
            except Exception:
                pass  # Not valid datetime strings
        
        return False

    def _looks_like_datetime(self, text: str) -> bool:
        """Check if text looks like it could be a datetime."""
        datetime_indicators = [
            '/', '-', ':', 
            '2024', '2025', '2026',  # Common years
            'jan', 'feb', 'mar', 'apr', 'may', 'jun',
            'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
        ]
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in datetime_indicators)

    def _parse_datetime_flexible(self, text: str):
        """
        Parse datetime from various formats.
        
        Returns datetime object or None if parsing fails.
        """
        try:
            from datetime import datetime
            
            # Common date formats to try
            formats = [
                '%Y-%m-%d %H:%M:%S',  # ISO format
                '%Y-%m-%d',           # ISO date only
                '%m/%d/%Y',           # US format
                '%m/%d/%y',           # US format 2-digit year
                '%d/%m/%Y',           # European format
                '%d/%m/%y',           # European format 2-digit year
                '%B %d, %Y',          # "January 1, 2025"
                '%b %d, %Y',          # "Jan 1, 2025"
                '%m-%d-%Y',           # US with dashes
                '%d-%m-%Y',           # European with dashes
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(text, fmt)
                except ValueError:
                    continue
                    
            return None
            
        except Exception:
            return None

    def _write_yaml_config(self, analysis: dict):
        """
        Write YAML configuration file with column analysis in the correct format.
        
        Args:
            analysis: Dictionary with column analysis results
        """
        output_lines = []
        
        # Generate header comments
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        output_lines.append('# Column configuration generated by generate_column_config processor')
        output_lines.append(f'# Generated: {timestamp}')
        output_lines.append('')
        
        # Add settings and variables structure with proper indentation
        output_lines.append('settings:')
        output_lines.append('  variables:')

        # Write original columns (cleaned)
        output_lines.append('')
        output_lines.append('    var_columns_original: [')
        raw_items = analysis['raw']
        if raw_items:
            for i in range(0, len(raw_items), 3):
                group = raw_items[i:i + 3]
                line_items = ', '.join(f'"{item}"' for item in group)
                
                if i + 3 >= len(raw_items):  # Last line
                    output_lines.append(f'      {line_items}')
                else:
                    output_lines.append(f'      {line_items},')
        output_lines.append('    ]')

        # Write rename mapping
        output_lines.append('')
        if analysis['rename_mapping']:
            output_lines.append('    var_columns_to_rename: {')
            items = list(analysis['rename_mapping'].items())
            for i, (old_name, new_name) in enumerate(items):
                comma = ',' if i < len(items) - 1 else ''
                output_lines.append(f'      "{old_name}": "{new_name}"{comma}')
            output_lines.append('    }')
        else:
            output_lines.append('    var_columns_to_rename: {}')

        # Write desired columns (template order, cleaned)
        output_lines.append('')
        output_lines.append('    var_columns_to_keep: [')
        desired_items = analysis['desired']
        if desired_items:
            for i in range(0, len(desired_items), 3):
                group = desired_items[i:i + 3]
                line_items = ', '.join(f'"{item}"' for item in group)
                
                if i + 3 >= len(desired_items):  # Last line
                    output_lines.append(f'      {line_items}')
                else:
                    output_lines.append(f'      {line_items},')
        output_lines.append('    ]')

        # Write columns to create (sorted for consistency)
        sorted_to_create = sorted(analysis['to_create'])
        output_lines.append('')
        if sorted_to_create:
            output_lines.append('    var_columns_to_create: [')
            for i in range(0, len(sorted_to_create), 3):
                group = sorted_to_create[i:i + 3]
                line_items = ', '.join(f'"{item}"' for item in group)
                
                if i + 3 >= len(sorted_to_create):  # Last line
                    output_lines.append(f'      {line_items}')
                else:
                    output_lines.append(f'      {line_items},')
            output_lines.append('    ]')
        else:
            output_lines.append('    var_columns_to_create: []')

        # Write orphaned columns (columns that will be lost)
        orphaned_columns = analysis['orphaned']
        output_lines.append('')
        if orphaned_columns:
            output_lines.append('    var_columns_orphaned: [')
            for i in range(0, len(orphaned_columns), 3):
                group = orphaned_columns[i:i + 3]
                line_items = ', '.join(f'"{item}"' for item in group)
                
                if i + 3 >= len(orphaned_columns):  # Last line
                    output_lines.append(f'      {line_items}')
                else:
                    output_lines.append(f'      {line_items},')
            output_lines.append('    ]')
        else:
            output_lines.append('    var_columns_orphaned: []')

        # NEW: Write columns removed from create list (troubleshooting helper)
        removed_from_create = analysis.get('removed_from_create', [])
        output_lines.append('')
        if removed_from_create:
            output_lines.append('    var_removed_from_create: [')
            for i in range(0, len(removed_from_create), 3):
                group = removed_from_create[i:i + 3]
                line_items = ', '.join(f'"{item}"' for item in group)
                
                if i + 3 >= len(removed_from_create):  # Last line
                    output_lines.append(f'      {line_items}')
                else:
                    output_lines.append(f'      {line_items},')
            output_lines.append('    ]')
        else:
            output_lines.append('    var_removed_from_create: []')
        
        # Write example recipe section if requested
        if self.include_recipe_section:
            output_lines.append('')
            output_lines.append('# Example recipe using this configuration:')
            output_lines.append('example_recipe:')
            output_lines.append('  description: "Example recipe generated from column analysis"')
            output_lines.append('  settings:')
            output_lines.append('    stages:')
            output_lines.append('    - stage_name: "stg_source_data"')
            output_lines.append('      description: "Raw imported data"')
            output_lines.append('    - stage_name: "stg_renamed_columns"')
            output_lines.append('      description: "Data with renamed columns"')
            output_lines.append('    - stage_name: "stg_final_structure"')
            output_lines.append('      description: "Data with final column structure"')
            output_lines.append('  recipe:')
            output_lines.append('  - step_description: "Import source data file"')
            output_lines.append('    processor_type: "import_file"')
            output_lines.append('    input_file: "source_data.xlsx"')
            output_lines.append('    save_to_stage: "stg_source_data"')
            output_lines.append('')
            
            # Only include rename step if there are actual renames
            if analysis['rename_mapping']:
                output_lines.append('  - step_description: "Rename columns to match template"')
                output_lines.append('    processor_type: "rename_columns"')
                output_lines.append('    source_stage: "stg_source_data"')
                output_lines.append('    rename_type: "mapping"')
                output_lines.append('    column_mapping: "{dict:var_columns_to_rename}"')
                output_lines.append('    save_to_stage: "stg_renamed_columns"')
                output_lines.append('')
                
                next_stage = 'stg_renamed_columns'
            else:
                next_stage = 'stg_source_data'
            
            # Add select_columns step to get final structure
            output_lines.append('  - step_description: "Select and create final column structure"')
            output_lines.append('    processor_type: "select_columns"')
            output_lines.append(f'    source_stage: "{next_stage}"')
            output_lines.append('    columns_to_keep: "{list:var_columns_to_keep}"')
            output_lines.append('    columns_to_create: "{list:var_columns_to_create}"')
            output_lines.append('    save_to_stage: "stg_final_structure"')
            output_lines.append('')
            
            output_lines.append('  - step_description: "Export processed data"')
            output_lines.append('    processor_type: "export_file"')
            output_lines.append('    source_stage: "stg_final_structure"')
            output_lines.append('    output_file: "output/processed_data.xlsx"')
            output_lines.append('    file_format: "excel"')

        # Write to file
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
                f.write('\n')  # Final newline
            
            logger.info(f"Generated column configuration: {self.output_file}")
            
            # Enhanced summary statistics
            num_renames = len(analysis['rename_mapping'])
            num_creates = len(analysis['to_create'])
            num_orphaned = len(analysis['orphaned'])
            num_exact_matches = len(analysis.get('exact_matches', []))
            num_removed_from_create = len(analysis.get('removed_from_create', []))
            num_total = len(analysis['desired'])
            
            logger.info(f"Configuration summary: {num_total} total columns, {num_exact_matches} exact matches, {num_renames} renames, {num_creates} to create, {num_orphaned} orphaned")
            
            if num_renames > 0:
                logger.debug(f"Detected renames: {list(analysis['rename_mapping'].items())}")
                
            if num_orphaned > 0:
                logger.warning(f"Orphaned columns (will be lost): {analysis['orphaned']}")
                
            if num_removed_from_create > 0:
                logger.info(f"Columns removed from create list (satisfied by renames): {analysis['removed_from_create']}")
            
        except Exception as e:
            raise StepProcessorError(f"Failed to write configuration file: {e}")

    def _write_bracketed_list(self, file_handle, columns: list, max_per_line: int = 3):
        """Write columns as bracketed lists with max items per line."""
        clean_columns = [col for col in columns if col.strip()]
        
        if not clean_columns:
            file_handle.write("[]\n")
            return
        
        # Start the list with newline after opening bracket
        file_handle.write("[\n")
        
        # Write columns with proper line breaks
        for i, col in enumerate(clean_columns):
            # Add proper quoting manually (no yaml.dump)
            if '"' in col:
                quoted_col = f"'{col}'"  # Use single quotes if double quotes in text
            elif "'" in col:
                quoted_col = f'"{col}"'  # Use double quotes if single quotes in text
            elif ' ' in col or '#' in col:
                quoted_col = f'"{col}"'  # Quote if spaces or special chars
            else:
                quoted_col = f'"{col}"'  # Default to double quotes
            
            # Add indentation for first item on line
            if i % max_per_line == 0:
                file_handle.write("  ")  # Just 2 spaces indentation
            
            # Write the column
            file_handle.write(quoted_col)
            
            # Add comma if not last item
            if i < len(clean_columns) - 1:
                file_handle.write(",")
                
                # Add line break for next line or space for same line
                if (i + 1) % max_per_line == 0:
                    file_handle.write("\n")
                else:
                    file_handle.write(" ")
        
        # Close the list with newline before closing bracket
        file_handle.write("\n  ]\n")  # Just 2 spaces indentation
    
    def _generate_example_recipe(self, analysis: dict) -> dict:
        """
        Generate example recipe using the column analysis.
        
        Args:
            analysis: Dictionary with column analysis results
            
        Returns:
            Dictionary with example recipe steps
        """
        steps = []
        
        # Step 1: Import source file
        steps.append({
            'step_description': 'Import source data file',
            'processor_type': 'import_file',
            'input_file': self.source_file,
            'save_to_stage': 'stg_source_data'
        })
        
        # Step 2: Rename columns if needed
        if analysis['rename_mapping']:
            steps.append({
                'step_description': 'Rename columns to match template',
                'processor_type': 'rename_columns',
                'source_stage': 'stg_source_data',
                'rename_type': 'mapping',
                'column_mapping': analysis['rename_mapping'],
                'save_to_stage': 'stg_renamed_columns'
            })
            last_stage = 'stg_renamed_columns'
        else:
            last_stage = 'stg_source_data'
        
        # Step 3: Add missing columns if needed
        if analysis['to_create']:
            create_mapping = {col: '' for col in analysis['to_create']}
            steps.append({
                'step_description': 'Add missing columns from template',
                'processor_type': 'add_columns',
                'source_stage': last_stage,
                'new_columns': create_mapping,
                'save_to_stage': 'stg_final_structure'
            })
            last_stage = 'stg_final_structure'
        
        # Step 4: Export result
        steps.append({
            'step_description': 'Export processed data',
            'processor_type': 'export_file',
            'source_stage': last_stage,
            'output_file': 'output/processed_data.xlsx',
            'file_format': 'excel'
        })
        
        return {
            'description': 'Example recipe generated from column analysis',
            'settings': {
                'stages': [
                    {'stage_name': 'stg_source_data', 'description': 'Raw imported data'},
                    {'stage_name': 'stg_renamed_columns', 'description': 'Data with renamed columns'},
                    {'stage_name': 'stg_final_structure', 'description': 'Data with final column structure'}
                ]
            },
            'recipe': steps
        }


# End of file #
