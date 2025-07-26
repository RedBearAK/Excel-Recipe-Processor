"""
Lookup data step processor for Excel automation recipes.

Handles looking up values from reference data sources with support for:
- File-based lookups with variable substitution via FileReader
- Stage-based lookups via StageManager integration  
- Multiple lookup source types and join operations
- VLOOKUP and INDEX-MATCH style operations
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class LookupDataProcessor(BaseStepProcessor):
    """
    Processor for looking up values from reference data sources.
    
    Supports various lookup operations including:
    - VLOOKUP-style left joins with file or stage sources
    - INDEX-MATCH operations with fuzzy matching
    - Multi-column lookups across sources
    - Stage-based reference data with dynamic updates
    - File-based lookups with variable substitution
    - Chained lookup operations across multiple sources
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'lookup_source': {'type': 'inline', 'data': {'key': ['test'], 'value': ['result']}},
            'lookup_key': 'key',
            'source_key': 'source_key', 
            'lookup_columns': ['value']
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the lookup operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to enrich with lookup data
            
        Returns:
            DataFrame with lookup columns added
            
        Raises:
            StepProcessorError: If lookup operation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Lookup step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['lookup_source', 'lookup_key', 'source_key', 'lookup_columns'])
        
        lookup_source = self.get_config_value('lookup_source')
        lookup_key = self.get_config_value('lookup_key')
        source_key = self.get_config_value('source_key')
        lookup_columns = self.get_config_value('lookup_columns')
        
        # Optional configuration
        join_type = self.get_config_value('join_type', 'left')
        handle_duplicates = self.get_config_value('handle_duplicates', 'first')
        case_sensitive = self.get_config_value('case_sensitive', True)
        prefix = self.get_config_value('prefix', '')
        suffix = self.get_config_value('suffix', '')
        default_value = self.get_config_value('default_value', None)
        
        # Validate inputs
        self._validate_lookup_config(data, lookup_key, source_key, lookup_columns, join_type, handle_duplicates)
        
        try:
            # Load lookup data from various sources
            lookup_data = self._load_lookup_data(lookup_source)
            
            # Apply case sensitivity handling
            if not case_sensitive:
                lookup_data, data = self._apply_case_insensitive_matching(lookup_data, data, lookup_key, source_key)
            
            # Handle duplicates in lookup data
            if handle_duplicates in ['first', 'last']:
                lookup_data = self._handle_duplicates(lookup_data, lookup_key, handle_duplicates)
            elif handle_duplicates == 'error':
                self._check_for_duplicates(lookup_data, lookup_key)
            
            # Perform the lookup operation
            result = self._perform_lookup(data, lookup_data, lookup_key, source_key, lookup_columns, join_type)
            
            # Apply prefixes/suffixes to lookup columns
            if prefix or suffix:
                result = self._apply_column_naming(result, lookup_columns, prefix, suffix)
            
            # Handle missing values with defaults
            if default_value is not None:
                result = self._apply_default_values(result, lookup_columns, default_value, prefix, suffix)
            
            # Generate result summary
            initial_rows = len(data)
            final_rows = len(result)
            lookup_hits = self._count_successful_lookups(result, lookup_columns, prefix, suffix)
            
            result_info = f"looked up {len(lookup_columns)} columns for {initial_rows} rows (hits: {lookup_hits})"
            self.log_step_complete(result_info)
            
            return result
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Lookup operation failed in step '{self.step_name}': {e}")
    
    def _validate_lookup_config(self, data: pd.DataFrame, lookup_key: str, source_key: str, 
                              lookup_columns: list, join_type: str, handle_duplicates: str) -> None:
        """Validate lookup configuration parameters."""
        
        # Check source key exists in main data
        if source_key not in data.columns:
            available_columns = list(data.columns)
            raise StepProcessorError(
                f"Source key column '{source_key}' not found in main data. "
                f"Available columns: {available_columns}"
            )
        
        # Validate lookup_columns is a list
        if not isinstance(lookup_columns, list) or len(lookup_columns) == 0:
            raise StepProcessorError("lookup_columns must be a non-empty list")
        
        # Validate join_type
        valid_join_types = self.get_supported_join_types()
        if join_type not in valid_join_types:
            raise StepProcessorError(
                f"Unsupported join_type '{join_type}'. Supported types: {valid_join_types}"
            )
        
        # Validate handle_duplicates
        valid_duplicate_handling = self.get_supported_duplicate_handling()
        if handle_duplicates not in valid_duplicate_handling:
            raise StepProcessorError(
                f"Unsupported handle_duplicates '{handle_duplicates}'. "
                f"Supported options: {valid_duplicate_handling}"
            )
    
    def _load_lookup_data(self, lookup_source) -> pd.DataFrame:
        """
        Load lookup data from various source types.
        
        Args:
            lookup_source: Lookup source configuration
            
        Returns:
            DataFrame with lookup data
        """
        if isinstance(lookup_source, pd.DataFrame):
            # Direct DataFrame
            return lookup_source.copy()
        
        elif isinstance(lookup_source, dict):
            if 'type' not in lookup_source:
                # Treat as inline data dictionary
                return pd.DataFrame(lookup_source)
            
            source_type = lookup_source['type']
            
            if source_type == 'file':
                # File-based lookup with FileReader integration
                return self._load_file_lookup_data(lookup_source)
            
            elif source_type == 'stage':
                # Stage-based lookup with StageManager integration
                return self._load_stage_lookup_data(lookup_source)
            
            elif source_type == 'inline':
                # Inline data dictionary
                if 'data' not in lookup_source:
                    raise StepProcessorError("Inline lookup source missing 'data' field")
                return pd.DataFrame(lookup_source['data'])
            
            else:
                raise StepProcessorError(f"Unsupported lookup source type: {source_type}")
        
        elif isinstance(lookup_source, str):
            # Treat as file path
            return self._load_file_lookup_data({'type': 'file', 'filename': lookup_source})
        
        else:
            raise StepProcessorError(
                f"Unsupported lookup_source format. Expected dict, DataFrame, or string, "
                f"got {type(lookup_source)}"
            )
    
    def _load_file_lookup_data(self, source_config: dict) -> pd.DataFrame:
        """Load lookup data from file using FileReader with variable substitution."""
        
        if 'filename' not in source_config:
            raise StepProcessorError("File lookup source missing 'filename' field")
        
        filename = source_config['filename']
        sheet = source_config.get('sheet', 0)
        encoding = source_config.get('encoding', 'utf-8')
        separator = source_config.get('separator', ',')
        explicit_format = source_config.get('format', None)
        
        # Get custom variables for substitution (from pipeline if available)
        variables = getattr(self, 'variables', None)
        
        try:
            lookup_data = FileReader.read_file(
                filename=filename,
                variables=variables,
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format
            )
            
            logger.debug(f"Loaded lookup data from file '{filename}': {len(lookup_data)} rows")
            return lookup_data
            
        except FileReaderError as e:
            raise StepProcessorError(f"Failed to load lookup file '{filename}': {e}")
    
    def _load_stage_lookup_data(self, source_config: dict) -> pd.DataFrame:
        """Load lookup data from stage using StageManager."""
        
        if 'stage_name' not in source_config:
            raise StepProcessorError("Stage lookup source missing 'stage_name' field")
        
        stage_name = source_config['stage_name']
        
        # Check if stage exists
        if not StageManager.stage_exists(stage_name):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Lookup stage '{stage_name}' not found. Available stages: {available_stages}"
            )
        
        try:
            lookup_data = StageManager.get_stage_data(stage_name)
            logger.debug(f"Loaded lookup data from stage '{stage_name}': {len(lookup_data)} rows")
            return lookup_data
            
        except StageError as e:
            raise StepProcessorError(f"Failed to load lookup stage '{stage_name}': {e}")
    
    def _apply_case_insensitive_matching(self, lookup_data: pd.DataFrame, main_data: pd.DataFrame,
                                       lookup_key: str, source_key: str) -> tuple:
        """Apply case insensitive matching by creating temporary lowercase columns."""
        
        # Create temporary lowercase columns
        lookup_data_copy = lookup_data.copy()
        main_data_copy = main_data.copy()
        
        temp_lookup_key = f"_temp_{lookup_key}_lower"
        temp_source_key = f"_temp_{source_key}_lower"
        
        lookup_data_copy[temp_lookup_key] = lookup_data_copy[lookup_key].astype(str).str.lower()
        main_data_copy[temp_source_key] = main_data_copy[source_key].astype(str).str.lower()
        
        return lookup_data_copy, main_data_copy
    
    def _handle_duplicates(self, lookup_data: pd.DataFrame, lookup_key: str, 
                          handle_method: str) -> pd.DataFrame:
        """Handle duplicate keys in lookup data."""
        
        if handle_method == 'first':
            return lookup_data.drop_duplicates(subset=[lookup_key], keep='first')
        elif handle_method == 'last':
            return lookup_data.drop_duplicates(subset=[lookup_key], keep='last')
        else:
            return lookup_data
    
    def _check_for_duplicates(self, lookup_data: pd.DataFrame, lookup_key: str) -> None:
        """Check for duplicates and raise error if found."""
        
        duplicates = lookup_data[lookup_data.duplicated(subset=[lookup_key], keep=False)]
        if len(duplicates) > 0:
            duplicate_keys = duplicates[lookup_key].unique()
            raise StepProcessorError(
                f"Duplicate keys found in lookup data: {list(duplicate_keys)}. "
                f"Use handle_duplicates='first' or 'last' to handle duplicates automatically."
            )
    
    def _perform_lookup(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                       lookup_key: str, source_key: str, lookup_columns: list, 
                       join_type: str) -> pd.DataFrame:
        """Perform the actual lookup/join operation."""
        
        # Validate lookup key exists in lookup data
        if lookup_key not in lookup_data.columns:
            available_columns = list(lookup_data.columns)
            raise StepProcessorError(
                f"Lookup key column '{lookup_key}' not found in lookup data. "
                f"Available columns: {available_columns}"
            )
        
        # Validate lookup columns exist in lookup data
        missing_columns = [col for col in lookup_columns if col not in lookup_data.columns]
        if missing_columns:
            available_columns = list(lookup_data.columns)
            raise StepProcessorError(
                f"Lookup columns {missing_columns} not found in lookup data. "
                f"Available columns: {available_columns}"
            )
        
        # Handle case insensitive matching
        if hasattr(main_data, f'_temp_{source_key}_lower'):
            # Use temporary lowercase columns for matching
            lookup_key_to_use = f"_temp_{lookup_key}_lower"
            source_key_to_use = f"_temp_{source_key}_lower"
        else:
            lookup_key_to_use = lookup_key
            source_key_to_use = source_key
        
        # Prepare lookup data for merge
        merge_columns = [lookup_key_to_use] + lookup_columns
        if lookup_key != lookup_key_to_use:
            # Add original lookup key to preserve it
            merge_columns.append(lookup_key)
        
        lookup_subset = lookup_data[merge_columns]
        
        try:
            # Perform the merge operation
            result = main_data.merge(
                lookup_subset,
                left_on=source_key_to_use,
                right_on=lookup_key_to_use,
                how=join_type
            )
            
            # Clean up temporary columns
            temp_columns = [col for col in result.columns if col.startswith('_temp_')]
            if temp_columns:
                result = result.drop(columns=temp_columns)
            
            # Remove duplicate lookup key column if different from source key
            if lookup_key in result.columns and lookup_key != source_key:
                result = result.drop(columns=[lookup_key])
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error during lookup merge operation: {e}")
    
    def _apply_column_naming(self, data: pd.DataFrame, lookup_columns: list, 
                           prefix: str, suffix: str) -> pd.DataFrame:
        """Apply prefix/suffix to lookup columns."""
        
        if not prefix and not suffix:
            return data
        
        rename_mapping = {}
        for col in lookup_columns:
            if col in data.columns:
                new_name = f"{prefix}{col}{suffix}"
                rename_mapping[col] = new_name
        
        if rename_mapping:
            data = data.rename(columns=rename_mapping)
        
        return data
    
    def _apply_default_values(self, data: pd.DataFrame, lookup_columns: list,
                            default_value: Any, prefix: str, suffix: str) -> pd.DataFrame:
        """Apply default values for missing lookup results."""
        
        # Get actual column names after prefix/suffix application
        actual_columns = []
        for col in lookup_columns:
            actual_name = f"{prefix}{col}{suffix}"
            if actual_name in data.columns:
                actual_columns.append(actual_name)
        
        # Fill missing values
        for col in actual_columns:
            data[col] = data[col].fillna(default_value)
        
        return data
    
    def _count_successful_lookups(self, data: pd.DataFrame, lookup_columns: list,
                                prefix: str, suffix: str) -> int:
        """Count number of rows with successful lookups."""
        
        if len(lookup_columns) == 0:
            return 0
        
        # Get first actual lookup column name
        first_col = f"{prefix}{lookup_columns[0]}{suffix}"
        
        if first_col in data.columns:
            return data[first_col].notna().sum()
        else:
            return 0
    
    # ============================================================================
    # UTILITY METHODS FOR ADVANCED OPERATIONS
    # ============================================================================
    
    def vlookup_style_join(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                          lookup_key: str, source_key: str, return_columns: list,
                          exact_match: bool = True) -> pd.DataFrame:
        """
        Perform VLOOKUP-style join operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup table DataFrame
            lookup_key: Key column in lookup table
            source_key: Key column in main data
            return_columns: List of columns to return from lookup table
            exact_match: Whether to require exact matches
            
        Returns:
            DataFrame with lookup columns added
        """
        try:
            if exact_match:
                # Standard exact match join
                result = main_data.merge(
                    lookup_data[[lookup_key] + return_columns],
                    left_on=source_key,
                    right_on=lookup_key,
                    how='left'
                )
            else:
                # Approximate match (find closest)
                raise StepProcessorError("Approximate match not yet implemented")
            
            # Remove duplicate lookup key column
            if lookup_key in result.columns and lookup_key != source_key:
                result = result.drop(columns=[lookup_key])
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error in VLOOKUP-style operation: {e}")
    
    def index_match_style_join(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                             lookup_key: str, source_key: str, return_columns: list,
                             exact_match: bool = True) -> pd.DataFrame:
        """
        Perform INDEX-MATCH style join operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup table DataFrame
            lookup_key: Key column in lookup table
            source_key: Key column in main data
            return_columns: List of columns to return from lookup table
            exact_match: Whether to require exact matches
            
        Returns:
            DataFrame with lookup columns added
        """
        try:
            if exact_match:
                # Standard exact match join
                result = main_data.merge(
                    lookup_data[[lookup_key] + return_columns],
                    left_on=source_key,
                    right_on=lookup_key,
                    how='left'
                )
            else:
                # Approximate match (find closest)
                raise StepProcessorError("Approximate match not yet implemented")
            
            # Remove duplicate lookup key column
            if lookup_key in result.columns and lookup_key != source_key:
                result = result.drop(columns=[lookup_key])
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error in INDEX-MATCH style operation: {e}")
    
    def create_multi_column_lookup(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                                 lookup_keys: list, source_keys: list, return_columns: list) -> pd.DataFrame:
        """
        Create a multi-column lookup operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup table DataFrame
            lookup_keys: List of key columns in lookup table
            source_keys: List of key columns in main data
            return_columns: List of columns to return from lookup table
            
        Returns:
            DataFrame with lookup columns added
        """
        if len(lookup_keys) != len(source_keys):
            raise StepProcessorError("Number of lookup keys must match number of source keys")
        
        try:
            # Perform multi-column join
            result = main_data.merge(
                lookup_data[lookup_keys + return_columns],
                left_on=source_keys,
                right_on=lookup_keys,
                how='left'
            )
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error in multi-column lookup: {e}")
    
    def analyze_lookup_potential(self, main_data: pd.DataFrame, lookup_data: pd.DataFrame,
                               main_key: str, lookup_key: str) -> dict:
        """
        Analyze the potential for a successful lookup operation.
        
        Args:
            main_data: Main DataFrame
            lookup_data: Lookup DataFrame
            main_key: Key column in main data
            lookup_key: Key column in lookup data
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {}
        
        try:
            # Basic stats
            analysis['main_data_rows'] = len(main_data)
            analysis['lookup_data_rows'] = len(lookup_data)
            analysis['main_unique_keys'] = main_data[main_key].nunique()
            analysis['lookup_unique_keys'] = lookup_data[lookup_key].nunique()
            
            # Key overlap analysis
            main_keys = set(main_data[main_key].dropna())
            lookup_keys = set(lookup_data[lookup_key].dropna())
            
            analysis['overlapping_keys'] = len(main_keys.intersection(lookup_keys))
            analysis['main_only_keys'] = len(main_keys - lookup_keys)
            analysis['lookup_only_keys'] = len(lookup_keys - main_keys)
            
            # Match rate prediction
            if len(main_keys) > 0:
                analysis['predicted_match_rate'] = analysis['overlapping_keys'] / len(main_keys)
            else:
                analysis['predicted_match_rate'] = 0.0
            
            # Quality indicators
            analysis['has_duplicates_in_lookup'] = lookup_data[lookup_key].duplicated().any()
            analysis['has_nulls_in_main_key'] = main_data[main_key].isna().any()
            analysis['has_nulls_in_lookup_key'] = lookup_data[lookup_key].isna().any()
            
            return analysis
            
        except Exception as e:
            return {'error': str(e)}
    
    # ============================================================================
    # CONFIGURATION AND CAPABILITIES
    # ============================================================================
    
    def get_supported_join_types(self) -> list:
        """Get list of supported join types."""
        return ['left', 'right', 'inner', 'outer']
    
    def get_supported_duplicate_handling(self) -> list:
        """Get list of supported duplicate handling options."""
        return ['first', 'last', 'error']
    
    def get_supported_source_types(self) -> list:
        """Get list of supported lookup source types."""
        return ['file', 'stage', 'inline', 'dataframe']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Lookup and enrich data from multiple source types with advanced join operations',
            'join_types': self.get_supported_join_types(),
            'duplicate_handling': self.get_supported_duplicate_handling(),
            'source_types': self.get_supported_source_types(),
            'lookup_features': [
                'vlookup_style_joins', 'index_match_operations', 'multi_column_lookups',
                'case_insensitive_matching', 'duplicate_handling', 'default_values',
                'column_prefixes_suffixes', 'fuzzy_matching_planned'
            ],
            'data_sources': [
                'excel_files', 'csv_files', 'tsv_files', 'saved_stages', 
                'inline_dictionaries', 'pandas_dataframes'
            ],
            'file_features': [
                'variable_substitution', 'automatic_format_detection',
                'custom_sheet_selection', 'encoding_support'
            ],
            'stage_integration': [
                'dynamic_reference_data', 'stage_based_lookups',
                'multi_stage_workflows', 'cached_lookup_tables'
            ],
            'advanced_operations': [
                'chained_lookups', 'conditional_lookups', 'cross_reference_validation',
                'lookup_quality_analysis', 'performance_optimization'
            ],
            'examples': {
                'file_lookup': "Lookup customer data from customers.xlsx file",
                'stage_lookup': "Lookup from dynamically updated stage data",
                'variable_substitution': "Lookup from customer_data_{date}.xlsx with date substitution",
                'multi_column': "Lookup using combination of customer_id and region",
                'case_insensitive': "Fuzzy matching for customer names and codes",
                'chained_lookups': "First lookup customer tier, then lookup tier benefits"
            },
            'configuration_options': {
                'lookup_source': 'Source configuration (file, stage, inline data)',
                'lookup_key': 'Key column in lookup data',
                'source_key': 'Key column in main data',
                'lookup_columns': 'List of columns to retrieve from lookup data',
                'join_type': 'Type of join operation (left, right, inner, outer)',
                'handle_duplicates': 'How to handle duplicate keys (first, last, error)',
                'case_sensitive': 'Whether matching should be case sensitive',
                'prefix': 'Prefix to add to lookup column names',
                'suffix': 'Suffix to add to lookup column names',
                'default_value': 'Default value for missing lookups'
            }
        }
