"""
Clean lookup data processor for Excel automation recipes.

excel_recipe_processor/processors/lookup_data_processor.py

Handles stage-to-stage lookups with focus on solving real matching problems:
- Type and format normalization (numeric vs string, .0 issues, whitespace)
- Clean pandas merge operations without architectural complexity
- Simple column naming with prefix/suffix support
- Enhanced logging for lookup success/failure analysis
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class LookupDataProcessor(BaseStepProcessor):
    """
    Clean processor for enriching data with lookups from stage sources.
    
    Focuses on solving real lookup problems:
    - Type mismatches (int vs str vs float)
    - Format issues (1001 vs "1001.0")
    - Whitespace normalization
    - Column naming conflicts with prefix/suffix
    - Missing value defaults with detailed logging
    
    Uses stage-to-stage workflow only - no file handling complexity.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'lookup_stage': 'test_lookup_stage',
            'match_col_in_lookup_data': 'test_key',
            'match_col_in_main_data': 'test_key',
            'lookup_columns': ['test_column']
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """Execute the lookup operation."""
        self.log_step_start()
        
        # Validate input
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Lookup step '{self.step_name}' requires a pandas DataFrame")
        
        if len(data) == 0:
            raise StepProcessorError(f"Lookup step '{self.step_name}' received empty DataFrame")
        
        # Get configuration
        lookup_stage = self.get_config_value('lookup_stage')
        match_col_in_lookup_data = self.get_config_value('match_col_in_lookup_data')
        match_col_in_main_data = self.get_config_value('match_col_in_main_data')
        lookup_columns = self.get_config_value('lookup_columns')
        join_type = self.get_config_value('join_type', 'left')
        prefix = self.get_config_value('prefix', '')
        suffix = self.get_config_value('suffix', '')
        default_values = self.get_config_value('default_values', {})
        normalize_keys = self.get_config_value('normalize_keys', True)
        handle_duplicates = self.get_config_value('handle_duplicates', 'first')
        
        # Validate configuration
        self._validate_config(data, lookup_stage, match_col_in_lookup_data, match_col_in_main_data, lookup_columns, join_type)
        
        try:
            # Load lookup data from stage
            lookup_data = self._load_lookup_stage(lookup_stage)
            
            # Validate lookup data structure
            self._validate_lookup_data(lookup_data, match_col_in_lookup_data, lookup_columns)
            
            # Handle duplicates in lookup data
            lookup_data = self._handle_lookup_duplicates(lookup_data, match_col_in_lookup_data, handle_duplicates)
            
            # Normalize keys to handle real matching problems
            if normalize_keys:
                data, lookup_data = self._normalize_keys(data, lookup_data, match_col_in_main_data, match_col_in_lookup_data)
            
            # Perform the lookup merge
            result = self._perform_lookup_merge(data, lookup_data, match_col_in_main_data, match_col_in_lookup_data, lookup_columns, join_type)
            
            # Apply column naming (prefix/suffix)
            if prefix or suffix:
                result = self._apply_column_naming(result, lookup_columns, prefix, suffix)
            
            # Analyze results BEFORE applying defaults (to get accurate match stats)
            match_stats = self._analyze_lookup_results(result, lookup_columns, prefix, suffix)
            
            # Apply default values for missing lookups
            if default_values:
                result = self._apply_default_values(result, lookup_columns, default_values, prefix, suffix)
            
            # Enhanced logging with detailed statistics
            self._log_detailed_results(len(data), len(result), match_stats, default_values)
            
            return result
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Lookup operation failed in step '{self.step_name}': {e}")
    
    def _validate_config(self, data: pd.DataFrame, lookup_stage: str, match_col_in_lookup_data: str, 
                        match_col_in_main_data: str, lookup_columns: list, join_type: str) -> None:
        """Validate configuration parameters."""
        
        # Check required fields
        if not lookup_stage:
            raise StepProcessorError("lookup_stage is required")
        
        if not match_col_in_lookup_data:
            raise StepProcessorError("match_col_in_lookup_data is required")
        
        if not match_col_in_main_data:
            raise StepProcessorError("match_col_in_main_data is required")
        
        if not lookup_columns or not isinstance(lookup_columns, list):
            raise StepProcessorError("lookup_columns must be a non-empty list")
        
        # Check source key exists in main data
        if match_col_in_main_data not in data.columns:
            available = list(data.columns)
            raise StepProcessorError(
                f"Main data key '{match_col_in_main_data}' not found in main data. Available columns: {available}"
            )
        
        # Validate join type
        valid_joins = ['left', 'right', 'inner', 'outer']
        if join_type not in valid_joins:
            raise StepProcessorError(f"Invalid join_type '{join_type}'. Valid options: {valid_joins}")
    
    def _load_lookup_stage(self, stage_name: str) -> pd.DataFrame:
        """Load lookup data from specified stage."""
        
        if not StageManager.stage_exists(stage_name):
            available = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Lookup stage '{stage_name}' not found. Available stages: {available}"
            )
        
        try:
            lookup_data = StageManager.load_stage(stage_name)
            logger.debug(f"Loaded lookup data from stage '{stage_name}': {len(lookup_data)} rows")
            return lookup_data
            
        except StageError as e:
            raise StepProcessorError(f"Failed to load lookup stage '{stage_name}': {e}")
    
    def _validate_lookup_data(self, lookup_data: pd.DataFrame, match_col_in_lookup_data: str, lookup_columns: list) -> None:
        """Validate that lookup data has required columns."""
        
        if len(lookup_data) == 0:
            raise StepProcessorError("Lookup stage contains no data")
        
        # Check lookup key exists
        if match_col_in_lookup_data not in lookup_data.columns:
            available = list(lookup_data.columns)
            raise StepProcessorError(
                f"Lookup data key '{match_col_in_lookup_data}' not found in lookup data. Available columns: {available}"
            )
        
        # Check lookup columns exist
        missing_columns = [col for col in lookup_columns if col not in lookup_data.columns]
        if missing_columns:
            available = list(lookup_data.columns)
            raise StepProcessorError(
                f"Lookup columns not found: {missing_columns}. Available columns: {available}"
            )
    
    def _handle_lookup_duplicates(self, lookup_data: pd.DataFrame, match_col_in_lookup_data: str, handle_method: str) -> pd.DataFrame:
        """Handle duplicate keys in lookup data."""
        
        if handle_method == 'error':
            duplicates = lookup_data[lookup_data.duplicated(subset=[match_col_in_lookup_data], keep=False)]
            if len(duplicates) > 0:
                duplicate_keys = duplicates[match_col_in_lookup_data].unique()[:5]  # Show first 5
                raise StepProcessorError(
                    f"Duplicate keys found in lookup data: {list(duplicate_keys)}... "
                    f"Use handle_duplicates='first' or 'last' to resolve automatically."
                )
        
        elif handle_method == 'first':
            lookup_data = lookup_data.drop_duplicates(subset=[match_col_in_lookup_data], keep='first')
        
        elif handle_method == 'last':
            lookup_data = lookup_data.drop_duplicates(subset=[match_col_in_lookup_data], keep='last')
        
        else:
            raise StepProcessorError(f"Invalid handle_duplicates '{handle_method}'. Valid options: first, last, error")
        
        return lookup_data
    
    def _normalize_keys(self, data: pd.DataFrame, lookup_data: pd.DataFrame, 
                       match_col_in_main_data: str, match_col_in_lookup_data: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Normalize keys to handle real matching problems."""
        
        # Make copies to avoid modifying original data
        data = data.copy()
        lookup_data = lookup_data.copy()
        
        # Convert both keys to string for consistent comparison
        data[match_col_in_main_data] = data[match_col_in_main_data].astype(str)
        lookup_data[match_col_in_lookup_data] = lookup_data[match_col_in_lookup_data].astype(str)
        
        # Handle common numeric format issues (1001.0 -> 1001)
        data[match_col_in_main_data] = data[match_col_in_main_data].str.replace(r'\.0+$', '', regex=True)
        lookup_data[match_col_in_lookup_data] = lookup_data[match_col_in_lookup_data].str.replace(r'\.0+$', '', regex=True)
        
        # Strip whitespace
        data[match_col_in_main_data] = data[match_col_in_main_data].str.strip()
        lookup_data[match_col_in_lookup_data] = lookup_data[match_col_in_lookup_data].str.strip()
        
        # Handle 'nan' strings from null values
        data[match_col_in_main_data] = data[match_col_in_main_data].replace('nan', pd.NA)
        lookup_data[match_col_in_lookup_data] = lookup_data[match_col_in_lookup_data].replace('nan', pd.NA)
        
        logger.debug(f"Normalized keys - main data: {data[match_col_in_main_data].nunique()} unique, "
                    f"lookup data: {lookup_data[match_col_in_lookup_data].nunique()} unique")
        
        return data, lookup_data
    
    def _perform_lookup_merge(self, data: pd.DataFrame, lookup_data: pd.DataFrame,
                             match_col_in_main_data: str, match_col_in_lookup_data: str, lookup_columns: list, 
                             join_type: str) -> pd.DataFrame:
        """Perform the core lookup merge operation."""
        
        # Select only the columns we need from lookup data
        lookup_subset = lookup_data[[match_col_in_lookup_data] + lookup_columns].copy()
        
        # Use clean suffixes to handle any column name conflicts
        result = data.merge(
            lookup_subset,
            left_on=match_col_in_main_data,
            right_on=match_col_in_lookup_data,
            how=join_type,
            suffixes=('', '_FROM_LOOKUP')
        )
        
        # Clean up duplicate key column if different names
        if match_col_in_lookup_data != match_col_in_main_data and match_col_in_lookup_data in result.columns:
            result = result.drop(columns=[match_col_in_lookup_data])
        
        # Handle any conflicts that got suffixed
        conflicted_columns = [col for col in result.columns if col.endswith('_FROM_LOOKUP')]
        for col in conflicted_columns:
            original_name = col.replace('_FROM_LOOKUP', '')
            if original_name in lookup_columns:
                # Replace original with lookup data
                result[original_name] = result[col]
                result = result.drop(columns=[col])
        
        return result
    
    def _apply_column_naming(self, data: pd.DataFrame, lookup_columns: list, 
                           prefix: str, suffix: str) -> pd.DataFrame:
        """Apply prefix and/or suffix to lookup columns."""
        
        rename_mapping = {}
        for col in lookup_columns:
            if col in data.columns:
                new_name = f"{prefix}{col}{suffix}"
                rename_mapping[col] = new_name
        
        if rename_mapping:
            data = data.rename(columns=rename_mapping)
            logger.debug(f"Renamed lookup columns: {rename_mapping}")
        
        return data
    
    def _analyze_lookup_results(self, data: pd.DataFrame, lookup_columns: list, 
                               prefix: str, suffix: str) -> dict:
        """Analyze lookup results before applying defaults."""
        
        stats = {}
        total_rows = len(data)
        
        for col in lookup_columns:
            # Determine actual column name after prefix/suffix
            actual_col = f"{prefix}{col}{suffix}"
            
            if actual_col in data.columns:
                successful_matches = data[actual_col].notna().sum()
                missing_matches = total_rows - successful_matches
                
                stats[col] = {
                    'actual_column': actual_col,
                    'successful_matches': successful_matches,
                    'missing_matches': missing_matches,
                    'success_rate': (successful_matches / total_rows * 100) if total_rows > 0 else 0
                }
        
        return stats
    
    def _apply_default_values(self, data: pd.DataFrame, lookup_columns: list, 
                            default_values: dict, prefix: str, suffix: str) -> pd.DataFrame:
        """Apply default values for missing lookup results."""
        
        for col in lookup_columns:
            # Determine actual column name after prefix/suffix
            actual_col = f"{prefix}{col}{suffix}"
            
            if actual_col in data.columns and col in default_values:
                data[actual_col] = data[actual_col].fillna(default_values[col])
                logger.debug(f"Applied default value '{default_values[col]}' to column '{actual_col}'")
        
        return data
    
    def _log_detailed_results(self, initial_rows: int, final_rows: int, match_stats: dict, default_values: dict) -> None:
        """Log detailed results with per-column statistics."""
        
        # Overall summary
        total_columns = len(match_stats)
        self.log_step_complete(f"enriched {initial_rows} rows with {total_columns} columns")
        
        # Per-column detailed statistics
        logger.info("📊 Lookup Results by Column:")
        
        for col, stats in match_stats.items():
            successful = stats['successful_matches']
            missing = stats['missing_matches']
            success_rate = stats['success_rate']
            
            # Determine what happened to missing values
            has_default = col in default_values
            default_info = f" → '{default_values[col]}'" if has_default else " (no default)"
            
            logger.info(f"   📈 {col}: {successful:,} matched ({success_rate:.1f}%), "
                        f"{missing:,} missing{default_info}")
        
        # Summary warnings for problematic columns
        problematic_columns = []
        for col, stats in match_stats.items():
            if stats['success_rate'] < 50:  # Less than 50% match rate
                problematic_columns.append(f"'{col}' ({stats['success_rate']:.1f}%)")
        
        if problematic_columns:
            logger.warning(f"⚠️  Low match rates: {', '.join(problematic_columns)}")
        
        # # Success summary
        # avg_success_rate = sum(stats['success_rate'] for stats in match_stats.values()) / len(match_stats)
        # if avg_success_rate >= 90:
        #     logger.info(f"✅ Excellent lookup performance: {avg_success_rate:.1f}% average match rate")
        # elif avg_success_rate >= 70:
        #     logger.info(f"✅ Good lookup performance: {avg_success_rate:.1f}% average match rate")
        # else:
        #     logger.warning(f"⚠️  Poor lookup performance: {avg_success_rate:.1f}% average match rate")
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Clean stage-to-stage lookup operations with smart key normalization and detailed logging',
            'data_sources': ['stages'],
            'join_types': ['left', 'right', 'inner', 'outer'],
            'key_features': [
                'type_and_format_normalization',
                'whitespace_handling',
                'numeric_format_cleanup',
                'prefix_suffix_naming',
                'per_column_default_values',
                'duplicate_handling',
                'detailed_match_statistics',
                'clean_error_messages'
            ],
            'normalization_features': [
                'handles_numeric_to_string_conversion',
                'removes_trailing_zeros',
                'strips_whitespace',
                'handles_null_values'
            ],
            'logging_features': [
                'per_column_match_statistics',
                'success_rate_analysis', 
                'low_match_rate_warnings',
                'default_value_tracking'
            ],
            'configuration_options': {
                'lookup_stage': 'Stage name containing lookup data',
                'match_col_in_lookup_data': 'Key column in lookup data',
                'match_col_in_main_data': 'Key column in main data',
                'lookup_columns': 'List of columns to retrieve from lookup data',
                'join_type': 'Type of join (default: left)',
                'prefix': 'Prefix for lookup column names',
                'suffix': 'Suffix for lookup column names',
                'default_values': 'Dict of per-column default values for missing lookups',
                'normalize_keys': 'Enable smart key normalization (default: true)',
                'handle_duplicates': 'How to handle duplicate lookup keys (first/last/error)'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the lookup_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('lookup_data')


# End of file #
