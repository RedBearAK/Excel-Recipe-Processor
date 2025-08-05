"""
Diff data processor for Excel automation recipes.

excel_recipe_processor/processors/diff_data_processor.py

Compares two datasets and identifies new, changed, unchanged, and deleted rows
between a baseline (reference) dataset and current (source) dataset.
"""

import json
import pandas as pd
import logging

from typing import Any, Union

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class DiffDataProcessor(BaseStepProcessor):
    """
    Processor for comparing two datasets and identifying changes.
    
    Compares current data against baseline data using key columns to match rows,
    then identifies which rows are new, changed, unchanged, or deleted.
    Adds metadata columns with change details and optionally creates separate
    stages for each change type.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        """
        Get the minimal configuration required to instantiate this processor.
        
        Returns:
            Dictionary with minimal configuration fields
        """
        return {
            'reference_stage': 'test_baseline',
            'key_columns': 'test_key',
            'source_stage': 'test_current',
            'save_to_stage': 'test_diff_results'
        }
    
    def __init__(self, step_config: dict):
        """Initialize the diff data processor."""
        super().__init__(step_config)
        
        # Validate required fields
        required_fields = ['reference_stage', 'key_columns']
        self.validate_required_fields(required_fields)
        
        # Get configuration values
        self.reference_stage = self.get_config_value('reference_stage')
        self.key_columns = self._normalize_key_columns(self.get_config_value('key_columns'))
        self.exclude_columns = self.get_config_value('exclude_columns', [])
        self.create_filtered_stages = self.get_config_value('create_filtered_stages', False)
        self.filtered_stage_prefix = self.get_config_value('filtered_stage_prefix', 'stg_diff')
        self.include_json_details = self.get_config_value('include_json_details', False)
        self.handle_deleted_rows = self.get_config_value('handle_deleted_rows', 'include')
        
        # Validate configuration
        self._validate_config()
    
    def _normalize_key_columns(self, key_columns: Union[str, list]) -> list:
        """
        Normalize key_columns to always be a list.
        
        Args:
            key_columns: Single column name or list of column names
            
        Returns:
            List of column names
        """
        if isinstance(key_columns, str):
            return [key_columns]
        elif isinstance(key_columns, list):
            return key_columns
        else:
            raise StepProcessorError(f"key_columns must be string or list, got {type(key_columns)}")
    
    def _validate_config(self):
        """Validate processor configuration."""
        # Validate handle_deleted_rows option
        valid_deleted_options = ['include', 'exclude', 'separate_stage']
        if self.handle_deleted_rows not in valid_deleted_options:
            raise StepProcessorError(
                f"handle_deleted_rows must be one of {valid_deleted_options}, "
                f"got '{self.handle_deleted_rows}'"
            )
        
        # Validate exclude_columns is a list
        if not isinstance(self.exclude_columns, list):
            raise StepProcessorError("exclude_columns must be a list")
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the diff analysis on current data against reference data.
        
        Args:
            data: Current/source data (pandas DataFrame)
            
        Returns:
            DataFrame with diff analysis and metadata columns
        """
        # Guard clause: data must be a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError("Input data must be a pandas DataFrame")
        
        self.log_step_start()
        
        # Load reference data
        reference_data = self._load_reference_data()
        
        # Perform diff analysis
        diff_result = self._perform_diff_analysis(data, reference_data)
        
        # Create filtered stages if requested (but don't save main result manually)
        if self.create_filtered_stages:
            self._create_filtered_stages(diff_result)
        
        self.log_step_complete(f"analyzed {len(diff_result)} rows, identified changes")
        
        # Return result - let base class save it to save_to_stage
        return diff_result
    
    def _load_reference_data(self) -> pd.DataFrame:
        """Load reference/baseline data from the specified stage."""
        from excel_recipe_processor.core.stage_manager import StageManager
        
        try:
            reference_data = StageManager.load_stage(self.reference_stage)
            logger.debug(f"Loaded {len(reference_data)} rows from reference stage '{self.reference_stage}'")
            return reference_data
        except Exception as e:
            raise StepProcessorError(f"Failed to load reference stage '{self.reference_stage}': {e}")
    
    def _perform_diff_analysis(self, current_data: pd.DataFrame, reference_data: pd.DataFrame) -> pd.DataFrame:
        """
        Perform the main diff analysis between current and reference data.
        
        Args:
            current_data: Current/source dataset
            reference_data: Reference/baseline dataset
            
        Returns:
            DataFrame with all rows and diff metadata
        """
        # Validate key columns exist in both datasets
        self._validate_key_columns(current_data, reference_data)
        
        # Create comparison datasets with standardized key handling
        current_indexed = self._prepare_data_for_comparison(current_data, 'current')
        reference_indexed = self._prepare_data_for_comparison(reference_data, 'reference')
        
        # Identify different types of rows
        all_keys = self._get_all_keys(current_indexed, reference_indexed)
        
        results = []
        
        for key in all_keys:
            current_row = current_indexed.get(key)
            reference_row = reference_indexed.get(key)
            
            if current_row is not None and reference_row is not None:
                # Row exists in both - check if changed
                result_row = self._analyze_row_changes(current_row, reference_row, key)
            elif current_row is not None:
                # New row (exists only in current)
                result_row = self._create_new_row_result(current_row)
            else:
                # Deleted row (exists only in reference)
                if self.handle_deleted_rows != 'exclude':
                    result_row = self._create_deleted_row_result(reference_row)
                else:
                    continue  # Skip deleted rows
            
            results.append(result_row)
        
        # Convert results to DataFrame
        if results:
            diff_result = pd.concat(results, ignore_index=True)
        else:
            # Create empty result with correct structure
            diff_result = self._create_empty_result_structure(current_data)
        
        return diff_result
    
    def _validate_key_columns(self, current_data: pd.DataFrame, reference_data: pd.DataFrame):
        """Validate that key columns exist in both datasets."""
        for key_col in self.key_columns:
            if key_col not in current_data.columns:
                raise StepProcessorError(f"Key column '{key_col}' not found in current data")
            if key_col not in reference_data.columns:
                raise StepProcessorError(f"Key column '{key_col}' not found in reference data")
    
    def _prepare_data_for_comparison(self, data: pd.DataFrame, data_type: str) -> dict:
        """
        Prepare data for comparison by creating a key-indexed dictionary.
        
        Args:
            data: DataFrame to prepare
            data_type: 'current' or 'reference' for logging
            
        Returns:
            Dictionary mapping composite keys to DataFrames (single row each)
        """
        indexed_data = {}
        
        for idx, row in data.iterrows():
            # Create composite key
            if len(self.key_columns) == 1:
                key = row[self.key_columns[0]]
            else:
                key = tuple(row[col] for col in self.key_columns)
            
            # Check for duplicate keys
            if key in indexed_data:
                logger.warning(f"Duplicate key {key} found in {data_type} data")
            
            # Store row as single-row DataFrame to preserve column structure
            indexed_data[key] = pd.DataFrame([row])
        
        logger.debug(f"Prepared {len(indexed_data)} unique keys from {data_type} data")
        return indexed_data
    
    def _get_all_keys(self, current_indexed: dict, reference_indexed: dict) -> set:
        """Get the union of all keys from both datasets."""
        return set(current_indexed.keys()) | set(reference_indexed.keys())
    
    def _analyze_row_changes(self, current_row: pd.DataFrame, reference_row: pd.DataFrame, key) -> pd.DataFrame:
        """
        Analyze changes between current and reference row.
        
        Args:
            current_row: Single-row DataFrame from current data
            reference_row: Single-row DataFrame from reference data
            key: The composite key for this row
            
        Returns:
            Single-row DataFrame with change analysis
        """
        # Get the actual Series for comparison
        current_series = current_row.iloc[0]
        reference_series = reference_row.iloc[0]
        
        # Find columns to compare (exclude key columns and excluded columns)
        comparison_columns = self._get_comparison_columns(current_row.columns)
        
        # Compare values and build change information
        changed_fields = []
        change_details = []
        change_details_json = {}
        
        for col in comparison_columns:
            current_val = current_series.get(col)
            reference_val = reference_series.get(col)
            
            # Handle potential NaN/None comparisons
            if self._values_different(current_val, reference_val):
                changed_fields.append(col)
                
                # Format change detail
                detail = f"{col}: '{reference_val}'→'{current_val}'"
                change_details.append(detail)
                
                # JSON format
                change_details_json[col] = {
                    'old': str(reference_val) if reference_val is not None else None,
                    'new': str(current_val) if current_val is not None else None
                }
        
        # Determine row status
        if changed_fields:
            row_status = 'CHANGED'
        else:
            row_status = 'UNCHANGED'
        
        # Create result row based on current data
        result_row = current_row.copy()
        
        # Add metadata columns
        result_row.loc[:, 'Row_Status'] = row_status
        result_row.loc[:, 'Changed_Fields'] = ', '.join(changed_fields) if changed_fields else ''
        result_row.loc[:, 'Change_Count'] = len(changed_fields)
        result_row.loc[:, 'Change_Details'] = ' | '.join(change_details) if change_details else ''
        
        if self.include_json_details:
            result_row.loc[:, 'Change_Details_JSON'] = json.dumps(change_details_json) if change_details_json else ''
        
        return result_row
    
    def _values_different(self, val1, val2) -> bool:
        """
        Check if two values are different, handling NaN/None cases.
        
        Args:
            val1: First value
            val2: Second value
            
        Returns:
            True if values are different
        """
        # Handle NaN cases
        if pd.isna(val1) and pd.isna(val2):
            return False
        if pd.isna(val1) or pd.isna(val2):
            return True
        
        # Regular comparison
        return val1 != val2
    
    def _create_new_row_result(self, current_row: pd.DataFrame) -> pd.DataFrame:
        """Create result for a new row (exists only in current data)."""
        result_row = current_row.copy()
        
        result_row.loc[:, 'Row_Status'] = 'NEW'
        result_row.loc[:, 'Changed_Fields'] = ''
        result_row.loc[:, 'Change_Count'] = 0
        result_row.loc[:, 'Change_Details'] = ''
        
        if self.include_json_details:
            result_row.loc[:, 'Change_Details_JSON'] = ''
        
        return result_row
    
    def _create_deleted_row_result(self, reference_row: pd.DataFrame) -> pd.DataFrame:
        """Create result for a deleted row (exists only in reference data)."""
        result_row = reference_row.copy()
        
        result_row.loc[:, 'Row_Status'] = 'DELETED'
        result_row.loc[:, 'Changed_Fields'] = ''
        result_row.loc[:, 'Change_Count'] = 0
        result_row.loc[:, 'Change_Details'] = ''
        
        if self.include_json_details:
            result_row.loc[:, 'Change_Details_JSON'] = ''
        
        return result_row
    
    def _get_comparison_columns(self, all_columns) -> list:
        """
        Get list of columns to use for change comparison.
        
        Args:
            all_columns: All available columns
            
        Returns:
            List of columns to compare (excluding keys and excluded columns)
        """
        excluded = set(self.key_columns + self.exclude_columns)
        return [col for col in all_columns if col not in excluded]
    
    def _create_empty_result_structure(self, sample_data: pd.DataFrame) -> pd.DataFrame:
        """Create an empty DataFrame with the correct result structure."""
        # Start with sample structure
        result_columns = list(sample_data.columns)
        
        # Add metadata columns
        result_columns.extend(['Row_Status', 'Changed_Fields', 'Change_Count', 'Change_Details'])
        
        if self.include_json_details:
            result_columns.append('Change_Details_JSON')
        
        return pd.DataFrame(columns=result_columns)
    
    def _create_filtered_stages(self, diff_result: pd.DataFrame):
        """Create separate stages for each change type."""
        from excel_recipe_processor.core.stage_manager import StageManager
        
        # Define stage filters
        stage_filters = {
            'new_rows': ('NEW', "Rows that exist in current data but not in baseline"),
            'changed_rows': ('CHANGED', "Rows that exist in both datasets but have different values"),
            'unchanged_rows': ('UNCHANGED', "Rows that exist in both datasets with identical values"),
        }
        
        # Add deleted rows stage if appropriate
        if self.handle_deleted_rows in ['include', 'separate_stage']:
            stage_filters['deleted_rows'] = ('DELETED', "Rows that exist in baseline but not in current data")
        
        # Create each filtered stage
        for stage_type, (status_filter, description) in stage_filters.items():
            # Filter data
            filtered_data = diff_result[diff_result['Row_Status'] == status_filter].copy()
            
            # Generate stage name
            if self.filtered_stage_prefix.endswith('_'):
                stage_name = f"{self.filtered_stage_prefix}{stage_type}_subset"
            else:
                stage_name = f"{self.filtered_stage_prefix}_{stage_type}_subset"
            
            # Save stage (even if empty)
            StageManager.save_stage(
                stage_name=stage_name,
                data=filtered_data,
                description=description,
                step_name=self.step_name,
                confirm_replacement=self.confirm_stage_replacement
            )
            
            logger.debug(f"Created filtered stage '{stage_name}' with {len(filtered_data)} rows")
    
    def get_capabilities(self) -> dict:
        """
        Get processor capabilities for system integration.
        
        Returns:
            Dictionary describing processor capabilities
        """
        return {
            'description': 'Compare two datasets and identify new, changed, unchanged, and deleted rows',
            'category': 'Data Analysis',
            'stage_to_stage': True,
            'comparison_features': [
                'Single or multiple key columns',
                'Row-level change detection',
                'Field-level change tracking',
                'Metadata column generation',
                'Configurable column exclusions'
            ],
            'output_options': [
                'Combined results with metadata',
                'Separate filtered stages by change type',
                'Human-readable change details',
                'Machine-readable JSON format'
            ],
            'key_columns_support': {
                'single_column': 'customer_id',
                'multiple_columns': ['region', 'product_code'],
                'composite_key': 'pre_built_composite_key'
            },
            'metadata_columns_added': [
                'Row_Status (NEW/CHANGED/UNCHANGED/DELETED)',
                'Changed_Fields (comma-separated list)',
                'Change_Count (integer)',
                'Change_Details (human-readable)',
                'Change_Details_JSON (optional machine-readable)'
            ],
            'filtered_stages': {
                'new_rows': '{prefix}_new_rows_subset',
                'changed_rows': '{prefix}_changed_rows_subset', 
                'unchanged_rows': '{prefix}_unchanged_rows_subset',
                'deleted_rows': '{prefix}_deleted_rows_subset'
            },
            'deleted_row_handling': ['include', 'exclude', 'separate_stage'],
            'use_cases': [
                'Database change tracking between time periods',
                'Inventory variance analysis',
                'Customer data updates monitoring',
                'Configuration drift detection',
                'Data quality auditing'
            ],
            'configuration_options': {
                'required': ['reference_stage', 'key_columns', 'source_stage', 'save_to_stage'],
                'optional': ['exclude_columns', 'create_filtered_stages', 'include_json_details', 'handle_deleted_rows']
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the diff_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('diff_data')


# End of file #
