"""
Filter data step processor for Excel automation recipes.

Handles filtering DataFrame rows based on various conditions including 
stage-based comparisons using StageManager integration.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class FilterDataProcessor(BaseStepProcessor):
    """
    Processor for filtering DataFrame rows based on specified conditions.
    
    Supports various filter operations including:
    - Basic comparisons: equals, contains, greater_than, etc.
    - List operations: in_list, not_in_list
    - Stage-based filtering: in_stage, not_in_stage, stage_comparison
    - Can apply multiple filters in sequence with AND logic
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'filters': [
                {
                    'column': 'test_column',
                    'condition': 'equals',
                    'value': 'test_value'
                }
            ]
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the filter operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to filter
            
        Returns:
            Filtered pandas DataFrame
            
        Raises:
            StepProcessorError: If filtering fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Filter step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['filters'])
        
        filters = self.get_config_value('filters')
        
        # Guard clause: filters must be a list
        if not isinstance(filters, list):
            raise StepProcessorError(f"Step '{self.step_name}' 'filters' must be a list")
        
        if len(filters) == 0:
            logger.warning(f"Step '{self.step_name}' has no filters defined, returning data unchanged")
            self.log_step_complete("no filters applied")
            return data
        
        # Start with the original data
        filtered_data = data.copy()
        initial_row_count = len(filtered_data)
        
        # Apply each filter in sequence
        for i, filter_rule in enumerate(filters):
            try:
                filtered_data = self._apply_filter(filtered_data, filter_rule, i)
            except Exception as e:
                raise StepProcessorError(f"Error applying filter {i+1} in step '{self.step_name}': {e}")
        
        final_row_count = len(filtered_data)
        removed_count = initial_row_count - final_row_count
        
        result_info = f"filtered {initial_row_count} → {final_row_count} rows (removed {removed_count})"
        self.log_step_complete(result_info)
        
        return filtered_data
    
    def _apply_filter(self, df: pd.DataFrame, filter_rule: dict, filter_index: int) -> pd.DataFrame:
        """
        Apply a single filter rule to the DataFrame.
        
        Args:
            df: DataFrame to filter
            filter_rule: Dictionary containing filter configuration
            filter_index: Index of the filter for error reporting
            
        Returns:
            Filtered DataFrame
        """
        # Guard clause: filter_rule must be a dict
        if not isinstance(filter_rule, dict):
            raise StepProcessorError(f"Filter {filter_index + 1} must be a dictionary")
        
        # Validate required filter fields
        required_fields = ['column', 'condition']
        for field in required_fields:
            if field not in filter_rule:
                raise StepProcessorError(f"Filter {filter_index + 1} missing required field: {field}")
        
        column = filter_rule['column']
        condition = filter_rule['condition']
        
        # Guard clauses for filter parameters
        if not isinstance(column, str) or not column.strip():
            raise StepProcessorError(f"Filter {filter_index + 1} 'column' must be a non-empty string")
        
        if not isinstance(condition, str) or not condition.strip():
            raise StepProcessorError(f"Filter {filter_index + 1} 'condition' must be a non-empty string")
        
        # Check if column exists
        if column not in df.columns:
            available_columns = list(df.columns)
            raise StepProcessorError(
                f"Filter {filter_index + 1} column '{column}' not found. "
                f"Available columns: {available_columns}"
            )
        
        # Get the value for comparison (not needed for all conditions)
        value = filter_rule.get('value')
        
        # Apply the appropriate filter condition
        try:
            # Basic comparison conditions
            if condition == 'equals':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'equals' condition requires a 'value'")
                mask = df[column] == value
                
            elif condition == 'not_equals':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'not_equals' condition requires a 'value'")
                mask = df[column] != value
                
            elif condition == 'contains':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'contains' condition requires a 'value'")
                if not isinstance(value, str):
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'contains' condition requires a string value")
                mask = df[column].astype(str).str.contains(value, na=False)
                
            elif condition == 'not_contains':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'not_contains' condition requires a 'value'")
                if not isinstance(value, str):
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'not_contains' condition requires a string value")
                mask = ~df[column].astype(str).str.contains(value, na=False)
                
            # Numeric comparison conditions
            elif condition == 'greater_than':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'greater_than' condition requires a 'value'")
                mask = pd.to_numeric(df[column], errors='coerce') > value
                
            elif condition == 'less_than':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'less_than' condition requires a 'value'")
                mask = pd.to_numeric(df[column], errors='coerce') < value
                
            elif condition == 'greater_equal':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'greater_equal' condition requires a 'value'")
                mask = pd.to_numeric(df[column], errors='coerce') >= value
                
            elif condition == 'less_equal':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'less_equal' condition requires a 'value'")
                mask = pd.to_numeric(df[column], errors='coerce') <= value
                
            # Empty/null conditions
            elif condition == 'not_empty':
                mask = df[column].notna() & (df[column].astype(str).str.strip() != '')
                
            elif condition == 'is_empty':
                mask = df[column].isna() | (df[column].astype(str).str.strip() == '')
                
            # List membership conditions
            elif condition == 'in_list':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'in_list' condition requires a 'value'")
                if not isinstance(value, list):
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'in_list' condition requires a list value")
                mask = df[column].isin(value)
                
            elif condition == 'not_in_list':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'not_in_list' condition requires a 'value'")
                if not isinstance(value, list):
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'not_in_list' condition requires a list value")
                mask = ~df[column].isin(value)
                
            # NEW: Stage-based filtering conditions
            elif condition == 'in_stage':
                mask = self._apply_stage_filter(df, filter_rule, filter_index, include=True)
                
            elif condition == 'not_in_stage':
                mask = self._apply_stage_filter(df, filter_rule, filter_index, include=False)
                
            elif condition == 'stage_comparison':
                mask = self._apply_stage_comparison_filter(df, filter_rule, filter_index)
                
            else:
                available_conditions = self.get_supported_conditions()
                raise StepProcessorError(
                    f"Filter {filter_index + 1} has unsupported condition: {condition}. "
                    f"Available conditions: {', '.join(available_conditions)}"
                )
            
            # Apply the mask to filter the DataFrame
            filtered_df = df[mask]
            
            # Log the filter result
            rows_before = len(df)
            rows_after = len(filtered_df)
            rows_removed = rows_before - rows_after
            
            logger.debug(
                f"Filter {filter_index + 1}: {column} {condition} {value} → "
                f"{rows_before} → {rows_after} rows (removed {rows_removed})"
            )
            
            return filtered_df
            
        except Exception as e:
            # Re-raise our own errors, wrap pandas errors
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error applying filter condition '{condition}' to column '{column}': {e}")
    
    def _apply_stage_filter(self, df: pd.DataFrame, filter_rule: dict, filter_index: int, include: bool) -> pd.Series:
        """
        Apply stage-based inclusion/exclusion filter.
        
        Args:
            df: DataFrame to filter
            filter_rule: Filter configuration
            filter_index: Filter index for error reporting  
            include: True for 'in_stage', False for 'not_in_stage'
            
        Returns:
            Boolean mask Series
        """
        # Validate stage filter requirements
        stage_name = filter_rule.get('stage_name')
        stage_column = filter_rule.get('stage_column')
        
        if not stage_name:
            raise StepProcessorError(f"Filter {filter_index + 1} with stage condition requires 'stage_name'")
        
        if not stage_column:
            raise StepProcessorError(f"Filter {filter_index + 1} with stage condition requires 'stage_column'")
        
        # Check if stage exists
        if not StageManager.stage_exists(stage_name):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Filter {filter_index + 1} references non-existent stage '{stage_name}'. "
                f"Available stages: {available_stages}"
            )
        
        try:
            # Get stage data
            stage_data = StageManager.load_stage(stage_name)
            
            # Check if stage column exists
            if stage_column not in stage_data.columns:
                available_columns = list(stage_data.columns)
                raise StepProcessorError(
                    f"Filter {filter_index + 1} stage column '{stage_column}' not found in stage '{stage_name}'. "
                    f"Available columns: {available_columns}"
                )
            
            # Get unique values from stage column
            stage_values = set(stage_data[stage_column].dropna().unique())
            
            # Get the column to compare from current data
            comparison_column = filter_rule['column']
            
            # Create mask based on inclusion/exclusion
            if include:
                mask = df[comparison_column].isin(stage_values)
                operation = "in"
            else:
                mask = ~df[comparison_column].isin(stage_values) 
                operation = "not in"
            
            logger.debug(
                f"Stage filter: {comparison_column} {operation} stage '{stage_name}[{stage_column}]' "
                f"({len(stage_values)} unique values)"
            )
            
            return mask
            
        except StageError as e:
            raise StepProcessorError(f"Filter {filter_index + 1} stage access error: {e}")
    
    def _apply_stage_comparison_filter(self, df: pd.DataFrame, filter_rule: dict, filter_index: int) -> pd.Series:
        """
        Apply stage-based value comparison filter.
        
        Compares values in current data against corresponding values in a stage
        using a key column to match rows.
        
        Args:
            df: DataFrame to filter
            filter_rule: Filter configuration
            filter_index: Filter index for error reporting
            
        Returns:
            Boolean mask Series
        """
        # Validate stage comparison requirements
        required_fields = ['stage_name', 'key_column', 'stage_key_column', 'stage_value_column', 'comparison_operator']
        for field in required_fields:
            if field not in filter_rule:
                raise StepProcessorError(f"Filter {filter_index + 1} with 'stage_comparison' requires '{field}'")
        
        stage_name = filter_rule['stage_name']
        key_column = filter_rule['key_column']  # Column in current data for matching
        stage_key_column = filter_rule['stage_key_column']  # Column in stage for matching
        stage_value_column = filter_rule['stage_value_column']  # Column in stage for comparison
        comparison_operator = filter_rule['comparison_operator']  # equals, greater_than, etc.
        
        # Check if stage exists
        if not StageManager.stage_exists(stage_name):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Filter {filter_index + 1} references non-existent stage '{stage_name}'. "
                f"Available stages: {available_stages}"
            )
        
        try:
            # Get stage data
            stage_data = StageManager.load_stage(stage_name)
            
            # Validate columns exist
            if key_column not in df.columns:
                raise StepProcessorError(f"Filter {filter_index + 1} key column '{key_column}' not found in current data")
                
            if stage_key_column not in stage_data.columns:
                available_columns = list(stage_data.columns)
                raise StepProcessorError(
                    f"Filter {filter_index + 1} stage key column '{stage_key_column}' not found in stage '{stage_name}'. "
                    f"Available columns: {available_columns}"
                )
                
            if stage_value_column not in stage_data.columns:
                available_columns = list(stage_data.columns)
                raise StepProcessorError(
                    f"Filter {filter_index + 1} stage value column '{stage_value_column}' not found in stage '{stage_name}'. "
                    f"Available columns: {available_columns}"
                )
            
            # Create lookup dictionary from stage
            stage_lookup = dict(zip(stage_data[stage_key_column], stage_data[stage_value_column]))
            
            # Map current data to stage values
            current_column = filter_rule['column']
            stage_values = df[key_column].map(stage_lookup)
            
            # Apply comparison operator
            if comparison_operator == 'equals':
                mask = df[current_column] == stage_values
            elif comparison_operator == 'not_equals':
                mask = df[current_column] != stage_values
            elif comparison_operator == 'greater_than':
                mask = pd.to_numeric(df[current_column], errors='coerce') > pd.to_numeric(stage_values, errors='coerce')
            elif comparison_operator == 'less_than':
                mask = pd.to_numeric(df[current_column], errors='coerce') < pd.to_numeric(stage_values, errors='coerce')
            elif comparison_operator == 'greater_equal':
                mask = pd.to_numeric(df[current_column], errors='coerce') >= pd.to_numeric(stage_values, errors='coerce')
            elif comparison_operator == 'less_equal':
                mask = pd.to_numeric(df[current_column], errors='coerce') <= pd.to_numeric(stage_values, errors='coerce')
            else:
                raise StepProcessorError(
                    f"Filter {filter_index + 1} unsupported comparison operator: {comparison_operator}"
                )
            
            # Handle missing stage values (no match found)
            mask = mask.fillna(False)
            
            logger.debug(
                f"Stage comparison: {current_column} {comparison_operator} stage '{stage_name}[{stage_value_column}]' "
                f"(matched {mask.sum()} rows)"
            )
            
            return mask
            
        except StageError as e:
            raise StepProcessorError(f"Filter {filter_index + 1} stage access error: {e}")
    
    def get_supported_conditions(self) -> list:
        """
        Get list of supported filter conditions.
        
        Returns:
            List of supported condition strings
        """
        return [
            # Basic comparisons
            'equals', 'not_equals', 'contains', 'not_contains',
            'greater_than', 'less_than', 'greater_equal', 'less_equal',
            
            # Empty/null conditions
            'not_empty', 'is_empty', 
            
            # List membership
            'in_list', 'not_in_list',
            
            # Stage-based conditions (NEW)
            'in_stage', 'not_in_stage', 'stage_comparison'
        ]
    
    def get_stage_based_conditions(self) -> list:
        """
        Get list of stage-based filter conditions.
        
        Returns:
            List of stage-based condition strings
        """
        return ['in_stage', 'not_in_stage', 'stage_comparison']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Filter DataFrame rows based on specified conditions including stage-based comparisons',
            'supported_conditions': self.get_supported_conditions(),
            'stage_based_conditions': self.get_stage_based_conditions(),
            'filter_operations': [
                'exact_matching', 'text_contains', 'numeric_comparisons',
                'list_membership', 'null_checking', 'pattern_matching',
                'stage_inclusion', 'stage_exclusion', 'stage_value_comparison'
            ],
            'comparison_operators': ['equals', 'not_equals', 'greater_than', 'less_than', 'contains'],
            'stage_integration': [
                'filter_by_stage_membership', 'exclude_by_stage_values', 
                'compare_against_stage_data', 'cross_reference_filtering'
            ],
            'examples': {
                'basic_filter': "Product_Name contains 'CANNED'",
                'numeric_filter': "Price > 15.00",
                'list_filter': "Department in ['Electronics', 'Tools']",
                'stage_inclusion': "Customer_ID in stage 'Approved Customers'",
                'stage_exclusion': "Product_Code not in stage 'Discontinued Items'",
                'stage_comparison': "Current_Price > Historical_Price from stage lookup"
            },
            'configuration_options': {
                'filters': 'List of filter rules to apply in sequence',
                'column': 'Column name to filter on',
                'condition': 'Filter condition type',
                'value': 'Value for comparison (required for most conditions)',
                'stage_name': 'Name of stage for stage-based conditions',
                'stage_column': 'Column in stage for inclusion/exclusion filters',
                'key_column': 'Column for matching in stage comparison filters',
                'stage_key_column': 'Stage column for matching in comparisons',
                'stage_value_column': 'Stage column for value comparison',
                'comparison_operator': 'Operator for stage value comparisons'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the filter_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('filter_data')
