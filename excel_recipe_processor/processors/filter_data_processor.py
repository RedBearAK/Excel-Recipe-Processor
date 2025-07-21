"""
Filter data step processor for Excel automation recipes.

Handles filtering DataFrame rows based on various conditions.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError

logger = logging.getLogger(__name__)


class FilterDataProcessor(BaseStepProcessor):
    """
    Processor for filtering DataFrame rows based on specified conditions.
    
    Supports various filter operations like equals, contains, greater_than, etc.
    Can apply multiple filters in sequence.
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
                
            elif condition == 'greater_than':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'greater_than' condition requires a 'value'")
                mask = df[column] > value
                
            elif condition == 'less_than':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'less_than' condition requires a 'value'")
                mask = df[column] < value
                
            elif condition == 'greater_equal':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'greater_equal' condition requires a 'value'")
                mask = df[column] >= value
                
            elif condition == 'less_equal':
                if value is None:
                    raise StepProcessorError(f"Filter {filter_index + 1} with 'less_equal' condition requires a 'value'")
                mask = df[column] <= value
                
            elif condition == 'not_empty':
                # Filter out rows where column is NaN, None, or empty string
                mask = df[column].notna() & (df[column] != '') & (df[column] != ' ')
                
            elif condition == 'is_empty':
                # Keep rows where column is NaN, None, or empty string
                mask = df[column].isna() | (df[column] == '') | (df[column] == ' ')
                
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
                
            else:
                available_conditions = [
                    'equals', 'not_equals', 'contains', 'not_contains',
                    'greater_than', 'less_than', 'greater_equal', 'less_equal',
                    'not_empty', 'is_empty', 'in_list', 'not_in_list'
                ]
                raise StepProcessorError(
                    f"Filter {filter_index + 1} unknown condition: '{condition}'. "
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
    
    def get_supported_conditions(self) -> list:
        """
        Get list of supported filter conditions.
        
        Returns:
            List of supported condition strings
        """
        return [
            'equals', 'not_equals', 'contains', 'not_contains',
            'greater_than', 'less_than', 'greater_equal', 'less_equal',
            'not_empty', 'is_empty', 'in_list', 'not_in_list'
        ]
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Filter DataFrame rows based on specified conditions',
            'supported_conditions': self.get_supported_conditions(),
            'filter_operations': [
                'exact_matching', 'text_contains', 'numeric_comparisons',
                'list_membership', 'null_checking', 'pattern_matching'
            ],
            'comparison_operators': ['equals', 'not_equals', 'greater_than', 'less_than', 'contains'],
            'examples': {
                'text_filter': "Product_Name contains 'CANNED'",
                'numeric_filter': "Price > 15.00",
                'list_filter': "Department in ['Electronics', 'Tools']"
            }
        }
