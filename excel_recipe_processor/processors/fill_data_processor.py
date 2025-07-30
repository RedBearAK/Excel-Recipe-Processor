"""
Fill data step processor for Excel automation recipes.

Handles filling missing/null values with various strategies similar to Excel's fill capabilities.
"""

import pandas as pd
import logging

from typing import Any, Optional, Union

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class FillDataProcessor(BaseStepProcessor):
    """
    Processor for filling missing/null values in DataFrame columns.
    
    Supports various fill strategies including constant values, forward/backward fill,
    statistical fills, and conditional filling based on other columns.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'columns': ['test_column'],
            'fill_method': 'constant',
            'fill_value': 'default_value'
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the fill operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            DataFrame with missing values filled
            
        Raises:
            StepProcessorError: If fill operation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Fill data step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['columns', 'fill_method'])
        
        columns = self.get_config_value('columns')
        fill_method = self.get_config_value('fill_method')
        fill_value = self.get_config_value('fill_value', None)
        conditions = self.get_config_value('conditions', [])
        limit = self.get_config_value('limit', None)
        inplace = self.get_config_value('inplace', False)
        
        # Validate configuration
        self._validate_fill_config(data, columns, fill_method, fill_value)
        
        # Work on a copy unless inplace is specified
        result_data = data if inplace else data.copy()
        
        try:
            # Convert single column to list
            if isinstance(columns, str):
                columns = [columns]
            
            # Apply fill operations
            if conditions:
                result_data = self._apply_conditional_fill(
                    result_data, columns, fill_method, fill_value, conditions, limit
                )
            else:
                result_data = self._apply_standard_fill(
                    result_data, columns, fill_method, fill_value, limit
                )
            
            # Count filled values
            filled_count = self._count_filled_values(data, result_data, columns)
            
            result_info = f"filled {filled_count} missing values across {len(columns)} column(s)"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error filling data in step '{self.step_name}': {e}")
    
    def _validate_fill_config(self, df: pd.DataFrame, columns, fill_method: str, fill_value) -> None:
        """
        Validate fill configuration parameters.
        
        Args:
            df: Input DataFrame
            columns: Column(s) to fill
            fill_method: Fill method to use
            fill_value: Value to fill with (if applicable)
        """
        # Validate columns
        if isinstance(columns, str):
            columns = [columns]
        
        if not isinstance(columns, list):
            raise StepProcessorError("'columns' must be a string or list of strings")
        
        if len(columns) == 0:
            raise StepProcessorError("'columns' list cannot be empty")
        
        for col in columns:
            if not isinstance(col, str):
                raise StepProcessorError(f"Column name must be a string, got: {type(col)}")
            if col not in df.columns:
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Column '{col}' not found. Available columns: {available_columns}"
                )
        
        # Validate fill method
        valid_methods = self.get_supported_fill_methods()
        if fill_method not in valid_methods:
            raise StepProcessorError(
                f"Unknown fill method: '{fill_method}'. "
                f"Supported methods: {', '.join(valid_methods)}"
            )
        
        # Validate fill value for methods that require it
        value_required_methods = ['constant', 'replace']
        if fill_method in value_required_methods and fill_value is None:
            raise StepProcessorError(f"Fill method '{fill_method}' requires 'fill_value' parameter")
    
    def _apply_standard_fill(self, df: pd.DataFrame, columns: list, 
                           fill_method: str, fill_value, limit: Optional[int]) -> pd.DataFrame:
        """
        Apply standard fill operations to specified columns.
        
        Args:
            df: DataFrame to modify
            columns: Columns to fill
            fill_method: Method to use for filling
            fill_value: Value to fill with
            limit: Maximum number of consecutive fills
            
        Returns:
            DataFrame with filled values
        """
        for col in columns:
            if fill_method == 'constant':
                df[col] = df[col].fillna(fill_value)
                
            elif fill_method == 'forward_fill' or fill_method == 'ffill':
                df[col] = df[col].ffill(limit=limit)
                
            elif fill_method == 'backward_fill' or fill_method == 'bfill':
                df[col] = df[col].bfill(limit=limit)
                
            elif fill_method == 'interpolate':
                # Only for numeric columns
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].interpolate(limit=limit)
                else:
                    logger.warning(f"Cannot interpolate non-numeric column '{col}', using forward fill")
                    df[col] = df[col].ffill(limit=limit)
                    
            elif fill_method == 'mean':
                if pd.api.types.is_numeric_dtype(df[col]):
                    mean_value = df[col].mean()
                    df[col] = df[col].fillna(mean_value)
                else:
                    raise StepProcessorError(f"Cannot calculate mean for non-numeric column '{col}'")
                    
            elif fill_method == 'median':
                if pd.api.types.is_numeric_dtype(df[col]):
                    median_value = df[col].median()
                    df[col] = df[col].fillna(median_value)
                else:
                    raise StepProcessorError(f"Cannot calculate median for non-numeric column '{col}'")
                    
            elif fill_method == 'mode':
                mode_values = df[col].mode()
                if len(mode_values) > 0:
                    df[col] = df[col].fillna(mode_values.iloc[0])
                else:
                    logger.warning(f"No mode found for column '{col}', leaving nulls unchanged")
                    
            elif fill_method == 'replace':
                # Replace specific values (not just nulls)
                old_value = self.get_config_value('old_value', None)
                if old_value is None:
                    raise StepProcessorError("Fill method 'replace' requires 'old_value' parameter")
                df[col] = df[col].replace(old_value, fill_value)
                
            elif fill_method == 'zero':
                df[col] = df[col].fillna(0)
                
            elif fill_method == 'empty_string':
                df[col] = df[col].fillna('')
                
            else:
                raise StepProcessorError(f"Fill method '{fill_method}' not implemented")
        
        logger.debug(f"Applied {fill_method} fill to columns: {columns}")
        return df
    
    def _apply_conditional_fill(self, df: pd.DataFrame, columns: list,
                              fill_method: str, fill_value, conditions: list,
                              limit: Optional[int]) -> pd.DataFrame:
        """
        Apply conditional fill operations based on other column values.
        
        Args:
            df: DataFrame to modify
            columns: Columns to fill
            fill_method: Method to use for filling
            fill_value: Value to fill with
            conditions: List of conditions for conditional filling
            limit: Maximum number of fills
            
        Returns:
            DataFrame with conditionally filled values
        """
        for condition in conditions:
            if not isinstance(condition, dict):
                raise StepProcessorError("Each condition must be a dictionary")
            
            # Required fields for conditions
            required_fields = ['condition_column', 'condition_type', 'condition_value']
            for field in required_fields:
                if field not in condition:
                    raise StepProcessorError(f"Condition missing required field: '{field}'")
            
            condition_column = condition['condition_column']
            condition_type = condition['condition_type']
            condition_value = condition['condition_value']
            condition_fill_value = condition.get('fill_value', fill_value)
            
            # Validate condition column exists
            if condition_column not in df.columns:
                raise StepProcessorError(f"Condition column '{condition_column}' not found")
            
            # Create mask based on condition
            mask = self._create_condition_mask(df, condition_column, condition_type, condition_value)
            
            # Apply fill only to rows matching condition
            for col in columns:
                if condition_fill_value is not None:
                    # Fill nulls where condition is true
                    null_mask = df[col].isnull()
                    combined_mask = mask & null_mask
                    df.loc[combined_mask, col] = condition_fill_value
                else:
                    # Apply fill method where condition is true
                    condition_df = df[mask].copy()
                    if len(condition_df) > 0:
                        condition_df = self._apply_standard_fill(
                            condition_df, [col], fill_method, fill_value, limit
                        )
                        df.loc[mask, col] = condition_df[col]
        
        logger.debug(f"Applied conditional fill with {len(conditions)} conditions")
        return df
    
    def _create_condition_mask(self, df: pd.DataFrame, column: str, 
                             condition_type: str, condition_value) -> pd.Series:
        """
        Create a boolean mask based on condition parameters.
        
        Args:
            df: DataFrame to create mask for
            column: Column to apply condition to
            condition_type: Type of condition
            condition_value: Value to compare against
            
        Returns:
            Boolean Series mask
        """
        if condition_type == 'equals':
            return df[column] == condition_value
        elif condition_type == 'not_equals':
            return df[column] != condition_value
        elif condition_type == 'greater_than':
            return df[column] > condition_value
        elif condition_type == 'less_than':
            return df[column] < condition_value
        elif condition_type == 'contains':
            return df[column].astype(str).str.contains(str(condition_value), na=False)
        elif condition_type == 'not_contains':
            return ~df[column].astype(str).str.contains(str(condition_value), na=False)
        elif condition_type == 'is_null':
            return df[column].isnull()
        elif condition_type == 'not_null':
            return df[column].notnull()
        elif condition_type == 'in_list':
            return df[column].isin(condition_value)
        elif condition_type == 'not_in_list':
            return ~df[column].isin(condition_value)
        else:
            valid_conditions = [
                'equals', 'not_equals', 'greater_than', 'less_than',
                'contains', 'not_contains', 'is_null', 'not_null',
                'in_list', 'not_in_list'
            ]
            raise StepProcessorError(
                f"Unknown condition type: '{condition_type}'. "
                f"Valid types: {', '.join(valid_conditions)}"
            )
    
    def _count_filled_values(self, original_df: pd.DataFrame, 
                           filled_df: pd.DataFrame, columns: list) -> int:
        """
        Count the number of values that were filled.
        
        Args:
            original_df: Original DataFrame
            filled_df: DataFrame after filling
            columns: Columns that were processed
            
        Returns:
            Number of values filled
        """
        total_filled = 0
        for col in columns:
            if col in original_df.columns and col in filled_df.columns:
                original_nulls = original_df[col].isnull().sum()
                remaining_nulls = filled_df[col].isnull().sum()
                filled = original_nulls - remaining_nulls
                total_filled += filled
        
        return total_filled
    
    def fill_blanks_with_value(self, df: pd.DataFrame, columns: Union[str, list], 
                              fill_value: Any) -> pd.DataFrame:
        """
        Simple helper method to fill blank values with a constant.
        
        Args:
            df: DataFrame to fill
            columns: Column(s) to fill
            fill_value: Value to use for filling
            
        Returns:
            DataFrame with blanks filled
        """
        result = df.copy()
        if isinstance(columns, str):
            columns = [columns]
        
        for col in columns:
            if col in result.columns:
                result[col] = result[col].fillna(fill_value)
        
        return result
    
    def forward_fill_series(self, df: pd.DataFrame, columns: Union[str, list], 
                           limit: Optional[int] = None) -> pd.DataFrame:
        """
        Forward fill (carry forward) missing values in specified columns.
        
        Args:
            df: DataFrame to fill
            columns: Column(s) to fill
            limit: Maximum number of consecutive fills
            
        Returns:
            DataFrame with forward filled values
        """
        result = df.copy()
        if isinstance(columns, str):
            columns = [columns]
        
        for col in columns:
            if col in result.columns:
                result[col] = result[col].ffill(limit=limit)
        
        return result
    
    def fill_with_statistical_value(self, df: pd.DataFrame, columns: Union[str, list],
                                   statistic: str = 'mean') -> pd.DataFrame:
        """
        Fill missing values with statistical measures.
        
        Args:
            df: DataFrame to fill
            columns: Column(s) to fill
            statistic: Statistical measure to use ('mean', 'median', 'mode')
            
        Returns:
            DataFrame with statistically filled values
        """
        result = df.copy()
        if isinstance(columns, str):
            columns = [columns]
        
        for col in columns:
            if col not in result.columns:
                continue
                
            if statistic == 'mean' and pd.api.types.is_numeric_dtype(result[col]):
                fill_value = result[col].mean()
                result[col] = result[col].fillna(fill_value)
            elif statistic == 'median' and pd.api.types.is_numeric_dtype(result[col]):
                fill_value = result[col].median()
                result[col] = result[col].fillna(fill_value)
            elif statistic == 'mode':
                mode_values = result[col].mode()
                if len(mode_values) > 0:
                    result[col] = result[col].fillna(mode_values.iloc[0])
        
        return result
    
    def analyze_missing_data(self, df: pd.DataFrame) -> dict:
        """
        Analyze missing data patterns in the DataFrame.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with missing data analysis
        """
        analysis = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns_with_missing': [],
            'missing_data_summary': {},
            'completely_empty_columns': [],
            'recommendations': []
        }
        
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                missing_pct = (missing_count / len(df)) * 100
                analysis['columns_with_missing'].append(col)
                analysis['missing_data_summary'][col] = {
                    'missing_count': missing_count,
                    'missing_percentage': round(missing_pct, 2),
                    'data_type': str(df[col].dtype)
                }
                
                # Generate recommendations
                if missing_pct > 90:
                    analysis['completely_empty_columns'].append(col)
                elif missing_pct > 50:
                    analysis['recommendations'].append(
                        f"Column '{col}' has {missing_pct:.1f}% missing - consider dropping or major fill strategy"
                    )
                elif missing_pct > 10:
                    analysis['recommendations'].append(
                        f"Column '{col}' has {missing_pct:.1f}% missing - good candidate for forward/backward fill"
                    )
                else:
                    analysis['recommendations'].append(
                        f"Column '{col}' has {missing_pct:.1f}% missing - suitable for constant/statistical fill"
                    )
        
        analysis['total_missing_values'] = sum(
            info['missing_count'] for info in analysis['missing_data_summary'].values()
        )
        
        return analysis
    
    def get_supported_fill_methods(self) -> list:
        """
        Get list of supported fill methods.
        
        Returns:
            List of supported fill method strings
        """
        return [
            'constant', 'forward_fill', 'ffill', 'backward_fill', 'bfill',
            'interpolate', 'mean', 'median', 'mode', 'replace',
            'zero', 'empty_string'
        ]
    
    def get_supported_condition_types(self) -> list:
        """
        Get list of supported condition types for conditional filling.
        
        Returns:
            List of supported condition type strings
        """
        return [
            'equals', 'not_equals', 'greater_than', 'less_than',
            'contains', 'not_contains', 'is_null', 'not_null',
            'in_list', 'not_in_list'
        ]
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Fill missing/null values using various strategies similar to Excel fill operations',
            'fill_methods': self.get_supported_fill_methods(),
            'condition_types': self.get_supported_condition_types(),
            'supported_features': [
                'constant_fill', 'forward_backward_fill', 'statistical_fill',
                'conditional_fill', 'interpolation', 'replacement_fill',
                'missing_data_analysis', 'limit_consecutive_fills'
            ],
            'helper_methods': [
                'fill_blanks_with_value', 'forward_fill_series', 
                'fill_with_statistical_value', 'analyze_missing_data'
            ],
            'excel_equivalents': {
                'fill_down': 'forward_fill method',
                'fill_up': 'backward_fill method',  
                'fill_series': 'interpolate method',
                'find_replace': 'replace method'
            },
            'examples': {
                'basic_fill': "Fill null values with 'Unknown'",
                'forward_fill': "Carry forward last known value",
                'conditional': "Fill based on other column values"
            }
        }
