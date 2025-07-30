"""
Sort data step processor for Excel automation recipes.

Handles sorting DataFrame rows by one or multiple columns with flexible options.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class SortDataProcessor(BaseStepProcessor):
    """
    Processor for sorting DataFrame rows by specified columns.
    
    Supports single and multi-column sorting with ascending/descending options,
    custom sort orders, and null value handling.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'columns': ['test_column']
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the sorting operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to sort
            
        Returns:
            Sorted pandas DataFrame
            
        Raises:
            StepProcessorError: If sorting fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Sort data step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['columns'])
        
        columns = self.get_config_value('columns')
        ascending = self.get_config_value('ascending', True)
        na_position = self.get_config_value('na_position', 'last')
        custom_orders = self.get_config_value('custom_orders', {})
        ignore_case = self.get_config_value('ignore_case', False)
        
        # Validate configuration
        self._validate_sort_config(data, columns, ascending, na_position, custom_orders)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Apply custom sorting if specified
            if custom_orders:
                result_data = self._apply_custom_sort(result_data, columns, custom_orders, ascending, na_position)
            else:
                # Apply standard sorting
                result_data = self._apply_standard_sort(result_data, columns, ascending, na_position, ignore_case)
            
            result_info = f"sorted by {len(columns) if isinstance(columns, list) else 1} column(s)"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error sorting data in step '{self.step_name}': {e}")
    
    def _validate_sort_config(self, df: pd.DataFrame, columns, ascending, na_position, custom_orders) -> None:
        """
        Validate sorting configuration parameters.
        
        Args:
            df: Input DataFrame
            columns: Column(s) to sort by
            ascending: Sort direction(s)
            na_position: Position for null values
            custom_orders: Custom sort orders
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
                    f"Sort column '{col}' not found. "
                    f"Available columns: {available_columns}"
                )
        
        # Validate ascending
        if isinstance(ascending, bool):
            # Single value applies to all columns
            pass
        elif isinstance(ascending, list):
            if len(ascending) != len(columns):
                raise StepProcessorError(
                    f"Length of 'ascending' list ({len(ascending)}) must match "
                    f"number of columns ({len(columns)})"
                )
            for val in ascending:
                if not isinstance(val, bool):
                    raise StepProcessorError("All values in 'ascending' list must be boolean")
        else:
            raise StepProcessorError("'ascending' must be a boolean or list of booleans")
        
        # Validate na_position
        if na_position not in ['first', 'last']:
            raise StepProcessorError("'na_position' must be 'first' or 'last'")
        
        # Validate custom_orders
        if not isinstance(custom_orders, dict):
            raise StepProcessorError("'custom_orders' must be a dictionary")
        
        for col, order in custom_orders.items():
            if col not in columns:
                raise StepProcessorError(f"Custom order specified for column '{col}' which is not in sort columns")
            if not isinstance(order, list):
                raise StepProcessorError(f"Custom order for column '{col}' must be a list")
    
    def _apply_standard_sort(self, df: pd.DataFrame, columns, ascending, na_position, ignore_case) -> pd.DataFrame:
        """
        Apply standard pandas sorting.
        
        Args:
            df: DataFrame to sort
            columns: Column(s) to sort by
            ascending: Sort direction(s)
            na_position: Position for null values
            ignore_case: Whether to ignore case for string sorting
            
        Returns:
            Sorted DataFrame
        """
        # Handle case-insensitive sorting for string columns
        if ignore_case:
            sort_data = df.copy()
            temp_columns = []
            
            for col in columns:
                if df[col].dtype == 'object':  # String-like column
                    temp_col = f"{col}_sort_temp"
                    sort_data[temp_col] = df[col].astype(str).str.lower()
                    temp_columns.append(temp_col)
                else:
                    temp_columns.append(col)
            
            # Sort by temporary columns
            sorted_data = sort_data.sort_values(
                by=temp_columns,
                ascending=ascending,
                na_position=na_position
            )
            
            # Remove temporary columns and return
            result = df.loc[sorted_data.index].copy()
            
        else:
            # Standard sorting
            result = df.sort_values(
                by=columns,
                ascending=ascending,
                na_position=na_position
            )
        
        logger.debug(f"Applied standard sort: columns={columns}, ascending={ascending}")
        return result
    
    def _apply_custom_sort(self, df: pd.DataFrame, columns, custom_orders, ascending, na_position) -> pd.DataFrame:
        """
        Apply custom sorting with specified value orders.
        
        Args:
            df: DataFrame to sort
            columns: Column(s) to sort by
            custom_orders: Custom sort orders for specified columns
            ascending: Sort direction(s)
            na_position: Position for null values
            
        Returns:
            Sorted DataFrame
        """
        sort_data = df.copy()
        
        # Create categorical columns with custom orders
        for col in columns:
            if col in custom_orders:
                custom_order = custom_orders[col]
                
                # Create categorical with specified order
                sort_data[col] = pd.Categorical(
                    df[col], 
                    categories=custom_order, 
                    ordered=True
                )
                
                logger.debug(f"Applied custom order to column '{col}': {custom_order}")
        
        # Sort using the categorical columns
        result = sort_data.sort_values(
            by=columns,
            ascending=ascending,
            na_position=na_position
        )
        
        # Return original data in the new order
        return df.loc[result.index].copy()
    
    def sort_by_frequency(self, df: pd.DataFrame, column: str, ascending: bool = False) -> pd.DataFrame:
        """
        Sort DataFrame by value frequency in a column.
        
        Args:
            df: DataFrame to sort
            column: Column to base frequency sorting on
            ascending: Whether to sort by frequency ascending (least frequent first)
            
        Returns:
            DataFrame sorted by value frequency
        """
        if column not in df.columns:
            raise StepProcessorError(f"Column '{column}' not found for frequency sorting")
        
        # Get value counts and create frequency mapping
        freq_map = df[column].value_counts(ascending=ascending).to_dict()
        
        # Add frequency column for sorting
        df_with_freq = df.copy()
        df_with_freq['_freq_sort'] = df[column].map(freq_map)
        
        # Sort by frequency, then by original values for ties
        result = df_with_freq.sort_values(
            by=['_freq_sort', column], 
            ascending=[ascending, True]
        )
        
        # Remove temporary frequency column
        return result.drop(columns=['_freq_sort'])
    
    def sort_by_custom_function(self, df: pd.DataFrame, column: str, sort_key_func) -> pd.DataFrame:
        """
        Sort DataFrame using a custom function to generate sort keys.
        
        Args:
            df: DataFrame to sort
            column: Column to apply custom sorting to
            sort_key_func: Function that takes a value and returns a sort key
            
        Returns:
            Sorted DataFrame
        """
        if column not in df.columns:
            raise StepProcessorError(f"Column '{column}' not found for custom function sorting")
        
        try:
            # Create temporary sort key column
            df_with_key = df.copy()
            df_with_key['_custom_sort_key'] = df[column].apply(sort_key_func)
            
            # Sort by the custom keys
            result = df_with_key.sort_values(by='_custom_sort_key')
            
            # Remove temporary key column
            return result.drop(columns=['_custom_sort_key'])
            
        except Exception as e:
            raise StepProcessorError(f"Error applying custom sort function: {e}")
    
    def sort_by_multiple_criteria(self, df: pd.DataFrame, criteria: list) -> pd.DataFrame:
        """
        Sort DataFrame by multiple criteria with different options per column.
        
        Args:
            df: DataFrame to sort
            criteria: List of dictionaries, each specifying column and options
            
        Returns:
            Sorted DataFrame
        """
        if not isinstance(criteria, list) or len(criteria) == 0:
            raise StepProcessorError("Criteria must be a non-empty list")
        
        sort_data = df.copy()
        sort_columns = []
        sort_ascending = []
        
        # Process each criterion
        for i, criterion in enumerate(criteria):
            if not isinstance(criterion, dict):
                raise StepProcessorError(f"Criterion {i+1} must be a dictionary")
            
            if 'column' not in criterion:
                raise StepProcessorError(f"Criterion {i+1} must have 'column' field")
            
            col = criterion['column']
            ascending = criterion.get('ascending', True)
            custom_order = criterion.get('custom_order', None)
            ignore_case = criterion.get('ignore_case', False)
            
            if col not in df.columns:
                raise StepProcessorError(f"Column '{col}' not found in criterion {i+1}")
            
            # Apply transformations for this criterion
            if custom_order:
                sort_data[col] = pd.Categorical(df[col], categories=custom_order, ordered=True)
            elif ignore_case and df[col].dtype == 'object':
                temp_col = f"{col}_case_insensitive"
                sort_data[temp_col] = df[col].astype(str).str.lower()
                col = temp_col
            
            sort_columns.append(col)
            sort_ascending.append(ascending)
        
        # Apply the sort
        result = sort_data.sort_values(by=sort_columns, ascending=sort_ascending)
        
        # Return original data in new order
        return df.loc[result.index].copy()
    
    def get_sort_analysis(self, df: pd.DataFrame, column: str) -> dict:
        """
        Analyze a column for sorting characteristics.
        
        Args:
            df: DataFrame to analyze
            column: Column to analyze
            
        Returns:
            Dictionary with sorting analysis
        """
        if column not in df.columns:
            raise StepProcessorError(f"Column '{column}' not found for analysis")
        
        series = df[column]
        
        analysis = {
            'column_name': column,
            'data_type': str(series.dtype),
            'total_values': len(series),
            'unique_values': series.nunique(),
            'null_count': series.isnull().sum(),
            'is_already_sorted': series.is_monotonic_increasing,
            'is_reverse_sorted': series.is_monotonic_decreasing
        }
        
        # Add type-specific analysis
        if pd.api.types.is_numeric_dtype(series):
            analysis.update({
                'min_value': series.min(),
                'max_value': series.max(),
                'mean_value': series.mean()
            })
        elif pd.api.types.is_string_dtype(series) or series.dtype == 'object':
            analysis.update({
                'avg_length': series.astype(str).str.len().mean(),
                'most_common': series.value_counts().head(5).to_dict()
            })
        elif pd.api.types.is_datetime64_any_dtype(series):
            analysis.update({
                'earliest_date': series.min(),
                'latest_date': series.max()
            })
        
        return analysis
    
    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.
        
        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Sort DataFrame rows by one or multiple columns',
            'supported_options': [
                'single_column_sort', 'multi_column_sort', 'custom_sort_orders',
                'case_insensitive_sort', 'null_position_control', 'frequency_based_sort'
            ],
            'na_positions': ['first', 'last'],
            'sort_directions': ['ascending', 'descending'],
            'special_methods': [
                'sort_by_frequency', 'sort_by_custom_function', 'sort_by_multiple_criteria'
            ]
        }
