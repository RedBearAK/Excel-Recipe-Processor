"""
Aggregate data step processor for Excel automation recipes.

Handles grouping data and applying aggregation functions to create summary statistics.
"""

import pandas as pd
import logging

from typing import Any, Union

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class AggregateDataProcessor(BaseStepProcessor):
    """
    Processor for aggregating DataFrame data by groups.
    
    Supports grouping by one or multiple columns and applying various
    aggregation functions to create summary statistics.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'group_by': ['test_group_column'],
            'aggregations': [
                {
                    'column': 'test_agg_column',
                    'function': 'sum',
                    'output_name': 'total_test'
                }
            ]
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the aggregation operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            Aggregated DataFrame with grouped results
            
        Raises:
            StepProcessorError: If aggregation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Aggregate data step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['group_by', 'aggregations'])
        
        group_by = self.get_config_value('group_by')
        aggregations = self.get_config_value('aggregations')
        keep_group_columns = self.get_config_value('keep_group_columns', True)
        sort_by_groups = self.get_config_value('sort_by_groups', True)
        reset_index = self.get_config_value('reset_index', True)
        
        # Validate configuration
        self._validate_aggregation_config(data, group_by, aggregations)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Perform the aggregation
            result_data = self._perform_aggregation(
                result_data, group_by, aggregations, keep_group_columns, 
                sort_by_groups, reset_index
            )
            
            result_info = f"aggregated {len(data)} rows into {len(result_data)} groups"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error performing aggregation in step '{self.step_name}': {e}")
    
    def _validate_aggregation_config(self, df: pd.DataFrame, group_by: Union[str, list[str]], 
                                   aggregations: Union[dict, list[dict]]) -> None:
        """
        Validate aggregation configuration parameters.
        
        Args:
            df: Input DataFrame
            group_by: Column(s) to group by
            aggregations: Aggregation specifications
        """
        # Validate group_by
        if isinstance(group_by, str):
            group_by = [group_by]
        elif not isinstance(group_by, list):
            raise StepProcessorError("'group_by' must be a string or list of strings")
        
        for col in group_by:
            if not isinstance(col, str):
                raise StepProcessorError(f"Group by column must be a string, got: {type(col)}")
            if col not in df.columns:
                raise StepProcessorError(f"Group by column '{col}' not found in data")
        
        # Validate aggregations
        if isinstance(aggregations, dict):
            aggregations = [aggregations]
        elif not isinstance(aggregations, list):
            raise StepProcessorError("'aggregations' must be a dictionary or list of dictionaries")
        
        for i, agg in enumerate(aggregations):
            if not isinstance(agg, dict):
                raise StepProcessorError(f"Aggregation {i+1} must be a dictionary")
            
            if 'column' not in agg:
                raise StepProcessorError(f"Aggregation {i+1} missing required 'column' field")
            
            if 'function' not in agg:
                raise StepProcessorError(f"Aggregation {i+1} missing required 'function' field")
            
            column = agg['column']
            function = agg['function']
            
            if column not in df.columns:
                raise StepProcessorError(f"Aggregation column '{column}' not found in data")
            
            if function not in self.get_supported_functions():
                supported = ', '.join(self.get_supported_functions())
                raise StepProcessorError(
                    f"Unsupported aggregation function '{function}'. "
                    f"Supported functions: {supported}"
                )
    
    def _perform_aggregation(self, df: pd.DataFrame, group_by: Union[str, list[str]], 
                           aggregations: Union[dict, list[dict]], keep_group_columns: bool,
                           sort_by_groups: bool, reset_index: bool) -> pd.DataFrame:
        """
        Perform the actual aggregation operation.
        
        Args:
            df: DataFrame to aggregate
            group_by: Column(s) to group by
            aggregations: Aggregation specifications
            keep_group_columns: Whether to keep group columns in result
            sort_by_groups: Whether to sort by group columns
            reset_index: Whether to reset index after grouping
            
        Returns:
            Aggregated DataFrame
        """
        # Ensure group_by is a list
        if isinstance(group_by, str):
            group_by = [group_by]
        
        # Ensure aggregations is a list
        if isinstance(aggregations, dict):
            aggregations = [aggregations]
        
        # Create the groupby object
        grouped = df.groupby(group_by, sort=sort_by_groups)
        
        # Build aggregation dictionary for pandas
        agg_dict = {}
        column_renames = {}
        
        for agg in aggregations:
            column = agg['column']
            function = agg['function']
            new_name = agg.get('new_column_name', f"{column}_{function}")
            
            # Handle multiple aggregations on the same column
            if column not in agg_dict:
                agg_dict[column] = []
            
            agg_dict[column].append(function)
            
            # Track what to rename columns to
            if column not in column_renames:
                column_renames[column] = []
            column_renames[column].append(new_name)
        
        # Perform the aggregation
        try:
            result = grouped.agg(agg_dict)
            
            # Flatten multi-level columns if needed
            if isinstance(result.columns, pd.MultiIndex):
                # Rename columns based on aggregation specs
                new_columns = []
                col_rename_idx = {}
                
                for col in result.columns:
                    if isinstance(col, tuple):
                        orig_col, func = col
                        if orig_col not in col_rename_idx:
                            col_rename_idx[orig_col] = 0
                        
                        rename_list = column_renames.get(orig_col, [])
                        if col_rename_idx[orig_col] < len(rename_list):
                            new_name = rename_list[col_rename_idx[orig_col]]
                        else:
                            new_name = f"{orig_col}_{func}"
                        
                        new_columns.append(new_name)
                        col_rename_idx[orig_col] += 1
                    else:
                        new_columns.append(str(col))
                
                result.columns = new_columns
            else:
                # Single level columns - apply renames
                new_columns = []
                for col in result.columns:
                    if col in column_renames and len(column_renames[col]) > 0:
                        new_columns.append(column_renames[col][0])
                    else:
                        new_columns.append(col)
                result.columns = new_columns
            
            # Reset index if requested
            if reset_index:
                result = result.reset_index()
            
            # Remove group columns if not wanted
            if not keep_group_columns and reset_index:
                result = result.drop(columns=group_by, errors='ignore')
            
            logger.debug(f"Aggregated by {group_by} with {len(aggregations)} functions")
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error during aggregation: {e}")
    
    def create_summary_aggregation(self, df: pd.DataFrame, group_by: Union[str, list[str]], 
                                 summary_columns: list[str]) -> pd.DataFrame:
        """
        Helper method to create a standard summary with count, sum, mean for specified columns.
        
        Args:
            df: DataFrame to summarize
            group_by: Column(s) to group by
            summary_columns: Columns to summarize
            
        Returns:
            DataFrame with summary statistics
        """
        aggregations = []
        
        for col in summary_columns:
            if col in df.columns:
                # Check if column is numeric
                if pd.api.types.is_numeric_dtype(df[col]):
                    aggregations.extend([
                        {'column': col, 'function': 'sum', 'new_column_name': f'{col}_total'},
                        {'column': col, 'function': 'mean', 'new_column_name': f'{col}_average'},
                        {'column': col, 'function': 'count', 'new_column_name': f'{col}_count'}
                    ])
                else:
                    aggregations.append({
                        'column': col, 'function': 'count', 'new_column_name': f'{col}_count'
                    })
        
        return self._perform_aggregation(df, group_by, aggregations, True, True, True)
    
    def create_crosstab_aggregation(self, df: pd.DataFrame, row_field: str, col_field: str, 
                                  value_field: str, aggfunc: str = 'count') -> pd.DataFrame:
        """
        Create a cross-tabulation style aggregation.
        
        Args:
            df: DataFrame to cross-tabulate
            row_field: Field for rows
            col_field: Field for columns  
            value_field: Field for values
            aggfunc: Aggregation function
            
        Returns:
            Cross-tabulated DataFrame
        """
        try:
            if aggfunc == 'count':
                result = pd.crosstab(df[row_field], df[col_field], margins=True)
            else:
                result = pd.crosstab(df[row_field], df[col_field], 
                                   values=df[value_field], aggfunc=aggfunc, margins=True)
            
            # Reset index to make row_field a regular column
            result = result.reset_index()
            
            logger.debug(f"Created crosstab: {row_field} vs {col_field}")
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error creating cross-tabulation: {e}")
    
    def get_aggregation_analysis(self, df: pd.DataFrame, group_by: Union[str, list[str]]) -> dict:
        """
        Analyze potential aggregations for the given grouping.
        
        Args:
            df: DataFrame to analyze
            group_by: Column(s) to group by
            
        Returns:
            Dictionary with aggregation analysis
        """
        if isinstance(group_by, str):
            group_by = [group_by]
        
        analysis = {
            'group_by_columns': group_by,
            'total_rows': len(df),
            'unique_groups': df[group_by].drop_duplicates().shape[0] if len(group_by) == 1 else df.groupby(group_by).ngroups,
            'numeric_columns': [],
            'categorical_columns': [],
            'suggested_aggregations': []
        }
        
        # Analyze each column
        for col in df.columns:
            if col not in group_by:
                if pd.api.types.is_numeric_dtype(df[col]):
                    analysis['numeric_columns'].append(col)
                    analysis['suggested_aggregations'].extend([
                        f"{col}: sum, mean, min, max",
                        f"{col}: count (non-null values)"
                    ])
                else:
                    analysis['categorical_columns'].append(col)
                    analysis['suggested_aggregations'].append(f"{col}: count, nunique")
        
        return analysis
    
    def get_supported_functions(self) -> list[str]:
        """
        Get list of supported aggregation functions.
        
        Returns:
            List of supported function names
        """
        return [
            'sum', 'mean', 'median', 'min', 'max', 
            'count', 'nunique', 'std', 'var',
            'first', 'last', 'size'
        ]
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Group data and apply aggregation functions for summary statistics',
            'aggregation_functions': self.get_supported_functions(),
            'grouping_features': [
                'single_column_grouping', 'multi_column_grouping', 'custom_column_naming',
                'multiple_aggregations_per_column', 'mixed_function_aggregations',
                'group_column_retention', 'automatic_sorting', 'index_management'
            ],
            'helper_methods': [
                'create_summary_aggregation', 'create_crosstab_aggregation', 
                'get_aggregation_analysis'
            ],
            'output_options': [
                'keep_group_columns', 'sort_by_groups', 'reset_index', 'custom_column_names'
            ],
            'examples': {
                'sales_summary': "Group by region, sum sales, count orders",
                'performance_stats': "Group by department, calculate mean/min/max metrics", 
                'cross_analysis': "Group by category and status, count occurrences"
            }
        }
