"""
Pivot table step processor for Excel automation recipes.

Handles creating pivot tables with various configurations and aggregation functions.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class PivotTableProcessor(BaseStepProcessor):
    """
    Processor for creating pivot tables from DataFrame data.
    
    Supports various pivot configurations including multiple index/column fields,
    different aggregation functions, and handling of missing data.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'index': ['test_index_column'],
            'columns': ['test_column_column'],
            'values': ['test_values_column']
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the pivot table operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to pivot
            
        Returns:
            Pivot table as pandas DataFrame
            
        Raises:
            StepProcessorError: If pivot operation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Pivot table step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Get configuration
        index = self.get_config_value('index', [])
        columns = self.get_config_value('columns', None)
        values = self.get_config_value('values', [])
        aggfunc = self.get_config_value('aggfunc', 'sum')
        fill_value = self.get_config_value('fill_value', 0)
        margins = self.get_config_value('margins', False)
        dropna = self.get_config_value('dropna', True)
        
        # Validate configuration
        self._validate_pivot_config(data, index, columns, values, aggfunc)
        
        try:
            # Create the pivot table
            pivot_result = pd.pivot_table(
                data=data,
                index=index if index else None,
                columns=columns,
                values=values if values else None,
                aggfunc=aggfunc,
                fill_value=fill_value,
                margins=margins,
                dropna=dropna
            )
            
            # Convert to regular DataFrame and reset index
            if isinstance(pivot_result, pd.DataFrame):
                result_df = pivot_result.reset_index()
            else:
                # Handle case where pivot returns a Series
                result_df = pivot_result.to_frame().reset_index()
            
            # Clean up column names if they're hierarchical
            result_df = self._clean_column_names(result_df)
            
            # Apply post-processing options
            if self.get_config_value('sort_by_index', False):
                result_df = self._sort_by_first_column(result_df)
            
            if self.get_config_value('fill_blanks', False):
                result_df = self._fill_blank_cells(result_df)
            
            result_info = f"created pivot table: {len(result_df)} rows, {len(result_df.columns)} columns"
            self.log_step_complete(result_info)
            
            return result_df
            
        except Exception as e:
            raise StepProcessorError(f"Error creating pivot table in step '{self.step_name}': {e}")
    
    def _validate_pivot_config(self, df: pd.DataFrame, index, columns, values, aggfunc):
        """
        Validate pivot table configuration parameters.
        
        Args:
            df: Input DataFrame
            index: Index field(s)
            columns: Column field(s)  
            values: Value field(s)
            aggfunc: Aggregation function
        """
        # Validate index fields
        if index:
            if isinstance(index, str):
                index = [index]
            if not isinstance(index, list):
                raise StepProcessorError(f"Pivot table 'index' must be a string or list of strings")
            
            for field in index:
                if not isinstance(field, str):
                    raise StepProcessorError(f"Index field must be a string, got: {type(field)}")
                if field not in df.columns:
                    raise StepProcessorError(f"Index field '{field}' not found in data columns")
        
        # Validate column fields
        if columns:
            if isinstance(columns, str):
                columns = [columns]
            if isinstance(columns, list):
                for field in columns:
                    if not isinstance(field, str):
                        raise StepProcessorError(f"Column field must be a string, got: {type(field)}")
                    if field not in df.columns:
                        raise StepProcessorError(f"Column field '{field}' not found in data columns")
            else:
                if not isinstance(columns, str):
                    raise StepProcessorError(f"Pivot table 'columns' must be a string or list of strings")
                if columns not in df.columns:
                    raise StepProcessorError(f"Column field '{columns}' not found in data columns")
        
        # Validate value fields
        if values:
            if isinstance(values, str):
                values = [values]
            if not isinstance(values, list):
                raise StepProcessorError(f"Pivot table 'values' must be a string or list of strings")
            
            for field in values:
                if not isinstance(field, str):
                    raise StepProcessorError(f"Value field must be a string, got: {type(field)}")
                if field not in df.columns:
                    raise StepProcessorError(f"Value field '{field}' not found in data columns")
        
        # Validate aggregation function
        valid_aggfuncs = ['sum', 'mean', 'count', 'min', 'max', 'std', 'var', 'first', 'last', 'nunique']
        if isinstance(aggfunc, str) and aggfunc not in valid_aggfuncs:
            raise StepProcessorError(
                f"Unknown aggregation function: '{aggfunc}'. "
                f"Valid options: {', '.join(valid_aggfuncs)}"
            )
    
    def _clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean up hierarchical column names from pivot tables.
        
        Args:
            df: DataFrame with potentially hierarchical columns
            
        Returns:
            DataFrame with flattened column names
        """
        if isinstance(df.columns, pd.MultiIndex):
            # Flatten hierarchical column names
            new_columns = []
            for col in df.columns:
                if isinstance(col, tuple):
                    # Join non-empty parts of the tuple
                    col_parts = [str(part) for part in col if str(part) != '']
                    new_col = '_'.join(col_parts) if col_parts else 'value'
                else:
                    new_col = str(col)
                new_columns.append(new_col)
            
            df.columns = new_columns
            logger.debug(f"Flattened hierarchical column names: {new_columns[:5]}...")
        
        return df
    
    def _sort_by_first_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sort DataFrame by the first column (typically the index column).
        
        Args:
            df: DataFrame to sort
            
        Returns:
            Sorted DataFrame
        """
        if len(df.columns) > 0:
            first_col = df.columns[0]
            df = df.sort_values(by=first_col)
            logger.debug(f"Sorted by first column: {first_col}")
        
        return df
    
    def _fill_blank_cells(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Fill blank cells with values from above (like Excel's "repeat item labels").
        
        Args:
            df: DataFrame to fill
            
        Returns:
            DataFrame with filled blank cells
        """
        # Forward fill the first column (typically contains the index labels)
        if len(df.columns) > 0:
            first_col = df.columns[0]
            df[first_col] = df[first_col].ffill()
            logger.debug(f"Forward filled blank cells in column: {first_col}")
        
        return df
    
    def create_summary_pivot(self, data: pd.DataFrame, row_field: str, value_field: str, 
                           aggfunc: str = 'sum') -> pd.DataFrame:
        """
        Create a simple summary pivot table.
        
        Args:
            data: Input DataFrame
            row_field: Field to use as rows
            value_field: Field to aggregate
            aggfunc: Aggregation function
            
        Returns:
            Summary pivot table
        """
        try:
            summary = data.groupby(row_field)[value_field].agg(aggfunc).reset_index()
            summary.columns = [row_field, f'{value_field}_{aggfunc}']
            return summary
        except Exception as e:
            raise StepProcessorError(f"Error creating summary pivot: {e}")
    
    def create_cross_tab(self, data: pd.DataFrame, row_field: str, col_field: str, 
                        value_field: str = None, aggfunc: str = 'count') -> pd.DataFrame:
        """
        Create a cross-tabulation (contingency table).
        
        Args:
            data: Input DataFrame
            row_field: Field for rows
            col_field: Field for columns
            value_field: Field to aggregate (optional)
            aggfunc: Aggregation function
            
        Returns:
            Cross-tabulation DataFrame
        """
        try:
            if value_field:
                # Pivot with values
                crosstab = pd.pivot_table(
                    data, 
                    index=row_field, 
                    columns=col_field, 
                    values=value_field,
                    aggfunc=aggfunc, 
                    fill_value=0
                )
            else:
                # Simple count cross-tab
                crosstab = pd.crosstab(data[row_field], data[col_field])
            
            # Convert to regular DataFrame
            result = crosstab.reset_index()
            result = self._clean_column_names(result)
            
            return result
            
        except Exception as e:
            raise StepProcessorError(f"Error creating cross-tabulation: {e}")
    
    def get_supported_aggfuncs(self) -> list:
        """
        Get list of supported aggregation functions.
        
        Returns:
            List of supported aggregation function strings
        """
        return ['sum', 'mean', 'count', 'min', 'max', 'std', 'var', 'first', 'last', 'nunique']
    
    def get_pivot_info(self, df: pd.DataFrame) -> dict:
        """
        Get information about a potential pivot operation.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with pivot analysis information
        """
        info = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'column_names': list(df.columns),
            'numeric_columns': list(df.select_dtypes(include=['number']).columns),
            'categorical_columns': list(df.select_dtypes(include=['object', 'category']).columns)
        }
        
        # Add cardinality info for categorical columns
        info['column_cardinality'] = {}
        for col in info['categorical_columns']:
            unique_count = df[col].nunique()
            info['column_cardinality'][col] = unique_count
        
        return info
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Create pivot tables with various aggregation functions',
            'pivot_features': [
                'multi_index_pivot', 'cross_tabulation', 'multiple_aggregations',
                'hierarchical_columns', 'margin_totals', 'fill_blank_cells'
            ],
            'aggregation_functions': self.get_supported_aggfuncs(),
            'helper_methods': [
                'create_summary_pivot', 'create_cross_tab', 'get_pivot_info'
            ],
            'examples': {
                'van_report': "PRODUCT_ORIGIN vs CARRIER matrix",
                'sales_summary': "Sum sales by region and product",
                'count_matrix': "Count occurrences by category"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the pivot_table processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('pivot_table')
