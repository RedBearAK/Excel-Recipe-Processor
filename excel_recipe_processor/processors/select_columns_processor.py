"""
Select columns step processor for Excel automation recipes.

excel_recipe_processor/processors/select_columns_processor.py

Handles selecting specific columns from DataFrames with support for reordering,
duplicating columns, and inclusion/exclusion patterns.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class SelectColumnsProcessor(BaseStepProcessor):
    """
    Processor for selecting and reordering DataFrame columns.
    
    Supports column selection by inclusion or exclusion lists, automatic 
    reordering based on column order specification, and column duplication 
    when the same column name appears multiple times in the selection list.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        """
        Get the minimal configuration required to instantiate this processor.
        
        Returns:
            Dictionary with minimal configuration fields
        """
        return {
            'columns_to_keep': ['test_column_1', 2, 'test_column_3']  # Mix of names and positions
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the column selection operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            DataFrame with selected columns in specified order
            
        Raises:
            StepProcessorError: If column selection fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Select columns step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Get configuration parameters
        columns_to_keep = self.get_config_value('columns_to_keep')
        columns_to_drop = self.get_config_value('columns_to_drop')
        columns_to_create = self.get_config_value('columns_to_create', [])
        allow_duplicates = self.get_config_value('allow_duplicates', True)
        strict_mode = self.get_config_value('strict_mode', True)
        
        # Validate configuration
        self._validate_select_config(data, columns_to_keep, columns_to_drop, columns_to_create)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Determine which columns to select
            if columns_to_keep is not None:
                result_data = self._select_by_inclusion(
                    result_data, columns_to_keep, columns_to_create, allow_duplicates, strict_mode
                )
                operation_desc = f"selected {len(columns_to_keep)} column specifications"
            elif columns_to_drop is not None:
                result_data = self._select_by_exclusion(
                    result_data, columns_to_drop, strict_mode
                )
                operation_desc = f"dropped {len(columns_to_drop)} columns"
            else:
                raise StepProcessorError("Either 'columns_to_keep' or 'columns_to_drop' must be specified")
            
            # Log results
            final_column_count = len(result_data.columns)
            result_info = f"{operation_desc}, result: {final_column_count} columns"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error selecting columns in step '{self.step_name}': {e}")
    
    def _validate_select_config(self, df: pd.DataFrame, columns_to_keep, columns_to_drop, columns_to_create) -> None:
        """
        Validate column selection configuration parameters.
        
        Args:
            df: Input DataFrame
            columns_to_keep: List of columns to keep (or None)
            columns_to_drop: List of columns to drop (or None)
            columns_to_create: List of columns to create if missing (or empty list)
        """
        # Ensure exactly one selection method is specified
        if columns_to_keep is not None and columns_to_drop is not None:
            raise StepProcessorError("Cannot specify both 'columns_to_keep' and 'columns_to_drop'")
        
        if columns_to_keep is None and columns_to_drop is None:
            raise StepProcessorError("Must specify either 'columns_to_keep' or 'columns_to_drop'")
        
        # Validate columns_to_keep if specified
        if columns_to_keep is not None:
            if not isinstance(columns_to_keep, list):
                raise StepProcessorError("'columns_to_keep' must be a list")
            
            if len(columns_to_keep) == 0:
                raise StepProcessorError("'columns_to_keep' cannot be empty")
            
            for col in columns_to_keep:
                if isinstance(col, str):
                    if not col.strip():
                        raise StepProcessorError(f"Column names must be non-empty strings, got: '{col}'")
                elif isinstance(col, int):
                    if col < 1:
                        raise StepProcessorError(f"Column numbers must be 1-based (1, 2, 3...), got: {col}")
                else:
                    raise StepProcessorError(f"Column references must be strings or integers, got: {type(col)}")
        
        # Validate columns_to_drop if specified
        if columns_to_drop is not None:
            if not isinstance(columns_to_drop, list):
                raise StepProcessorError("'columns_to_drop' must be a list")
            
            if len(columns_to_drop) == 0:
                raise StepProcessorError("'columns_to_drop' cannot be empty")
            
            for col in columns_to_drop:
                if not isinstance(col, str) or not col.strip():
                    raise StepProcessorError(f"Column names must be non-empty strings, got: {col}")
        
        # Validate columns_to_create if specified
        if columns_to_create:  # Only validate if non-empty
            if not isinstance(columns_to_create, list):
                raise StepProcessorError("'columns_to_create' must be a list")
            
            for col in columns_to_create:
                if not isinstance(col, str) or not col.strip():
                    raise StepProcessorError(f"Column names to create must be non-empty strings, got: {col}")
            
            # Check for duplicates in columns_to_create
            if len(columns_to_create) != len(set(columns_to_create)):
                duplicates = [col for col in columns_to_create if columns_to_create.count(col) > 1]
                raise StepProcessorError(f"Duplicate column names in columns_to_create: {duplicates}")
            
            # columns_to_create can only be used with columns_to_keep
            if columns_to_keep is None:
                raise StepProcessorError("'columns_to_create' can only be used with 'columns_to_keep'")
            
            # Check that all columns_to_create are also in columns_to_keep
            columns_to_create_set = set(columns_to_create)
            columns_to_keep_set = set(columns_to_keep)
            not_in_keep = columns_to_create_set - columns_to_keep_set
            if not_in_keep:
                raise StepProcessorError(
                    f"Columns in 'columns_to_create' must also be in 'columns_to_keep': {list(not_in_keep)}"
                )
    
    def _select_by_inclusion(self, df: pd.DataFrame, columns_to_keep: list, columns_to_create: list,
                           allow_duplicates: bool, strict_mode: bool) -> pd.DataFrame:
        """
        Select columns by specifying which ones to keep.
        
        Args:
            df: Input DataFrame
            columns_to_keep: List of column names to keep (order matters)
            columns_to_create: List of column names that should be created if missing
            allow_duplicates: Whether to allow duplicate column selections
            strict_mode: Whether to fail on missing columns or skip them
            
        Returns:
            DataFrame with only the specified columns in the specified order
        """
        available_columns = set(df.columns)
        columns_to_create_set = set(columns_to_create)
        missing_columns = []
        selected_columns = []
        
        # Check each requested column
        for col in columns_to_keep:
            if isinstance(col, str) and col in available_columns:
                # String column name exists - use it directly
                selected_columns.append(col)
            elif isinstance(col, int):
                # Numeric reference - convert 1-based to 0-based
                if col < 1:
                    raise StepProcessorError(f"Column numbers must be 1-based (1, 2, 3...), got: {col}")
                
                pandas_col_idx = col - 1  # Convert to 0-based
                if pandas_col_idx >= len(df.columns):
                    raise StepProcessorError(f"Column number {col} exceeds available columns ({len(df.columns)})")
                
                # Get the actual column name at this position
                actual_col_name = df.columns[pandas_col_idx]
                selected_columns.append(actual_col_name)
                logger.debug(f"Column {col} (1-based) → '{actual_col_name}' (position {pandas_col_idx})")
                
            elif isinstance(col, str) and col in columns_to_create_set:
                # Column should be created - add to selected list
                selected_columns.append(col)
            else:
                # Column is genuinely missing
                missing_columns.append(col)
        
        # Handle missing columns based on strict_mode
        if missing_columns:
            if strict_mode:
                raise StepProcessorError(
                    f"Columns not found: {missing_columns}. "
                    f"Available columns: {list(df.columns)}. "
                    f"Add missing columns to 'columns_to_create' if you want to create them."
                )
            else:
                logger.warning(f"Skipping missing columns: {missing_columns}")
        
        # Check for duplicates if not allowed
        if not allow_duplicates:
            seen_columns = set()
            duplicates = []
            for col in selected_columns:
                if col in seen_columns:
                    duplicates.append(col)
                seen_columns.add(col)
            
            if duplicates:
                raise StepProcessorError(f"Duplicate columns found: {duplicates}. Set allow_duplicates=true to permit duplicates")
        
        # Select/create columns in the specified order
        if not selected_columns:
            raise StepProcessorError("No valid columns found to select")
        
        # Build result DataFrame column by column to handle duplicates
        result_columns = []
        
        for col in selected_columns:
            # col is now always a string (column name)
            if col in available_columns:
                # Use existing column
                result_columns.append(df[col])
            elif col in columns_to_create_set:
                # Create new column with default value
                default_value = self.get_config_value('default_value', pd.NA)
                new_column = pd.Series([default_value] * len(df), index=df.index, name=col)
                result_columns.append(new_column)
                logger.debug(f"Created new column '{col}' with default value: {default_value}")
            else:
                # This shouldn't happen if our selection logic is correct
                raise StepProcessorError(f"Internal error: column '{col}' not found and not in create list")
            # Note: genuinely missing columns were already handled above
        
        # Combine all columns into result DataFrame
        result = pd.concat(result_columns, axis=1)
        
        # Set column names to match the requested order (handles duplicates)
        result.columns = selected_columns
        
        created_count = len([col for col in selected_columns if col in columns_to_create_set and col not in available_columns])
        logger.debug(f"Selected {len(selected_columns)} columns ({created_count} created): {selected_columns[:5]}{'...' if len(selected_columns) > 5 else ''}")
        
        return result
    
    def _select_by_exclusion(self, df: pd.DataFrame, columns_to_drop: list, 
                           strict_mode: bool) -> pd.DataFrame:
        """
        Select columns by specifying which ones to drop.
        
        Args:
            df: Input DataFrame
            columns_to_drop: List of column names to exclude
            strict_mode: Whether to fail on missing columns or skip them
            
        Returns:
            DataFrame with specified columns removed
        """
        available_columns = set(df.columns)
        missing_columns = []
        columns_to_remove = []
        
        # Check each column to drop
        for col in columns_to_drop:
            if col in available_columns:
                columns_to_remove.append(col)
            else:
                missing_columns.append(col)
        
        # Handle missing columns based on strict_mode
        if missing_columns:
            if strict_mode:
                raise StepProcessorError(
                    f"Columns to drop not found: {missing_columns}. "
                    f"Available columns: {list(df.columns)}"
                )
            else:
                logger.warning(f"Skipping missing drop columns: {missing_columns}")
        
        # Drop the specified columns
        if columns_to_remove:
            result = df.drop(columns=columns_to_remove).copy()
        else:
            result = df.copy()
            logger.warning("No columns were dropped")
        
        # Ensure we don't end up with an empty DataFrame
        if len(result.columns) == 0:
            raise StepProcessorError("Column dropping would result in empty DataFrame")
        
        logger.debug(f"Dropped {len(columns_to_remove)} columns, {len(result.columns)} remaining")
        
        return result
    
    def get_column_info(self, df: pd.DataFrame) -> dict:
        """
        Get information about DataFrame columns for analysis.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with column information
        """
        return {
            'total_columns': len(df.columns),
            'column_names': list(df.columns),
            'column_types': {col: str(df[col].dtype) for col in df.columns},
            'numeric_columns': list(df.select_dtypes(include=['number']).columns),
            'text_columns': list(df.select_dtypes(include=['object']).columns),
            'datetime_columns': list(df.select_dtypes(include=['datetime']).columns)
        }
    
    def suggest_common_columns(self, df: pd.DataFrame) -> dict:
        """
        Suggest common column patterns for selection.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with suggested column groupings
        """
        columns = list(df.columns)
        
        suggestions = {
            'id_columns': [col for col in columns if 'id' in col.lower()],
            'name_columns': [col for col in columns if 'name' in col.lower()],
            'date_columns': [col for col in columns if any(term in col.lower() for term in ['date', 'time', 'created', 'modified'])],
            'amount_columns': [col for col in columns if any(term in col.lower() for term in ['amount', 'price', 'cost', 'total', 'value'])],
            'status_columns': [col for col in columns if any(term in col.lower() for term in ['status', 'state', 'active', 'enabled'])],
            'numeric_columns': list(df.select_dtypes(include=['number']).columns)
        }
        
        # Remove empty suggestions
        return {key: value for key, value in suggestions.items() if value}
    
    def get_supported_selection_modes(self) -> list:
        """Get list of supported column selection modes."""
        return ['inclusion', 'exclusion']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Select and reorder DataFrame columns with flexible inclusion/exclusion patterns',
            'selection_modes': self.get_supported_selection_modes(),
            'column_operations': [
                'column_selection', 'column_reordering', 'column_duplication',
                'inclusion_filtering', 'exclusion_filtering', 'missing_column_handling'
            ],
            'configuration_options': {
                'columns_to_keep': 'List of columns to select by name (string) or position (1-based integer)',
                'columns_to_drop': 'List of columns to exclude from result',
                'columns_to_create': 'List of columns to create if missing (used with columns_to_keep)',
                'allow_duplicates': 'Allow same column to appear multiple times (default: true)',
                'strict_mode': 'Fail on missing columns vs skip them (default: true)',
                'default_value': 'Default value for created columns (default: pd.NA)'
            },
            'helper_methods': [
                'get_column_info', 'suggest_common_columns'
            ],
            'features': [
                'automatic_reordering', 'duplicate_column_support', 'missing_column_handling',
                'column_type_analysis', 'smart_column_suggestions', 'dynamic_column_creation',
                'numeric_position_references', '1_based_indexing'
            ],
            'examples': {
                'basic_selection': "Keep only Customer_ID, Product_Name, Price columns",
                'reordering': "Reorder columns by specifying them in desired sequence", 
                'duplication': "Duplicate columns by listing them multiple times",
                'exclusion': "Drop unwanted columns while keeping everything else",
                'column_creation': "Create new empty columns alongside existing ones",
                'numeric_references': "Select columns by position: [1, 3, 5] (1-based indexing)"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the select_columns processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('select_columns')


# End of file #
