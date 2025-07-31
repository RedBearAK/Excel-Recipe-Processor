"""
Add subtotals step processor for Excel automation recipes.

Handles inserting subtotal rows into grouped data with various aggregation functions.
"""

import pandas as pd
import logging

from typing import Any, Union

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class AddSubtotalsProcessor(BaseStepProcessor):
    """
    Processor for adding subtotal rows to grouped data.
    
    Inserts subtotal rows at group boundaries with calculated values
    using specified aggregation functions. Works with any grouped data
    including pivot table results.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'group_by': ['test_group'],
            'subtotal_columns': ['test_value'],
            'subtotal_functions': ['sum']
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the subtotal addition on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to add subtotals to
            
        Returns:
            DataFrame with subtotal rows inserted
            
        Raises:
            StepProcessorError: If subtotal addition fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Add subtotals step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['group_by', 'subtotal_columns'])
        
        group_by = self.get_config_value('group_by')
        subtotal_columns = self.get_config_value('subtotal_columns')
        subtotal_functions = self.get_config_value('subtotal_functions', ['sum'])
        subtotal_label = self.get_config_value('subtotal_label', 'Subtotal')
        position = self.get_config_value('position', 'after_group')
        preserve_totals = self.get_config_value('preserve_totals', True)
        
        # Validate configuration
        self._validate_subtotal_config(data, group_by, subtotal_columns, subtotal_functions, position)
        
        try:
            # Detect and preserve existing grand totals if requested
            existing_totals = None
            if preserve_totals:
                existing_totals = self._extract_existing_totals(data)
            
            # Add subtotals to the data
            result_data = self._add_subtotals_to_data(
                data, group_by, subtotal_columns, subtotal_functions, 
                subtotal_label, position, existing_totals
            )
            
            subtotal_count = self._count_subtotal_rows(result_data, subtotal_label)
            result_info = f"added {subtotal_count} subtotal rows across {len(group_by)} grouping levels"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error adding subtotals in step '{self.step_name}': {e}")
    
    def _validate_subtotal_config(self, df: pd.DataFrame, group_by: list, 
                                 subtotal_columns: list, subtotal_functions: list, 
                                 position: str) -> None:
        """
        Validate subtotal configuration parameters.
        
        Args:
            df: Input DataFrame
            group_by: Grouping columns
            subtotal_columns: Columns to calculate subtotals for
            subtotal_functions: Aggregation functions
            position: Where to place subtotals
        """
        # Validate group_by
        if not isinstance(group_by, list) or len(group_by) == 0:
            raise StepProcessorError("'group_by' must be a non-empty list of column names")
        
        for col in group_by:
            if not isinstance(col, str):
                raise StepProcessorError(f"Group column name must be a string, got: {type(col)}")
            if col not in df.columns:
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Group column '{col}' not found. Available columns: {available_columns}"
                )
        
        # Validate subtotal_columns
        if not isinstance(subtotal_columns, list) or len(subtotal_columns) == 0:
            raise StepProcessorError("'subtotal_columns' must be a non-empty list of column names")
        
        for col in subtotal_columns:
            if not isinstance(col, str):
                raise StepProcessorError(f"Subtotal column name must be a string, got: {type(col)}")
            if col not in df.columns:
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Subtotal column '{col}' not found. Available columns: {available_columns}"
                )
        
        # Validate subtotal_functions
        if not isinstance(subtotal_functions, list):
            raise StepProcessorError("'subtotal_functions' must be a list")
        
        valid_functions = ['sum', 'count', 'mean', 'min', 'max', 'nunique', 'std', 'var']
        for func in subtotal_functions:
            if func not in valid_functions:
                raise StepProcessorError(
                    f"Subtotal function '{func}' not supported. Valid functions: {valid_functions}"
                )
        
        # Validate position
        valid_positions = ['before_group', 'after_group']
        if position not in valid_positions:
            raise StepProcessorError(f"Position '{position}' not supported. Valid positions: {valid_positions}")
    
    def _extract_existing_totals(self, df: pd.DataFrame) -> Union[pd.DataFrame, None]:
        """
        Extract existing grand total rows to preserve them.
        
        Args:
            df: DataFrame that may contain grand totals
            
        Returns:
            DataFrame with grand total rows, or None if none found
        """
        # Look for common grand total indicators
        total_indicators = ['Grand Total', 'Total', 'All', 'Grand_Total', 'TOTAL']
        
        # Check if any rows contain these indicators in the first few columns
        total_rows = []
        for idx, row in df.iterrows():
            row_values = [str(val).strip() for val in row.iloc[:3]]  # Check first 3 columns
            if any(indicator in ' '.join(row_values) for indicator in total_indicators):
                total_rows.append(idx)
        
        if total_rows:
            logger.debug(f"Found {len(total_rows)} existing total rows to preserve")
            return df.loc[total_rows].copy()
        
        return None
    
    def _add_subtotals_to_data(self, df: pd.DataFrame, group_by: list, 
                              subtotal_columns: list, subtotal_functions: list,
                              subtotal_label: str, position: str, 
                              existing_totals: Union[pd.DataFrame, None]) -> pd.DataFrame:
        """
        Add subtotal rows to the DataFrame.
        
        Args:
            df: Input DataFrame
            group_by: Columns to group by
            subtotal_columns: Columns to calculate subtotals for
            subtotal_functions: Functions to use for aggregation
            subtotal_label: Label for subtotal rows
            position: Where to place subtotals
            existing_totals: Previously extracted total rows
            
        Returns:
            DataFrame with subtotal rows added
        """
        # Remove existing totals temporarily if preserving them
        working_df = df.copy()
        if existing_totals is not None:
            # Remove total rows from working data
            total_indicators = ['Grand Total', 'Total', 'All', 'Grand_Total', 'TOTAL']
            mask = working_df.apply(
                lambda row: not any(indicator in str(val) for val in row.iloc[:3] for indicator in total_indicators),
                axis=1
            )
            working_df = working_df[mask]
        
        # Sort by grouping columns to ensure proper grouping
        sorted_df = working_df.sort_values(group_by)
        
        # Build result by processing groups
        result_rows = []
        
        # Group by the specified columns
        grouped = sorted_df.groupby(group_by, sort=False)
        
        for group_key, group_data in grouped:
            # Add subtotal before group if requested
            if position == 'before_group':
                subtotal_row = self._create_subtotal_row(
                    group_data, group_by, subtotal_columns, subtotal_functions, 
                    subtotal_label, 0  # Use 0 for level since we're doing simple grouping
                )
                result_rows.append(subtotal_row)
            
            # Add all data rows for this group
            for _, row in group_data.iterrows():
                result_rows.append(row.copy())
            
            # Add subtotal after group if requested
            if position == 'after_group':
                subtotal_row = self._create_subtotal_row(
                    group_data, group_by, subtotal_columns, subtotal_functions, 
                    subtotal_label, 0  # Use 0 for level since we're doing simple grouping
                )
                result_rows.append(subtotal_row)
        
        # Create result DataFrame
        if result_rows:
            result_df = pd.DataFrame(result_rows)
            result_df = result_df.reset_index(drop=True)
        else:
            result_df = working_df.copy()
        
        # Re-add preserved totals at the end
        if existing_totals is not None:
            result_df = pd.concat([result_df, existing_totals], ignore_index=True)
        
        return result_df
    
    def _create_subtotal_row(self, group_data: pd.DataFrame, group_columns: list,
                            subtotal_columns: list, subtotal_functions: list,
                            subtotal_label: str, level: int) -> pd.Series:
        """
        Create a single subtotal row for a group.
        
        Args:
            group_data: Data for the current group
            group_columns: Columns that define the group
            subtotal_columns: Columns to calculate subtotals for
            subtotal_functions: Functions to use for calculation
            subtotal_label: Label for the subtotal row
            level: Grouping level (0 = outermost)
            
        Returns:
            Series representing the subtotal row
        """
        # Start with the first row as a template
        subtotal_row = group_data.iloc[0].copy()
        
        # Set group identification values
        for i, col in enumerate(group_columns):
            if i == level:
                # This is the level we're subtotaling - use label
                group_value = group_data.iloc[0][col]
                subtotal_row[col] = f"{subtotal_label}: {group_value}"
            elif i < level:
                # Higher level - keep the group value
                subtotal_row[col] = group_data.iloc[0][col]
            else:
                # Lower level - clear the value
                subtotal_row[col] = ""
        
        # Calculate subtotals for specified columns
        for col in subtotal_columns:
            if col in group_data.columns:
                # Use the first function if only one specified, otherwise try to match column to function
                func_idx = min(subtotal_columns.index(col), len(subtotal_functions) - 1)
                func = subtotal_functions[func_idx]
                
                try:
                    if func == 'sum':
                        subtotal_row[col] = group_data[col].sum()
                    elif func == 'count':
                        subtotal_row[col] = group_data[col].count()
                    elif func == 'mean':
                        subtotal_row[col] = group_data[col].mean()
                    elif func == 'min':
                        subtotal_row[col] = group_data[col].min()
                    elif func == 'max':
                        subtotal_row[col] = group_data[col].max()
                    elif func == 'nunique':
                        subtotal_row[col] = group_data[col].nunique()
                    elif func == 'std':
                        subtotal_row[col] = group_data[col].std()
                    elif func == 'var':
                        subtotal_row[col] = group_data[col].var()
                except Exception as e:
                    logger.warning(f"Could not calculate {func} for column {col}: {e}")
                    subtotal_row[col] = 0
        
        return subtotal_row
    
    def _count_subtotal_rows(self, df: pd.DataFrame, subtotal_label: str) -> int:
        """
        Count the number of subtotal rows in the result.
        
        Args:
            df: Result DataFrame
            subtotal_label: Label used for subtotal rows
            
        Returns:
            Number of subtotal rows
        """
        count = 0
        for _, row in df.iterrows():
            row_str = ' '.join(str(val) for val in row.iloc[:3])
            if subtotal_label in row_str:
                count += 1
        return count
    
    def get_supported_functions(self) -> list:
        """
        Get list of supported aggregation functions.
        
        Returns:
            List of supported function strings
        """
        return ['sum', 'count', 'mean', 'min', 'max', 'nunique', 'std', 'var']
    
    def get_supported_positions(self) -> list:
        """
        Get list of supported subtotal positions.
        
        Returns:
            List of supported position strings
        """
        return ['before_group', 'after_group']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Add subtotal rows to grouped data with various aggregation functions',
            'subtotal_features': [
                'hierarchical_grouping', 'multiple_aggregation_functions', 'flexible_positioning',
                'grand_total_preservation', 'custom_labeling', 'multi_level_subtotals'
            ],
            'aggregation_functions': self.get_supported_functions(),
            'subtotal_positions': self.get_supported_positions(),
            'data_compatibility': [
                'pivot_table_results', 'aggregate_data_results', 'any_grouped_data'
            ],
            'examples': {
                'sales_by_region': "Add regional subtotals to sales data",
                'pivot_enhancement': "Add subtotals to existing pivot table results",
                'hierarchical_reporting': "Multi-level subtotals for complex reports"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the add_subtotals processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('add_subtotals')



class SubtotalUtils:
    """
    Utility class for subtotal operations that can be shared across processors.
    
    This provides the core subtotal logic that can be used by both the AddSubtotalsProcessor
    and potentially enhanced pivot table processors.
    """
    
    @staticmethod
    def add_subtotals_to_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
        """
        Add subtotals to a DataFrame using the provided configuration.
        
        This is the core subtotal logic that can be reused by other processors.
        
        Args:
            df: DataFrame to add subtotals to
            config: Subtotal configuration dictionary
            
        Returns:
            DataFrame with subtotals added
        """
        # Create a temporary processor to use the existing logic
        temp_config = {
            'processor_type': 'add_subtotals',
            'step_description': 'Utility subtotal addition',
            **config
        }
        
        processor = AddSubtotalsProcessor(temp_config)
        return processor.execute(df)
    
    @staticmethod
    def validate_subtotal_config(config: dict) -> bool:
        """
        Validate a subtotal configuration without executing it.
        
        Args:
            config: Subtotal configuration to validate
            
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        required_fields = ['group_by', 'subtotal_columns']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Subtotal configuration missing required field: {field}")
        
        return True
    
    @staticmethod
    def get_default_subtotal_config() -> dict:
        """
        Get a default subtotal configuration for reference.
        
        Returns:
            Dictionary with default subtotal configuration
        """
        return {
            'group_by': [],
            'subtotal_columns': [],
            'subtotal_functions': ['sum'],
            'subtotal_label': 'Subtotal',
            'position': 'after_group',
            'preserve_totals': True
        }
