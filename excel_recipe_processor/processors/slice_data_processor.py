"""
Slice data step processor for Excel automation recipes.

Handles extracting portions of DataFrames by row ranges, column ranges,
or other criteria. Uses 1-based indexing to match Excel user expectations.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class SliceDataProcessor(BaseStepProcessor):
    """
    Processor for extracting portions of DataFrames.
    
    Supports various slicing operations including:
    - Row range slicing with 1-based indexing
    - Column range slicing 
    - Header promotion from within slices
    - Stage-based source data loading
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'slice_type': 'row_range',
            'start_row': 1,
            'end_row': 3
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the data slicing operation.
        
        Args:
            data: Input pandas DataFrame to slice, or None if using source_stage
            
        Returns:
            Sliced pandas DataFrame
            
        Raises:
            StepProcessorError: If slicing fails
        """
        self.log_step_start()
        
        # Check if we should load from a stage instead of using input data
        source_stage = self.get_config_value('source_stage')
        
        if source_stage:
            try:
                data = StageManager.get_stage_data(source_stage)
                logger.debug(f"Loaded data from stage '{source_stage}' for slicing")
            except StageError as e:
                raise StepProcessorError(f"Error loading source stage '{source_stage}': {e}")
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Slice step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['slice_type'])
        
        slice_type = self.get_config_value('slice_type')
        
        # Dispatch to appropriate slicing method
        if slice_type == 'row_range':
            result = self._slice_row_range(data)
        elif slice_type == 'column_range':
            result = self._slice_column_range(data)
        else:
            raise StepProcessorError(f"Unsupported slice_type: {slice_type}. Supported types: {self.get_supported_slice_types()}")
        
        # Handle header promotion if requested
        slice_result_contains_headers = self.get_config_value('slice_result_contains_headers', False)
        if slice_result_contains_headers:
            result = self._promote_first_row_to_headers(result)
        
        rows_before = len(data)
        rows_after = len(result)
        cols_before = len(data.columns)
        cols_after = len(result.columns)
        
        result_info = f"sliced {rows_before}×{cols_before} → {rows_after}×{cols_after} ({slice_type})"
        self.log_step_complete(result_info)
        
        return result
    
    def _slice_row_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Slice DataFrame by row range using 1-based indexing.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Sliced DataFrame
        """
        start_row = self.get_config_value('start_row', 1)  # Default to row 1
        end_row = self.get_config_value('end_row')
        
        # Validate row parameters
        if not isinstance(start_row, int) or start_row < 1:
            raise StepProcessorError(f"start_row must be a positive integer (1-based), got: {start_row}")
        
        if end_row is not None:
            if not isinstance(end_row, int) or end_row < start_row:
                raise StepProcessorError(f"end_row must be >= start_row, got: {end_row}")
        
        # Convert to 0-based indexing for pandas
        start_idx = start_row - 1
        end_idx = None if end_row is None else end_row  # end_row is inclusive in user terms
        
        # Validate bounds
        if start_idx >= len(df):
            raise StepProcessorError(f"start_row {start_row} exceeds data length ({len(df)} rows)")
        
        if end_idx is not None and end_idx > len(df):
            logger.warning(f"end_row {end_row} exceeds data length ({len(df)} rows), truncating")
            end_idx = len(df)
        
        # Perform the slice
        if end_idx is None:
            result = df.iloc[start_idx:].copy()
        else:
            result = df.iloc[start_idx:end_idx].copy()
        
        result.reset_index(drop=True, inplace=True)
        
        logger.debug(f"Row slice: rows {start_row}-{end_row or 'end'} → {len(result)} rows")
        return result
    
    def _slice_column_range(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Slice DataFrame by column range.
        
        Args:
            df: Source DataFrame
            
        Returns:
            Sliced DataFrame
        """
        start_col = self.get_config_value('start_col')
        end_col = self.get_config_value('end_col')
        
        # Validate required parameters for column slicing
        if start_col is None:
            raise StepProcessorError("start_col is required for column_range slice_type")
        
        # Handle both column names and Excel-style references
        if isinstance(start_col, str) and len(start_col) <= 3 and start_col.isalpha():
            # Excel column reference (A, B, AA, etc.)
            start_idx = self._excel_col_to_index(start_col)
            end_idx = self._excel_col_to_index(end_col) + 1 if end_col else len(df.columns)
        elif isinstance(start_col, int):
            # 1-based column number
            start_idx = start_col - 1
            end_idx = end_col if end_col else len(df.columns)
        else:
            # Column name
            try:
                start_idx = df.columns.get_loc(start_col)
                end_idx = df.columns.get_loc(end_col) + 1 if end_col else len(df.columns)
            except KeyError as e:
                raise StepProcessorError(f"Column not found: {e}")
        
        # Validate bounds
        if start_idx >= len(df.columns):
            raise StepProcessorError(f"start_col exceeds available columns ({len(df.columns)})")
        
        # Perform the slice
        result = df.iloc[:, start_idx:end_idx].copy()
        
        logger.debug(f"Column slice: cols {start_col}-{end_col or 'end'} → {len(result.columns)} columns")
        return result
    
    def _promote_first_row_to_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Promote the first row of the DataFrame to become column headers.
        
        Args:
            df: DataFrame to modify
            
        Returns:
            DataFrame with first row promoted to headers
        """
        if len(df) == 0:
            raise StepProcessorError("Cannot promote headers from empty slice")
        
        # Promote the first row to headers
        new_columns = df.iloc[0].astype(str)
        df.columns = new_columns
        
        # Remove the first row from data and reset index
        df = df.drop(df.index[0]).reset_index(drop=True)
        
        logger.debug(f"Promoted first row to headers: {list(df.columns)}")
        return df
    
    def _excel_col_to_index(self, col_ref: str) -> int:
        """
        Convert Excel column reference (A, B, AA, etc.) to 0-based index.
        
        Args:
            col_ref: Excel column reference (e.g., 'A', 'B', 'AA', 'BH')
            
        Returns:
            0-based column index
        """
        col_ref = col_ref.upper()
        result = 0
        
        for char in col_ref:
            result = result * 26 + (ord(char) - ord('A') + 1)
        
        return result - 1  # Convert to 0-based
    
    def get_supported_slice_types(self) -> list:
        """Get list of supported slice types."""
        return ['row_range', 'column_range']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Extract portions of DataFrames with flexible slicing options',
            'supported_slice_types': self.get_supported_slice_types(),
            'indexing': '1-based for user configuration (converted internally)',
            'slice_operations': [
                'row_range_extraction', 'column_range_extraction', 
                'header_promotion'
            ],
            'configuration_options': {
                'slice_type': 'Type of slice operation to perform',
                'start_row': '1-based starting row number (inclusive)',
                'end_row': '1-based ending row number (inclusive, optional)',
                'start_col': 'Starting column (name, Excel ref, or 1-based number)',
                'end_col': 'Ending column (name, Excel ref, or 1-based number, optional)',
                'slice_result_contains_headers': 'Whether first row of slice contains headers (default: false)',
                'source_stage': 'Stage name to load data from (instead of input data)'
            },
            'examples': {
                'extract_metadata': "Extract rows 1-3 from Excel file",
                'extract_data_section': "Extract rows 5-end with header promotion",
                'extract_columns': "Extract columns A-D or Product-Price"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the slice_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('slice_data')
