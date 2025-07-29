"""
Combine data step processor for Excel automation recipes.

Handles combining multiple DataFrames from various sources (stages, current data)
with options for blank row/column insertion and different combination methods.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class CombineDataProcessor(BaseStepProcessor):
    """
    Processor for combining multiple DataFrames from various sources.
    
    Supports various combination operations including:
    - Vertical stacking (rows on top of each other)
    - Horizontal concatenation (columns side by side)
    - Blank row/column insertion between sections
    - Loading from multiple stages and current pipeline data
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'combine_type': 'vertical_stack',
            'data_sources': [
                {'insert_from_stage': 'test_stage_1'},
                {'insert_blank_rows': 1},
                {'insert_from_stage': 'test_stage_2'}
            ]
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the data combination operation.
        
        Args:
            data: Input pandas DataFrame (may be used as additional source)
            
        Returns:
            Combined pandas DataFrame
            
        Raises:
            StepProcessorError: If combination fails
        """
        self.log_step_start()
        
        # Validate required configuration
        self.validate_required_fields(['combine_type', 'data_sources'])
        
        combine_type = self.get_config_value('combine_type')
        data_sources = self.get_config_value('data_sources')
        
        # Validate configuration
        self._validate_combine_config(combine_type, data_sources)
        
        try:
            # Process data sources sequentially
            if combine_type == 'vertical_stack':
                result = self._combine_vertical_sequential(data_sources, data)
            elif combine_type == 'horizontal_concat':
                result = self._combine_horizontal_sequential(data_sources, data)
            else:
                raise StepProcessorError(f"Unsupported combine_type: {combine_type}")
            
            total_sources = len([ds for ds in data_sources if 'insert_from_stage' in ds])
            result_info = f"combined {total_sources} sources → {len(result)} rows, {len(result.columns)} columns ({combine_type})"
            self.log_step_complete(result_info)
            
            return result
            
        except StageError as e:
            raise StepProcessorError(f"Error accessing stage data in step '{self.step_name}': {e}")
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error during combination in step '{self.step_name}': {e}")
    
    def _validate_combine_config(self, combine_type: str, data_sources: list) -> None:
        """
        Validate combination configuration parameters.
        
        Args:
            combine_type: Type of combination operation
            data_sources: List of data source configurations
            
        Raises:
            StepProcessorError: If configuration is invalid
        """
        # Validate combine_type
        valid_types = self.get_supported_combine_types()
        if combine_type not in valid_types:
            raise StepProcessorError(
                f"Invalid combine_type: {combine_type}. "
                f"Supported types: {valid_types}"
            )
        
        # Validate data_sources
        if not isinstance(data_sources, list):
            raise StepProcessorError("data_sources must be a list")
        
        if len(data_sources) < 1:
            raise StepProcessorError("At least one data source must be specified")
        
        # Validate each data source configuration
        has_data_source = False
        for i, source in enumerate(data_sources):
            if not isinstance(source, dict):
                raise StepProcessorError(f"Data source {i+1} must be a dictionary")
            
            # Check for valid source types
            valid_keys = ['insert_from_stage', 'insert_blank_rows', 'insert_blank_cols']
            source_keys = list(source.keys())
            
            if len(source_keys) != 1:
                raise StepProcessorError(
                    f"Data source {i+1} must specify exactly one operation, "
                    f"got: {source_keys}"
                )
            
            key = source_keys[0]
            if key not in valid_keys:
                raise StepProcessorError(
                    f"Data source {i+1}: invalid operation '{key}'. "
                    f"Valid operations: {valid_keys}"
                )
            
            # Track if we have at least one data source
            if key == 'insert_from_stage':
                has_data_source = True
            
            # Validate blank operations for combine type
            if key == 'insert_blank_rows' and combine_type != 'vertical_stack':
                raise StepProcessorError(
                    f"Data source {i+1}: insert_blank_rows only valid for vertical_stack"
                )
            
            if key == 'insert_blank_cols' and combine_type != 'horizontal_concat':
                raise StepProcessorError(
                    f"Data source {i+1}: insert_blank_cols only valid for horizontal_concat"
                )
        
        # Must have at least one actual data source
        if not has_data_source:
            raise StepProcessorError("At least one insert_from_stage must be specified")
    
    def _combine_vertical_sequential(self, data_sources: list, current_data: pd.DataFrame) -> pd.DataFrame:
        """
        Process data sources sequentially for vertical combination.
        
        Args:
            data_sources: List of data source configurations
            current_data: Current pipeline DataFrame
            
        Returns:
            Vertically combined DataFrame
        """
        combined_dfs = []
        data_dfs = []  # Track actual data (not blank rows) for column validation
        ignore_column_mismatch = self.get_config_value('ignore_column_mismatch', False)
        
        for i, source in enumerate(data_sources):
            try:
                if 'insert_from_stage' in source:
                    stage_name = source['insert_from_stage']
                    df = self._load_data_from_source(stage_name, current_data, i+1)
                    combined_dfs.append(df)
                    data_dfs.append(df)
                    
                    # Validate columns against previous data sources
                    if not ignore_column_mismatch and len(data_dfs) > 1:
                        self._validate_column_consistency(data_dfs)
                    
                    logger.debug(f"Added stage '{stage_name}': {len(df)} rows")
                    
                elif 'insert_blank_rows' in source:
                    num_rows = source['insert_blank_rows']
                    if not isinstance(num_rows, int) or num_rows < 0:
                        raise StepProcessorError(f"Data source {i+1}: insert_blank_rows must be a non-negative integer")
                    
                    if combined_dfs:  # Only add blanks if we have data to match columns
                        blank_df = self._create_blank_rows(combined_dfs[-1].columns, num_rows)
                        combined_dfs.append(blank_df)
                        logger.debug(f"Added {num_rows} blank rows")
                    
            except Exception as e:
                if isinstance(e, StepProcessorError):
                    raise
                else:
                    raise StepProcessorError(f"Data source {i+1}: {e}")
        
        if not combined_dfs:
            raise StepProcessorError("No DataFrames were created from data sources")
        
        try:
            if ignore_column_mismatch:
                result = pd.concat(combined_dfs, ignore_index=True, sort=False)
            else:
                result = pd.concat(combined_dfs, ignore_index=True)
            return result
        except Exception as e:
            raise StepProcessorError(f"Error in vertical combination: {e}")
    
    def _combine_horizontal_sequential(self, data_sources: list, current_data: pd.DataFrame) -> pd.DataFrame:
        """
        Process data sources sequentially for horizontal combination.
        
        Args:
            data_sources: List of data source configurations
            current_data: Current pipeline DataFrame
            
        Returns:
            Horizontally combined DataFrame
        """
        combined_dfs = []
        
        for i, source in enumerate(data_sources):
            try:
                if 'insert_from_stage' in source:
                    stage_name = source['insert_from_stage']
                    df = self._load_data_from_source(stage_name, current_data, i+1)
                    combined_dfs.append(df)
                    logger.debug(f"Added stage '{stage_name}': {len(df.columns)} columns")
                    
                elif 'insert_blank_cols' in source:
                    num_cols = source['insert_blank_cols']
                    if not isinstance(num_cols, int) or num_cols < 0:
                        raise StepProcessorError(f"Data source {i+1}: insert_blank_cols must be a non-negative integer")
                    
                    if combined_dfs:  # Only add blanks if we have data to match rows
                        blank_df = self._create_blank_columns(len(combined_dfs[-1]), num_cols)
                        combined_dfs.append(blank_df)
                        logger.debug(f"Added {num_cols} blank columns")
                    
            except Exception as e:
                if isinstance(e, StepProcessorError):
                    raise
                else:
                    raise StepProcessorError(f"Data source {i+1}: {e}")
        
        if not combined_dfs:
            raise StepProcessorError("No DataFrames were created from data sources")
        
        try:
            result = pd.concat(combined_dfs, axis=1, ignore_index=False)
            return result
        except Exception as e:
            raise StepProcessorError(f"Error in horizontal combination: {e}")
    
    def _load_data_from_source(self, source_name: str, current_data: pd.DataFrame, source_index: int) -> pd.DataFrame:
        """
        Load DataFrame from a specified source.
        
        Args:
            source_name: Name of source (stage name or 'current_dataframe')
            current_data: Current pipeline DataFrame
            source_index: Index of source for error messages
            
        Returns:
            Loaded DataFrame
            
        Raises:
            StepProcessorError: If loading fails
        """
        if source_name == 'current_dataframe':
            if current_data is None:
                raise StepProcessorError(f"Data source {source_index}: current_dataframe is None")
            
            if not isinstance(current_data, pd.DataFrame):
                raise StepProcessorError(f"Data source {source_index}: current_dataframe must be a pandas DataFrame")
            
            return current_data.copy()
            
        else:
            # Load from stage
            if not isinstance(source_name, str):
                raise StepProcessorError(f"Data source {source_index}: stage name must be a string")
            
            try:
                df = StageManager.get_stage_data(source_name)
                return df
            except StageError as e:
                raise StepProcessorError(f"Data source {source_index}: {e}")
    
    def _create_blank_rows(self, columns, num_rows: int) -> pd.DataFrame:
        """
        Create a DataFrame with blank rows matching the given columns.
        
        Args:
            columns: Column structure to match
            num_rows: Number of blank rows to create
            
        Returns:
            DataFrame with blank rows
        """
        blank_data = [['' for _ in columns] for _ in range(num_rows)]
        blank_df = pd.DataFrame(blank_data, columns=columns)
        return blank_df
    
    def _create_blank_columns(self, num_rows: int, num_columns: int) -> pd.DataFrame:
        """
        Create a DataFrame with blank columns.
        
        Args:
            num_rows: Number of rows to create
            num_columns: Number of blank columns to create
            
        Returns:
            DataFrame with blank columns
        """
        blank_data = [['' for _ in range(num_columns)] for _ in range(num_rows)]
        column_names = [f'Blank_{i+1}' for i in range(num_columns)]
        blank_df = pd.DataFrame(blank_data, columns=column_names)
        return blank_df
    
    def _validate_column_consistency(self, dataframes: list) -> None:
        """
        Validate that all DataFrames have consistent columns for vertical combination.
        
        Args:
            dataframes: List of DataFrames to check
            
        Raises:
            StepProcessorError: If columns are inconsistent
        """
        if len(dataframes) < 2:
            logger.debug("Less than 2 DataFrames to validate")
            return
        
        first_columns = list(dataframes[0].columns)
        logger.debug(f"Validating column consistency. First DF columns: {first_columns}")
        
        for i, df in enumerate(dataframes[1:], 1):
            current_columns = list(df.columns)
            logger.debug(f"DataFrame {i+1} columns: {current_columns}")
            
            if current_columns != first_columns:
                raise StepProcessorError(
                    f"Column mismatch between data sources. "
                    f"First data source columns: {first_columns}, "
                    f"Data source {i+1} columns: {current_columns}. "
                    f"Use ignore_column_mismatch: true to allow mismatched columns."
                )
        
        logger.debug("Column validation passed")
    
    def get_supported_combine_types(self) -> list:
        """Get list of supported combination types."""
        return ['vertical_stack', 'horizontal_concat']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Combine multiple DataFrames from stages and current data',
            'supported_combine_types': self.get_supported_combine_types(),
            'combination_operations': [
                'vertical_stacking', 'horizontal_concatenation',
                'blank_row_insertion', 'blank_column_insertion'
            ],
            'data_sources': [
                'saved_stages', 'current_pipeline_data'
            ],
            'configuration_options': {
                'combine_type': 'Type of combination operation to perform',
                'data_sources': 'Sequential list of data sources and blank insertions',
                'insert_from_stage': 'Load data from a saved stage or current_dataframe',
                'insert_blank_rows': 'Insert N blank rows (vertical_stack only)',
                'insert_blank_cols': 'Insert N blank columns (horizontal_concat only)'
            },
            'data_source_operations': {
                'insert_from_stage': 'Load data from saved stage or current_dataframe',
                'insert_blank_rows': 'Insert blank rows between sections',
                'insert_blank_cols': 'Insert blank columns between sections'
            },
            'examples': {
                'combine_metadata_and_data': "Vertically stack metadata and processed data",
                'side_by_side_comparison': "Horizontally concatenate related datasets",
                'spaced_sections': "Combine with blank rows for visual separation",
                'multi_stage_report': "Combine data from multiple processing stages"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the combine_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('combine_data')
