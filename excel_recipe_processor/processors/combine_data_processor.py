"""
Combine data step processor for Excel automation recipes.

Handles combining multiple DataFrames from various sources (stages, current data)
with enhanced column handling, header retention, and different combination methods.
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
    - Enhanced column handling for mismatched structures
    - Header retention as data rows for complex documents
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'combine_type': 'vertical_stack',
            'column_handling': 'require_matching_columns',
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
            data: Current pipeline DataFrame (used when referencing 'current_dataframe')
        
        Returns:
            Combined DataFrame based on configuration
        """
        self.log_step_start()
        
        # Validate required fields
        self.validate_required_fields(['combine_type', 'column_handling', 'data_sources'])
        
        combine_type = self.get_config_value('combine_type')
        column_handling = self.get_config_value('column_handling')
        data_sources = self.get_config_value('data_sources', [])
        
        # Validate configuration values
        self._validate_configuration(combine_type, column_handling, data_sources)
        
        logger.debug(f"Starting {combine_type} combination with {column_handling} column policy")
        logger.debug(f"Processing {len(data_sources)} data sources")
        
        try:
            if combine_type == 'vertical_stack':
                result = self._combine_vertical_sequential(data_sources, data)
            elif combine_type == 'horizontal_concat':
                result = self._combine_horizontal_sequential(data_sources, data)
            else:
                raise StepProcessorError(f"Unsupported combine_type: {combine_type}")
            
            result_info = f"combined {len(data_sources)} sources into {len(result)} rows, {len(result.columns)} columns"
            self.log_step_complete(result_info)
            
            return result
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Combination failed in step '{self.step_name}': {e}")
    
    def _validate_configuration(self, combine_type: str, column_handling: str, data_sources: list) -> None:
        """Validate configuration parameters."""
        
        # Validate combine_type
        valid_combine_types = self.get_supported_combine_types()
        if combine_type not in valid_combine_types:
            raise StepProcessorError(
                f"Invalid combine_type '{combine_type}'. "
                f"Supported types: {', '.join(valid_combine_types)}"
            )
        
        # Validate column_handling
        valid_column_policies = ['require_matching_columns', 'allow_mismatched_columns']
        if column_handling not in valid_column_policies:
            raise StepProcessorError(
                f"Invalid column_handling '{column_handling}'. "
                f"Supported policies: {', '.join(valid_column_policies)}"
            )
        
        # Validate data_sources
        if not isinstance(data_sources, list) or len(data_sources) == 0:
            raise StepProcessorError("data_sources must be a non-empty list")
        
        # Validate each data source configuration
        for i, source in enumerate(data_sources):
            if not isinstance(source, dict):
                raise StepProcessorError(f"Data source {i+1} must be a dictionary")
            
            # Check for exactly one operation per source
            operations = ['insert_from_stage', 'insert_blank_rows', 'insert_blank_cols']
            found_operations = [op for op in operations if op in source]
            
            if len(found_operations) != 1:
                raise StepProcessorError(
                    f"Data source {i+1} must have exactly one operation. "
                    f"Found: {found_operations}"
                )
            
            # Validate operation-specific parameters
            if 'insert_blank_rows' in source:
                if combine_type != 'vertical_stack':
                    raise StepProcessorError(
                        f"Data source {i+1}: insert_blank_rows only valid for vertical_stack"
                    )
                num_rows = source['insert_blank_rows']
                if not isinstance(num_rows, int) or num_rows < 0:
                    raise StepProcessorError(
                        f"Data source {i+1}: insert_blank_rows must be a non-negative integer"
                    )
            
            elif 'insert_blank_cols' in source:
                if combine_type != 'horizontal_concat':
                    raise StepProcessorError(
                        f"Data source {i+1}: insert_blank_cols only valid for horizontal_concat"
                    )
                num_cols = source['insert_blank_cols']
                if not isinstance(num_cols, int) or num_cols < 0:
                    raise StepProcessorError(
                        f"Data source {i+1}: insert_blank_cols must be a non-negative integer"
                    )
            
            # Validate retain_column_names parameter
            if 'retain_column_names' in source:
                retain_headers = source['retain_column_names']
                if not isinstance(retain_headers, bool):
                    raise StepProcessorError(
                        f"Data source {i+1}: retain_column_names must be a boolean"
                    )
                if 'insert_from_stage' not in source:
                    raise StepProcessorError(
                        f"Data source {i+1}: retain_column_names only valid with insert_from_stage"
                    )
    
    def _load_data_from_source(self, stage_name: str, current_data: pd.DataFrame, 
                             source_index: int, retain_headers: bool = False) -> pd.DataFrame:
        """
        Load data from a stage or current pipeline data.
        
        Args:
            stage_name: Name of stage to load or 'current_dataframe'
            current_data: Current pipeline DataFrame
            source_index: Index of source for error reporting
            retain_headers: Whether to insert column names as first data row
            
        Returns:
            Loaded DataFrame, optionally with headers as first row
        """
        logger.debug(f"Loading data from source: {stage_name}")
        
        if stage_name == 'current_dataframe':
            if current_data is None:
                raise StepProcessorError(
                    f"Data source {source_index}: current_dataframe is None"
                )
            df = current_data.copy()
        else:
            try:
                df = StageManager.load_stage(stage_name)
                if df is None:
                    raise StepProcessorError(
                        f"Data source {source_index}: Stage '{stage_name}' not found"
                    )
                df = df.copy()
            except StageError as e:
                raise StepProcessorError(
                    f"Data source {source_index}: Error loading stage '{stage_name}': {e}"
                )
        
        logger.debug(f"Loaded DataFrame: {len(df)} rows, {len(df.columns)} columns")
        
        # Insert column names as first row if requested
        if retain_headers and not df.empty:
            # Create header row with column names
            header_row = pd.DataFrame([list(df.columns)], columns=df.columns)
            # Combine header row with data
            df = pd.concat([header_row, df], ignore_index=True)
            logger.debug(f"Added header row: {list(df.columns)}")
        
        return df
    
    def _get_source_retain_headers_setting(self, source: dict, column_handling: str) -> bool:
        """
        Determine whether to retain headers for a data source based on settings and smart defaults.
        
        Args:
            source: Data source configuration
            column_handling: Global column handling policy
            
        Returns:
            Whether to retain column headers as data row
        """
        # Explicit setting takes precedence
        if 'retain_column_names' in source:
            return source['retain_column_names']
        
        # Smart defaults based on global policy
        if column_handling == 'allow_mismatched_columns':
            return True  # Preserve meaning when columns don't match
        else:  # require_matching_columns
            return False  # Let pandas handle matching columns
    
    def _create_blank_rows(self, columns: pd.Index, num_rows: int) -> pd.DataFrame:
        """Create DataFrame with blank rows matching given columns."""
        if num_rows <= 0:
            return pd.DataFrame(columns=columns)
        
        # Create DataFrame with NaN values
        blank_data = {}
        for col in columns:
            blank_data[col] = [None] * num_rows
        
        return pd.DataFrame(blank_data)
    
    def _create_blank_columns(self, num_rows: int, num_cols: int) -> pd.DataFrame:
        """Create DataFrame with blank columns."""
        if num_cols <= 0 or num_rows <= 0:
            return pd.DataFrame()
        
        # Create columns with names like 'Blank_1', 'Blank_2', etc.
        blank_data = {}
        for i in range(num_cols):
            col_name = f"Blank_{i+1}"
            blank_data[col_name] = [None] * num_rows
        
        return pd.DataFrame(blank_data)
    
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
        column_handling = self.get_config_value('column_handling')
        allow_mismatched = (column_handling == 'allow_mismatched_columns')
        
        for i, source in enumerate(data_sources):
            try:
                if 'insert_from_stage' in source:
                    stage_name = source['insert_from_stage']
                    retain_headers = self._get_source_retain_headers_setting(source, column_handling)
                    
                    df = self._load_data_from_source(stage_name, current_data, i+1, retain_headers)
                    combined_dfs.append(df)
                    data_dfs.append(df)
                    
                    # Validate columns against previous data sources
                    if not allow_mismatched and len(data_dfs) > 1:
                        self._validate_column_consistency(data_dfs)
                    
                    logger.debug(f"Added stage '{stage_name}': {len(df)} rows, retain_headers={retain_headers}")
                    
                elif 'insert_blank_rows' in source:
                    num_rows = source['insert_blank_rows']
                    
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
            # Use appropriate concatenation method based on column policy
            if allow_mismatched:
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
        column_handling = self.get_config_value('column_handling')
        
        for i, source in enumerate(data_sources):
            try:
                if 'insert_from_stage' in source:
                    stage_name = source['insert_from_stage']
                    retain_headers = self._get_source_retain_headers_setting(source, column_handling)
                    
                    df = self._load_data_from_source(stage_name, current_data, i+1, retain_headers)
                    combined_dfs.append(df)
                    logger.debug(f"Added stage '{stage_name}': {len(df)} rows, {len(df.columns)} columns")
                    
                elif 'insert_blank_cols' in source:
                    num_cols = source['insert_blank_cols']
                    
                    if combined_dfs:  # Only add blanks if we have data to determine row count
                        num_rows = len(combined_dfs[-1])
                        blank_df = self._create_blank_columns(num_rows, num_cols)
                        combined_dfs.append(blank_df)
                        logger.debug(f"Added {num_cols} blank columns")
                    
            except Exception as e:
                if isinstance(e, StepProcessorError):
                    raise
                else:
                    raise StepProcessorError(f"Data source {i+1}: {e}")
        
        if not combined_dfs:
            raise StepProcessorError("No DataFrames were created from data sources")
        
        # Validate row counts for horizontal concatenation
        first_row_count = len(combined_dfs[0])
        for i, df in enumerate(combined_dfs[1:], 1):
            if len(df) != first_row_count:
                raise StepProcessorError(
                    f"Row count mismatch for horizontal concatenation. "
                    f"First DataFrame: {first_row_count} rows, "
                    f"DataFrame {i+1}: {len(df)} rows"
                )
        
        try:
            result = pd.concat(combined_dfs, axis=1, ignore_index=False)
            return result
        except Exception as e:
            raise StepProcessorError(f"Error in horizontal combination: {e}")
    
    def _validate_column_consistency(self, dataframes: list) -> None:
        """
        Validate that DataFrames have consistent column structures.
        
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
                    f"Use column_handling: 'allow_mismatched_columns' to allow mismatched columns."
                )
        
        logger.debug("Column validation passed")
    
    def get_supported_combine_types(self) -> list:
        """Get list of supported combination types."""
        return ['vertical_stack', 'horizontal_concat']
    
    def get_supported_column_policies(self) -> list:
        """Get list of supported column handling policies."""
        return ['require_matching_columns', 'allow_mismatched_columns']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Combine multiple DataFrames from stages and current data with enhanced column handling',
            'supported_combine_types': self.get_supported_combine_types(),
            'supported_column_policies': self.get_supported_column_policies(),
            'combination_operations': [
                'vertical_stacking', 'horizontal_concatenation',
                'blank_row_insertion', 'blank_column_insertion',
                'header_retention', 'smart_column_defaults'
            ],
            'data_sources': [
                'saved_stages', 'current_pipeline_data'
            ],
            'configuration_options': {
                'combine_type': 'Type of combination operation to perform',
                'column_handling': 'Global policy for handling column mismatches',
                'data_sources': 'Sequential list of data sources and blank insertions',
                'insert_from_stage': 'Load data from a saved stage or current_dataframe',
                'insert_blank_rows': 'Insert N blank rows (vertical_stack only)',
                'insert_blank_cols': 'Insert N blank columns (horizontal_concat only)',
                'retain_column_names': 'Insert column headers as data row (per-source setting)'
            },
            'data_source_operations': {
                'insert_from_stage': 'Load data from saved stage or current_dataframe',
                'insert_blank_rows': 'Insert blank rows between sections',
                'insert_blank_cols': 'Insert blank columns between sections'
            },
            'column_policies': {
                'require_matching_columns': 'All DataFrames must have identical column structures',
                'allow_mismatched_columns': 'Allow DataFrames with different columns (NaN fill)'
            },
            'smart_defaults': {
                'require_matching_columns': 'retain_column_names defaults to false',
                'allow_mismatched_columns': 'retain_column_names defaults to true'
            },
            'examples': {
                'desktop_publishing': "Combine title sections, headers, and data preserving visual meaning",
                'metadata_and_data': "Vertically stack metadata and processed data",
                'side_by_side_comparison': "Horizontally concatenate related datasets",
                'spaced_sections': "Combine with blank rows for visual separation",
                'multi_stage_report': "Combine data from multiple processing stages"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the combine_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('combine_data')
