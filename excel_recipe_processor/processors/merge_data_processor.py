"""
Merge data step processor for Excel automation recipes.

Handles merging DataFrames with external data sources using various join strategies.
"""

import pandas as pd
import logging

from typing import Any
from pathlib import Path

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError

logger = logging.getLogger(__name__)


class MergeDataProcessor(BaseStepProcessor):
    """
    Processor for merging DataFrames with external data sources.
    
    Supports merging with Excel files, CSV files, or dictionaries using
    various join types (left, right, inner, outer). Handles key column
    specification and column naming conflicts.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'merge_source': {
                'type': 'dictionary',
                'data': {
                    'key1': {'value_col': 'test_value1'},
                    'key2': {'value_col': 'test_value2'}
                }
            },
            'left_key': 'test_column',
            'right_key': 'key_column'
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the merge operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to merge with external source
            
        Returns:
            Merged pandas DataFrame
            
        Raises:
            StepProcessorError: If merge operation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Merge step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['merge_source', 'left_key', 'right_key'])
        
        merge_source = self.get_config_value('merge_source')
        left_key = self.get_config_value('left_key')
        right_key = self.get_config_value('right_key')
        join_type = self.get_config_value('join_type', 'left')
        suffixes = self.get_config_value('suffixes', ('_x', '_y'))
        
        # Validate configuration
        self._validate_merge_config(data, merge_source, left_key, right_key, join_type)
        
        try:
            # Load the merge source data
            merge_data = self._load_merge_source(merge_source)
            
            # Validate merge keys exist
            self._validate_merge_keys(data, merge_data, left_key, right_key)
            
            # Perform the merge
            initial_rows = len(data)
            result_data = self._perform_merge(data, merge_data, left_key, right_key, join_type, suffixes)
            final_rows = len(result_data)
            
            # Handle column conflicts if needed
            result_data = self._handle_column_conflicts(result_data)
            
            result_info = f"merged {initial_rows} → {final_rows} rows using {join_type} join"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error during merge in step '{self.step_name}': {e}")
    
    def _validate_merge_config(self, df: pd.DataFrame, merge_source: dict, 
                              left_key: str, right_key: str, join_type: str) -> None:
        """
        Validate merge configuration parameters.
        
        Args:
            df: Input DataFrame
            merge_source: Merge source configuration
            left_key: Left DataFrame key column
            right_key: Right DataFrame key column  
            join_type: Type of join to perform
        """
        # Validate merge source
        if not isinstance(merge_source, dict):
            raise StepProcessorError("'merge_source' must be a dictionary")
        
        if 'type' not in merge_source:
            raise StepProcessorError("'merge_source' must specify a 'type'")
        
        source_type = merge_source['type']
        valid_types = ['excel', 'csv', 'dictionary']
        if source_type not in valid_types:
            raise StepProcessorError(f"merge_source type '{source_type}' not supported. Valid types: {valid_types}")
        
        # Validate key columns
        if not isinstance(left_key, str) or not left_key.strip():
            raise StepProcessorError("'left_key' must be a non-empty string")
        
        if not isinstance(right_key, str) or not right_key.strip():
            raise StepProcessorError("'right_key' must be a non-empty string")
        
        if left_key not in df.columns:
            available_columns = list(df.columns)
            raise StepProcessorError(
                f"Left key column '{left_key}' not found. Available columns: {available_columns}"
            )
        
        # Validate join type
        valid_joins = ['left', 'right', 'inner', 'outer']
        if join_type not in valid_joins:
            raise StepProcessorError(f"join_type '{join_type}' not supported. Valid types: {valid_joins}")
    
    def _load_merge_source(self, merge_source: dict) -> pd.DataFrame:
        """
        Load data from the specified merge source.
        
        Args:
            merge_source: Configuration for the merge source
            
        Returns:
            DataFrame with merge data
        """
        source_type = merge_source['type']
        
        if source_type == 'excel':
            return self._load_excel_source(merge_source)
        elif source_type == 'csv':
            return self._load_csv_source(merge_source)
        elif source_type == 'dictionary':
            return self._load_dictionary_source(merge_source)
        else:
            raise StepProcessorError(f"Unsupported merge source type: {source_type}")
    
    def _load_excel_source(self, merge_source: dict) -> pd.DataFrame:
        """Load data from Excel file."""
        if 'path' not in merge_source:
            raise StepProcessorError("Excel merge source requires 'path' field")
        
        file_path = Path(merge_source['path'])
        if not file_path.exists():
            raise StepProcessorError(f"Excel file not found: {file_path}")
        
        sheet_name = merge_source.get('sheet', 0)
        
        try:
            return pd.read_excel(file_path, sheet_name=sheet_name)
        except Exception as e:
            raise StepProcessorError(f"Error reading Excel file '{file_path}': {e}")
    
    def _load_csv_source(self, merge_source: dict) -> pd.DataFrame:
        """Load data from CSV file."""
        if 'path' not in merge_source:
            raise StepProcessorError("CSV merge source requires 'path' field")
        
        file_path = Path(merge_source['path'])
        if not file_path.exists():
            raise StepProcessorError(f"CSV file not found: {file_path}")
        
        try:
            # Use common CSV reading options
            return pd.read_csv(
                file_path,
                encoding=merge_source.get('encoding', 'utf-8'),
                sep=merge_source.get('separator', ',')
            )
        except Exception as e:
            raise StepProcessorError(f"Error reading CSV file '{file_path}': {e}")
    
    def _load_dictionary_source(self, merge_source: dict) -> pd.DataFrame:
        """Load data from dictionary configuration."""
        if 'data' not in merge_source:
            raise StepProcessorError("Dictionary merge source requires 'data' field")
        
        data_dict = merge_source['data']
        if not isinstance(data_dict, dict):
            raise StepProcessorError("Dictionary merge source 'data' must be a dictionary")
        
        try:
            # Convert nested dictionary to DataFrame
            # Format: {key: {col1: val1, col2: val2}, key2: {col1: val3, col2: val4}}
            df = pd.DataFrame.from_dict(data_dict, orient='index')
            
            # Reset index to make the keys a column
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'key'}, inplace=True)
            
            return df
        except Exception as e:
            raise StepProcessorError(f"Error creating DataFrame from dictionary: {e}")
    
    def _validate_merge_keys(self, left_df: pd.DataFrame, right_df: pd.DataFrame,
                            left_key: str, right_key: str) -> None:
        """
        Validate that merge keys exist in both DataFrames.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_key: Left key column
            right_key: Right key column
        """
        if right_key not in right_df.columns:
            available_columns = list(right_df.columns)
            raise StepProcessorError(
                f"Right key column '{right_key}' not found in merge source. "
                f"Available columns: {available_columns}"
            )
    
    def _perform_merge(self, left_df: pd.DataFrame, right_df: pd.DataFrame,
                      left_key: str, right_key: str, join_type: str, suffixes: tuple) -> pd.DataFrame:
        """
        Perform the actual merge operation.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_key: Left key column
            right_key: Right key column
            join_type: How to join the DataFrames
            suffixes: Suffixes for duplicate columns
            
        Returns:
            Merged DataFrame
        """
        try:
            # Handle data type mismatches between merge keys
            left_key_type = left_df[left_key].dtype
            right_key_type = right_df[right_key].dtype
            
            # If data types don't match, try to convert right key to match left key type
            if left_key_type != right_key_type:
                logger.debug(f"Data type mismatch: left key ({left_key_type}) vs right key ({right_key_type})")
                
                # Create a copy to avoid modifying original data
                right_df_copy = right_df.copy()
                
                try:
                    # Try to convert right key to match left key type
                    if pd.api.types.is_numeric_dtype(left_df[left_key]):
                        # Left key is numeric, try to convert right key to numeric
                        right_df_copy[right_key] = pd.to_numeric(right_df_copy[right_key], errors='coerce')
                    elif pd.api.types.is_string_dtype(left_df[left_key]) or left_df[left_key].dtype == 'object':
                        # Left key is string/object, convert right key to string
                        right_df_copy[right_key] = right_df_copy[right_key].astype(str)
                    
                    logger.debug(f"Converted right key to type: {right_df_copy[right_key].dtype}")
                    right_df = right_df_copy
                    
                except Exception as convert_error:
                    logger.warning(f"Could not convert data types, attempting merge anyway: {convert_error}")
            
            merged_df = pd.merge(
                left_df,
                right_df,
                left_on=left_key,
                right_on=right_key,
                how=join_type,
                suffixes=suffixes
            )
            
            # Log merge statistics
            left_unique = left_df[left_key].nunique()
            right_unique = right_df[right_key].nunique()
            matched_keys = merged_df[left_key].nunique()
            
            logger.debug(
                f"Merge statistics: {left_unique} left keys, {right_unique} right keys, "
                f"{matched_keys} matched keys"
            )
            
            return merged_df
            
        except Exception as e:
            raise StepProcessorError(f"Error performing {join_type} merge: {e}")
    
    def _handle_column_conflicts(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle any remaining column naming conflicts.
        
        Args:
            df: DataFrame after merge
            
        Returns:
            DataFrame with cleaned column names
        """
        # Get configuration for column handling
        drop_duplicate_keys = self.get_config_value('drop_duplicate_keys', True)
        column_prefix = self.get_config_value('column_prefix', '')
        
        result_df = df.copy()
        
        # Drop duplicate key columns if requested
        if drop_duplicate_keys:
            # Look for duplicate key columns (e.g., 'key_x', 'key_y')
            right_key = self.get_config_value('right_key')
            potential_duplicate = f"{right_key}_y"
            if potential_duplicate in result_df.columns:
                result_df = result_df.drop(columns=[potential_duplicate])
                logger.debug(f"Dropped duplicate key column: {potential_duplicate}")
        
        # Add column prefix if specified
        if column_prefix:
            # Only add prefix to columns from the right DataFrame
            merge_source = self.get_config_value('merge_source')
            if 'columns_to_prefix' in merge_source:
                columns_to_prefix = merge_source['columns_to_prefix']
                rename_dict = {
                    col: f"{column_prefix}{col}" 
                    for col in columns_to_prefix 
                    if col in result_df.columns
                }
                result_df = result_df.rename(columns=rename_dict)
        
        return result_df
    
    def get_supported_join_types(self) -> list:
        """
        Get list of supported join types.
        
        Returns:
            List of supported join type strings
        """
        return ['left', 'right', 'inner', 'outer']
    
    def get_supported_source_types(self) -> list:
        """
        Get list of supported data source types.
        
        Returns:
            List of supported source type strings
        """
        return ['excel', 'csv', 'dictionary']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Merge DataFrames with external data sources using various join strategies',
            'join_types': self.get_supported_join_types(),
            'data_sources': self.get_supported_source_types(),
            'merge_features': [
                'external_file_merging', 'multiple_join_types', 'key_column_specification',
                'column_conflict_handling', 'duplicate_key_removal', 'column_prefixing'
            ],
            'file_formats': ['xlsx', 'xls', 'csv', 'dictionary'],
            'examples': {
                'excel_lookup': "Merge with product catalog from Excel file",
                'csv_enrichment': "Add customer data from CSV export", 
                'dictionary_mapping': "Add category mappings from configuration"
            }
        }
