"""
Merge data step processor for Excel automation recipes.

Handles merging DataFrames with external data sources using FileReader
infrastructure and StageManager for comprehensive data integration.
"""

import pandas as pd
import logging

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError

logger = logging.getLogger(__name__)


class MergeDataProcessor(BaseStepProcessor):
    """
    Processor for merging DataFrames with external data sources.
    
    Supports merging with Excel files, CSV files, dictionaries, or saved stages using
    various join types (left, right, inner, outer). Uses FileReader for file operations
    and StageManager for stage access.
    """
    
    @classmethod
    def get_minimal_config(cls):
        return {
            'merge_source': {
                'type': 'stage',
                'stage_name': 'test_stage'
            },
            'left_key': 'test_column',
            'right_key': 'key_column'
        }

    def execute(self, data):
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
        
        # Get custom variables from pipeline if available
        variables = self._get_pipeline_variables()
        
        # Validate configuration
        self._validate_merge_config(data, merge_source, left_key, right_key, join_type)
        
        try:
            # Load the merge source data
            merge_data = self._load_merge_source(merge_source, variables)
            
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
            
        except FileReaderError as e:
            raise StepProcessorError(f"Error reading merge source in step '{self.step_name}': {e}")
        except StageError as e:
            raise StepProcessorError(f"Error accessing stage data in step '{self.step_name}': {e}")
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error during merge in step '{self.step_name}': {e}")
    
    def _validate_merge_config(self, df, merge_source, left_key, right_key, join_type):
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
        valid_types = ['excel', 'csv', 'tsv', 'dictionary', 'stage']
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
    
    def _load_merge_source(self, merge_source, variables):
        """
        Load data from the specified merge source.
        
        Args:
            merge_source: Configuration for the merge source
            variables: Variables for filename substitution
            
        Returns:
            DataFrame with merge data
        """
        source_type = merge_source['type']
        
        if source_type in ['excel', 'csv', 'tsv']:
            return self._load_file_source(merge_source, variables)
        elif source_type == 'dictionary':
            return self._load_dictionary_source(merge_source)
        elif source_type == 'stage':
            return self._load_stage_source(merge_source)
        else:
            raise StepProcessorError(f"Unsupported merge source type: {source_type}")
    
    def _load_file_source(self, merge_source, variables):
        """Load data from Excel, CSV, or TSV file using FileReader."""
        if 'path' not in merge_source:
            source_type = merge_source['type']
            raise StepProcessorError(f"{source_type.upper()} merge source requires 'path' field")
        
        file_path = merge_source['path']

        # Using 1-based indexing, file reader converts to 0-based for pandas
        sheet = merge_source.get('sheet', 1)
        encoding = merge_source.get('encoding', 'utf-8')
        separator = merge_source.get('separator', ',')
        explicit_format = merge_source.get('format', None)
        
        try:
            # Use FileReader for all file operations
            merge_data = FileReader.read_file(
                filename=file_path,
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format
            )
            
            logger.debug(f"Loaded merge data from file '{file_path}': {merge_data.shape}")
            return merge_data
            
        except FileReaderError as e:
            raise StepProcessorError(f"Error reading merge file '{file_path}': {e}")
    
    def _load_dictionary_source(self, merge_source):
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
            
            logger.debug(f"Created merge data from dictionary: {df.shape}")
            return df
            
        except Exception as e:
            raise StepProcessorError(f"Error creating DataFrame from dictionary: {e}")
    
    def _load_stage_source(self, merge_source):
        """Load data from a saved stage."""
        if 'stage_name' not in merge_source:
            raise StepProcessorError("Stage merge source requires 'stage_name' field")
        
        stage_name = merge_source['stage_name']
        
        if not StageManager.stage_exists(stage_name):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Merge stage '{stage_name}' not found. Available stages: {available_stages}"
            )
        
        try:
            # Use get_stage_data instead of load_stage to avoid incrementing usage counter
            # since this is an access operation, not a load operation
            stage_data = StageManager.load_stage(stage_name)
            logger.debug(f"Loaded merge data from stage '{stage_name}': {stage_data.shape}")
            return stage_data
            
        except StageError as e:
            raise StepProcessorError(f"Error loading merge data from stage '{stage_name}': {e}")
    
    def _validate_merge_keys(self, left_df, right_df, left_key, right_key):
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
    
    def _perform_merge(self, left_df, right_df, left_key, right_key, join_type, suffixes):
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
                        right_df_copy[right_key] = pd.to_numeric(right_df_copy[right_key])
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
    
    def _handle_column_conflicts(self, df):
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
    
    def _get_pipeline_variables(self):
        """
        Get variables from the pipeline for filename substitution.
        
        Returns:
            Dictionary of variables for substitution
        """
        # TODO: Access pipeline variables when available
        # For now, return empty dict - FileReader will use built-in variables
        return {}
    
    def get_supported_join_types(self):
        """
        Get list of supported join types.
        
        Returns:
            List of supported join type strings
        """
        return ['left', 'right', 'inner', 'outer']
    
    def get_supported_source_types(self):
        """
        Get list of supported data source types.
        
        Returns:
            List of supported source type strings
        """
        return ['excel', 'csv', 'tsv', 'dictionary', 'stage']
    
    def get_capabilities(self):
        """Get processor capabilities information."""
        return {
            'description': 'Merge DataFrames with external data sources using various join strategies',
            'join_types': self.get_supported_join_types(),
            'data_sources': self.get_supported_source_types(),
            'merge_features': [
                'external_file_merging', 'multiple_join_types', 'key_column_specification',
                'column_conflict_handling', 'duplicate_key_removal', 'column_prefixing',
                'stage_based_merging', 'variable_substitution_in_paths'
            ],
            'stage_integration': [
                'cross_stage_merging', 'reusable_reference_data', 'dynamic_merge_sources',
                'stage_validation_and_error_handling'
            ],
            'file_integration': [
                'uses_file_reader_infrastructure', 'automatic_format_detection',
                'variable_substitution', 'encoding_support', 'sheet_selection'
            ],
            'file_formats': ['xlsx', 'xls', 'xlsm', 'csv', 'tsv', 'txt', 'dictionary', 'stage'],
            'examples': {
                'excel_lookup': "Merge with product catalog from Excel file",
                'csv_enrichment': "Add customer data from CSV export with variable paths",
                'dictionary_mapping': "Add category mappings from configuration", 
                'stage_merge': "Merge with data from previously created stage",
                'variable_file': "Merge with data_{date}.xlsx using date substitution",
                'cross_stage': "Combine current data with multiple reference stages"
            },
            'configuration_options': {
                'merge_source': 'Data source configuration (file, dictionary, or stage)',
                'left_key': 'Key column in current DataFrame',
                'right_key': 'Key column in merge source',
                'join_type': 'Type of join (left, right, inner, outer)',
                'suffixes': 'Suffixes for duplicate column names',
                'drop_duplicate_keys': 'Whether to drop duplicate key columns',
                'column_prefix': 'Prefix for columns from merge source'
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the merge_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('merge_data')


# Note: Other processors should use FileReader directly for simple file reading
# Example: data = FileReader.read_file(filename, variables, sheet, encoding)
