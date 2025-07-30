"""
Create stage step processor for Excel automation recipes.

Handles creating stages from inline data with support for lists, tables, and dictionaries.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError

logger = logging.getLogger(__name__)


class CreateStageProcessor(BaseStepProcessor):
    """
    Processor for creating stages from inline data definitions.
    
    Supports creating stages with small amounts of reference data directly
    in the recipe YAML, useful for lookup tables, filter lists, and 
    configuration data without requiring external files.
    """
    
    # Data size limits to prevent YAML bloat
    MAX_LIST_ITEMS = 100
    MAX_TABLE_ROWS = 200  
    MAX_DICT_ENTRIES = 150
    
    # Warning thresholds (75% of max)
    WARN_LIST_ITEMS = 75
    WARN_TABLE_ROWS = 150
    WARN_DICT_ENTRIES = 112
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'stage_name': 'test_stage',
            'description': 'Test stage for validation',
            'data': {
                'format': 'list',
                'column': 'test_column',
                'values': ['item1', 'item2', 'item3']
            }
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute stage creation while passing input data through unchanged.
        
        Args:
            data: Input pandas DataFrame (passed through unchanged)
            
        Returns:
            Original DataFrame unchanged (stage creation is a side effect)
            
        Raises:
            StepProcessorError: If stage creation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame (even though we pass it through)
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Create stage step '{self.step_name}' requires a pandas DataFrame")
        
        # Validate required configuration
        self.validate_required_fields(['stage_name', 'data'])
        
        stage_name = self.get_config_value('stage_name')
        data_config = self.get_config_value('data')
        overwrite = self.get_config_value('overwrite', False)
        description = self.get_config_value('description', '')
        
        # Validate configuration
        self._validate_stage_config(stage_name, data_config, overwrite, description)
        
        try:
            # Create DataFrame from inline data
            stage_data = self._create_dataframe_from_config(data_config, stage_name)
            
            # Save to pipeline stages
            self._save_stage_to_pipeline(stage_name, stage_data, overwrite, description)
            
            rows_created = len(stage_data)
            cols_created = len(stage_data.columns)
            result_info = f"created stage '{stage_name}' with {rows_created} rows, {cols_created} columns"
            self.log_step_complete(result_info)
            
            # Return original data unchanged
            return data
            
        except (StepProcessorError, StageError):
            raise
        except (ValueError, KeyError, TypeError) as e:
            raise StepProcessorError(f"Error creating stage in step '{self.step_name}': {e}")
    
    def _validate_stage_config(self, stage_name: str, data_config: dict, overwrite: bool, description: str) -> None:
        """
        Validate stage creation configuration.
        
        Args:
            stage_name: Name of stage to create
            data_config: Data configuration dictionary
            overwrite: Whether to allow overwriting existing stages
            description: Optional description for the stage
        """
        # Validate stage name
        if not isinstance(stage_name, str) or not stage_name.strip():
            raise StepProcessorError("'stage_name' must be a non-empty string")
        
        # Validate description if provided
        if description and not isinstance(description, str):
            raise StepProcessorError("'description' must be a string")
        
        # Validate data config
        if not isinstance(data_config, dict):
            raise StepProcessorError("'data' must be a dictionary")
        
        if 'format' not in data_config:
            raise StepProcessorError("Data configuration missing required 'format' field")
        
        data_format = data_config['format']
        valid_formats = ['list', 'table', 'dictionary']
        if data_format not in valid_formats:
            raise StepProcessorError(f"Data format '{data_format}' not supported. Valid formats: {valid_formats}")
        
        # Validate format-specific requirements
        self._validate_format_specific_config(data_config, data_format)
    
    def _validate_format_specific_config(self, data_config: dict, data_format: str) -> None:
        """
        Validate configuration specific to each data format.
        
        Args:
            data_config: Data configuration dictionary
            data_format: Format type ('list', 'table', 'dictionary')
        """
        if data_format == 'list':
            # List format requires 'column' and 'values'
            if 'column' not in data_config:
                raise StepProcessorError("List format requires 'column' field")
            if 'values' not in data_config:
                raise StepProcessorError("List format requires 'values' field")
            
            values = data_config['values']
            if not isinstance(values, list):
                raise StepProcessorError("List format 'values' must be a list")
            
            # Check size limits
            if len(values) > self.MAX_LIST_ITEMS:
                raise StepProcessorError(
                    f"List format has {len(values)} items, maximum allowed is {self.MAX_LIST_ITEMS}. "
                    f"Consider using an external CSV file for large datasets."
                )
            elif len(values) > self.WARN_LIST_ITEMS:
                logger.warning(
                    f"List format has {len(values)} items, approaching limit of {self.MAX_LIST_ITEMS}. "
                    f"Consider using an external file for better maintainability."
                )
        
        elif data_format == 'table':
            # Table format requires 'columns' and 'rows'
            if 'columns' not in data_config:
                raise StepProcessorError("Table format requires 'columns' field")
            if 'rows' not in data_config:
                raise StepProcessorError("Table format requires 'rows' field")
            
            columns = data_config['columns']
            rows = data_config['rows']
            
            if not isinstance(columns, list):
                raise StepProcessorError("Table format 'columns' must be a list")
            if not isinstance(rows, list):
                raise StepProcessorError("Table format 'rows' must be a list")
            
            # Check size limits
            if len(rows) > self.MAX_TABLE_ROWS:
                raise StepProcessorError(
                    f"Table format has {len(rows)} rows, maximum allowed is {self.MAX_TABLE_ROWS}. "
                    f"Consider using an external Excel or CSV file for large datasets."
                )
            elif len(rows) > self.WARN_TABLE_ROWS:
                logger.warning(
                    f"Table format has {len(rows)} rows, approaching limit of {self.MAX_TABLE_ROWS}. "
                    f"Consider using an external file for better maintainability."
                )
            
            # Validate row structure
            for i, row in enumerate(rows):
                if not isinstance(row, list):
                    raise StepProcessorError(f"Table row {i+1} must be a list")
                if len(row) != len(columns):
                    raise StepProcessorError(
                        f"Table row {i+1} has {len(row)} values but {len(columns)} columns defined"
                    )
        
        elif data_format == 'dictionary':
            # Dictionary format requires 'key_column', 'value_column', and 'data'
            required_fields = ['key_column', 'value_column', 'data']
            for field in required_fields:
                if field not in data_config:
                    raise StepProcessorError(f"Dictionary format requires '{field}' field")
            
            dict_data = data_config['data']
            if not isinstance(dict_data, dict):
                raise StepProcessorError("Dictionary format 'data' must be a dictionary")
            
            # Check size limits
            if len(dict_data) > self.MAX_DICT_ENTRIES:
                raise StepProcessorError(
                    f"Dictionary format has {len(dict_data)} entries, maximum allowed is {self.MAX_DICT_ENTRIES}. "
                    f"Consider using an external Excel or CSV file for large datasets."
                )
            elif len(dict_data) > self.WARN_DICT_ENTRIES:
                logger.warning(
                    f"Dictionary format has {len(dict_data)} entries, approaching limit of {self.MAX_DICT_ENTRIES}. "
                    f"Consider using an external file for better maintainability."
                )
    
    def _create_dataframe_from_config(self, data_config: dict, stage_name: str) -> pd.DataFrame:
        """
        Create a pandas DataFrame from the data configuration.
        
        Args:
            data_config: Data configuration dictionary
            stage_name: Name of the stage (for error reporting)
            
        Returns:
            DataFrame with the inline data
        """
        data_format = data_config['format']
        
        try:
            if data_format == 'list':
                return self._create_list_dataframe(data_config)
            elif data_format == 'table':
                return self._create_table_dataframe(data_config)
            elif data_format == 'dictionary':
                return self._create_dictionary_dataframe(data_config)
            else:
                raise StepProcessorError(f"Unsupported data format: {data_format}")
                
        except StepProcessorError:
            raise
        except (ValueError, KeyError, TypeError) as e:
            raise StepProcessorError(f"Error creating DataFrame for stage '{stage_name}': {e}")
    
    def _create_list_dataframe(self, data_config: dict) -> pd.DataFrame:
        """Create DataFrame from list format data."""
        column_name = data_config['column']
        values = data_config['values']
        
        df = pd.DataFrame({column_name: values})
        
        logger.debug(f"Created list DataFrame: {len(df)} rows, column '{column_name}'")
        return df
    
    def _create_table_dataframe(self, data_config: dict) -> pd.DataFrame:
        """Create DataFrame from table format data."""
        columns = data_config['columns']
        rows = data_config['rows']
        
        df = pd.DataFrame(rows, columns=columns)
        
        logger.debug(f"Created table DataFrame: {len(df)} rows, {len(df.columns)} columns")
        return df
    
    def _create_dictionary_dataframe(self, data_config: dict) -> pd.DataFrame:
        """Create DataFrame from dictionary format data."""
        key_column = data_config['key_column']
        value_column = data_config['value_column']
        dict_data = data_config['data']
        
        # Convert dictionary to DataFrame
        keys = list(dict_data.keys())
        values = list(dict_data.values())
        
        df = pd.DataFrame({
            key_column: keys,
            value_column: values
        })
        
        logger.debug(f"Created dictionary DataFrame: {len(df)} rows, columns '{key_column}', '{value_column}'")
        return df
    
    def _save_stage_to_pipeline(self, stage_name: str, stage_data: pd.DataFrame, overwrite: bool, description: str) -> None:
        """
        Save the created stage using the StageManager.
        
        Args:
            stage_name: Name of the stage
            stage_data: DataFrame to save as stage
            overwrite: Whether to allow overwriting existing stages
            description: Optional description for the stage
        """
        try:
            # Use the StageManager to save the stage
            StageManager.save_stage(
                stage_name=stage_name,
                data=stage_data,
                overwrite=overwrite,
                description=description,
                step_name=self.step_name
            )
            
        except StageError as e:
            raise StepProcessorError(f"Failed to save stage '{stage_name}': {e}")
    
    def get_supported_formats(self) -> list:
        """
        Get list of supported data formats.
        
        Returns:
            List of supported format strings
        """
        return ['list', 'table', 'dictionary']
    
    def get_size_limits(self) -> dict:
        """
        Get size limits for different data formats.
        
        Returns:
            Dictionary with size limits for each format
        """
        return {
            'list_items': self.MAX_LIST_ITEMS,
            'table_rows': self.MAX_TABLE_ROWS,
            'dictionary_entries': self.MAX_DICT_ENTRIES,
            'warning_thresholds': {
                'list_items': self.WARN_LIST_ITEMS,
                'table_rows': self.WARN_TABLE_ROWS,
                'dictionary_entries': self.WARN_DICT_ENTRIES
            }
        }
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Create stages from inline data with support for lists, tables, and dictionaries',
            'data_formats': self.get_supported_formats(),
            'size_limits': self.get_size_limits(),
            'inline_features': [
                'reference_lists', 'lookup_tables', 'configuration_mappings',
                'size_validation', 'format_validation', 'overwrite_protection',
                'stage_descriptions', 'usage_tracking', 'memory_monitoring'
            ],
            'integration': [
                'stages_module_integration', 'pipeline_stage_management',
                'cross_processor_stage_access', 'automatic_cleanup'
            ],
            'use_cases': [
                'filter_criteria', 'lookup_data', 'category_mappings',
                'validation_lists', 'test_data', 'configuration_driven_processing'
            ],
            'examples': {
                'approved_customers': "List of approved customer IDs for filtering",
                'region_mapping': "Table mapping states to regions",
                'tier_lookup': "Dictionary mapping customer IDs to tier levels",
                'test_scenarios': "Inline test data for recipe development"
            }
        }


class CreateStageUtils:
    """
    Utility functions for working with inline stage data.
    
    These utilities can be used by other processors that need to work
    with inline data or validate data formats.
    """
    
    @staticmethod
    def validate_inline_data_size(data_config: dict) -> tuple[bool, str]:
        """
        Validate the size of inline data configuration.
        
        Args:
            data_config: Data configuration dictionary
            
        Returns:
            Tuple of (is_valid, message)
        """
        data_format = data_config.get('format')
        
        if data_format == 'list':
            values = data_config.get('values', [])
            if len(values) > CreateStageProcessor.MAX_LIST_ITEMS:
                return False, f"List has {len(values)} items, maximum is {CreateStageProcessor.MAX_LIST_ITEMS}"
        
        elif data_format == 'table':
            rows = data_config.get('rows', [])
            if len(rows) > CreateStageProcessor.MAX_TABLE_ROWS:
                return False, f"Table has {len(rows)} rows, maximum is {CreateStageProcessor.MAX_TABLE_ROWS}"
        
        elif data_format == 'dictionary':
            dict_data = data_config.get('data', {})
            if len(dict_data) > CreateStageProcessor.MAX_DICT_ENTRIES:
                return False, f"Dictionary has {len(dict_data)} entries, maximum is {CreateStageProcessor.MAX_DICT_ENTRIES}"
        
        return True, "Data size is within limits"
    
    @staticmethod
    def estimate_yaml_size(data_config: dict) -> int:
        """
        Estimate the YAML size of inline data configuration.
        
        Args:
            data_config: Data configuration dictionary
            
        Returns:
            Estimated size in characters
        """
        try:
            import yaml
            yaml_str = yaml.dump(data_config, default_flow_style=False)
            return len(yaml_str)
        except (ImportError, AttributeError, TypeError, ValueError):
            # Fallback estimation if yaml module unavailable or data not serializable
            data_format = data_config.get('format')
            if data_format == 'list':
                return len(data_config.get('values', [])) * 20  # Rough estimate
            elif data_format == 'table':
                rows = data_config.get('rows', [])
                cols = len(data_config.get('columns', []))
                return len(rows) * cols * 15  # Rough estimate
            elif data_format == 'dictionary':
                return len(data_config.get('data', {})) * 25  # Rough estimate
            return 100  # Default estimate
    
    @staticmethod
    def suggest_external_file_format(data_config: dict) -> str:
        """
        Suggest the best external file format for large inline data.
        
        Args:
            data_config: Data configuration dictionary
            
        Returns:
            Suggested file format ('csv', 'xlsx', etc.)
        """
        data_format = data_config.get('format')
        
        if data_format == 'list':
            return 'csv'  # Single column CSV
        elif data_format == 'table':
            return 'xlsx'  # Multi-column Excel
        elif data_format == 'dictionary':
            return 'csv'  # Two-column CSV
        else:
            return 'xlsx'  # Default to Excel
