"""
Aggregate data step processor for Excel automation recipes.

Handles grouping data and applying aggregation functions to create summary statistics
with support for:
- Stage-based aggregation configuration via StageManager integration
- File-based aggregation configuration with variable substitution via FileReader
- Dynamic aggregation updates and cross-reference workflows
- Multiple aggregation source types and validation features
"""

import pandas as pd
import logging

from typing import Any, Union

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class AggregateDataProcessor(BaseStepProcessor):
    """
    Processor for aggregating DataFrame data by groups.
    
    Supports grouping by one or multiple columns and applying various
    aggregation functions to create summary statistics. Enhanced with:
    - Stage-based aggregation configuration with dynamic updates
    - File-based aggregation configuration with variable substitution
    - Cross-reference aggregation with staged validation data
    - Optional stage saving of aggregation results
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
        
        # Load aggregation configuration from various sources
        group_by, aggregations = self._load_aggregation_configuration(data)
        
        # Get other configuration options
        keep_group_columns = self.get_config_value('keep_group_columns', True)
        sort_by_groups = self.get_config_value('sort_by_groups', True)
        reset_index = self.get_config_value('reset_index', True)
        save_to_stage = self.get_config_value('save_to_stage', None)
        stage_overwrite = self.get_config_value('stage_overwrite', False)
        stage_description = self.get_config_value('stage_description', '')
        
        # Apply variable substitution to configuration
        variables = getattr(self, '_variables', {})
        group_by, aggregations = self._apply_variable_substitution(group_by, aggregations, variables)
        
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
            
            # Save to stage if requested
            if save_to_stage:
                self._save_aggregation_to_stage(result_data, save_to_stage, stage_overwrite, stage_description)
            
            result_info = f"aggregated {len(data)} rows into {len(result_data)} groups"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error performing aggregation in step '{self.step_name}': {e}")
    
    def _load_aggregation_configuration(self, data: pd.DataFrame) -> tuple:
        """Load aggregation configuration from various sources."""
        
        # Check for aggregation_source first (new pattern)
        aggregation_source = self.get_config_value('aggregation_source', None)
        
        if aggregation_source:
            source_type = aggregation_source.get('type')
            
            if source_type == 'file':
                return self._load_aggregation_from_file(aggregation_source)
            elif source_type == 'stage':
                return self._load_aggregation_from_stage(aggregation_source)
            elif source_type == 'lookup':
                return self._load_aggregation_from_lookup(aggregation_source, data)
            else:
                raise StepProcessorError(f"Unsupported aggregation_source type: {source_type}")
        
        # Fall back to inline configuration (existing pattern)
        self.validate_required_fields(['group_by', 'aggregations'])
        group_by = self.get_config_value('group_by')
        aggregations = self.get_config_value('aggregations')
        
        return group_by, aggregations
    
    def _load_aggregation_from_file(self, source_config: dict) -> tuple:
        """Load aggregation configuration from external file."""
        
        required_fields = ['filename']
        for field in required_fields:
            if field not in source_config:
                raise StepProcessorError(f"File aggregation_source missing '{field}' field")
        
        filename = source_config['filename']
        sheet = source_config.get('sheet', 0)
        encoding = source_config.get('encoding', 'utf-8')
        separator = source_config.get('separator', ',')
        format_type = source_config.get('format', 'table')
        group_by_column = source_config.get('group_by_column', 'group_by')
        aggregations_column = source_config.get('aggregations_column', 'aggregations')
        
        try:
            # Get variables for substitution
            variables = getattr(self, '_variables', {})
            
            # Read configuration file
            config_data = FileReader.read_file(
                filename, 
                sheet=sheet,
                encoding=encoding,
                separator=separator
            )
            
            if format_type == 'table':
                # Configuration stored in tabular format
                return self._parse_tabular_aggregation_config(config_data, group_by_column, aggregations_column)
            else:
                raise StepProcessorError(f"Unsupported aggregation config format: {format_type}")
            
        except FileReaderError as e:
            raise StepProcessorError(f"Failed to load aggregation config from file '{filename}': {e}")
    
    def _load_aggregation_from_stage(self, source_config: dict) -> tuple:
        """Load aggregation configuration from named stage."""
        
        required_fields = ['stage_name']
        for field in required_fields:
            if field not in source_config:
                raise StepProcessorError(f"Stage aggregation_source missing '{field}' field")
        
        stage_name = source_config['stage_name']
        format_type = source_config.get('format', 'table')
        group_by_column = source_config.get('group_by_column', 'group_by')
        aggregations_column = source_config.get('aggregations_column', 'aggregations')
        filter_condition = source_config.get('filter_condition', None)
        
        # Check if stage exists
        if not StageManager.stage_exists(stage_name):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Aggregation config stage '{stage_name}' not found. "
                f"Available stages: {available_stages}"
            )
        
        try:
            config_data = StageManager.load_stage(stage_name)
            
            # Apply filter if specified
            if filter_condition:
                filter_column = filter_condition.get('column')
                filter_value = filter_condition.get('value')
                filter_operator = filter_condition.get('operator', 'equals')
                
                if filter_operator == 'equals':
                    config_data = config_data[config_data[filter_column] == filter_value]
                elif filter_operator == 'in':
                    config_data = config_data[config_data[filter_column].isin(filter_value)]
                # Add more operators as needed
            
            if format_type == 'table':
                return self._parse_tabular_aggregation_config(config_data, group_by_column, aggregations_column)
            else:
                raise StepProcessorError(f"Unsupported aggregation config format: {format_type}")
            
        except StageError as e:
            raise StepProcessorError(f"Failed to load aggregation config from stage '{stage_name}': {e}")
    
    def _load_aggregation_from_lookup(self, source_config: dict, data: pd.DataFrame) -> tuple:
        """Load aggregation configuration based on lookup/cross-reference with stage data."""
        
        required_fields = ['lookup_stage', 'lookup_key', 'group_by_column', 'aggregations_column']
        for field in required_fields:
            if field not in source_config:
                raise StepProcessorError(f"Lookup aggregation_source missing '{field}' field")
        
        lookup_stage = source_config['lookup_stage']
        lookup_key = source_config['lookup_key']
        group_by_column = source_config['group_by_column']
        aggregations_column = source_config['aggregations_column']
        data_key = source_config.get('data_key', lookup_key)
        filter_condition = source_config.get('filter_condition', None)
        
        # Check if lookup stage exists
        if not StageManager.stage_exists(lookup_stage):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Lookup stage '{lookup_stage}' not found. "
                f"Available stages: {available_stages}"
            )
        
        try:
            lookup_data = StageManager.load_stage(lookup_stage)
            
            # Apply filter if specified
            if filter_condition:
                filter_column = filter_condition.get('column')
                filter_value = filter_condition.get('value')
                filter_operator = filter_condition.get('operator', 'equals')
                
                if filter_operator == 'equals':
                    lookup_data = lookup_data[lookup_data[filter_column] == filter_value]
                elif filter_operator == 'in':
                    lookup_data = lookup_data[lookup_data[filter_column].isin(filter_value)]
            
            # Validate required columns exist
            required_columns = [lookup_key, group_by_column, aggregations_column]
            missing_columns = [col for col in required_columns if col not in lookup_data.columns]
            if missing_columns:
                available_columns = list(lookup_data.columns)
                raise StepProcessorError(
                    f"Required columns {missing_columns} not found in lookup stage '{lookup_stage}'. "
                    f"Available columns: {available_columns}"
                )
            
            # Get unique values from data for lookup
            if data_key not in data.columns:
                raise StepProcessorError(f"Data key column '{data_key}' not found in input data")
            
            unique_data_values = data[data_key].unique()
            
            # Filter lookup data to only include relevant configs
            relevant_configs = lookup_data[lookup_data[lookup_key].isin(unique_data_values)]
            
            if len(relevant_configs) == 0:
                raise StepProcessorError(f"No matching aggregation configs found in lookup stage")
            
            # For now, use the first matching config (could be enhanced to merge multiple configs)
            config_row = relevant_configs.iloc[0]
            group_by = config_row[group_by_column]
            aggregations_str = config_row[aggregations_column]
            
            # Parse aggregations (assuming JSON or comma-separated format)
            aggregations = self._parse_aggregations_string(aggregations_str)
            
            logger.debug(f"Loaded aggregation config from lookup stage '{lookup_stage}' for key '{config_row[lookup_key]}'")
            return group_by, aggregations
            
        except StageError as e:
            raise StepProcessorError(f"Failed to load aggregation config from lookup stage '{lookup_stage}': {e}")
    
    def _parse_tabular_aggregation_config(self, config_data: pd.DataFrame, group_by_column: str, aggregations_column: str) -> tuple:
        """Parse aggregation configuration from tabular data."""
        
        # Validate required columns
        if group_by_column not in config_data.columns:
            available_columns = list(config_data.columns)
            raise StepProcessorError(
                f"Group by column '{group_by_column}' not found in config data. "
                f"Available columns: {available_columns}"
            )
        
        if aggregations_column not in config_data.columns:
            available_columns = list(config_data.columns)
            raise StepProcessorError(
                f"Aggregations column '{aggregations_column}' not found in config data. "
                f"Available columns: {available_columns}"
            )
        
        # For now, use the first row (could be enhanced to handle multiple configs)
        if len(config_data) == 0:
            raise StepProcessorError("Aggregation config data is empty")
        
        first_row = config_data.iloc[0]
        group_by = first_row[group_by_column]
        aggregations_str = first_row[aggregations_column]
        
        # Parse aggregations
        aggregations = self._parse_aggregations_string(aggregations_str)
        
        return group_by, aggregations
    
    def _parse_aggregations_string(self, aggregations_str: str) -> list:
        """Parse aggregations from string format (JSON or simple format)."""
        
        import json
        
        # Try JSON first
        try:
            return json.loads(aggregations_str)
        except:
            pass
        
        # Try simple format: "column1:sum:Total_Col1,column2:mean:Avg_Col2"
        try:
            aggregations = []
            parts = aggregations_str.split(',')
            
            for part in parts:
                components = part.strip().split(':')
                if len(components) >= 2:
                    agg_config = {
                        'column': components[0].strip(),
                        'function': components[1].strip()
                    }
                    if len(components) >= 3:
                        agg_config['output_name'] = components[2].strip()
                    
                    aggregations.append(agg_config)
            
            return aggregations
        except:
            raise StepProcessorError(f"Could not parse aggregations string: {aggregations_str}")
    
    def _apply_variable_substitution(self, group_by: Union[str, list], aggregations: list, variables: dict) -> tuple:
        """Apply variable substitution to aggregation configuration."""
        
        if not variables:
            return group_by, aggregations
        
        # Import here to avoid circular imports
        from excel_recipe_processor.core.variable_substitution import substitute_variables
        
        # Apply substitution to group_by columns
        if isinstance(group_by, str):
            group_by = substitute_variables(group_by, custom_variables=variables)
        elif isinstance(group_by, list):
            group_by = [substitute_variables(col, custom_variables=variables) for col in group_by]
        
        # Apply substitution to aggregation configs
        substituted_aggregations = []
        for agg_config in aggregations:
            substituted_config = agg_config.copy()
            
            for key in ['column', 'output_name', 'new_column_name']:
                if key in substituted_config:
                    substituted_config[key] = substitute_variables(substituted_config[key], custom_variables=variables)
            
            substituted_aggregations.append(substituted_config)
        
        return group_by, substituted_aggregations
    
    def _save_aggregation_to_stage(self, result_data: pd.DataFrame, stage_name: str, overwrite: bool, description: str) -> None:
        """Save aggregation results to a named stage."""
        
        try:
            # Use provided description or create default
            if not description:
                description = f"Aggregated data from step '{self.step_name}'"
            
            StageManager.save_stage(
                stage_name, 
                result_data.copy(), 
                overwrite=overwrite,
                description=description,
                step_name=self.step_name
            )
            
            logger.info(f"Saved aggregation results to stage '{stage_name}' ({len(result_data)} rows)")
            
        except StageError as e:
            raise StepProcessorError(f"Failed to save aggregation results to stage '{stage_name}': {e}")
    
    def _validate_aggregation_config(self, df: pd.DataFrame, group_by: Union[str, list], aggregations: list) -> None:
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
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Group by column '{col}' not found in data. "
                    f"Available columns: {available_columns}"
                )
        
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
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Aggregation column '{column}' not found in data. "
                    f"Available columns: {available_columns}"
                )
            
            if function not in self.get_supported_functions():
                supported = ', '.join(self.get_supported_functions())
                raise StepProcessorError(
                    f"Unsupported aggregation function '{function}'. "
                    f"Supported functions: {supported}"
                )
    
    def _perform_aggregation(self, df: pd.DataFrame, group_by: Union[str, list], 
                           aggregations: list, keep_group_columns: bool,
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
            # Support both 'new_column_name' and 'output_name' for backward compatibility
            new_name = agg.get('new_column_name', agg.get('output_name', f"{column}_{function}"))
            
            # Handle multiple aggregations on the same column
            if column not in agg_dict:
                agg_dict[column] = []
            
            agg_dict[column].append(function)
            
            # Track what to rename columns to
            current_name = f"{column}_{function}" if len(agg_dict[column]) > 1 else column
            column_renames[current_name] = new_name
        
        # Perform the aggregation
        result = grouped.agg(agg_dict)
        
        # Flatten multi-level columns if necessary
        if result.columns.nlevels > 1:
            result.columns = ['_'.join(col).strip() for col in result.columns.values]
        
        # Apply column renames
        rename_dict = {}
        for old_name, new_name in column_renames.items():
            # Find the actual column name in result
            matching_cols = [col for col in result.columns if old_name in col]
            if matching_cols:
                rename_dict[matching_cols[0]] = new_name
        
        if rename_dict:
            result = result.rename(columns=rename_dict)
        
        # Reset index if requested (brings group columns back as regular columns)
        if reset_index:
            result = result.reset_index()
        
        # Remove group columns if not wanted
        if not keep_group_columns and reset_index:
            result = result.drop(columns=group_by, errors='ignore')
        
        return result
    
    # =============================================================================
    # ENHANCED CAPABILITIES AND ANALYSIS METHODS
    # =============================================================================
    
    def get_supported_functions(self) -> list:
        """Get list of supported aggregation functions."""
        return [
            'sum', 'mean', 'median', 'min', 'max', 'count', 'nunique', 'std', 'var',
            'first', 'last', 'size', 'sem', 'mad', 'prod', 'quantile'
        ]
    
    def get_supported_source_types(self) -> list:
        """Get list of supported aggregation source types."""
        return ['inline', 'file', 'stage', 'lookup']
    
    def get_supported_file_formats(self) -> list:
        """Get list of supported file formats for aggregation configs."""
        return ['xlsx', 'csv', 'tsv']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities and features."""
        return {
            'description': 'Groups data and calculates summary statistics',
            'aggregation_functions': self.get_supported_functions(),
            'source_types': self.get_supported_source_types(),
            'file_formats': self.get_supported_file_formats(),
            'grouping_features': [
                'single_column_grouping',
                'multi_column_grouping',
                'sorting_control',
                'index_management'
            ],
            'output_features': [
                'custom_column_naming',
                'group_column_inclusion',
                'stage_saving',
                'multiple_functions_per_column'
            ],
            'helper_methods': [
                'create_summary_aggregation',
                'create_crosstab_aggregation',
                'analyze_aggregation_results'
            ],
            'integration_features': [
                'stage_manager_integration',
                'file_reader_integration', 
                'variable_substitution',
                'configuration_from_external_sources'
            ]
        }
    
    def create_summary_aggregation(self, df: pd.DataFrame, group_columns: Union[str, list], 
                                 numeric_columns: list = None) -> pd.DataFrame:
        """
        Create a standard summary aggregation with common statistics.
        
        Args:
            df: DataFrame to aggregate
            group_columns: Column(s) to group by
            numeric_columns: Numeric columns to summarize (auto-detect if None)
            
        Returns:
            DataFrame with summary statistics
        """
        if numeric_columns is None:
            numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        
        aggregations = []
        for col in numeric_columns:
            aggregations.extend([
                {'column': col, 'function': 'count', 'output_name': f'{col}_count'},
                {'column': col, 'function': 'sum', 'output_name': f'{col}_total'},
                {'column': col, 'function': 'mean', 'output_name': f'{col}_average'},
                {'column': col, 'function': 'min', 'output_name': f'{col}_minimum'},
                {'column': col, 'function': 'max', 'output_name': f'{col}_maximum'}
            ])
        
        return self._perform_aggregation(df, group_columns, aggregations, True, True, True)
    
    def create_crosstab_aggregation(self, df: pd.DataFrame, row_columns: Union[str, list], 
                                  col_columns: Union[str, list], value_column: str,
                                  agg_function: str = 'sum') -> pd.DataFrame:
        """
        Create a crosstab-style aggregation.
        
        Args:
            df: DataFrame to aggregate
            row_columns: Column(s) for rows
            col_columns: Column(s) for columns
            value_column: Column to aggregate
            agg_function: Aggregation function to use
            
        Returns:
            DataFrame in crosstab format
        """
        # This is a simplified version - could be enhanced with pivot_table functionality
        if isinstance(row_columns, str):
            row_columns = [row_columns]
        if isinstance(col_columns, str):
            col_columns = [col_columns]
        
        group_by = row_columns + col_columns
        aggregations = [{'column': value_column, 'function': agg_function, 'output_name': f'{value_column}_{agg_function}'}]
        
        result = self._perform_aggregation(df, group_by, aggregations, True, True, True)
        
        # Could add pivot functionality here for true crosstab format
        return result
    
    def analyze_aggregation_results(self, result_df: pd.DataFrame, group_columns: list) -> dict:
        """
        Analyze aggregation results and provide insights.
        
        Args:
            result_df: Aggregated DataFrame
            group_columns: Original grouping columns
            
        Returns:
            Dictionary with analysis insights
        """
        analysis = {
            'total_groups': len(result_df),
            'group_columns': group_columns,
            'aggregated_columns': [col for col in result_df.columns if col not in group_columns],
            'summary': {}
        }
        
        # Analyze numeric aggregated columns
        numeric_cols = result_df.select_dtypes(include=['number']).columns
        aggregated_numeric = [col for col in numeric_cols if col not in group_columns]
        
        for col in aggregated_numeric:
            analysis['summary'][col] = {
                'min': result_df[col].min(),
                'max': result_df[col].max(),
                'mean': result_df[col].mean(),
                'total': result_df[col].sum() if 'total' in col.lower() or 'sum' in col.lower() else None
            }
        
        return analysis
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the aggregate_data processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('aggregate_data')
