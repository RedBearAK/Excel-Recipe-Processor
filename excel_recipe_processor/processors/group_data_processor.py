"""
Group data step processor for Excel automation recipes.

Handles grouping individual values into categories with support for:
- Stage-based group definitions via StageManager integration
- File-based group definitions with variable substitution via FileReader
- Dynamic group updates and cross-reference workflows
- Multiple grouping source types and validation features
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class GroupDataProcessor(BaseStepProcessor):
    """
    Processor for grouping individual values into categories.
    
    Takes a source column and maps individual values to group categories
    using various source types including:
    - Inline group definitions (existing functionality)
    - Stage-based group definitions with dynamic updates
    - File-based group definitions with variable substitution
    - Cross-reference grouping with staged validation data
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'source_column': 'test_column',
            'groups': {
                'Group1': ['value1', 'value2'],
                'Group2': ['value3', 'value4']
            }
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the data grouping operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to group
            
        Returns:
            DataFrame with new group column added
            
        Raises:
            StepProcessorError: If grouping fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Group data step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration - groups can come from various sources
        self.validate_required_fields(['source_column'])
        
        source_column = self.get_config_value('source_column')
        target_column = self.get_config_value('target_column', f'{source_column}_Group')
        replace_source = self.get_config_value('replace_source', False)
        unmatched_action = self.get_config_value('unmatched_action', 'keep_original')
        unmatched_value = self.get_config_value('unmatched_value', 'Other')
        case_sensitive = self.get_config_value('case_sensitive', True)
        save_to_stage = self.get_config_value('save_to_stage', None)
        stage_description = self.get_config_value('stage_description', '')
        
        # Load group definitions from various sources
        groups = self._load_group_definitions()
        
        # Validate configuration
        self._validate_grouping_config(data, source_column, groups, target_column)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Create the grouping mapping
            value_to_group_map = self._create_mapping(groups, case_sensitive)
            
            # Apply the grouping
            result_data = self._apply_grouping(
                result_data, source_column, target_column, value_to_group_map,
                unmatched_action, unmatched_value, case_sensitive
            )
            
            # Replace source column if requested
            if replace_source and target_column != source_column:
                result_data = result_data.drop(columns=[source_column])
                result_data = result_data.rename(columns={target_column: source_column})
                final_column = source_column
            else:
                final_column = target_column
            
            # Save results to stage if requested
            if save_to_stage:
                self._save_grouping_to_stage(result_data, save_to_stage, stage_description)
            
            # Count successful groupings
            grouped_count = len(result_data)
            unique_groups = result_data[final_column].nunique()
            
            result_info = f"grouped {grouped_count} rows into {unique_groups} categories"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error applying grouping in step '{self.step_name}': {e}")
    
    def _load_group_definitions(self) -> dict:
        """
        Load group definitions from various sources.
        
        Returns:
            Dictionary mapping group names to lists of values
        """
        # Check for different group definition sources in priority order
        
        # 1. Inline groups (traditional approach)
        if self.step_config.get('groups'):
            return self.get_config_value('groups')
        
        # 2. Stage-based groups
        if self.step_config.get('groups_source'):
            groups_source = self.get_config_value('groups_source')
            return self._load_groups_from_source(groups_source)
        
        # 3. File-based groups (backward compatibility)
        if self.step_config.get('groups_file'):
            groups_file = self.get_config_value('groups_file')
            return self._load_groups_from_file(groups_file)
        
        # 4. Predefined group sets
        if self.step_config.get('predefined_groups'):
            predefined_type = self.get_config_value('predefined_groups')
            return self._load_predefined_groups(predefined_type)
        
        raise StepProcessorError(
            f"No group definitions found in step '{self.step_name}'. "
            f"Provide one of: 'groups', 'groups_source', 'groups_file', or 'predefined_groups'"
        )
    
    def _load_groups_from_source(self, source_config) -> dict:
        """Load group definitions from various source types."""
        
        if isinstance(source_config, dict):
            source_type = source_config.get('type')
            
            if source_type == 'stage':
                return self._load_groups_from_stage(source_config)
            elif source_type == 'file':
                return self._load_groups_from_file_config(source_config)
            elif source_type == 'lookup':
                return self._load_groups_from_lookup(source_config)
            else:
                raise StepProcessorError(f"Unsupported groups_source type: {source_type}")
        
        elif isinstance(source_config, str):
            # Treat as stage name for backward compatibility
            return self._load_groups_from_stage({'stage_name': source_config})
        
        else:
            raise StepProcessorError(
                f"Invalid groups_source format. Expected dict or string, got {type(source_config)}"
            )
    
    def _load_groups_from_stage(self, source_config: dict) -> dict:
        """Load group definitions from a stage."""
        
        if 'stage_name' not in source_config:
            raise StepProcessorError("Stage groups_source missing 'stage_name' field")
        
        stage_name = source_config['stage_name']
        group_name_column = source_config.get('group_name_column', 'Group_Name')
        values_column = source_config.get('values_column', 'Values')
        format_type = source_config.get('format', 'wide')  # 'wide' or 'long'
        
        # Check if stage exists
        if not StageManager.stage_exists(stage_name):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Groups stage '{stage_name}' not found. Available stages: {available_stages}"
            )
        
        try:
            stage_data = StageManager.load_stage(stage_name)
            
            if format_type == 'wide':
                # Each column represents a group, values are the items in that group
                groups = {}
                for column in stage_data.columns:
                    # Filter out null values and convert to list
                    values = stage_data[column].dropna().astype(str).tolist()
                    if values:  # Only include non-empty groups
                        groups[column] = values
                
            elif format_type == 'long':
                # Two columns: group name and values
                if group_name_column not in stage_data.columns:
                    available_columns = list(stage_data.columns)
                    raise StepProcessorError(
                        f"Group name column '{group_name_column}' not found in stage '{stage_name}'. "
                        f"Available columns: {available_columns}"
                    )
                
                if values_column not in stage_data.columns:
                    available_columns = list(stage_data.columns)
                    raise StepProcessorError(
                        f"Values column '{values_column}' not found in stage '{stage_name}'. "
                        f"Available columns: {available_columns}"
                    )
                
                # Group by group name and collect values
                groups = {}
                for group_name, group_data in stage_data.groupby(group_name_column):
                    values = group_data[values_column].dropna().astype(str).tolist()
                    if values:
                        groups[str(group_name)] = values
            
            else:
                raise StepProcessorError(f"Unsupported stage format: {format_type}")
            
            logger.debug(f"Loaded {len(groups)} groups from stage '{stage_name}'")
            return groups
            
        except StageError as e:
            raise StepProcessorError(f"Failed to load groups from stage '{stage_name}': {e}")
    
    def _load_groups_from_file_config(self, source_config: dict) -> dict:
        """Load group definitions from file with full configuration."""
        
        if 'filename' not in source_config:
            raise StepProcessorError("File groups_source missing 'filename' field")
        
        filename = source_config['filename']
        sheet = source_config.get('sheet', 0)
        encoding = source_config.get('encoding', 'utf-8')
        separator = source_config.get('separator', ',')
        explicit_format = source_config.get('format_type', None)
        group_name_column = source_config.get('group_name_column', 'Group_Name')
        values_column = source_config.get('values_column', 'Values')
        file_format = source_config.get('format', 'wide')  # 'wide' or 'long'
        
        return self._load_groups_from_file(
            filename, sheet, encoding, separator, explicit_format,
            group_name_column, values_column, file_format
        )
    
    def _load_groups_from_file(self, filename, sheet=0, encoding='utf-8', separator=',',
                             explicit_format=None, group_name_column='Group_Name', 
                             values_column='Values', file_format='wide') -> dict:
        """Load group definitions from file using FileReader."""
        
        # Get custom variables for substitution (from pipeline if available)
        variables = getattr(self, 'variables', None)
        
        try:
            groups_data = FileReader.read_file(
                filename=filename,
                variables=variables,
                sheet=sheet,
                encoding=encoding,
                separator=separator,
                explicit_format=explicit_format
            )
            
            if file_format == 'wide':
                # Each column represents a group
                groups = {}
                for column in groups_data.columns:
                    values = groups_data[column].dropna().astype(str).tolist()
                    if values:
                        groups[column] = values
            
            elif file_format == 'long':
                # Two columns: group name and values
                if group_name_column not in groups_data.columns:
                    available_columns = list(groups_data.columns)
                    raise StepProcessorError(
                        f"Group name column '{group_name_column}' not found in file '{filename}'. "
                        f"Available columns: {available_columns}"
                    )
                
                if values_column not in groups_data.columns:
                    available_columns = list(groups_data.columns)
                    raise StepProcessorError(
                        f"Values column '{values_column}' not found in file '{filename}'. "
                        f"Available columns: {available_columns}"
                    )
                
                groups = {}
                for group_name, group_data in groups_data.groupby(group_name_column):
                    values = group_data[values_column].dropna().astype(str).tolist()
                    if values:
                        groups[str(group_name)] = values
            
            else:
                raise StepProcessorError(f"Unsupported file format: {file_format}")
            
            logger.debug(f"Loaded {len(groups)} groups from file '{filename}'")
            return groups
            
        except FileReaderError as e:
            raise StepProcessorError(f"Failed to load groups from file '{filename}': {e}")
    
    def _load_groups_from_lookup(self, source_config: dict) -> dict:
        """Load group definitions based on lookup/cross-reference with stage data."""
        
        required_fields = ['lookup_stage', 'lookup_key', 'group_column', 'values_column']
        for field in required_fields:
            if field not in source_config:
                raise StepProcessorError(f"Lookup groups_source missing '{field}' field")
        
        lookup_stage = source_config['lookup_stage']
        lookup_key = source_config['lookup_key']
        group_column = source_config['group_column']
        values_column = source_config['values_column']
        filter_condition = source_config.get('filter_condition', None)
        
        # Check if lookup stage exists
        if not StageManager.stage_exists(lookup_stage):
            available_stages = list(StageManager.list_stages().keys())
            raise StepProcessorError(
                f"Lookup stage '{lookup_stage}' not found. Available stages: {available_stages}"
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
                # Add more operators as needed
            
            # Validate required columns exist
            required_columns = [group_column, values_column]
            missing_columns = [col for col in required_columns if col not in lookup_data.columns]
            if missing_columns:
                available_columns = list(lookup_data.columns)
                raise StepProcessorError(
                    f"Required columns {missing_columns} not found in lookup stage '{lookup_stage}'. "
                    f"Available columns: {available_columns}"
                )
            
            # Create groups from lookup data
            groups = {}
            for group_name, group_data in lookup_data.groupby(group_column):
                values = group_data[values_column].dropna().astype(str).tolist()
                if values:
                    groups[str(group_name)] = values
            
            logger.debug(f"Loaded {len(groups)} groups from lookup stage '{lookup_stage}'")
            return groups
            
        except StageError as e:
            raise StepProcessorError(f"Failed to load groups from lookup stage '{lookup_stage}': {e}")
    
    def _load_predefined_groups(self, predefined_type: str) -> dict:
        """Load predefined group sets."""
        
        predefined_groups = {
            'van_report_regions': {
                'Bristol Bay': ['Dillingham', 'False Pass', 'Naknek', 'Naknek West', 'Wood River'],
                'Kodiak': ['Kodiak', 'Kodiak West'],
                'PWS': ['Cordova', 'Seward', 'Valdez'],
                'SE': ['Craig', 'Ketchikan', 'Petersburg', 'Sitka']
            },
            'us_regions': {
                'West': ['CA', 'OR', 'WA', 'NV', 'AZ', 'UT', 'CO', 'NM'],
                'Midwest': ['IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI'],
                'South': ['AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'OK', 'SC', 'TN', 'TX', 'VA', 'WV'],
                'Northeast': ['CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT']
            },
            'product_categories': {
                'Electronics': ['laptop', 'phone', 'tablet', 'camera', 'headphones'],
                'Clothing': ['shirt', 'pants', 'dress', 'shoes', 'jacket'],
                'Home': ['furniture', 'kitchenware', 'bedding', 'decor', 'appliances']
            }
        }
        
        if predefined_type not in predefined_groups:
            available_types = list(predefined_groups.keys())
            raise StepProcessorError(
                f"Unknown predefined group type: {predefined_type}. "
                f"Available types: {available_types}"
            )
        
        return predefined_groups[predefined_type]
    
    def _save_grouping_to_stage(self, data: pd.DataFrame, stage_name: str, description: str) -> None:
        """Save grouping results to a stage."""
        
        try:
            stage_description = description or f"Grouping results from step '{self.step_name}'"
            
            StageManager.save_stage(
                stage_name=stage_name,
                data=data,
                description=stage_description,
                step_name=self.step_name
            )
            
            logger.debug(f"Saved grouping results to stage '{stage_name}'")
            
        except StageError as e:
            raise StepProcessorError(f"Failed to save grouping results to stage '{stage_name}': {e}")
    
    def _validate_grouping_config(self, df: pd.DataFrame, source_column: str, 
                                groups: dict, target_column: str) -> None:
        """Validate grouping configuration parameters."""
        
        # Validate source column
        if not isinstance(source_column, str) or not source_column.strip():
            raise StepProcessorError("'source_column' must be a non-empty string")
        
        if source_column not in df.columns:
            available_columns = list(df.columns)
            raise StepProcessorError(
                f"Source column '{source_column}' not found. "
                f"Available columns: {available_columns}"
            )
        
        # Validate groups
        if not isinstance(groups, dict):
            raise StepProcessorError("'groups' must be a dictionary mapping group names to value lists")
        
        if len(groups) == 0:
            raise StepProcessorError("'groups' dictionary cannot be empty")
        
        for group_name, values in groups.items():
            if not isinstance(group_name, str) or not group_name.strip():
                raise StepProcessorError(f"Group name must be a non-empty string, got: {group_name}")
            
            if not isinstance(values, list):
                raise StepProcessorError(f"Group '{group_name}' values must be a list, got: {type(values)}")
            
            if len(values) == 0:
                raise StepProcessorError(f"Group '{group_name}' cannot have empty values list")
        
        # Validate target column
        if not isinstance(target_column, str) or not target_column.strip():
            raise StepProcessorError("'target_column' must be a non-empty string")
    
    def _create_mapping(self, groups: dict, case_sensitive: bool) -> dict:
        """Create a mapping from individual values to group names."""
        
        value_to_group = {}
        
        for group_name, values in groups.items():
            for value in values:
                # Convert value to string for consistent handling
                value_str = str(value)
                
                # Handle case sensitivity
                map_key = value_str if case_sensitive else value_str.lower()
                
                # Check for duplicates
                if map_key in value_to_group:
                    existing_group = value_to_group[map_key]
                    raise StepProcessorError(
                        f"Value '{value_str}' appears in both group '{group_name}' "
                        f"and group '{existing_group}'"
                    )
                
                value_to_group[map_key] = group_name
        
        logger.debug(f"Created mapping for {len(value_to_group)} values across {len(groups)} groups")
        return value_to_group
    
    def _apply_grouping(self, df: pd.DataFrame, source_column: str, target_column: str,
                       value_to_group_map: dict, unmatched_action: str, 
                       unmatched_value: str, case_sensitive: bool) -> pd.DataFrame:
        """Apply the grouping mapping to the DataFrame."""
        
        def map_value(value):
            """Map a single value to its group."""
            if pd.isna(value):
                return self._handle_unmatched(None, unmatched_action, unmatched_value)
            
            value_str = str(value)
            lookup_key = value_str if case_sensitive else value_str.lower()
            
            if lookup_key in value_to_group_map:
                return value_to_group_map[lookup_key]
            else:
                return self._handle_unmatched(value_str, unmatched_action, unmatched_value)
        
        # Apply the mapping
        df[target_column] = df[source_column].apply(map_value)
        
        # Log mapping statistics
        matched_count = df[target_column].isin(value_to_group_map.values()).sum()
        total_count = len(df)
        unmatched_count = total_count - matched_count
        
        logger.debug(f"Grouping results: {matched_count} matched, {unmatched_count} unmatched")
        
        return df
    
    def _handle_unmatched(self, value: str, unmatched_action: str, unmatched_value: str):
        """Handle values that don't match any group."""
        
        if unmatched_action == 'keep_original':
            return value
        elif unmatched_action == 'set_default':
            return unmatched_value
        elif unmatched_action == 'error':
            raise StepProcessorError(f"Unmatched value: '{value}' not found in any group")
        else:
            raise StepProcessorError(
                f"Unknown unmatched_action: '{unmatched_action}'. "
                "Valid options: 'keep_original', 'set_default', 'error'"
            )
    
    # ============================================================================
    # UTILITY METHODS FOR ADVANCED OPERATIONS
    # ============================================================================
    
    def create_regional_groups(self, data: pd.DataFrame, origin_column: str) -> pd.DataFrame:
        """Helper method to create the specific regional groups from the van report."""
        
        van_report_groups = {
            'Bristol Bay': ['Dillingham', 'False Pass', 'Naknek', 'Naknek West', 'Wood River'],
            'Kodiak': ['Kodiak', 'Kodiak West'],
            'PWS': ['Cordova', 'Seward', 'Valdez'],
            'SE': ['Craig', 'Ketchikan', 'Petersburg', 'Sitka']
        }
        
        # Apply the grouping
        value_to_group_map = self._create_mapping(van_report_groups, case_sensitive=True)
        
        result = self._apply_grouping(
            data.copy(), origin_column, f'{origin_column}_Region', 
            value_to_group_map, 'keep_original', 'Other', case_sensitive=True
        )
        
        return result
    
    def analyze_grouping_potential(self, df: pd.DataFrame, column: str) -> dict:
        """Analyze a column for potential grouping opportunities."""
        
        if column not in df.columns:
            raise StepProcessorError(f"Column '{column}' not found in DataFrame")
        
        series = df[column]
        
        analysis = {
            'column_name': column,
            'total_rows': len(df),
            'unique_values': series.nunique(),
            'null_count': series.isnull().sum(),
            'value_counts': series.value_counts().to_dict(),
            'most_common': series.value_counts().head(10).to_dict(),
            'data_type': str(series.dtype)
        }
        
        # Suggest grouping potential based on cardinality
        unique_ratio = analysis['unique_values'] / analysis['total_rows']
        if unique_ratio > 0.5:
            analysis['grouping_recommendation'] = 'High cardinality - consider grouping'
        elif unique_ratio > 0.1:
            analysis['grouping_recommendation'] = 'Medium cardinality - grouping may be useful'
        else:
            analysis['grouping_recommendation'] = 'Low cardinality - grouping may not be needed'
        
        return analysis
    
    def create_hierarchical_groups(self, data: pd.DataFrame, source_column: str, 
                                 hierarchy_levels: list) -> pd.DataFrame:
        """Create hierarchical grouping with multiple levels."""
        
        result = data.copy()
        
        for level_config in hierarchy_levels:
            level_name = level_config['level_name']
            level_groups = level_config['groups']
            parent_column = level_config.get('parent_column', source_column)
            
            # Apply grouping for this level
            value_to_group_map = self._create_mapping(level_groups, case_sensitive=True)
            result = self._apply_grouping(
                result, parent_column, level_name, value_to_group_map,
                'keep_original', 'Other', case_sensitive=True
            )
        
        return result
    
    # ============================================================================
    # CONFIGURATION AND CAPABILITIES
    # ============================================================================
    
    def get_supported_unmatched_actions(self) -> list:
        """Get list of supported actions for unmatched values."""
        return ['keep_original', 'set_default', 'error']
    
    def get_supported_source_types(self) -> list:
        """Get list of supported group source types."""
        return ['inline', 'stage', 'file', 'lookup', 'predefined']
    
    def get_supported_file_formats(self) -> list:
        """Get list of supported file formats for group definitions."""
        return ['wide', 'long']
    
    def get_predefined_group_types(self) -> list:
        """Get list of available predefined group types."""
        return ['van_report_regions', 'us_regions', 'product_categories']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Group individual values into categories using various source types and advanced workflows',
            'source_types': self.get_supported_source_types(),
            'unmatched_actions': self.get_supported_unmatched_actions(),
            'file_formats': self.get_supported_file_formats(),
            'predefined_groups': self.get_predefined_group_types(),
            'grouping_features': [
                'category_mapping', 'regional_grouping', 'case_sensitivity_control',
                'unmatched_value_handling', 'duplicate_detection', 'source_column_replacement',
                'stage_based_definitions', 'file_based_definitions', 'cross_reference_grouping'
            ],
            'stage_integration': [
                'stage_based_group_definitions', 'dynamic_group_updates', 
                'cross_reference_grouping', 'hierarchical_grouping_workflows',
                'grouping_result_caching', 'multi_stage_group_chains'
            ],
            'file_features': [
                'variable_substitution', 'automatic_format_detection',
                'multiple_file_formats', 'encoding_support', 'dynamic_group_files'
            ],
            'advanced_operations': [
                'hierarchical_grouping', 'conditional_grouping', 'lookup_based_grouping',
                'grouping_analysis', 'validation_grouping', 'multi_level_categorization'
            ],
            'helper_methods': [
                'create_regional_groups', 'analyze_grouping_potential', 'create_hierarchical_groups'
            ],
            'examples': {
                'inline_groups': "Group cities into regions using hardcoded mappings",
                'stage_groups': "Group using dynamic definitions from saved stage",
                'file_groups': "Group using definitions from external Excel/CSV file",
                'lookup_groups': "Group based on cross-reference with validation data",
                'hierarchical': "Multi-level grouping (city → region → territory)",
                'variable_substitution': "Use group definitions from file_{date}.xlsx"
            },
            'configuration_options': {
                'source_column': 'Column to group',
                'groups': 'Inline group definitions (traditional)',
                'groups_source': 'Advanced source configuration (stage, file, lookup)',
                'target_column': 'New column name for groups',
                'replace_source': 'Replace source column with grouped values',
                'unmatched_action': 'How to handle unmatched values',
                'unmatched_value': 'Default value for unmatched items',
                'case_sensitive': 'Whether matching is case sensitive',
                'save_to_stage': 'Save grouping results to stage',
                'stage_description': 'Description for saved stage'
            }
        }
