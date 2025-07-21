"""
Group data step processor for Excel automation recipes.

Handles grouping individual values into categories, like cities into regions.
"""

import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class GroupDataProcessor(BaseStepProcessor):
    """
    Processor for grouping individual values into categories.
    
    Takes a source column and maps individual values to group categories
    using a provided mapping dictionary. Useful for operations like
    grouping cities into regions, products into categories, etc.
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
        
        # Validate required configuration
        self.validate_required_fields(['source_column', 'groups'])
        
        source_column = self.get_config_value('source_column')
        groups = self.get_config_value('groups')
        target_column = self.get_config_value('target_column', f'{source_column}_Group')
        replace_source = self.get_config_value('replace_source', False)
        unmatched_action = self.get_config_value('unmatched_action', 'keep_original')
        unmatched_value = self.get_config_value('unmatched_value', 'Other')
        case_sensitive = self.get_config_value('case_sensitive', True)
        
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
            
            # Count successful groupings
            grouped_count = len(result_data)
            unique_groups = result_data[final_column].nunique()
            
            result_info = f"grouped {grouped_count} rows into {unique_groups} categories"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            raise StepProcessorError(f"Error applying grouping in step '{self.step_name}': {e}")
    
    def _validate_grouping_config(self, df: pd.DataFrame, source_column: str, 
                                groups: dict, target_column: str) -> None:
        """
        Validate grouping configuration parameters.
        
        Args:
            df: Input DataFrame
            source_column: Source column name
            groups: Group mapping dictionary
            target_column: Target column name
        """
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
        """
        Create a mapping from individual values to group names.
        
        Args:
            groups: Dictionary mapping group names to lists of values
            case_sensitive: Whether mapping should be case sensitive
            
        Returns:
            Dictionary mapping individual values to group names
        """
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
        """
        Apply the grouping mapping to the DataFrame.
        
        Args:
            df: DataFrame to modify
            source_column: Source column name
            target_column: Target column name
            value_to_group_map: Mapping from values to groups
            unmatched_action: How to handle unmatched values
            unmatched_value: Default value for unmatched items
            case_sensitive: Whether matching should be case sensitive
            
        Returns:
            DataFrame with grouping applied
        """
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
        """
        Handle values that don't match any group.
        
        Args:
            value: The unmatched value
            unmatched_action: Action to take ('keep_original', 'set_default', 'error')
            unmatched_value: Default value to use
            
        Returns:
            The value to use for unmatched items
        """
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
    
    def create_regional_groups(self, data: pd.DataFrame, origin_column: str) -> pd.DataFrame:
        """
        Helper method to create the specific regional groups from the van report.
        
        Args:
            data: DataFrame with origin data
            origin_column: Name of the column containing origin cities
            
        Returns:
            DataFrame with regional grouping applied
        """
        van_report_groups = {
            'Bristol Bay': ['Dillingham', 'False Pass', 'Naknek', 'Naknek West', 'Wood River'],
            'Kodiak': ['Kodiak', 'Kodiak West'],
            'PWS': ['Cordova', 'Seward', 'Valdez'],
            'SE': ['Craig', 'Ketchikan', 'Petersburg', 'Sitka']
        }
        
        # Create a temporary step config for this operation
        temp_config = {
            'process_type': 'group_data',
            'source_column': origin_column,
            'groups': van_report_groups,
            'target_column': f'{origin_column}_Region',
            'unmatched_action': 'keep_original'
        }
        
        # Apply the grouping
        value_to_group_map = self._create_mapping(van_report_groups, case_sensitive=True)
        
        result = self._apply_grouping(
            data.copy(), origin_column, f'{origin_column}_Region', 
            value_to_group_map, 'keep_original', 'Other', case_sensitive=True
        )
        
        return result
    
    def analyze_grouping_potential(self, df: pd.DataFrame, column: str) -> dict:
        """
        Analyze a column for potential grouping opportunities.
        
        Args:
            df: DataFrame to analyze
            column: Column name to analyze
            
        Returns:
            Dictionary with analysis results
        """
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
    
    def get_supported_unmatched_actions(self) -> list:
        """
        Get list of supported actions for unmatched values.
        
        Returns:
            List of supported action strings
        """
        return ['keep_original', 'set_default', 'error']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Group individual values into categories using mapping rules',
            'grouping_features': [
                'category_mapping', 'regional_grouping', 'case_sensitivity_control',
                'unmatched_value_handling', 'duplicate_detection', 'source_column_replacement'
            ],
            'unmatched_actions': self.get_supported_unmatched_actions(),
            'special_methods': [
                'create_regional_groups', 'analyze_grouping_potential'
            ],
            'examples': {
                'regional': "Group cities by region (Bristol Bay, Kodiak, PWS, SE)",
                'category': "Group products by category",
                'status': "Group various status values into standard categories"
            }
        }
