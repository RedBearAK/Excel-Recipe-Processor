"""
Rename columns step processor for Excel automation recipes.

Handles renaming DataFrame columns with flexible mapping options.
"""

import re
import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)

proc_type = 'processor_type'


class RenameColumnsProcessor(BaseStepProcessor):
    """
    Processor for renaming DataFrame columns.
    
    Supports direct mapping, pattern-based renaming, case conversion,
    and systematic column name transformations.
    """
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the column renaming operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            DataFrame with renamed columns
            
        Raises:
            StepProcessorError: If renaming fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Rename columns step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Get configuration
        rename_type = self.get_config_value('rename_type', 'mapping')
        mapping = self.get_config_value('mapping', {})
        pattern = self.get_config_value('pattern', None)
        replacement = self.get_config_value('replacement', '')
        case_conversion = self.get_config_value('case_conversion', None)
        add_prefix = self.get_config_value('add_prefix', None)
        add_suffix = self.get_config_value('add_suffix', None)
        strip_characters = self.get_config_value('strip_characters', None)
        replace_spaces = self.get_config_value('replace_spaces', None)
        
        # Validate configuration
        self._validate_rename_config(data, rename_type, mapping)
        
        # Work on a copy
        result_data = data.copy()
        original_columns = list(result_data.columns)
        
        try:
            # Apply renaming based on type
            if rename_type == 'mapping':
                result_data = self._apply_mapping_rename(result_data, mapping)
                
            elif rename_type == 'pattern':
                result_data = self._apply_pattern_rename(result_data, pattern, replacement)
                
            elif rename_type == 'transform':
                result_data = self._apply_transform_rename(
                    result_data, case_conversion, add_prefix, add_suffix, 
                    strip_characters, replace_spaces
                )
                
            else:
                available_types = ['mapping', 'pattern', 'transform']
                raise StepProcessorError(
                    f"Unknown rename type: '{rename_type}'. "
                    f"Available types: {', '.join(available_types)}"
                )
            
            new_columns = list(result_data.columns)
            renamed_count = sum(1 for old, new in zip(original_columns, new_columns) if old != new)
            
            result_info = f"renamed {renamed_count}/{len(original_columns)} columns"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error renaming columns in step '{self.step_name}': {e}")
    
    def _validate_rename_config(self, df: pd.DataFrame, rename_type: str, mapping: dict) -> None:
        """
        Validate renaming configuration parameters.
        
        Args:
            df: Input DataFrame
            rename_type: Type of renaming operation
            mapping: Column mapping dictionary
        """
        # Validate rename type
        if not isinstance(rename_type, str):
            raise StepProcessorError(f"'rename_type' must be a string")
        
        # Validate mapping if using mapping type
        if rename_type == 'mapping':
            if not isinstance(mapping, dict):
                raise StepProcessorError("'mapping' must be a dictionary when using mapping type")
            
            if len(mapping) == 0:
                raise StepProcessorError("'mapping' dictionary cannot be empty for mapping type")
            
            # Check that all old column names exist
            missing_columns = []
            for old_name in mapping.keys():
                if not isinstance(old_name, str):
                    raise StepProcessorError(f"Column name must be a string, got: {type(old_name)}")
                if old_name not in df.columns:
                    missing_columns.append(old_name)
            
            if missing_columns:
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Columns not found for renaming: {missing_columns}. "
                    f"Available columns: {available_columns}"
                )
            
            # Check that new names are valid strings
            for new_name in mapping.values():
                if not isinstance(new_name, str) or not new_name.strip():
                    raise StepProcessorError(f"New column name must be a non-empty string, got: {new_name}")
            
            # Check for duplicate new names
            new_names = list(mapping.values())
            if len(new_names) != len(set(new_names)):
                duplicates = [name for name in new_names if new_names.count(name) > 1]
                raise StepProcessorError(f"Duplicate new column names not allowed: {duplicates}")
    
    def _apply_mapping_rename(self, df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        """
        Apply direct column name mapping.
        
        Args:
            df: DataFrame to rename columns in
            mapping: Dictionary mapping old names to new names
            
        Returns:
            DataFrame with renamed columns
        """
        # Apply the mapping
        df = df.rename(columns=mapping)
        
        renamed_items = list(mapping.items())
        logger.debug(f"Applied column mapping: {renamed_items}")
        
        return df
    
    def _apply_pattern_rename(self, df: pd.DataFrame, pattern: str, replacement: str) -> pd.DataFrame:
        """
        Apply pattern-based renaming using regex.
        
        Args:
            df: DataFrame to rename columns in
            pattern: Regex pattern to match
            replacement: Replacement string
            
        Returns:
            DataFrame with renamed columns
        """
        if not pattern:
            raise StepProcessorError("Pattern rename requires 'pattern' field")
        
        if not isinstance(pattern, str):
            raise StepProcessorError("Pattern must be a string")
        
        if not isinstance(replacement, str):
            raise StepProcessorError("Replacement must be a string")
        
        # Apply pattern replacement to all column names
        new_columns = []
        for col in df.columns:
            try:
                new_col = re.sub(pattern, replacement, col)
                new_columns.append(new_col)
            except re.error as e:
                raise StepProcessorError(f"Invalid regex pattern '{pattern}': {e}")
        
        df.columns = new_columns
        logger.debug(f"Applied pattern rename: '{pattern}' → '{replacement}'")
        
        return df
    
    def _apply_transform_rename(self, df: pd.DataFrame, case_conversion: str, 
                              add_prefix: str, add_suffix: str, 
                              strip_characters: str, replace_spaces: str) -> pd.DataFrame:
        """
        Apply systematic transformations to column names.
        
        Args:
            df: DataFrame to rename columns in
            case_conversion: Case conversion option
            add_prefix: Prefix to add
            add_suffix: Suffix to add
            strip_characters: Characters to strip
            replace_spaces: Character to replace spaces with
            
        Returns:
            DataFrame with transformed column names
        """
        new_columns = []
        
        for col in df.columns:
            new_col = str(col)  # Start with string version
            
            # Apply transformations in order
            
            # 1. Strip characters
            if strip_characters:
                if not isinstance(strip_characters, str):
                    raise StepProcessorError("'strip_characters' must be a string")
                new_col = new_col.strip(strip_characters)
            
            # 2. Replace spaces
            if replace_spaces is not None:
                if not isinstance(replace_spaces, str):
                    raise StepProcessorError("'replace_spaces' must be a string")
                new_col = new_col.replace(' ', replace_spaces)
            
            # 3. Case conversion
            if case_conversion:
                if case_conversion == 'upper':
                    new_col = new_col.upper()
                elif case_conversion == 'lower':
                    new_col = new_col.lower()
                elif case_conversion == 'title':
                    new_col = new_col.title()
                elif case_conversion == 'snake_case':
                    new_col = self._to_snake_case(new_col)
                elif case_conversion == 'camel_case':
                    new_col = self._to_camel_case(new_col)
                else:
                    available_cases = ['upper', 'lower', 'title', 'snake_case', 'camel_case']
                    raise StepProcessorError(
                        f"Unknown case conversion: '{case_conversion}'. "
                        f"Available options: {', '.join(available_cases)}"
                    )
            
            # 4. Add prefix
            if add_prefix:
                if not isinstance(add_prefix, str):
                    raise StepProcessorError("'add_prefix' must be a string")
                new_col = add_prefix + new_col
            
            # 5. Add suffix
            if add_suffix:
                if not isinstance(add_suffix, str):
                    raise StepProcessorError("'add_suffix' must be a string")
                new_col = new_col + add_suffix
            
            new_columns.append(new_col)
        
        df.columns = new_columns
        logger.debug(f"Applied column transformations: {len(new_columns)} columns processed")
        
        return df
    
    def _to_snake_case(self, name: str) -> str:
        """Convert a string to snake_case."""
        # Replace spaces and hyphens with underscores first
        name = re.sub(r'[-\s]+', '_', name)
        
        # Handle sequences of uppercase letters followed by lowercase (e.g., "XMLHttp" -> "XML_Http")
        name = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
        
        # Insert underscores before uppercase letters that follow lowercase letters (e.g., "camelCase" -> "camel_Case")
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        
        # Convert to lowercase
        name = name.lower()
        
        # Remove special characters (except underscores and alphanumeric)
        name = re.sub(r'[^a-z0-9_]', '_', name)
        
        # Remove multiple consecutive underscores
        name = re.sub(r'_+', '_', name)
        
        # Strip leading/trailing underscores
        name = name.strip('_')
        
        return name
    
    def _to_camel_case(self, name: str) -> str:
        """Convert a string to camelCase."""
        # Split on spaces, hyphens, and underscores
        parts = re.split(r'[-_\s]+', name)
        # First part lowercase, rest title case
        if len(parts) > 0:
            result = parts[0].lower()
            for part in parts[1:]:
                if part:  # Skip empty parts
                    result += part.capitalize()
            return result
        return name.lower()
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper method to apply standard column name conventions.
        
        Args:
            df: DataFrame to standardize
            
        Returns:
            DataFrame with standardized column names
        """
        # Apply common standardization: snake_case, strip whitespace, replace special chars
        new_columns = []
        for col in df.columns:
            # Start with string version
            new_col = str(col).strip()
            # Replace special characters with underscores
            new_col = re.sub(r'[^a-zA-Z0-9_\s]', '_', new_col)
            # Convert to snake_case
            new_col = self._to_snake_case(new_col)
            new_columns.append(new_col)
        
        result_df = df.copy()
        result_df.columns = new_columns
        return result_df
    
    def get_column_analysis(self, df: pd.DataFrame) -> dict:
        """
        Analyze column names for potential renaming opportunities.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with column analysis
        """
        columns = list(df.columns)
        
        analysis = {
            'total_columns': len(columns),
            'column_names': columns,
            'naming_issues': [],
            'recommendations': []
        }
        
        for col in columns:
            col_str = str(col)
            
            # Check for common issues
            if ' ' in col_str:
                analysis['naming_issues'].append(f"'{col}' contains spaces")
            
            if re.search(r'[^a-zA-Z0-9_]', col_str):
                analysis['naming_issues'].append(f"'{col}' contains special characters")
            
            if col_str != col_str.strip():
                analysis['naming_issues'].append(f"'{col}' has leading/trailing whitespace")
            
            if col_str.isupper():
                analysis['naming_issues'].append(f"'{col}' is all uppercase")
        
        # Generate recommendations
        if any('spaces' in issue for issue in analysis['naming_issues']):
            analysis['recommendations'].append("Consider replacing spaces with underscores")
        
        if any('special characters' in issue for issue in analysis['naming_issues']):
            analysis['recommendations'].append("Consider removing special characters")
        
        if any('uppercase' in issue for issue in analysis['naming_issues']):
            analysis['recommendations'].append("Consider converting to lowercase or snake_case")
        
        return analysis
    
    def get_supported_rename_types(self) -> list:
        """
        Get list of supported rename types.
        
        Returns:
            List of supported rename type strings
        """
        return ['mapping', 'pattern', 'transform']
    
    def get_supported_case_conversions(self) -> list:
        """
        Get list of supported case conversion options.
        
        Returns:
            List of supported case conversion strings
        """
        return ['upper', 'lower', 'title', 'snake_case', 'camel_case']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Rename DataFrame columns with flexible transformation options',
            'rename_types': self.get_supported_rename_types(),
            'case_conversions': self.get_supported_case_conversions(),
            'transformation_features': [
                'direct_mapping', 'pattern_replacement', 'case_conversion',
                'prefix_suffix_addition', 'special_character_removal',
                'standardization', 'snake_case_conversion', 'camel_case_conversion'
            ],
            'helper_methods': [
                'standardize_column_names', 'get_column_analysis'
            ],
            'examples': {
                'cleanup': "Convert 'Product Name!' to 'product_name'",
                'standardize': "Apply consistent naming conventions",
                'mapping': "Rename specific columns to business-friendly names"
            }
        }
