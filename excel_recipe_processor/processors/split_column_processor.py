"""
Split column step processor for Excel automation recipes.

Handles splitting single columns into multiple columns using various methods.
"""

import re
import pandas as pd
import logging

from typing import Any, Optional

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class SplitColumnProcessor(BaseStepProcessor):
    """
    Processor for splitting DataFrame columns into multiple columns.
    
    Supports delimiter-based splitting, fixed-width splitting, regex patterns,
    and various options for handling edge cases.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'source_column': 'test_column',
            'delimiter': ',',
            'new_columns': ['part1', 'part2']
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the column splitting operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            DataFrame with split columns added
            
        Raises:
            StepProcessorError: If splitting fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Split column step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['source_column', 'split_type'])
        
        source_column = self.get_config_value('source_column')
        split_type = self.get_config_value('split_type')
        new_column_names = self.get_config_value('new_column_names', [])
        remove_original = self.get_config_value('remove_original', False)
        max_splits = self.get_config_value('max_splits', None)
        fill_missing = self.get_config_value('fill_missing', '')
        
        # Validate configuration
        self._validate_split_config(data, source_column, split_type, new_column_names)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Apply the splitting based on type
            if split_type == 'delimiter':
                result_data = self._split_by_delimiter(
                    result_data, source_column, new_column_names, 
                    remove_original, max_splits, fill_missing
                )
            elif split_type == 'fixed_width':
                result_data = self._split_by_fixed_width(
                    result_data, source_column, new_column_names,
                    remove_original, fill_missing
                )
            elif split_type == 'regex':
                result_data = self._split_by_regex(
                    result_data, source_column, new_column_names,
                    remove_original, max_splits, fill_missing
                )
            elif split_type == 'position':
                result_data = self._split_by_position(
                    result_data, source_column, new_column_names,
                    remove_original, fill_missing
                )
            else:
                available_types = ['delimiter', 'fixed_width', 'regex', 'position']
                raise StepProcessorError(
                    f"Unknown split type: '{split_type}'. "
                    f"Available types: {', '.join(available_types)}"
                )
            
            # Count new columns added
            original_cols = len(data.columns)
            new_cols = len(result_data.columns)
            if remove_original:
                added_cols = new_cols - original_cols + 1
            else:
                added_cols = new_cols - original_cols
            
            result_info = f"split '{source_column}' into {added_cols} new columns"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error splitting column '{source_column}': {e}")
    
    def _validate_split_config(self, df: pd.DataFrame, source_column: str, 
                             split_type: str, new_column_names: list[str]) -> None:
        """
        Validate split configuration parameters.
        
        Args:
            df: Input DataFrame
            source_column: Column to split
            split_type: Type of split operation
            new_column_names: Names for new columns
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
        
        # Validate split type
        if not isinstance(split_type, str):
            raise StepProcessorError("'split_type' must be a string")
        
        # Validate new column names
        if not isinstance(new_column_names, list):
            raise StepProcessorError("'new_column_names' must be a list")
        
        for name in new_column_names:
            if not isinstance(name, str) or not name.strip():
                raise StepProcessorError("All new column names must be non-empty strings")
        
        # Check for duplicate new names
        if len(new_column_names) != len(set(new_column_names)):
            duplicates = [name for name in new_column_names if new_column_names.count(name) > 1]
            raise StepProcessorError(f"Duplicate new column names not allowed: {duplicates}")
        
        # Check type-specific requirements
        if split_type == 'delimiter':
            delimiter = self.get_config_value('delimiter')
            if not delimiter:
                raise StepProcessorError("Delimiter split requires 'delimiter' field")
        
        elif split_type == 'fixed_width':
            widths = self.get_config_value('widths')
            if not widths or not isinstance(widths, list):
                raise StepProcessorError("Fixed width split requires 'widths' list")
        
        elif split_type == 'regex':
            pattern = self.get_config_value('pattern')
            if not pattern:
                raise StepProcessorError("Regex split requires 'pattern' field")
        
        elif split_type == 'position':
            positions = self.get_config_value('positions')
            if not positions or not isinstance(positions, list):
                raise StepProcessorError("Position split requires 'positions' list")
    
    def _split_by_delimiter(self, df: pd.DataFrame, source_column: str, 
                          new_column_names: list[str], remove_original: bool,
                          max_splits: Optional[int], fill_missing: str) -> pd.DataFrame:
        """
        Split column by delimiter.
        
        Args:
            df: DataFrame to modify
            source_column: Column to split
            new_column_names: Names for new columns
            remove_original: Whether to remove source column
            max_splits: Maximum number of splits
            fill_missing: Value for missing parts
            
        Returns:
            DataFrame with split columns
        """
        delimiter = self.get_config_value('delimiter')
        expand_to_columns = self.get_config_value('expand_to_columns', True)
        
        # Handle null values by converting to string
        split_data = df[source_column].fillna('').astype(str)
        
        # Split the column
        if max_splits is not None:
            split_result = split_data.str.split(delimiter, n=max_splits, expand=expand_to_columns)
        else:
            split_result = split_data.str.split(delimiter, expand=expand_to_columns)
        
        # Generate column names if not provided
        if not new_column_names:
            if expand_to_columns and hasattr(split_result, 'columns'):
                num_cols = len(split_result.columns)
            else:
                # Find max number of parts to determine column count
                max_parts = split_data.str.split(delimiter).str.len().max()
                num_cols = max_parts
            
            new_column_names = [f"{source_column}_part_{i+1}" for i in range(num_cols)]
        
        # If expanding to columns, assign column names
        if expand_to_columns and hasattr(split_result, 'columns'):
            # Truncate or extend to match desired number of columns
            num_result_cols = len(split_result.columns)
            num_desired_cols = len(new_column_names)
            
            if num_result_cols < num_desired_cols:
                # Add missing columns with fill value
                for i in range(num_result_cols, num_desired_cols):
                    split_result[i] = fill_missing
            elif num_result_cols > num_desired_cols:
                # Keep only desired number of columns
                split_result = split_result.iloc[:, :num_desired_cols]
            
            split_result.columns = new_column_names[:len(split_result.columns)]
            
            # Fill any missing values
            split_result = split_result.fillna(fill_missing)
            
            # Add split columns to dataframe
            for col in split_result.columns:
                df[col] = split_result[col]
        else:
            # Return lists - less common case
            df[new_column_names[0] if new_column_names else f"{source_column}_split"] = split_result
        
        # Remove original column if requested
        if remove_original:
            df = df.drop(columns=[source_column])
        
        logger.debug(f"Split '{source_column}' by delimiter '{delimiter}' into {len(new_column_names)} columns")
        return df
    
    def _split_by_fixed_width(self, df: pd.DataFrame, source_column: str,
                            new_column_names: list[str], remove_original: bool,
                            fill_missing: str) -> pd.DataFrame:
        """
        Split column by fixed character widths.
        
        Args:
            df: DataFrame to modify
            source_column: Column to split
            new_column_names: Names for new columns
            remove_original: Whether to remove source column
            fill_missing: Value for missing parts
            
        Returns:
            DataFrame with split columns
        """
        widths = self.get_config_value('widths')
        strip_whitespace = self.get_config_value('strip_whitespace', True)
        
        # Generate column names if not provided
        if not new_column_names:
            new_column_names = [f"{source_column}_part_{i+1}" for i in range(len(widths))]
        
        # Ensure we have enough column names
        while len(new_column_names) < len(widths):
            new_column_names.append(f"{source_column}_part_{len(new_column_names)+1}")
        
        # Handle null values
        split_data = df[source_column].fillna('').astype(str)
        
        # Split by fixed widths
        start_pos = 0
        for i, width in enumerate(widths):
            if i < len(new_column_names):
                col_name = new_column_names[i]
                
                # Extract substring
                end_pos = start_pos + width
                df[col_name] = split_data.str.slice(start_pos, end_pos)
                
                # Strip whitespace if requested
                if strip_whitespace:
                    df[col_name] = df[col_name].str.strip()
                
                # Fill empty values
                df[col_name] = df[col_name].replace('', fill_missing)
                
                start_pos = end_pos
        
        # Remove original column if requested
        if remove_original:
            df = df.drop(columns=[source_column])
        
        logger.debug(f"Split '{source_column}' by fixed widths {widths}")
        return df
    
    def _split_by_regex(self, df: pd.DataFrame, source_column: str,
                       new_column_names: list[str], remove_original: bool,
                       max_splits: Optional[int], fill_missing: str) -> pd.DataFrame:
        """
        Split column using regex pattern.
        
        Args:
            df: DataFrame to modify
            source_column: Column to split
            new_column_names: Names for new columns
            remove_original: Whether to remove source column
            max_splits: Maximum number of splits
            fill_missing: Value for missing parts
            
        Returns:
            DataFrame with split columns
        """
        pattern = self.get_config_value('pattern')
        
        try:
            # Compile regex pattern to validate it
            compiled_pattern = re.compile(pattern)
        except re.error as e:
            raise StepProcessorError(f"Invalid regex pattern '{pattern}': {e}")
        
        # Handle null values
        split_data = df[source_column].fillna('').astype(str)
        
        # Split using regex
        if max_splits is not None:
            split_result = split_data.str.split(pattern, n=max_splits, expand=True)
        else:
            split_result = split_data.str.split(pattern, expand=True)
        
        # Generate column names if not provided
        if not new_column_names:
            num_cols = len(split_result.columns) if hasattr(split_result, 'columns') else 1
            new_column_names = [f"{source_column}_part_{i+1}" for i in range(num_cols)]
        
        # Assign column names and handle missing columns
        if hasattr(split_result, 'columns'):
            num_result_cols = len(split_result.columns)
            num_desired_cols = len(new_column_names)
            
            if num_result_cols < num_desired_cols:
                # Add missing columns
                for i in range(num_result_cols, num_desired_cols):
                    split_result[i] = fill_missing
            elif num_result_cols > num_desired_cols:
                # Keep only desired columns
                split_result = split_result.iloc[:, :num_desired_cols]
            
            split_result.columns = new_column_names[:len(split_result.columns)]
            split_result = split_result.fillna(fill_missing)
            
            # Add to dataframe
            for col in split_result.columns:
                df[col] = split_result[col]
        
        # Remove original column if requested
        if remove_original:
            df = df.drop(columns=[source_column])
        
        logger.debug(f"Split '{source_column}' by regex pattern '{pattern}'")
        return df
    
    def _split_by_position(self, df: pd.DataFrame, source_column: str,
                         new_column_names: list[str], remove_original: bool,
                         fill_missing: str) -> pd.DataFrame:
        """
        Split column at specific character positions.
        
        Args:
            df: DataFrame to modify
            source_column: Column to split
            new_column_names: Names for new columns
            remove_original: Whether to remove source column
            fill_missing: Value for missing parts
            
        Returns:
            DataFrame with split columns
        """
        positions = self.get_config_value('positions')
        strip_whitespace = self.get_config_value('strip_whitespace', True)
        
        # Validate positions
        if not all(isinstance(pos, int) and pos >= 0 for pos in positions):
            raise StepProcessorError("All positions must be non-negative integers")
        
        # Sort positions to ensure proper splitting
        sorted_positions = sorted(set(positions))
        
        # Generate column names if not provided
        if not new_column_names:
            new_column_names = [f"{source_column}_part_{i+1}" for i in range(len(sorted_positions) + 1)]
        
        # Ensure we have enough column names
        while len(new_column_names) < len(sorted_positions) + 1:
            new_column_names.append(f"{source_column}_part_{len(new_column_names)+1}")
        
        # Handle null values
        split_data = df[source_column].fillna('').astype(str)
        
        # Split at positions
        start_pos = 0
        for i, end_pos in enumerate(sorted_positions):
            if i < len(new_column_names):
                col_name = new_column_names[i]
                
                # Extract substring
                df[col_name] = split_data.str.slice(start_pos, end_pos)
                
                # Strip whitespace if requested
                if strip_whitespace:
                    df[col_name] = df[col_name].str.strip()
                
                # Fill empty values
                df[col_name] = df[col_name].replace('', fill_missing)
                
                start_pos = end_pos
        
        # Handle the final part (after last position)
        if len(new_column_names) > len(sorted_positions):
            final_col_name = new_column_names[len(sorted_positions)]
            df[final_col_name] = split_data.str.slice(start_pos)
            
            if strip_whitespace:
                df[final_col_name] = df[final_col_name].str.strip()
            
            df[final_col_name] = df[final_col_name].replace('', fill_missing)
        
        # Remove original column if requested
        if remove_original:
            df = df.drop(columns=[source_column])
        
        logger.debug(f"Split '{source_column}' at positions {sorted_positions}")
        return df
    
    def split_name_column(self, df: pd.DataFrame, name_column: str, 
                         format_type: str = 'last_first') -> pd.DataFrame:
        """
        Helper method to split common name formats.
        
        Args:
            df: DataFrame containing name column
            name_column: Column with names to split
            format_type: Format of names ('last_first', 'first_last', 'first_middle_last')
            
        Returns:
            DataFrame with split name columns
        """
        result_df = df.copy()
        
        if format_type == 'last_first':
            # "Smith, John" format
            split_result = result_df[name_column].str.split(',', n=1, expand=True)
            if len(split_result.columns) >= 2:
                result_df['Last_Name'] = split_result[0].str.strip()
                result_df['First_Name'] = split_result[1].str.strip()
            
        elif format_type == 'first_last':
            # "John Smith" format
            split_result = result_df[name_column].str.split(' ', n=1, expand=True)
            if len(split_result.columns) >= 2:
                result_df['First_Name'] = split_result[0].str.strip()
                result_df['Last_Name'] = split_result[1].str.strip()
        
        elif format_type == 'first_middle_last':
            # "John Michael Smith" format - split into max 3 parts
            split_result = result_df[name_column].str.split(' ', n=2, expand=True)
            if len(split_result.columns) >= 1:
                result_df['First_Name'] = split_result[0].str.strip()
            if len(split_result.columns) >= 2:
                result_df['Middle_Name'] = split_result[1].str.strip()
            if len(split_result.columns) >= 3:
                result_df['Last_Name'] = split_result[2].str.strip()
        
        return result_df
    
    def analyze_column_patterns(self, df: pd.DataFrame, column: str) -> dict:
        """
        Analyze a column to suggest splitting strategies.
        
        Args:
            df: DataFrame to analyze
            column: Column to analyze
            
        Returns:
            Dictionary with pattern analysis and suggestions
        """
        if column not in df.columns:
            raise StepProcessorError(f"Column '{column}' not found in DataFrame")
        
        # Get non-null string values
        values = df[column].dropna().astype(str)
        sample_size = min(len(values), 100)  # Analyze first 100 non-null values
        
        analysis = {
            'column': column,
            'total_values': len(df[column]),
            'non_null_values': len(values),
            'sample_values': values.head(5).tolist(),
            'suggested_splits': []
        }
        
        if len(values) == 0:
            analysis['suggested_splits'].append("No non-null values to analyze")
            return analysis
        
        sample_values = values.head(sample_size)
        
        # Check for common delimiters
        delimiters = [',', ';', '|', ':', '-', '_', ' ', '\t']
        for delimiter in delimiters:
            delimiter_count = sample_values.str.count(re.escape(delimiter)).sum()
            if delimiter_count > len(sample_values) * 0.5:  # More than 50% contain delimiter
                avg_parts = (sample_values.str.count(re.escape(delimiter)) + 1).mean()
                analysis['suggested_splits'].append(
                    f"Delimiter '{delimiter}': avg {avg_parts:.1f} parts per value"
                )
        
        # Check for consistent lengths (fixed width potential)
        lengths = sample_values.str.len()
        if lengths.std() < 2:  # Low variance in length
            avg_length = lengths.mean()
            analysis['suggested_splits'].append(
                f"Fixed width potential: avg length {avg_length:.1f} chars"
            )
        
        # Check for name patterns
        if any(keyword in column.lower() for keyword in ['name', 'customer', 'contact', 'person']):
            comma_count = sample_values.str.count(',').sum()
            space_count = sample_values.str.count(' ').sum()
            
            if comma_count > len(sample_values) * 0.3:
                analysis['suggested_splits'].append("Name pattern: 'Last, First' format detected")
            elif space_count > len(sample_values) * 0.5:
                analysis['suggested_splits'].append("Name pattern: 'First Last' format detected")
        
        return analysis
    
    def get_supported_split_types(self) -> list[str]:
        """
        Get list of supported split types.
        
        Returns:
            List of supported split type strings
        """
        return ['delimiter', 'fixed_width', 'regex', 'position']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Split single columns into multiple columns using various methods',
            'split_types': self.get_supported_split_types(),
            'splitting_methods': [
                'delimiter_splitting', 'fixed_width_splitting', 'regex_pattern_splitting',
                'position_based_splitting', 'custom_column_naming', 'whitespace_handling',
                'missing_value_filling', 'original_column_retention'
            ],
            'helper_methods': [
                'split_name_column', 'analyze_column_patterns'
            ],
            'common_delimiters': [',', ';', '|', ':', '-', '_', ' ', '\t'],
            'options': [
                'max_splits', 'remove_original', 'fill_missing', 'strip_whitespace',
                'expand_to_columns'
            ],
            'examples': {
                'name_splitting': "Split 'Smith, John' into First_Name and Last_Name",
                'csv_data': "Split 'A|B|C' into separate columns",
                'fixed_format': "Split fixed-width data like product codes"
            }
        }
