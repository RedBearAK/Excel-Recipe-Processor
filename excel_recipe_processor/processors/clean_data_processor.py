"""
Enhanced clean data step processor for Excel automation recipes.

Handles various data cleaning operations including conditional replacements.
"""

import pandas as pd
import logging

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class CleanDataProcessor(BaseStepProcessor):
    """
    Processor for cleaning and transforming DataFrame data.
    
    Supports various cleaning operations like find/replace, case conversion,
    removing special characters, fixing data types, and conditional operations.
    """

    @classmethod
    def get_minimal_config(cls) -> dict:
        """
        Get the minimal configuration required to instantiate this processor.
        
        Returns:
            Dictionary with minimal configuration fields
        """
        return {
            'rules': [
                {
                    'columns': ['test_column'],  # Changed from 'column' to 'columns' as list
                    'action': 'strip_whitespace'
                }
            ]
        }

    def execute(self, data) -> pd.DataFrame:
        """
        Execute the data cleaning operations on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame to clean
            
        Returns:
            Cleaned pandas DataFrame
            
        Raises:
            StepProcessorError: If cleaning fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Clean data step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['rules'])
        
        rules = self.get_config_value('rules')
        
        # Guard clause: rules must be a list
        if not isinstance(rules, list):
            raise StepProcessorError(f"Step '{self.step_name}' 'rules' must be a list")
        
        if len(rules) == 0:
            logger.warning(f"Step '{self.step_name}' has no cleaning rules defined, returning data unchanged")
            self.log_step_complete("no rules applied")
            return data
        
        # Work on a copy to avoid modifying original data
        cleaned_data = data.copy()
        
        # Apply each cleaning rule
        rules_applied = 0
        for i, rule in enumerate(rules):
            try:
                cleaned_data = self._apply_cleaning_rule(cleaned_data, rule, i)
                rules_applied += 1
            except Exception as e:
                raise StepProcessorError(f"Error applying cleaning rule {i+1} in step '{self.step_name}': {e}")
        
        result_info = f"applied {rules_applied} cleaning rules"
        self.log_step_complete(result_info)
        
        return cleaned_data

    def _apply_cleaning_rule(self, df: pd.DataFrame, rule: dict, rule_index: int) -> pd.DataFrame:
        """
        Apply a single cleaning rule to the DataFrame.
        
        Args:
            df: DataFrame to clean
            rule: Dictionary containing cleaning rule configuration
            rule_index: Index of the rule for error reporting
            
        Returns:
            Cleaned DataFrame
        """
        # Guard clause: rule must be a dict
        if not isinstance(rule, dict):
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} must be a dictionary")
        
        # Validate required rule fields
        required_fields = ['columns', 'action']  # Changed from 'column' to 'columns'
        for field in required_fields:
            if field not in rule:
                raise StepProcessorError(f"Cleaning rule {rule_index + 1} missing required field: {field}")
        
        columns = rule['columns']
        action = rule['action']
        
        # Guard clauses for rule parameters
        if not isinstance(columns, list):
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} 'columns' must be a list")
        
        if len(columns) == 0:
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} 'columns' list cannot be empty")
        
        # Validate each column name in the list
        for i, column in enumerate(columns):
            if not isinstance(column, str) or not column.strip():
                raise StepProcessorError(f"Cleaning rule {rule_index + 1} 'columns[{i}]' must be a non-empty string")
        
        if not isinstance(action, str) or not action.strip():
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} 'action' must be a non-empty string")
        
        # Check conditional column exists if this is a conditional rule
        conditional_fields = ['condition_column', 'condition', 'condition_value']
        has_any_conditional = any(field in rule for field in conditional_fields)
        
        if has_any_conditional:
            # If any conditional field is present, all must be present
            missing_conditional = [field for field in conditional_fields if field not in rule]
            if missing_conditional:
                raise StepProcessorError(
                    f"Cleaning rule {rule_index + 1} has partial conditional replacement config. "
                    f"Missing required fields: {missing_conditional}. "
                    f"For conditional replacement, all of {conditional_fields} are required."
                )
            
            # Validate condition column exists
            condition_column = rule['condition_column']
            if condition_column not in df.columns:
                available_columns = list(df.columns)
                raise StepProcessorError(
                    f"Cleaning rule {rule_index + 1} condition column '{condition_column}' not found. "
                    f"Available columns: {available_columns}"
                )
        
        # Check which target columns exist and apply rule to existing ones
        existing_columns = []
        missing_columns = []
        
        for column in columns:
            if column in df.columns:
                existing_columns.append(column)
            else:
                missing_columns.append(column)
        
        # Warn about missing columns but continue with existing ones
        if missing_columns:
            logger.warning(
                f"Cleaning rule {rule_index + 1}: columns not found (skipping): {missing_columns}. "
                f"Available columns: {list(df.columns)}"
            )
        
        if not existing_columns:
            logger.warning(f"Cleaning rule {rule_index + 1}: no target columns found, skipping rule")
            return df
        
        # Apply the cleaning action to each existing column
        successful_columns = []
        failed_columns = []
        
        for column in existing_columns:
            try:
                # Create a temporary rule for this single column (for compatibility with existing methods)
                single_column_rule = rule.copy()
                single_column_rule['column'] = column  # Individual methods still expect 'column' key
                
                # Apply the appropriate cleaning action
                if action == 'replace':
                    df = self._apply_replace(df, single_column_rule, column, rule_index)
                elif action == 'regex_replace':
                    df = self._apply_regex_replace(df, single_column_rule, column, rule_index)
                elif action == 'uppercase':
                    df[column] = df[column].astype(str).str.upper()
                    logger.debug(f"Applied uppercase to column '{column}'")
                elif action == 'lowercase':
                    df[column] = df[column].astype(str).str.lower()
                    logger.debug(f"Applied lowercase to column '{column}'")
                elif action == 'title_case':
                    df[column] = df[column].astype(str).str.title()
                    logger.debug(f"Applied title case to column '{column}'")
                elif action == 'strip_whitespace':
                    df[column] = df[column].astype(str).str.strip()
                    logger.debug(f"Stripped whitespace from column '{column}'")
                elif action == 'remove_special_chars':
                    pattern = rule.get('pattern', r'[^a-zA-Z0-9\s]')
                    replacement = rule.get('replacement', '')
                    df[column] = df[column].astype(str).str.replace(pattern, replacement, regex=True)
                    logger.debug(f"Removed special characters from column '{column}' using pattern: {pattern}")
                elif action == 'fix_numeric':
                    df = self._apply_fix_numeric(df, single_column_rule, column, rule_index)
                elif action == 'fix_dates':
                    df = self._apply_fix_dates(df, single_column_rule, column, rule_index)
                elif action == 'fill_empty':
                    df = self._apply_fill_empty(df, single_column_rule, column, rule_index)
                elif action == 'remove_duplicates':
                    # This operates on the whole DataFrame, not just one column
                    initial_count = len(df)
                    df = df.drop_duplicates(subset=[column] if rule.get('subset_column', True) else None)
                    removed_count = initial_count - len(df)
                    logger.debug(f"Removed {removed_count} duplicate rows based on column '{column}'")
                elif action == 'standardize_values':
                    df = self._apply_standardize_values(df, single_column_rule, column, rule_index)
                elif action == 'remove_invisible_chars':
                    df = self._apply_remove_invisible_chars(df, single_column_rule, column, rule_index)
                elif action == 'normalize_whitespace':
                    df = self._apply_normalize_whitespace(df, single_column_rule, column, rule_index)
                else:
                    available_actions = self.get_supported_actions()  # Use dynamic list
                    raise StepProcessorError(
                        f"Cleaning rule {rule_index + 1} unknown action: '{action}'. "
                        f"Available actions: {', '.join(available_actions)}"
                    )
                
                successful_columns.append(column)
                
            except Exception as e:
                # Log the failure but continue with other columns
                if isinstance(e, StepProcessorError):
                    logger.warning(f"Cleaning rule {rule_index + 1} failed on column '{column}': {e}")
                else:
                    logger.warning(f"Cleaning rule {rule_index + 1} failed on column '{column}': {e}")
                failed_columns.append(column)
        
        # Log summary of what was applied
        if successful_columns:
            logger.debug(f"Cleaning rule {rule_index + 1} applied '{action}' to columns: {successful_columns}")
        
        if failed_columns:
            logger.warning(f"Cleaning rule {rule_index + 1} failed on columns: {failed_columns}")
        
        return df

    def _apply_replace(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Apply find and replace operation, with optional conditional logic."""
        if 'old_value' not in rule or 'new_value' not in rule:
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} with 'replace' action requires 'old_value' and 'new_value'")
        
        # Validate conditional replacement parameters if any are present
        conditional_fields = ['condition_column', 'condition', 'condition_value']
        has_any_conditional = any(field in rule for field in conditional_fields)
        
        if has_any_conditional:
            # If any conditional field is present, all must be present
            missing_conditional = [field for field in conditional_fields if field not in rule]
            if missing_conditional:
                raise StepProcessorError(
                    f"Cleaning rule {rule_index + 1} has partial conditional replacement config. "
                    f"Missing required fields: {missing_conditional}. "
                    f"For conditional replacement, all of {conditional_fields} are required."
                )
            # Use conditional replacement
            return self._apply_conditional_replace(df, rule, column, rule_index)
        
        # Standard unconditional replacement
        old_value = rule['old_value']
        new_value = rule['new_value']
        case_sensitive = rule.get('case_sensitive', True)
        
        if case_sensitive:
            df[column] = df[column].replace(old_value, new_value)
        else:
            # Case-insensitive replacement
            df[column] = df[column].astype(str).str.replace(
                str(old_value), str(new_value), case=False, regex=False
            )
        
        logger.debug(f"Replaced '{old_value}' with '{new_value}' in column '{column}'")
        return df
    
    def _apply_conditional_replace(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Apply conditional find and replace based on another column's value."""
        
        # Validate conditional replacement parameters
        required_conditional_fields = ['condition_column', 'condition', 'condition_value']
        for field in required_conditional_fields:
            if field not in rule:
                raise StepProcessorError(
                    f"Cleaning rule {rule_index + 1} with conditional replacement requires '{field}'"
                )
        
        condition_column = rule['condition_column']
        condition = rule['condition']
        condition_value = rule['condition_value']
        old_value = rule['old_value']
        new_value = rule['new_value']
        case_sensitive = rule.get('case_sensitive', True)
        
        # Check if condition column exists
        if condition_column not in df.columns:
            available_columns = list(df.columns)
            raise StepProcessorError(
                f"Cleaning rule {rule_index + 1} condition column '{condition_column}' not found. "
                f"Available columns: {available_columns}"
            )
        
        # Create mask based on condition
        try:
            if condition == 'equals':
                mask = df[condition_column] == condition_value
            elif condition == 'contains':
                mask = df[condition_column].astype(str).str.contains(str(condition_value), na=False, case=case_sensitive)
            elif condition == 'not_equals':
                mask = df[condition_column] != condition_value
            elif condition == 'not_contains':
                mask = ~df[condition_column].astype(str).str.contains(str(condition_value), na=False, case=case_sensitive)
            elif condition == 'greater_than':
                mask = df[condition_column] > condition_value
            elif condition == 'less_than':
                mask = df[condition_column] < condition_value
            elif condition == 'is_null':
                mask = df[condition_column].isnull()
            elif condition == 'not_null':
                mask = df[condition_column].notnull()
            else:
                available_conditions = ['equals', 'contains', 'not_equals', 'not_contains', 
                                        'greater_than', 'less_than', 'is_null', 'not_null']
                raise StepProcessorError(
                    f"Cleaning rule {rule_index + 1} unknown condition: '{condition}'. "
                    f"Available conditions: {', '.join(available_conditions)}"
                )
            
            # Apply replacement only where condition is true
            if case_sensitive:
                df.loc[mask, column] = df.loc[mask, column].replace(old_value, new_value)
            else:
                df.loc[mask, column] = df.loc[mask, column].astype(str).str.replace(
                    str(old_value), str(new_value), case=False, regex=False
                )
            
            affected_rows = mask.sum()
            logger.debug(
                f"Conditionally replaced '{old_value}' with '{new_value}' in column '{column}' "
                f"where {condition_column} {condition} '{condition_value}' ({affected_rows} rows affected)"
            )
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error applying conditional replacement: {e}")
        
        return df
    
    def _apply_regex_replace(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Apply regex find and replace operation."""
        if 'pattern' not in rule:
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} with 'regex_replace' action requires 'pattern'")
        
        pattern = rule['pattern']
        replacement = rule.get('replacement', '')
        
        # Guard clause: pattern should be a string
        if not isinstance(pattern, str):
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} 'pattern' must be a string")
        
        df[column] = df[column].astype(str).str.replace(pattern, replacement, regex=True)
        logger.debug(f"Applied regex replace in column '{column}': {pattern} → {replacement}")
        return df
    
    def _apply_fix_numeric(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Fix numeric data types and formats."""
        try:
            # Remove common non-numeric characters and convert
            df[column] = df[column].astype(str).str.replace(r'[,$%]', '', regex=True)
            df[column] = pd.to_numeric(df[column], errors='coerce')
            
            # Fill NaN values if specified
            if 'fill_na' in rule:
                df[column] = df[column].fillna(rule['fill_na'])
            
            logger.debug(f"Fixed numeric format in column '{column}'")
            return df
        except Exception as e:
            raise StepProcessorError(f"Error fixing numeric data in column '{column}': {e}")
    
    def _apply_fix_dates(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Fix date formats and convert to datetime."""
        date_format = rule.get('format', None)
        
        try:
            df[column] = pd.to_datetime(df[column], format=date_format, errors='coerce')
            logger.debug(f"Fixed date format in column '{column}'")
            return df
        except Exception as e:
            raise StepProcessorError(f"Error fixing date data in column '{column}': {e}")
    
    def _apply_fill_empty(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Fill empty/null values."""
        if 'fill_value' not in rule:
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} with 'fill_empty' action requires 'fill_value'")
        
        fill_value = rule['fill_value']
        method = rule.get('method', 'value')
        
        if method == 'value':
            df[column] = df[column].fillna(fill_value)

        # Produces FutureWarning for fillna(method=)
        # elif method == 'forward':
        #     df[column] = df[column].fillna(method='ffill')
        # elif method == 'backward':
        #     df[column] = df[column].fillna(method='bfill')

        # Prevents FutureWarning for fillna(method=)
        elif method == 'forward':
            df[column] = df[column].ffill()
        elif method == 'backward':
            df[column] = df[column].bfill()

        else:
            raise StepProcessorError(f"Unknown fill method: {method}. Use 'value', 'forward', or 'backward'")
        
        logger.debug(f"Filled empty values in column '{column}' using method '{method}'")
        return df
    
    def _apply_standardize_values(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Standardize values using a mapping dictionary."""
        if 'mapping' not in rule:
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} with 'standardize_values' action requires 'mapping'")
        
        mapping = rule['mapping']
        
        # Guard clause: mapping should be a dict
        if not isinstance(mapping, dict):
            raise StepProcessorError(f"Cleaning rule {rule_index + 1} 'mapping' must be a dictionary")
        
        # Apply the mapping
        df[column] = df[column].replace(mapping)
        logger.debug(f"Standardized values in column '{column}' using mapping: {mapping}")
        return df
    
    def _apply_remove_invisible_chars(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Remove invisible Unicode characters that break exact string matching."""
        
        # Characters that should be replaced with regular space (not removed)
        space_chars = [
            '\u00A0',  # Non-breaking space
            '\u1680',  # Ogham space mark
            '\u2000',  # En quad
            '\u2001',  # Em quad
            '\u2002',  # En space
            '\u2003',  # Em space
            '\u2004',  # Three-per-em space
            '\u2005',  # Four-per-em space
            '\u2006',  # Six-per-em space
            '\u2007',  # Figure space
            '\u2008',  # Punctuation space
            '\u2009',  # Thin space
            '\u200A',  # Hair space
            '\u202F',  # Narrow no-break space
            '\u205F',  # Medium mathematical space
            '\u3000',  # Ideographic space
        ]
        
        # Characters that should be completely removed
        remove_chars = [
            '\u200B',  # Zero-width space
            '\u200C',  # Zero-width non-joiner
            '\u200D',  # Zero-width joiner
            '\u200E',  # Left-to-right mark
            '\u200F',  # Right-to-left mark
            '\u2060',  # Word joiner
            '\uFEFF',  # Byte order mark (BOM)
        ]
        
        # First replace space-like chars with regular space
        space_pattern = '[' + ''.join(space_chars) + ']'
        df[column] = df[column].astype(str).str.replace(space_pattern, ' ', regex=True)
        
        # Then remove zero-width chars completely
        remove_pattern = '[' + ''.join(remove_chars) + ']'
        df[column] = df[column].astype(str).str.replace(remove_pattern, '', regex=True)
        
        # Finally, trim leading/trailing spaces that may have resulted from replacements
        df[column] = df[column].str.strip()
        
        logger.debug(f"Cleaned invisible characters from column '{column}'")
        return df
    
    def _apply_normalize_whitespace(self, df: pd.DataFrame, rule: dict, column: str, rule_index: int) -> pd.DataFrame:
        """Comprehensive whitespace normalization including invisible characters."""
        
        # Step 1: Remove invisible characters
        df = self._apply_remove_invisible_chars(df, rule, column, rule_index)
        
        # Step 2: Normalize regular whitespace
        # Remove leading/trailing whitespace
        df[column] = df[column].astype(str).str.strip()
        
        # Collapse multiple whitespace into single spaces
        df[column] = df[column].str.replace(r'\s+', ' ', regex=True)
        
        # Remove line endings and carriage returns
        df[column] = df[column].str.replace(r'[\r\n]+', ' ', regex=True)
        
        logger.debug(f"Normalized all whitespace in column '{column}'")
        return df
    
    def get_supported_actions(self) -> list:
        """
        Get list of supported cleaning actions.
        
        Returns:
            List of supported action strings
        """
        return [
            'replace', 'regex_replace', 'uppercase', 'lowercase', 'title_case',
            'strip_whitespace', 'normalize_whitespace', 'remove_invisible_chars',
            'remove_special_chars', 'fix_numeric', 'fix_dates',
            'fill_empty', 'remove_duplicates', 'standardize_values'
        ]
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Clean and transform data with various operations',
            'supported_actions': self.get_supported_actions(),
            'cleaning_operations': [
                'find_replace', 'conditional_replace', 'regex_replace', 'case_conversion', 
                'whitespace_removal', 'special_character_removal', 'numeric_formatting', 
                'date_formatting', 'null_value_handling', 'duplicate_removal', 'value_standardization'
            ],
            'conditional_operations': [
                'equals', 'contains', 'not_equals', 'not_contains', 'greater_than', 
                'less_than', 'is_null', 'not_null'
            ],
            'case_conversions': ['upper', 'lower', 'title'],
            'examples': {
                'conditional_replace': "Replace 'FLESH' with 'CANS' in rows where Product_Name contains 'Canned'",
                'clean_price': "Remove $ signs and convert to numeric",
                'standardize': "Map various status values to standard terms"
            }
        }
    
    def get_usage_examples(self) -> dict:
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('clean_data')
