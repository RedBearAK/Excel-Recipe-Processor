"""
Add calculated column step processor for Excel automation recipes.

Handles creating new columns with calculated values based on existing data.
"""

import re
import numpy as np
import pandas as pd
import logging

from typing import Any

from excel_recipe_processor.processors.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class AddCalculatedColumnProcessor(BaseStepProcessor):
    """
    Processor for adding calculated columns to DataFrames.
    
    Supports various calculation types including mathematical operations,
    string manipulations, date calculations, and conditional logic.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        """
        Get the minimal configuration required to instantiate this processor.
        
        Returns:
            Dictionary with minimal configuration fields
        """
        return {
            'new_column': 'test_column',
            'calculation': {'formula': 'test_value'}
        }
    
    def execute(self, data: Any) -> pd.DataFrame:
        """
        Execute the calculated column operation on the provided DataFrame.
        
        Args:
            data: Input pandas DataFrame
            
        Returns:
            DataFrame with new calculated column added
            
        Raises:
            StepProcessorError: If calculation fails
        """
        self.log_step_start()
        
        # Guard clause: ensure we have a DataFrame
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Add calculated column step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        
        # Validate required configuration
        self.validate_required_fields(['new_column', 'calculation'])
        
        new_column              = self.get_config_value('new_column')
        calculation             = self.get_config_value('calculation')
        calculation_type        = self.get_config_value('calculation_type', 'expression')
        overwrite               = self.get_config_value('overwrite', False)
        
        # Validate configuration
        self._validate_calculation_config(data, new_column, calculation, overwrite)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Apply the calculation based on type
            if calculation_type == 'expression':
                result_data = self._apply_expression_calculation(result_data, new_column, calculation)
            elif calculation_type == 'concat':
                result_data = self._apply_concatenation(result_data, new_column, calculation)
            elif calculation_type == 'conditional':
                result_data = self._apply_conditional_logic(result_data, new_column, calculation)
            elif calculation_type == 'math':
                result_data = self._apply_math_operation(result_data, new_column, calculation)
            elif calculation_type == 'date':
                result_data = self._apply_date_calculation(result_data, new_column, calculation)
            elif calculation_type == 'text':
                result_data = self._apply_text_operation(result_data, new_column, calculation)
            else:
                available_types = ['expression', 'concat', 'conditional', 'math', 'date', 'text']
                raise StepProcessorError(
                    f"Unknown calculation type: '{calculation_type}'. "
                    f"Available types: {', '.join(available_types)}"
                )
            
            # Verify the new column was created
            if new_column not in result_data.columns:
                raise StepProcessorError(f"Failed to create calculated column '{new_column}'")
            
            result_info = f"added calculated column '{new_column}'"
            self.log_step_complete(result_info)
            
            return result_data
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error creating calculated column '{new_column}': {e}")
    
    def _validate_calculation_config(self, df: pd.DataFrame, new_column: str, 
                                   calculation: dict, overwrite: bool) -> None:
        """
        Validate calculation configuration parameters.
        
        Args:
            df: Input DataFrame
            new_column: Name of new column to create
            calculation: Calculation configuration
            overwrite: Whether to overwrite existing column
        """
        # Validate new column name
        if not isinstance(new_column, str) or not new_column.strip():
            raise StepProcessorError("'new_column' must be a non-empty string")
        
        # Check if column already exists
        if new_column in df.columns and not overwrite:
            raise StepProcessorError(
                f"Column '{new_column}' already exists. Set 'overwrite: true' to replace it."
            )
        
        # Validate calculation
        if not isinstance(calculation, dict):
            raise StepProcessorError("'calculation' must be a dictionary")
        
        if len(calculation) == 0:
            raise StepProcessorError("'calculation' dictionary cannot be empty")
    
    def _apply_expression_calculation(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply a general expression calculation.
        
        This is the most flexible type - allows pandas-style operations.
        """
        if 'formula' not in calculation:
            raise StepProcessorError("Expression calculation requires 'formula' field")
        
        formula = calculation['formula']
        
        # Guard clause: formula must be a string
        if not isinstance(formula, str):
            raise StepProcessorError("Formula must be a string")
        
        # Replace column references in formula
        safe_formula = self._make_formula_safe(df, formula)
        
        try:
            # Evaluate the formula
            df[new_column] = eval(safe_formula)
            logger.debug(f"Applied expression formula: {formula}")
            
        except Exception as e:
            raise StepProcessorError(f"Error evaluating formula '{formula}': {e}")
        
        return df
    
    def _apply_concatenation(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply string concatenation calculation.
        """
        if 'columns' not in calculation:
            raise StepProcessorError("Concatenation calculation requires 'columns' field")
        
        columns = calculation['columns']
        separator = calculation.get('separator', '')
        
        # Guard clauses
        if not isinstance(columns, list):
            raise StepProcessorError("Concatenation 'columns' must be a list")
        
        if len(columns) < 2:
            raise StepProcessorError("Concatenation requires at least 2 columns")
        
        # Validate columns exist
        for col in columns:
            if not isinstance(col, str):
                raise StepProcessorError(f"Column name must be a string, got: {type(col)}")
            if col not in df.columns:
                raise StepProcessorError(f"Column '{col}' not found for concatenation")
        
        try:
            # Concatenate columns
            df[new_column] = df[columns].astype(str).agg(separator.join, axis=1)
            logger.debug(f"Concatenated columns: {columns} with separator '{separator}'")
            
        except Exception as e:
            raise StepProcessorError(f"Error concatenating columns: {e}")
        
        return df
    
    def _apply_conditional_logic(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply conditional (if-then-else) logic.
        """
        required_fields = ['condition_column', 'condition', 'value_if_true', 'value_if_false']
        for field in required_fields:
            if field not in calculation:
                raise StepProcessorError(f"Conditional calculation requires '{field}' field")
        
        condition_column = calculation['condition_column']
        condition = calculation['condition']
        value_if_true = calculation['value_if_true']
        value_if_false = calculation['value_if_false']
        condition_value = calculation.get('condition_value')
        
        # Validate condition column
        if condition_column not in df.columns:
            raise StepProcessorError(f"Condition column '{condition_column}' not found")
        
        try:
            # Apply conditional logic based on condition type
            if condition == 'equals':
                if condition_value is None:
                    raise StepProcessorError("Condition 'equals' requires 'condition_value'")
                mask = df[condition_column] == condition_value
                
            elif condition == 'greater_than':
                if condition_value is None:
                    raise StepProcessorError("Condition 'greater_than' requires 'condition_value'")
                mask = df[condition_column] > condition_value
                
            elif condition == 'less_than':
                if condition_value is None:
                    raise StepProcessorError("Condition 'less_than' requires 'condition_value'")
                mask = df[condition_column] < condition_value
                
            elif condition == 'contains':
                if condition_value is None:
                    raise StepProcessorError("Condition 'contains' requires 'condition_value'")
                mask = df[condition_column].astype(str).str.contains(str(condition_value), na=False)
                
            elif condition == 'is_null':
                mask = df[condition_column].isnull()
                
            elif condition == 'not_null':
                mask = df[condition_column].notnull()
                
            else:
                available_conditions = ['equals', 'greater_than', 'less_than', 'contains', 'is_null', 'not_null']
                raise StepProcessorError(
                    f"Unknown condition: '{condition}'. "
                    f"Available conditions: {', '.join(available_conditions)}"
                )
            
            # Apply the conditional logic
            df[new_column] = np.where(mask, value_if_true, value_if_false)
            logger.debug(f"Applied conditional: {condition_column} {condition} → {value_if_true}/{value_if_false}")
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error applying conditional logic: {e}")
        
        return df
    
    def _apply_math_operation(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply mathematical operations between columns.
        """
        if 'operation' not in calculation:
            raise StepProcessorError("Math calculation requires 'operation' field")
        
        operation = calculation['operation']
        
        if operation in ['add', 'subtract', 'multiply', 'divide']:
            # Binary operations
            if 'column1' not in calculation or 'column2' not in calculation:
                raise StepProcessorError(f"Operation '{operation}' requires 'column1' and 'column2' fields")
            
            col1 = calculation['column1']
            col2 = calculation['column2']
            
            # Validate columns
            for col in [col1, col2]:
                if col not in df.columns:
                    raise StepProcessorError(f"Column '{col}' not found for math operation")
            
            try:
                if operation == 'add':
                    df[new_column] = df[col1] + df[col2]
                elif operation == 'subtract':
                    df[new_column] = df[col1] - df[col2]
                elif operation == 'multiply':
                    df[new_column] = df[col1] * df[col2]
                elif operation == 'divide':
                    df[new_column] = df[col1] / df[col2]
                
                logger.debug(f"Applied math operation: {col1} {operation} {col2}")
                
            except Exception as e:
                raise StepProcessorError(f"Error applying math operation '{operation}': {e}")
        
        elif operation in ['sum', 'mean', 'min', 'max']:
            # Multi-column aggregation
            if 'columns' not in calculation:
                raise StepProcessorError(f"Operation '{operation}' requires 'columns' field")
            
            columns = calculation['columns']
            
            if not isinstance(columns, list):
                raise StepProcessorError("Math operation 'columns' must be a list")
            
            # Validate columns
            for col in columns:
                if col not in df.columns:
                    raise StepProcessorError(f"Column '{col}' not found for math operation")
            
            try:
                if operation == 'sum':
                    df[new_column] = df[columns].sum(axis=1)
                elif operation == 'mean':
                    df[new_column] = df[columns].mean(axis=1)
                elif operation == 'min':
                    df[new_column] = df[columns].min(axis=1)
                elif operation == 'max':
                    df[new_column] = df[columns].max(axis=1)
                
                logger.debug(f"Applied aggregation: {operation} of {columns}")
                
            except Exception as e:
                raise StepProcessorError(f"Error applying aggregation '{operation}': {e}")
        
        else:
            available_operations = ['add', 'subtract', 'multiply', 'divide', 'sum', 'mean', 'min', 'max']
            raise StepProcessorError(
                f"Unknown math operation: '{operation}'. "
                f"Available operations: {', '.join(available_operations)}"
            )
        
        return df
    
    def _apply_date_calculation(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply date-based calculations.
        """
        if 'operation' not in calculation:
            raise StepProcessorError("Date calculation requires 'operation' field")
        
        operation = calculation['operation']
        
        if operation == 'days_between':
            if 'start_date_column' not in calculation or 'end_date_column' not in calculation:
                raise StepProcessorError("Operation 'days_between' requires 'start_date_column' and 'end_date_column'")
            
            start_col = calculation['start_date_column']
            end_col = calculation['end_date_column']
            
            # Validate columns
            for col in [start_col, end_col]:
                if col not in df.columns:
                    raise StepProcessorError(f"Date column '{col}' not found")
            
            try:
                # Convert to datetime if not already
                start_dates = pd.to_datetime(df[start_col])
                end_dates = pd.to_datetime(df[end_col])
                
                # Calculate difference in days
                df[new_column] = (end_dates - start_dates).dt.days
                logger.debug(f"Calculated days between {start_col} and {end_col}")
                
            except Exception as e:
                raise StepProcessorError(f"Error calculating days between dates: {e}")
        
        else:
            available_operations = ['days_between']
            raise StepProcessorError(
                f"Unknown date operation: '{operation}'. "
                f"Available operations: {', '.join(available_operations)}"
            )
        
        return df
    
    def _apply_text_operation(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply text/string operations.
        """
        if 'operation' not in calculation or 'column' not in calculation:
            raise StepProcessorError("Text calculation requires 'operation' and 'column' fields")
        
        operation = calculation['operation']
        column = calculation['column']
        
        # Validate column
        if column not in df.columns:
            raise StepProcessorError(f"Text column '{column}' not found")
        
        try:
            if operation == 'length':
                df[new_column] = df[column].astype(str).str.len()
                
            elif operation == 'upper':
                df[new_column] = df[column].astype(str).str.upper()
                
            elif operation == 'lower':
                df[new_column] = df[column].astype(str).str.lower()
                
            elif operation == 'extract_numbers':
                df[new_column] = df[column].astype(str).str.extract(r'(\d+)')[0]
                
            elif operation == 'substring':
                start = calculation.get('start', 0)
                length = calculation.get('length', None)
                if length:
                    df[new_column] = df[column].astype(str).str.slice(start, start + length)
                else:
                    df[new_column] = df[column].astype(str).str.slice(start)
                    
            else:
                available_operations = ['length', 'upper', 'lower', 'extract_numbers', 'substring']
                raise StepProcessorError(
                    f"Unknown text operation: '{operation}'. "
                    f"Available operations: {', '.join(available_operations)}"
                )
            
            logger.debug(f"Applied text operation '{operation}' to column '{column}'")
            
        except Exception as e:
            if isinstance(e, StepProcessorError):
                raise
            else:
                raise StepProcessorError(f"Error applying text operation '{operation}': {e}")
        
        return df
    
    def _make_formula_safe(self, df: pd.DataFrame, formula: str) -> str:
        """
        Make a formula safe for evaluation by replacing column names with df references.
        
        Args:
            df: DataFrame with columns
            formula: Original formula string
            
        Returns:
            Safe formula string with proper df references
        """
        safe_formula = formula
        
        # Replace column names with df['column'] references
        for col in df.columns:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(col) + r'\b'
            replacement = f"df['{col}']"
            safe_formula = re.sub(pattern, replacement, safe_formula)
        
        logger.debug(f"Formula: {formula} → {safe_formula}")
        return safe_formula
    
    def get_supported_calculation_types(self) -> list:
        """
        Get list of supported calculation types.
        
        Returns:
            List of supported calculation type strings
        """
        return ['expression', 'concat', 'conditional', 'math', 'date', 'text']
    
    def get_supported_conditions(self) -> list:
        """
        Get list of supported conditional logic conditions.
        
        Returns:
            List of supported condition strings
        """
        return ['equals', 'greater_than', 'less_than', 'contains', 'is_null', 'not_null']
    
    def get_supported_math_operations(self) -> list:
        """
        Get list of supported mathematical operations.
        
        Returns:
            List of supported math operation strings
        """
        return ['add', 'subtract', 'multiply', 'divide', 'sum', 'mean', 'min', 'max']
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Add new columns with calculated values based on existing data',
            'calculation_types': self.get_supported_calculation_types(),
            'conditional_operations': self.get_supported_conditions(),
            'math_operations': self.get_supported_math_operations(),
            'supported_features': [
                'expression_calculations', 'string_concatenation', 'conditional_logic',
                'mathematical_operations', 'date_calculations', 'text_operations',
                'multi_column_aggregations', 'column_overwriting'
            ],
            'examples': {
                'simple_math': "Price * Quantity = Total_Value",
                'concatenation': "First_Name + Last_Name = Full_Name",
                'conditional': "If Quantity > 100 then 'High' else 'Low'"
            }
        }
    
    def get_usage_examples(self) -> dict:
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('add_calculated_column')
