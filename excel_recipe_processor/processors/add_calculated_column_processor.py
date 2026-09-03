"""
Add calculated column step processor for Excel automation recipes.

excel_recipe_processor/processors/add_calculated_column_processor.py

Handles creating new columns with calculated values based on existing data.
"""

import re
import logging

import numpy as np
import pandas as pd

from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class AddCalculatedColumnProcessor(BaseStepProcessor):
    """
    Processor for adding calculated columns to DataFrames.
    
    Supports various calculation types including mathematical operations,
    string manipulations, date calculations, and conditional logic.

    KEY CONVENTION: an evaluated string never sits under a bare key. The
    key names the dialect (pandas_formula, pandas_rules, pandas_default);
    plain names (when, then) are structure INSIDE a dialect-declared
    container. Declarative types (math, text, date, concat, conditional)
    name columns and operations and evaluate nothing.

    SPILL (2026-09-03): a calculation may produce more than one column,
    the way an Excel formula spills horizontally. new_column is the
    calculated column; spill_columns names the rest, in order. The
    result's shape is checked against the declaration - a spill that
    was not declared, a declared spill that did not arrive, or a
    result that is not one value per row (a vertical or 2-D spill)
    is an error. Only the evaluated types (expression, first_match)
    may spill.
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
            'calculation': {'pandas_formula': 'test_value'}
        }
    
    def execute(self, data) -> pd.DataFrame:
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
        spill_columns           = self.get_config_value('spill_columns', None)
        
        # Validate configuration
        self._validate_calculation_config(data, new_column, calculation, overwrite)
        spill_columns = self._validate_spill_columns(
            data, new_column, spill_columns, calculation_type, overwrite)
        
        # Work on a copy
        result_data = data.copy()
        
        try:
            # Apply the calculation based on type
            if calculation_type == 'constant':
                result_data = self._apply_constant(result_data, new_column, calculation)
            elif calculation_type == 'row_number':
                result_data = self._apply_row_number(result_data, new_column, calculation)
            elif calculation_type == 'expression':
                result_data = self._apply_expression_calculation(
                    result_data, new_column, calculation, spill_columns)
            elif calculation_type == 'first_match':
                result_data = self._apply_first_match(
                    result_data, new_column, calculation, spill_columns)
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
                available_types = self.get_supported_calculation_types()
                raise StepProcessorError(
                    f"Unknown calculation type: '{calculation_type}'. "
                    f"Available types: {', '.join(available_types)}"
                )
            
            # Verify every declared column was created
            for column in [new_column] + spill_columns:
                if column not in result_data.columns:
                    raise StepProcessorError(f"Failed to create calculated column '{column}'")
            
            result_info = f"added calculated column '{new_column}'"
            if spill_columns:
                result_info += f" spilling into {spill_columns}"
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

    SPILLING_CALCULATION_TYPES = ('expression', 'first_match')

    def _validate_spill_columns(self, df: pd.DataFrame, new_column: str, spill_columns,
                                calculation_type: str, overwrite: bool) -> list:
        """
        Validate the optional spill_columns declaration and return it as a list.

        Absent means no spill: an empty list. Only the evaluated calculation
        types may spill; the declarative ones have nothing to spill from.
        """
        if spill_columns is None:
            return []
        if calculation_type not in self.SPILLING_CALCULATION_TYPES:
            raise StepProcessorError(
                f"'spill_columns' applies only to calculation types "
                f"{list(self.SPILLING_CALCULATION_TYPES)}, not '{calculation_type}'"
            )
        if (not isinstance(spill_columns, list) or len(spill_columns) == 0
                or not all(isinstance(name, str) and name.strip() for name in spill_columns)):
            raise StepProcessorError(
                "'spill_columns' must be a non-empty list of column name strings"
            )
        declared = [new_column] + spill_columns
        if len(set(declared)) != len(declared):
            raise StepProcessorError(
                f"'new_column' and 'spill_columns' must all be distinct, got {declared}"
            )
        for name in spill_columns:
            if name in df.columns and not overwrite:
                raise StepProcessorError(
                    f"Spill column '{name}' already exists. Set 'overwrite: true' to replace it."
                )
        return spill_columns

    def _assign_result(self, df: pd.DataFrame, new_column: str, spill_columns: list,
                       result, context: str) -> pd.DataFrame:
        """
        Place an evaluated result into the declared column(s), checking its shape.

        One value per row is the only shape this processor fills: a Series
        (or scalar, broadcast) goes to new_column; a DataFrame or a
        tuple/list of Series spills its columns 2..k into spill_columns.
        Declared and delivered widths must agree, and every column must
        be row-length - a vertical or 2-D spill is refused, not coerced.
        """
        declared = [new_column] + spill_columns
        row_count = len(df)

        if isinstance(result, pd.DataFrame):
            pieces = [result.iloc[:, i] for i in range(result.shape[1])]
        elif isinstance(result, (tuple, list)) and result and all(
                isinstance(piece, (pd.Series, np.ndarray)) for piece in result):
            pieces = list(result)
        else:
            pieces = [result]

        if len(pieces) != len(declared):
            raise StepProcessorError(
                f"{context}: declared {len(declared)} column(s) {declared} but the "
                f"calculation returned {len(pieces)} - "
                + ("declare the extra column(s) in 'spill_columns'"
                   if len(pieces) > len(declared) else
                   "the calculation must return one value per declared column")
            )

        for name, piece in zip(declared, pieces):
            if isinstance(piece, (pd.Series, np.ndarray, list)):
                if len(piece) != row_count:
                    raise StepProcessorError(
                        f"{context}: result for '{name}' has {len(piece)} values for "
                        f"{row_count} rows - this processor fills columns, one value "
                        f"per row; a vertical or 2-D spill is not a column"
                    )
                if isinstance(piece, pd.Series):
                    piece = piece.reset_index(drop=True)
                    piece.index = df.index
            df[name] = piece
        return df

    def _apply_expression_calculation(self, df: pd.DataFrame, new_column: str, calculation: dict,
                                      spill_columns=None) -> pd.DataFrame:
        """
        Apply expression calculation with support for both legacy formula and new formula_components.
        """
        spill_columns = spill_columns or []
        # Check for new formula_components syntax first
        if 'formula_components' in calculation:
            if spill_columns:
                raise StepProcessorError("'formula_components' cannot spill; use 'pandas_formula'")
            return self._apply_formula_components(df, new_column, calculation)
        
        # The key names its language (2026-08-26): the expression is
        # pandas syntax with {col:Name} column references
        if 'formula' in calculation:
            raise StepProcessorError(
                "'formula' was renamed 'pandas_formula' (2026-08-26): the "
                "expression is pandas syntax with {col:Name} column "
                "references, and the key now says so. Rename the key; the "
                "value is unchanged."
            )
        if 'pandas_formula' not in calculation:
            raise StepProcessorError(
                "Expression calculation requires a 'pandas_formula' field "
                "(or 'formula_components')"
            )

        formula = calculation['pandas_formula']
        
        # Guard clause: formula must be a string
        if not isinstance(formula, str):
            raise StepProcessorError("Formula must be a string")
        
        # Replace column references in formula (legacy method)
        safe_formula = self._make_formula_safe(df, formula)
        
        try:
            # Evaluate the formula
            result = eval(safe_formula)
            logger.debug(f"Applied legacy expression formula: {formula}")
            
        except Exception as e:
            from excel_recipe_processor.core.column_tokens import (
                name_error_guidance, formula_failure_guidance,
            )
            columns = [str(c) for c in df.columns]
            guidance = (name_error_guidance(e, columns)
                        or formula_failure_guidance(formula, columns))
            if guidance:
                raise StepProcessorError(
                    f"Error evaluating formula '{formula}': {guidance}")
            raise StepProcessorError(f"Error evaluating formula '{formula}': {e}")
        
        return self._assign_result(df, new_column, spill_columns, result, 'pandas_formula')

    def _apply_first_match(self, df: pd.DataFrame, new_column: str, calculation: dict,
                           spill_columns: list) -> pd.DataFrame:
        """
        First-match rule table: ordered rules, exactly one wins per row, and
        every declared column takes its value from the WINNING RULE'S ROW.

        calculation:
          pandas_rules:
            - when: "<pandas predicate>"
              then: ["<slot for new_column>", "<slot per spill column>", ...]
          pandas_default: ["<slot>", ...]

        A slot is pandas text: an expression (Series of row length), a
        quoted literal or number (broadcast), or "" for the column's typed
        blank. Every rule and the default must carry exactly one slot per
        declared column - the Excel HSTACK discipline, where a blank is a
        visible "" and never an omitted term. Everything is validated and
        column references compiled BEFORE any evaluation, so a typo in
        rule 37 is reported by rule number rather than mid-run.
        """
        declared = [new_column] + spill_columns
        rules, default = self._validate_first_match_config(df, calculation, declared)

        # Compile every string against the frame's columns first
        compiled_rules = []
        for index, rule in enumerate(rules):
            context = f"pandas_rules rule {index + 1}"
            compiled_when = self._compile_slot(df, rule['when'], f"{context} 'when'")
            compiled_then = [self._compile_slot(df, slot, f"{context} 'then' slot {j + 1}")
                             for j, slot in enumerate(rule['then'])]
            compiled_rules.append((compiled_when, compiled_then))
        compiled_default = [self._compile_slot(df, slot, f"pandas_default slot {j + 1}")
                            for j, slot in enumerate(default)]

        # Which rule wins each row: first true predicate, -1 for the default
        winner = pd.Series(-1, index=df.index)
        undecided = pd.Series(True, index=df.index)
        hit_counts = []
        for index, (compiled_when, _) in enumerate(compiled_rules):
            mask = self._evaluate_predicate(df, compiled_when, rules[index]['when'], index)
            takes = undecided & mask
            winner[takes] = index
            undecided = undecided & ~mask
            hit_counts.append(int(takes.sum()))
        default_count = int(undecided.sum())

        # Evaluate slots and place each declared column from its winning row
        for position, column in enumerate(declared):
            slot_values = [self._evaluate_slot(df, compiled[position], rules[i]['then'][position],
                                               f"rule {i + 1}")
                           for i, (_, compiled) in enumerate(compiled_rules)]
            default_value = self._evaluate_slot(df, compiled_default[position],
                                                default[position], 'pandas_default')
            df[column] = self._select_by_winner(df, winner, slot_values, default_value, column)

        # The census: one line per rule that won rows, never-matched rules
        # gathered onto one line (a long cascade has many by design)
        for index, count in enumerate(hit_counts):
            if count:
                logger.info(f"   rule {index + 1:>3}: {count:>7,} row(s)")
        logger.info(f"   default: {default_count:>7,} row(s)")
        never = [str(index + 1) for index, count in enumerate(hit_counts) if not count]
        if never:
            logger.info(f"   never matched ({len(never)} rule(s)): {', '.join(never)}")
        return df

    def _validate_first_match_config(self, df: pd.DataFrame, calculation: dict, declared: list) -> tuple:
        """Shape-check the rule table before anything is evaluated."""
        if 'rules' in calculation or 'default' in calculation:
            raise StepProcessorError(
                "first_match keys name their dialect: use 'pandas_rules' and "
                "'pandas_default' (an evaluated string never sits under a bare key)"
            )
        unknown = set(calculation.keys()) - {'pandas_rules', 'pandas_default'}
        if unknown:
            raise StepProcessorError(
                f"first_match calculation has unknown key(s) {sorted(unknown)}; "
                f"supported: pandas_rules, pandas_default"
            )
        rules = calculation.get('pandas_rules')
        default = calculation.get('pandas_default')
        width = len(declared)
        if not isinstance(rules, list) or len(rules) == 0:
            raise StepProcessorError("first_match requires 'pandas_rules': a non-empty list of rules")
        if default is None:
            raise StepProcessorError(
                "first_match requires 'pandas_default': the result when no rule matches, "
                "stated as one slot per declared column"
            )
        if not isinstance(default, list) or len(default) != width:
            raise StepProcessorError(
                f"'pandas_default' must be a list of exactly {width} slot(s) for "
                f"{declared}, got {default!r}"
            )
        for index, rule in enumerate(rules):
            context = f"pandas_rules rule {index + 1}"
            if not isinstance(rule, dict):
                raise StepProcessorError(f"{context} must be a mapping with 'when' and 'then'")
            extra = set(rule.keys()) - {'when', 'then'}
            if extra:
                raise StepProcessorError(f"{context} has unknown key(s) {sorted(extra)}; only 'when' and 'then' are allowed")
            when = rule.get('when')
            then = rule.get('then')
            if not isinstance(when, str) or not when.strip():
                raise StepProcessorError(f"{context} requires a non-empty 'when' predicate string")
            if not isinstance(then, list) or len(then) != width:
                raise StepProcessorError(
                    f"{context} 'then' must be a list of exactly {width} slot(s) for "
                    f"{declared}, got {len(then) if isinstance(then, list) else then!r}"
                )
            for j, slot in enumerate(then):
                if not isinstance(slot, (str, int, float)):
                    raise StepProcessorError(
                        f"{context} 'then' slot {j + 1} must be pandas text, a number, "
                        f"or \"\" for blank, got {type(slot).__name__}"
                    )
        for j, slot in enumerate(default):
            if not isinstance(slot, (str, int, float)):
                raise StepProcessorError(
                    f"'pandas_default' slot {j + 1} must be pandas text, a number, "
                    f"or \"\" for blank, got {type(slot).__name__}"
                )
        return rules, default

    def _compile_slot(self, df: pd.DataFrame, slot, context: str):
        """Translate column references now, so unknown columns fail by rule number."""
        if not isinstance(slot, str):
            return slot
        if slot == '':
            return ''
        try:
            return self._make_formula_safe(df, slot)
        except StepProcessorError as error:
            raise StepProcessorError(f"{context}: {error}")

    def _evaluate_predicate(self, df: pd.DataFrame, compiled: str, original: str, index: int) -> pd.Series:
        """Evaluate a 'when' to a boolean Series; blanks count as false."""
        try:
            result = eval(compiled)
        except Exception as error:
            raise StepProcessorError(
                f"pandas_rules rule {index + 1} 'when' failed: {error} - predicate: {original}"
            )
        if isinstance(result, (bool, np.bool_)):
            return pd.Series(bool(result), index=df.index)
        if not isinstance(result, pd.Series) or len(result) != len(df):
            raise StepProcessorError(
                f"pandas_rules rule {index + 1} 'when' must yield one boolean per row, "
                f"got {type(result).__name__} - predicate: {original}"
            )
        return result.fillna(False).astype(bool)

    def _evaluate_slot(self, df: pd.DataFrame, compiled, original, context: str):
        """
        Evaluate a 'then' slot: None for blank, a scalar, or a row-length Series.
        """
        if isinstance(compiled, str) and compiled == '':
            return None
        if not isinstance(compiled, str):
            return compiled
        try:
            result = eval(compiled)
        except Exception as error:
            raise StepProcessorError(f"{context} slot failed: {error} - slot: {original}")
        if isinstance(result, (pd.Series, np.ndarray, list)):
            if len(result) != len(df):
                raise StepProcessorError(
                    f"{context} slot has {len(result)} values for {len(df)} rows - "
                    f"one value per row, or a scalar - slot: {original}"
                )
            if isinstance(result, pd.Series):
                result = result.reset_index(drop=True)
                result.index = df.index
            return result
        return result

    def _select_by_winner(self, df: pd.DataFrame, winner: pd.Series, slot_values: list,
                          default_value, column: str) -> pd.Series:
        """
        Assemble one output column from per-rule slot values by winning rule.

        The empty is typed from the populated slots: datetime slots give a
        NaT column, numeric slots NaN, anything else a missing object - so
        an output nobody fills still lands with the right dtype.
        """
        populated = [value for value in slot_values + [default_value] if value is not None]
        kinds = set()
        for value in populated:
            if isinstance(value, pd.Series):
                kinds.add('datetime' if pd.api.types.is_datetime64_any_dtype(value)
                          else 'number' if pd.api.types.is_numeric_dtype(value) else 'object')
            elif isinstance(value, (pd.Timestamp, np.datetime64)):
                kinds.add('datetime')
            elif isinstance(value, (int, float, np.integer, np.floating)) and not isinstance(value, bool):
                kinds.add('number')
            else:
                kinds.add('object')
        if kinds == {'datetime'}:
            out = pd.Series(pd.NaT, index=df.index, dtype='datetime64[ns]')
        elif kinds == {'number'}:
            out = pd.Series(np.nan, index=df.index, dtype='float64')
        else:
            out = pd.Series(np.nan, index=df.index, dtype='object')
            if len(kinds) > 1:
                logger.debug(f"Column '{column}': mixed slot kinds {sorted(kinds)}, landing as object")

        for index, value in enumerate(slot_values):
            mask = winner == index
            if value is None or not mask.any():
                continue
            out[mask] = value[mask] if isinstance(value, pd.Series) else value
        mask = winner == -1
        if default_value is not None and mask.any():
            out[mask] = default_value[mask] if isinstance(default_value, pd.Series) else default_value
        return out

    def _apply_formula_components(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Apply formula using the new robust components-based approach.
        
        Supports:
        - Simple operations: ["Price", "*", "Net Weight"]
        - Complex math: [["Revenue", "-", "Cost"], "/", "Revenue", "*", "100"]
        - Conditionals: [{"condition": {...}, "if_true": [...], "if_false": [...]}]
        - Mixed expressions with any combination of the above
        """
        formula_components = calculation['formula_components']
        
        # Validate components
        if not isinstance(formula_components, list):
            raise StepProcessorError("'formula_components' must be a list")
        
        if len(formula_components) == 0:
            raise StepProcessorError("'formula_components' cannot be empty")
        
        try:
            # Build and evaluate the expression
            pandas_expression = self._build_pandas_expression(df, formula_components)
            
            # Execute the expression
            df[new_column] = eval(pandas_expression)
            
            logger.debug(f"Applied formula_components: {formula_components}")
            logger.debug(f"Generated pandas expression: {pandas_expression}")
            
        except Exception as e:
            raise StepProcessorError(f"Error evaluating formula_components: {e}")
        
        return df

    def _build_pandas_expression(self, df: pd.DataFrame, components) -> str:
        """
        Recursively build a pandas expression from formula components.
        
        Args:
            df: DataFrame for column validation
            components: List or dict of formula components
            
        Returns:
            String pandas expression ready for eval()
        """
        if isinstance(components, list):
            return self._build_list_expression(df, components)
        elif isinstance(components, dict):
            return self._build_conditional_expression(df, components)
        elif isinstance(components, str):
            return self._build_column_or_operator(df, components)
        elif isinstance(components, (int, float)):
            return str(components)
        else:
            raise StepProcessorError(f"Invalid component type: {type(components)}")

    def _build_list_expression(self, df: pd.DataFrame, components: list) -> str:
        """
        Build expression from a list of components.
        
        Examples:
        - ["Price", "*", "Net Weight"] → "df['Price'] * df['Net Weight']"
        - [["A", "+", "B"], "*", "C"] → "(df['A'] + df['B']) * df['C']"
        """
        if len(components) == 1:
            # Single component - could be nested
            return self._build_pandas_expression(df, components[0])
        
        # Multiple components - alternate between operands and operators
        expression_parts = []
        
        for i, component in enumerate(components):
            if isinstance(component, list):
                # Nested list - wrap in parentheses
                nested_expr = self._build_list_expression(df, component)
                expression_parts.append(f"({nested_expr})")
            elif isinstance(component, dict):
                # Conditional logic
                conditional_expr = self._build_conditional_expression(df, component)
                expression_parts.append(f"({conditional_expr})")
            else:
                # Simple component
                expression_parts.append(self._build_column_or_operator(df, component))
        
        return " ".join(expression_parts)

    def _build_conditional_expression(self, df: pd.DataFrame, conditional: dict) -> str:
        """
        Build conditional expression from dictionary specification.
        
        Example:
        {
            "condition": {"column": "Weight", "operator": "<", "value": 50},
            "if_true": ["Weight", "*", "2.50"],
            "if_false": ["Weight", "*", "1.80"]
        }
        → "np.where(df['Weight'] < 50, df['Weight'] * 2.50, df['Weight'] * 1.80)"
        """
        required_keys = ['condition', 'if_true', 'if_false']
        for key in required_keys:
            if key not in conditional:
                raise StepProcessorError(f"Conditional expression missing required key: '{key}'")
        
        # Build condition
        condition_expr = self._build_condition_expression(df, conditional['condition'])
        
        # Build true and false branches
        true_expr = self._build_pandas_expression(df, conditional['if_true'])
        false_expr = self._build_pandas_expression(df, conditional['if_false'])
        
        # Return numpy.where expression
        return f"np.where({condition_expr}, {true_expr}, {false_expr})"

    def _build_condition_expression(self, df: pd.DataFrame, condition: dict) -> str:
        """
        Build condition expression for use in np.where.
        
        Example:
        {"column": "Weight", "operator": "<", "value": 50}
        → "df['Weight'] < 50"
        """
        required_keys = ['column', 'operator', 'value']
        for key in required_keys:
            if key not in condition:
                raise StepProcessorError(f"Condition missing required key: '{key}'")
        
        column = condition['column']
        operator = condition['operator']
        value = condition['value']
        
        # Validate column exists
        if column not in df.columns:
            available_columns = list(df.columns)
            raise StepProcessorError(
                f"Condition column '{column}' not found. Available columns: {available_columns}"
            )
        
        # Validate operator
        valid_operators = ['==', '!=', '<', '>', '<=', '>=', 'in', 'not_in', 'contains', 'not_contains']
        if operator not in valid_operators:
            raise StepProcessorError(f"Invalid operator '{operator}'. Valid operators: {valid_operators}")
        
        # Build column reference
        column_ref = f"df['{column}']"
        
        # Handle different operators
        if operator in ['==', '!=', '<', '>', '<=', '>=']:
            if isinstance(value, str):
                return f"{column_ref} {operator} '{value}'"
            else:
                return f"{column_ref} {operator} {value}"
        
        elif operator == 'in':
            if not isinstance(value, list):
                raise StepProcessorError("'in' operator requires a list value")
            return f"{column_ref}.isin({value})"
        
        elif operator == 'not_in':
            if not isinstance(value, list):
                raise StepProcessorError("'not_in' operator requires a list value")
            return f"~{column_ref}.isin({value})"
        
        elif operator == 'contains':
            return f"{column_ref}.astype(str).str.contains('{value}', na=False)"
        
        elif operator == 'not_contains':
            return f"~{column_ref}.astype(str).str.contains('{value}', na=False)"

    def _build_column_or_operator(self, df: pd.DataFrame, component: str) -> str:
        """
        Convert a string component to either a column reference or validate as operator.
        
        Args:
            df: DataFrame for column validation
            component: String component (column name or operator)
            
        Returns:
            Either df['column'] reference or validated operator
        """
        # Check if it's a column name
        if component in df.columns:
            return f"df['{component}']"
        
        # Check if it's a valid operator
        valid_operators = ['+', '-', '*', '/', '//', '%', '**', '&', '|', '^', '==', '!=', '<', '>', '<=', '>=']
        if component in valid_operators:
            return component
        
        # Check if it's a number (quoted)
        try:
            float(component)
            return component
        except ValueError:
            pass
        
        # Check if it's a quoted string value
        if component.startswith('"') and component.endswith('"'):
            return component
        elif component.startswith("'") and component.endswith("'"):
            return component
        
        # If we get here, it's an unknown component
        available_columns = list(df.columns)
        raise StepProcessorError(
            f"Unknown component '{component}'. Must be a column name, operator, or quoted value. "
            f"Available columns: {available_columns}"
        )

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
        # Replace column names with df['column'] references in ONE PASS.
        #
        # A loop of separate substitutions cannot do this correctly when one
        # column name contains another. With "Species" and "Major Species"
        # both present, replacing the short name first mangles the long one
        # ("Major df['Species']"), and replacing the long one first is no
        # better - the later short-name pass then matches INSIDE the text
        # just substituted. A single pass over an alternation, longest name
        # first so the regex prefers it, cannot re-enter its own output.
        # GRAMMAR, NOT RECOGNITION (2026-08-26). The heuristic era -
        # boundary regexes, literal masking, longest-first alternation -
        # produced four production incidents (truncation at a # in a
        # name, literals corrupted, and a column named like a python
        # identifier rewriting method calls). Column names now enter a
        # formula ONLY as backticked tokens (`Van Seq #`), parsed by a
        # single-pass typed tokenizer; a bare column name loose in code
        # is a HARD guided error, so the old ambiguity is
        # unrepresentable. See core/column_tokens.py.
        from excel_recipe_processor.core.column_tokens import (
            ColumnTokenError, build_dataframe_expression,
        )
        try:
            safe_formula = build_dataframe_expression(
                formula, [str(c) for c in df.columns])
        except ColumnTokenError as error:
            raise StepProcessorError(
                f"Formula rejected by the column-token grammar: {error}"
            )

        logger.debug(f"Formula: {formula} → {safe_formula}")
        return safe_formula
    
    def _apply_constant(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Fill the new column with one literal value on every row.

        The simplest calculation there is, and the one that was missing: a
        marker or category column ("Record Status": "current") has no source
        column to compute from.

        Args:
            df:          Frame to add the column to
            new_column:  Column to create
            calculation: Must carry 'value', the literal to fill with

        Returns:
            Frame with the new column added
        """
        if 'value' not in calculation:
            raise StepProcessorError(
                "Constant calculation requires a 'value' to fill the column with"
            )

        result = df.copy()
        result[new_column] = calculation['value']
        return result

    def _apply_row_number(self, df: pd.DataFrame, new_column: str, calculation: dict) -> pd.DataFrame:
        """
        Number the rows 1..N in current order.

        The motivating case: a display sheet whose meaning depends on row
        order (pivot-style blanked repeats) gets a sort-anchor column, so a
        user who re-sorts in Excel can always sort back to baseline.

        Position in the recipe matters twice over: add this AFTER the final
        sort (so the numbers describe the order that ships) and BEFORE any
        order-dependent display step (so what it restores is the state those
        steps assumed).

        Args:
            df:          Frame to number
            new_column:  Column to create
            calculation: Optional 'start' for the first row's number (default 1)

        Returns:
            Frame with the numbering column added
        """
        start = calculation.get('start', 1)

        if not isinstance(start, int):
            raise StepProcessorError(
                f"row_number 'start' must be an integer, got {type(start).__name__}"
            )

        result = df.copy()
        result[new_column] = range(start, start + len(result))
        return result

    def get_supported_calculation_types(self) -> list:
        """
        Get list of supported calculation types.
        
        Returns:
            List of supported calculation type strings
        """
        return ['constant', 'row_number', 'expression', 'first_match', 'concat', 'conditional', 'math', 'date', 'text']
    
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
                'expression_calculations', 'first_match_rule_tables', 'horizontal_spill',
                'string_concatenation', 'conditional_logic',
                'mathematical_operations', 'date_calculations', 'text_operations',
                'multi_column_aggregations', 'column_overwriting'
            ],
            'spill_columns': "Extra columns a calculation fills beside new_column, in order; "
                             "only for expression and first_match; the result width must match",
            'first_match': "calculation: {pandas_rules: [{when, then: [one slot per column]}], "
                           "pandas_default: [one slot per column]}; first true 'when' wins the row; "
                           "\"\" is the typed blank",
            'examples': {
                'simple_math': "Price * Quantity = Total_Value",
                'concatenation': "First_Name + Last_Name = Full_Name",
                'conditional': "If Quantity > 100 then 'High' else 'Low'"
            }
        }
    
    def get_usage_examples(self) -> dict:
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('add_calculated_column')

# End of file #
