"""
Enhanced Variable substitution for Excel automation recipes.

excel_recipe_processor/core/variable_substitution.py

Handles dynamic variable replacement in filenames and other strings
using date/time variables, file-based variables, and custom variables.
Now supports structured data types with explicit type declarations.

# Potential future extensions:
price_display: "{float:cost:,.2f}"     # 1234.56 → "1,234.56"  
item_count: "{int:quantity:,}"         # 1000 → "1,000"
file_size: "{int:bytes:size}"          # Custom formatters
"""

import re
import logging

from pathlib import Path
from datetime import datetime
from typing import Any, Union


logger = logging.getLogger(__name__)


class VariableSubstitutionError(Exception):
    """Raised when variable substitution fails."""
    pass


class VariableSubstitution:
    """
    Handles variable substitution in strings and structures for dynamic naming.
    
    Supports:
    - Date/time variables, file-based variables, and custom variables
    - String substitution: {variable} (backwards compatible)
    - Typed substitution: {type:variable} for lists, dicts, etc.
    """
    
    # Supported types for structured variables
    SUPPORTED_TYPES = {
        'str': str,
        'list': list, 
        'dict': dict,
        'int': int,
        'float': float,
        'bool': bool
    }
    
    def __init__(self, input_path=None, recipe_path=None, custom_variables=None):
        """
        Initialize variable substitution system.
        
        Args:
            input_path: Path to input file (for input-based variables)
            recipe_path: Path to recipe file (for recipe-based variables)
            custom_variables: Dictionary of custom variables (any type)
        """
        self.input_path = Path(input_path) if input_path else None
        self.recipe_path = Path(recipe_path) if recipe_path else None
        self.custom_variables = custom_variables or {}
        self.now = datetime.now()
        
        logger.debug(f"Initialized VariableSubstitution with {len(self.custom_variables)} custom variables")
    
    def substitute(self, template: str) -> str:
        """
        Substitute variables in a template string (string output only).
        
        Supports:
        - {variable} - untyped (assumes string, backwards compatible)
        - {str:variable} - explicit string
        - Other types will raise error in string context
        
        Args:
            template: Template string with {variable} placeholders
            
        Returns:
            String with variables substituted
            
        Raises:
            VariableSubstitutionError: If substitution fails
        """
        if not isinstance(template, str):
            return template
        
        if '{' not in template:
            # No variables to substitute
            return template
        
        try:
            # Handle typed variables first: {type:variable}
            result = self._substitute_typed_variables_in_string(template)
            
            # Then handle untyped variables: {variable} (backwards compatible)
            result = self._substitute_untyped_variables(result)
            
            # Finally handle formatted variables: {date:MMDD}
            result = self._substitute_formatted_variables(result)
            
            logger.debug(f"Variable substitution: '{template}' → '{result}'")
            return result
            
        except Exception as e:
            raise VariableSubstitutionError(f"Error substituting variables in '{template}': {e}")
    
    def substitute_structure(self, value: Any) -> Any:
        """
        Substitute variables in any structure (strings, lists, dicts).
        
        This is the main method for recursive config substitution.
        Handles both string and structure replacement.
        
        Args:
            value: Any value that might contain variable references
            
        Returns:
            Value with variables substituted
        """
        if isinstance(value, str):
            return self._substitute_in_string_or_structure(value)
        elif isinstance(value, dict):
            return {key: self.substitute_structure(val) for key, val in value.items()}
        elif isinstance(value, list):
            return [self.substitute_structure(item) for item in value]
        else:
            return value
    
    def _substitute_in_string_or_structure(self, template: str) -> Any:
        """
        Substitute variables in a string, returning either string or structure.
        
        If the entire string is a single typed variable like "{list:columns}",
        return the actual structure. Otherwise, do string substitution.
        """
        if not isinstance(template, str) or '{' not in template:
            return template
        
        # Check if entire string is a single typed variable: "{type:variable}"
        typed_match = re.fullmatch(r'\{(\w+):(\w+)\}', template)
        if typed_match:
            type_name, var_name = typed_match.groups()
            return self._get_typed_variable_value(type_name, var_name)
        
        # Otherwise, do string substitution
        return self.substitute(template)
    
    def _substitute_typed_variables_in_string(self, template: str) -> str:
        """
        Substitute typed variables in string context.
        
        Only str type is allowed in string context.
        Other types will raise an error.
        """
        def replace_typed_var(match):
            type_name = match.group(1)
            var_name = match.group(2)
            
            # Only string type allowed in string context
            if type_name != 'str':
                raise VariableSubstitutionError(
                    f"Cannot use {{{type_name}:{var_name}}} in string context. "
                    f"Only {{str:variable}} is allowed in strings."
                )
            
            # Get variable value and convert to string
            if var_name not in self.custom_variables:
                available_vars = list(self.custom_variables.keys())
                raise VariableSubstitutionError(
                    f"Unknown variable '{var_name}' in {{str:{var_name}}}. "
                    f"Available variables: {available_vars}"
                )
            
            value = self.custom_variables[var_name]
            return str(value)
        
        # Pattern for typed variables: {type:variable}
        pattern = r'\{(\w+):(\w+)\}'
        return re.sub(pattern, replace_typed_var, template)
    
    def _substitute_untyped_variables(self, template: str) -> str:
        """
        Substitute untyped variables (backwards compatible string substitution).
        """
        try:
            # Build variable dictionary (string values only for compatibility)
            variables = self._build_string_variable_dict()
            
            # Simple variable substitution: {variable}
            return template.format(**variables)
            
        except KeyError as e:
            # Provide helpful error message
            available_vars = list(self._build_string_variable_dict().keys())
            raise VariableSubstitutionError(
                f"Unknown variable {e} in template '{template}'. "
                f"Available variables: {available_vars}"
            )
    
    def _get_typed_variable_value(self, type_name: str, var_name: str) -> Any:
        """
        Get a typed variable value with validation.
        
        Args:
            type_name: Expected type name
            var_name: Variable name
            
        Returns:
            Variable value, optionally converted to expected type
        """
        # Validate type is supported
        if type_name not in self.SUPPORTED_TYPES:
            supported_types = list(self.SUPPORTED_TYPES.keys())
            raise VariableSubstitutionError(
                f"Unsupported type '{type_name}' in {{{type_name}:{var_name}}}. "
                f"Supported types: {supported_types}"
            )
        
        # Check variable exists
        if var_name not in self.custom_variables:
            available_vars = list(self.custom_variables.keys())
            raise VariableSubstitutionError(
                f"Unknown variable '{var_name}' in {{{type_name}:{var_name}}}. "
                f"Available variables: {available_vars}"
            )
        
        value = self.custom_variables[var_name]
        expected_type = self.SUPPORTED_TYPES[type_name]
        
        # Type validation and conversion
        if type_name == 'str':
            # String: convert anything to string
            return str(value)
        elif type_name in ['list', 'dict']:
            # Structured types: must match exactly
            if not isinstance(value, expected_type):
                raise VariableSubstitutionError(
                    f"Variable '{var_name}' is {type(value).__name__} "
                    f"but {{{type_name}:{var_name}}} expects {type_name}"
                )
            return value
        elif type_name in ['int', 'float', 'bool']:
            # Convertible types: try conversion
            try:
                if type_name == 'bool':
                    # Special handling for bool conversion
                    if isinstance(value, bool):
                        return value
                    elif isinstance(value, str):
                        return value.lower() in ('true', '1', 'yes', 'on')
                    else:
                        return bool(value)
                else:
                    return expected_type(value)
            except (ValueError, TypeError):
                raise VariableSubstitutionError(
                    f"Cannot convert variable '{var_name}' (value: {value}) "
                    f"to {type_name} for {{{type_name}:{var_name}}}"
                )
        
        return value
    
    def _build_string_variable_dict(self) -> dict:
        """
        Build dictionary of variables as strings (for backwards compatibility).
        """
        variables = {}
        
        # Date and time variables
        variables.update({
            'year':             self.now.strftime('%Y'),
            'month':            self.now.strftime('%m'),
            'day':              self.now.strftime('%d'),
            'hour':             self.now.strftime('%H'),
            'minute':           self.now.strftime('%M'),
            'second':           self.now.strftime('%S'),
            'date':             self.now.strftime('%Y%m%d'),
            'time':             self.now.strftime('%H%M%S'),
            'timestamp':        self.now.strftime('%Y%m%d_%H%M%S'),
            'MMDD':             self.now.strftime('%m%d'),
            'YYYY':             self.now.strftime('%Y'),
            'YY':               self.now.strftime('%y'),
            'MM':               self.now.strftime('%m'),
            'DD':               self.now.strftime('%d'),
            'HH':               self.now.strftime('%H'),
            'HHMM':             self.now.strftime('%H%M')
        })
        
        # Input file variables
        if self.input_path:
            variables.update({
                'input_filename':   self.input_path.name,
                'input_basename':   self.input_path.stem,
                'input_extension':  self.input_path.suffix
            })
        
        # Recipe file variables
        if self.recipe_path:
            variables.update({
                'recipe_filename':  self.recipe_path.name,
                'recipe_basename':  self.recipe_path.stem
            })
        
        # Custom variables - convert to strings for backwards compatibility
        for name, value in self.custom_variables.items():
            try:
                variables[name] = str(value)
            except:
                # Skip variables that can't be converted to strings
                logger.debug(f"Skipping variable '{name}' in string context (type: {type(value).__name__})")
        
        return variables
    
    def _build_variable_dict(self) -> dict:
        """Build dictionary of all available variables (preserving original types)."""
        variables = {}
        
        # Date and time variables (always strings)
        variables.update({
            'year':             self.now.strftime('%Y'),
            'month':            self.now.strftime('%m'),
            'day':              self.now.strftime('%d'),
            'hour':             self.now.strftime('%H'),
            'minute':           self.now.strftime('%M'),
            'second':           self.now.strftime('%S'),
            'date':             self.now.strftime('%Y%m%d'),
            'time':             self.now.strftime('%H%M%S'),
            'timestamp':        self.now.strftime('%Y%m%d_%H%M%S'),
            'MMDD':             self.now.strftime('%m%d'),
            'YYYY':             self.now.strftime('%Y'),
            'YY':               self.now.strftime('%y'),
            'MM':               self.now.strftime('%m'),
            'DD':               self.now.strftime('%d'),
            'HH':               self.now.strftime('%H'),
            'HHMM':             self.now.strftime('%H%M')
        })
        
        # Input file variables (always strings)
        if self.input_path:
            variables.update({
                'input_filename':   self.input_path.name,
                'input_basename':   self.input_path.stem,
                'input_extension':  self.input_path.suffix
            })
        
        # Recipe file variables (always strings)
        if self.recipe_path:
            variables.update({
                'recipe_filename':  self.recipe_path.name,
                'recipe_basename':  self.recipe_path.stem
            })
        
        # Custom variables (preserve original types)
        variables.update(self.custom_variables)
        
        return variables
    
    def _substitute_formatted_variables(self, text: str) -> str:
        """Handle formatted variables like {date:MMDD}."""
        # Pattern to match {variable:format}
        pattern = r'\{(\w+):([^}]+)\}'
        
        def replace_formatted(match):
            var_name            = match.group(1)
            format_spec         = match.group(2)
            
            if var_name == 'date':
                return self._format_date(format_spec)
            elif var_name == 'time':
                return self._format_time(format_spec)
            else:
                # For other variables, just return the variable value
                variables = self._build_string_variable_dict()
                return variables.get(var_name, match.group(0))
        
        return re.sub(pattern, replace_formatted, text)
    
    def _format_date(self, format_spec: str) -> str:
        """Format date according to specification."""
        format_map = {
            'YYYY': '%Y',
            'YY': '%y', 
            'MM': '%m',
            'DD': '%d',
            'MMDD': '%m%d',
            'YYYYMMDD': '%Y%m%d',
            'MonthDay': '%b%d',
            'Month': '%b',
            'MonthName': '%B'
        }
        
        # Try direct mapping first
        if format_spec in format_map:
            return self.now.strftime(format_map[format_spec])
        
        # Try as direct strftime format
        try:
            return self.now.strftime(format_spec)
        except ValueError:
            logger.warning(f"Invalid date format: {format_spec}")
            return format_spec  # Return original if format invalid
    
    def _format_time(self, format_spec: str) -> str:
        """Format time according to specification."""
        format_map = {
            'HH': '%H',
            'MM': '%M',
            'SS': '%S',
            'HHMM': '%H%M',
            'HHMMSS': '%H%M%S'
        }
        
        if format_spec in format_map:
            return self.now.strftime(format_map[format_spec])
        
        try:
            return self.now.strftime(format_spec)
        except ValueError:
            logger.warning(f"Invalid time format: {format_spec}")
            return format_spec
    
    def validate_template(self, template: str) -> list:
        """
        Validate a template string and return any unknown variables.
        
        Args:
            template: Template string to validate
            
        Returns:
            List of unknown variable names (empty if all variables are valid)
        """
        if not isinstance(template, str) or '{' not in template:
            return []
        
        available_vars = set(self._build_variable_dict().keys())
        unknown_vars = []
        
        # Find typed variables: {type:variable}
        typed_vars = re.findall(r'\{(\w+):(\w+)\}', template)
        for type_name, var_name in typed_vars:
            if type_name not in self.SUPPORTED_TYPES:
                unknown_vars.append(f"unsupported_type:{type_name}")
            elif var_name not in available_vars:
                unknown_vars.append(var_name)
        
        # Find simple variables: {variable} (excluding already processed typed ones)
        simple_pattern = r'\{(\w+)\}'
        simple_vars = re.findall(simple_pattern, template)
        for var_name in simple_vars:
            # Skip if this is part of a typed variable or formatted variable
            if f'{{{var_name}:' not in template and var_name not in available_vars:
                unknown_vars.append(var_name)
        
        return list(set(unknown_vars))  # Remove duplicates
    
    def add_custom_variable(self, name: str, value: Any) -> None:
        """
        Add or update a custom variable (any type).
        
        Args:
            name: Variable name
            value: Variable value (any type)
        """
        if not isinstance(name, str) or not name.strip():
            raise VariableSubstitutionError("Variable name must be a non-empty string")
        
        self.custom_variables[name] = value
        logger.debug(f"Added custom variable: {name} = {value} (type: {type(value).__name__})")
    
    def remove_custom_variable(self, name: str) -> None:
        """
        Remove a custom variable.
        
        Args:
            name: Variable name to remove
        """
        if name in self.custom_variables:
            del self.custom_variables[name]
            logger.debug(f"Removed custom variable: {name}")
    
    def get_available_variables(self) -> dict:
        """
        Get dictionary of all available variables and their current values.
        
        Returns:
            Dictionary mapping variable names to their current values
        """
        return self._build_variable_dict()
    
    def has_variables(self, text: str) -> bool:
        """
        Check if a text string contains variable placeholders.
        
        Args:
            text: Text to check
            
        Returns:
            True if text contains variables
        """
        return isinstance(text, str) and '{' in text and '}' in text
    
    def get_supported_types(self) -> list:
        """Get list of supported variable types."""
        return list(self.SUPPORTED_TYPES.keys())


# =============================================================================
# MODULE-LEVEL CONVENIENCE FUNCTIONS  
# =============================================================================

def substitute_variables(template: str, input_path=None, recipe_path=None, 
                        custom_variables=None) -> str:
    """
    Convenience function for one-time variable substitution.
    
    Args:
        template: Template string with variables
        input_path: Optional input file path
        recipe_path: Optional recipe file path  
        custom_variables: Optional custom variables dict
        
    Returns:
        String with variables substituted
    """
    substitution = VariableSubstitution(input_path, recipe_path, custom_variables)
    return substitution.substitute(template)


def substitute_structure(value: Any, input_path=None, recipe_path=None, 
                        custom_variables=None) -> Any:
    """
    Convenience function for one-time structure substitution.
    
    Args:
        value: Value that might contain variable references
        input_path: Optional input file path
        recipe_path: Optional recipe file path  
        custom_variables: Optional custom variables dict
        
    Returns:
        Value with variables substituted
    """
    substitution = VariableSubstitution(input_path, recipe_path, custom_variables)
    return substitution.substitute_structure(value)


def get_available_variables(input_path=None, recipe_path=None, custom_variables=None) -> dict:
    """
    Get dictionary of all available variables for documentation/debugging.
    
    Args:
        input_path: Optional input file path
        recipe_path: Optional recipe file path
        custom_variables: Optional custom variables dict
        
    Returns:
        Dictionary of all available variables
    """
    substitution = VariableSubstitution(input_path, recipe_path, custom_variables)
    return substitution.get_available_variables()


def validate_template(template: str, input_path=None, recipe_path=None, 
                        custom_variables=None) -> list:
    """
    Validate a template and return unknown variables.
    
    Args:
        template: Template string to validate
        input_path: Optional input file path
        recipe_path: Optional recipe file path
        custom_variables: Optional custom variables dict
        
    Returns:
        List of unknown variable names
    """
    substitution = VariableSubstitution(input_path, recipe_path, custom_variables)
    return substitution.validate_template(template)


# =============================================================================
# DOCUMENTATION HELPERS
# =============================================================================

def get_variable_documentation() -> dict:
    """
    Get documentation of all available variable types.
    
    Returns:
        Dictionary with variable documentation
    """
    return {
        'string_variables': {
            'untyped_syntax':   '{variable}',
            'typed_syntax':     '{str:variable}',
            'description':      'Backwards compatible string substitution'
        },
        'structured_variables': {
            'list_syntax':      '{list:variable}',
            'dict_syntax':      '{dict:variable}',
            'description':      'Direct structure replacement (lists, dicts)'
        },
        'convertible_variables': {
            'int_syntax':       '{int:variable}',
            'float_syntax':     '{float:variable}', 
            'bool_syntax':      '{bool:variable}',
            'description':      'Type conversion from string or compatible types'
        },
        'date_time_variables': [
            'year', 'month', 'day', 'hour', 'minute', 'second',
            'date', 'time', 'timestamp', 'MMDD', 'YYYY', 'YY', 'MM', 'DD', 'HH', 'HHMM'
        ],
        'formatted_variables': [
            '{date:MMDD}', '{date:YYYYMMDD}', '{date:MonthDay}', 
            '{time:HHMM}', '{time:HHMMSS}'
        ],
        'file_variables': [
            'input_filename',   'input_basename', 'input_extension',
            'recipe_filename',  'recipe_basename'
        ],
        'custom_variables':     'Any variables defined in settings.variables section',
        'examples': {
            'string_usage': {
                'untyped':      'output_file: "report_{timestamp}.xlsx"',
                'typed':        'output_file: "{str:filename_template}"'
            },
            'structure_usage': {
                'list':         'columns_to_keep: "{list:customer_columns}"',
                'dict':         'lookup_mapping: "{dict:status_codes}"'
            },
            'mixed_recipe': '''
settings:
  variables:
    customer_cols: ["Customer_ID", "Name", "Region"]
    status_codes: {"active": "A", "inactive": "I"}
    report_name: "customer_report"

recipe:
  - processor_type: "select_columns"
    columns_to_keep: "{list:customer_cols}"
    output_file: "{str:report_name}_{date}.xlsx"
            '''
        }
    }


# End of file #
