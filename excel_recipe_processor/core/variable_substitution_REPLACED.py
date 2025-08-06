"""
Variable substitution for Excel automation recipes.

Handles dynamic variable replacement in filenames and other strings
using date/time variables, file-based variables, and custom variables.
"""

import re
import logging

from pathlib import Path
from datetime import datetime


logger = logging.getLogger(__name__)


class VariableSubstitutionError(Exception):
    """Raised when variable substitution fails."""
    pass


class VariableSubstitution:
    """
    Handles variable substitution in strings for dynamic naming.
    
    Supports date/time variables, file-based variables, and custom variables
    defined in recipe settings.
    """
    
    def __init__(self, input_path=None, recipe_path=None, custom_variables=None):
        """
        Initialize variable substitution system.
        
        Args:
            input_path: Path to input file (for input-based variables)
            recipe_path: Path to recipe file (for recipe-based variables)
            custom_variables: Dictionary of custom variables from recipe settings
        """
        self.input_path = Path(input_path) if input_path else None
        self.recipe_path = Path(recipe_path) if recipe_path else None
        self.custom_variables = custom_variables or {}
        self.now = datetime.now()
        
        logger.debug(f"Initialized VariableSubstitution with {len(self.custom_variables)} custom variables")
    
    def substitute(self, template: str) -> str:
        """
        Substitute variables in a template string.
        
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
            # Build variable dictionary
            variables = self._build_variable_dict()
            
            # Simple variable substitution: {variable}
            result = template.format(**variables)
            
            # Advanced formatted substitution: {date:MMDD}
            result = self._substitute_formatted_variables(result)
            
            logger.debug(f"Variable substitution: '{template}' → '{result}'")
            return result
            
        except KeyError as e:
            # Provide helpful error message
            available_vars = list(self._build_variable_dict().keys())
            raise VariableSubstitutionError(
                f"Unknown variable {e} in template '{template}'. "
                f"Available variables: {available_vars}"
            )
        except Exception as e:
            raise VariableSubstitutionError(f"Error substituting variables in '{template}': {e}")
    
    def substitute_variables(self, template: str) -> str:
        """
        Alias for substitute() method for backward compatibility.
        
        Args:
            template: Template string with {variable} placeholders
            
        Returns:
            String with variables substituted
        """
        return self.substitute(template)
    
    def _build_variable_dict(self) -> dict:
        """Build dictionary of all available variables."""
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
        
        # Custom variables override built-ins
        variables.update(self.custom_variables)
        
        return variables
    
    def _substitute_formatted_variables(self, text: str) -> str:
        """Handle formatted variables like {date:MMDD}."""
        
        # Pattern to match {variable:format}
        pattern = r'\{(\w+):([^}]+)\}'
        
        def replace_formatted(match):
            var_name = match.group(1)
            format_spec = match.group(2)
            
            if var_name == 'date':
                return self._format_date(format_spec)
            elif var_name == 'time':
                return self._format_time(format_spec)
            else:
                # For other variables, just return the variable value
                variables = self._build_variable_dict()
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
        
        # Find all variables in template
        variables_in_template = set()
        
        # Simple variables: {variable}
        simple_vars = re.findall(r'\{(\w+)\}', template)
        variables_in_template.update(simple_vars)
        
        # Formatted variables: {variable:format} 
        formatted_vars = re.findall(r'\{(\w+):[^}]+\}', template)
        variables_in_template.update(formatted_vars)
        
        # Return unknown variables
        unknown_vars = variables_in_template - available_vars
        return list(unknown_vars)
    
    def add_custom_variable(self, name: str, value: str) -> None:
        """
        Add or update a custom variable.
        
        Args:
            name: Variable name
            value: Variable value
        """
        if not isinstance(name, str) or not name.strip():
            raise VariableSubstitutionError("Variable name must be a non-empty string")
        
        self.custom_variables[name] = str(value)
        logger.debug(f"Added custom variable: {name} = {value}")
    
    def remove_custom_variable(self, name: str) -> None:
        """
        Remove a custom variable.
        
        Args:
            name: Variable name to remove
        """
        if name in self.custom_variables:
            del self.custom_variables[name]
            logger.debug(f"Removed custom variable: {name}")


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
        'date_time_variables': [
            'year', 'month', 'day', 'hour', 'minute', 'second',
            'date', 'time', 'timestamp', 'MMDD', 'YYYY', 'YY', 'MM', 'DD', 'HH', 'HHMM'
        ],
        'formatted_variables': [
            '{date:MMDD}', '{date:YYYYMMDD}', '{date:MonthDay}', 
            '{time:HHMM}', '{time:HHMMSS}'
        ],
        'file_variables': [
            'input_filename', 'input_basename', 'input_extension',
            'recipe_filename', 'recipe_basename'
        ],
        'custom_variables': 'Any variables defined in settings.variables section',
        'examples': {
            'timestamp_filename': 'report_{timestamp}.xlsx',
            'date_filename': 'sales_{date}.csv', 
            'custom_filename': '{quarter}_{year}_report.xlsx',
            'formatted_date': 'data_{date:MMDD}.xlsx'
        }
    }
