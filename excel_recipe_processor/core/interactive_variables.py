"""
Interactive variable prompting for Excel automation recipes.

excel_recipe_processor/core/interactive_variables.py

Handles prompting users for required external variables, validation,
and CLI variable parsing with support for defaults and choices.
Enhanced with prompt_toolkit for better UX when available.
"""

import re
import logging
from typing import Any, Optional

from excel_recipe_processor.core.variable_substitution import VariableSubstitution

# Enhanced input support
try:
    from prompt_toolkit import prompt
    HAS_PROMPT_TOOLKIT = True
except ImportError:
    HAS_PROMPT_TOOLKIT = False

logger = logging.getLogger(__name__)


class InteractiveVariableError(Exception):
    """Raised when interactive variable processing fails."""
    pass


def enhanced_input(prompt_text: str, default_value: str = None) -> str:
    """
    Enhanced input function with editable default support.
    
    Uses prompt_toolkit when available for better UX, falls back to
    standard input() with manual default handling.
    
    Args:
        prompt_text: Text to display as prompt
        default_value: Default value to show (pre-filled if prompt_toolkit available)
        
    Returns:
        User input string
    """
    if HAS_PROMPT_TOOLKIT and default_value:
        # prompt_toolkit provides editable default - user can edit directly
        return prompt(prompt_text, default=default_value)
    else:
        # Fallback to traditional input with default handling
        if default_value:
            user_input = input(f"{prompt_text}[{default_value}]: ").strip()
            return user_input if user_input else default_value
        else:
            return input(prompt_text).strip()


class InteractiveVariablePrompt:
    """
    Handles interactive prompting for external variables with validation.
    
    Supports variable defaults with substitution, regex validation,
    constrained choices, and empty value control. Enhanced with prompt_toolkit
    for better UX when available.
    """
    
    def __init__(self, variable_substitution: Optional[VariableSubstitution] = None):
        """
        Initialize interactive variable prompting.
        
        Args:
            variable_substitution: Variable substitution system for resolving defaults
        """
        self.variable_substitution = variable_substitution
        self.collected_variables = {}
    
    def collect_variables(self, required_vars: dict, cli_overrides: dict = None) -> dict:
        """
        Collect all required variables through interactive prompts or CLI overrides.
        
        Args:
            required_vars: Dictionary of required variable definitions
            cli_overrides: Dictionary of CLI variable overrides
            
        Returns:
            Dictionary of collected variable values
            
        Raises:
            InteractiveVariableError: If variable collection fails
        """
        cli_overrides = cli_overrides or {}
        self.collected_variables = {}
        
        # Check if any variables need interactive input
        missing_vars = [name for name in required_vars.keys() if name not in cli_overrides]
        
        if not missing_vars:
            logger.info("All required variables provided via CLI")
            return cli_overrides
        
        # Show enhanced input status with installation hint if needed
        if HAS_PROMPT_TOOLKIT:
            input_mode = "prompt_toolkit (enhanced)"
            print(f"\nRecipe requires {len(required_vars)} external variables (using {input_mode}):")
        else:
            print(f"\nRecipe requires {len(required_vars)} external variables:")
            print("⚠️  Enhanced input not available. For better UX with editable defaults:")
            print("   pip install prompt-toolkit")
            print("   (Currently using basic input with manual default handling)\n")
        
        # Process each required variable
        for i, (var_name, var_config) in enumerate(required_vars.items(), 1):
            if var_name in cli_overrides:
                # Use CLI override
                value = cli_overrides[var_name]
                self._validate_variable_value(var_name, value, var_config)
                self.collected_variables[var_name] = value
                logger.debug(f"Using CLI override for {var_name}: {value}")
            else:
                # Interactive prompt
                value = self._prompt_for_variable(i, len(required_vars), var_name, var_config)
                self.collected_variables[var_name] = value
        
        # Show confirmation and allow editing
        return self._confirm_variables(required_vars)
    
    def _prompt_for_variable(self, current_num: int, total_num: int, 
                           var_name: str, var_config: dict) -> str:
        """
        Prompt for a single variable with validation and enhanced input.
        
        Args:
            current_num: Current variable number (1-based)
            total_num: Total number of variables
            var_name: Variable name
            var_config: Variable configuration
            
        Returns:
            Validated variable value
        """
        description = var_config.get('description', 'No description provided')
        example = var_config.get('example')
        choices = var_config.get('choices')
        default_template = var_config.get('default_value')
        allow_empty = var_config.get('allow_empty', True)
        
        # Resolve default value with variable substitution
        default_value = None
        if default_template is not None:
            if self.variable_substitution:
                try:
                    default_value = self.variable_substitution.substitute(default_template)
                except Exception as e:
                    logger.warning(f"Failed to resolve default for {var_name}: {e}")
                    default_value = default_template
            else:
                default_value = default_template
        
        # Display prompt header
        print(f"\n({current_num}/{total_num}) {description}")
        
        if example:
            print(f"      Example: {example}")
        
        if choices:
            print(f"      Choices: {', '.join(choices)}")
        
        if HAS_PROMPT_TOOLKIT and default_value:
            print(f"      Default: {default_value} (pre-filled, editable)")
        elif default_value:
            print(f"      Default: {default_value}")
        
        # Add visual separation before the actual input prompt
        print()
        
        # Prompt for input with validation loop
        while True:
            try:
                if HAS_PROMPT_TOOLKIT:
                    # Enhanced input - default is pre-filled and editable
                    prompt_text = f"      {var_name}: "
                    user_input = enhanced_input(prompt_text, default_value)
                else:
                    # Standard input with traditional default handling
                    if default_value is not None:
                        prompt_text = f"      {var_name} [Enter for default]: "
                    else:
                        prompt_text = f"      {var_name}: "
                    
                    user_input = enhanced_input(prompt_text, default_value)
                
                # Show confirmation of value used
                if user_input == default_value and default_value is not None:
                    print(f"\n      ✓ Using default: {user_input}")
                elif user_input:
                    print(f"\n      ✓ Using: {user_input}")
                
                # Validate input
                self._validate_variable_value(var_name, user_input, var_config)
                return user_input
                
            except InteractiveVariableError as e:
                print(f"\n      ❌ {e}")
                print("      Please try again.")
    
    def _validate_variable_value(self, var_name: str, value: str, var_config: dict) -> None:
        """
        Validate a variable value against its configuration.
        
        Args:
            var_name: Variable name
            value: Value to validate
            var_config: Variable configuration
            
        Raises:
            InteractiveVariableError: If validation fails
        """
        allow_empty = var_config.get('allow_empty', True)
        
        # Check empty values
        if not value and not allow_empty:
            raise InteractiveVariableError(f"Variable '{var_name}' cannot be empty")
        
        # Skip further validation for empty values (when allowed)
        if not value:
            return
        
        # Validate against choices
        choices = var_config.get('choices')
        if choices and value not in choices:
            raise InteractiveVariableError(
                f"Value '{value}' not in allowed choices: {', '.join(choices)}"
            )
        
        # Validate against regex pattern
        validation_pattern = var_config.get('validation')
        if validation_pattern:
            if not re.match(validation_pattern, value):
                example = var_config.get('example', 'see documentation')
                raise InteractiveVariableError(
                    f"Value '{value}' doesn't match required pattern. Example: {example}"
                )
    
    def _confirm_variables(self, required_vars: dict) -> dict:
        """
        Show collected variables and allow user to confirm or edit.
        
        Args:
            required_vars: Required variable definitions
            
        Returns:
            Final confirmed variables
        """
        while True:
            # Always show current state (re-displayed after edits)
            print("\n" + "="*50)
            print("VARIABLE SUMMARY:")
            print("="*50)
            
            for var_name, value in self.collected_variables.items():
                display_value = value if value else "(empty)"
                print(f"  {var_name} = {display_value}")
            
            print("\nChoices: [C]ontinue, [E]dit a variable, [Q]uit")
            choice = input("Enter choice: ").strip().upper()
            
            if choice == 'C':
                return self.collected_variables
            elif choice == 'Q':
                raise InteractiveVariableError("User cancelled variable input")
            elif choice == 'E':
                self._edit_variable(required_vars)
                # Loop continues and will re-display the summary and choices
            else:
                print("Please enter C, E, or Q.")
    
    def _edit_variable(self, required_vars: dict) -> None:
        """
        Allow user to edit a specific variable.
        
        Args:
            required_vars: Required variable definitions
        """
        print("\nWhich variable would you like to edit?")
        var_names = list(self.collected_variables.keys())
        
        for i, var_name in enumerate(var_names, 1):
            current_value = self.collected_variables[var_name]
            display_value = current_value if current_value else "(empty)"
            print(f"  {i}. {var_name} = {display_value}")
        
        try:
            choice = input("Enter number: ").strip()
            index = int(choice) - 1
            
            if 0 <= index < len(var_names):
                var_name = var_names[index]
                var_config = required_vars[var_name]
                
                print(f"\nEditing '{var_name}':")
                new_value = self._prompt_for_variable(1, 1, var_name, var_config)
                self.collected_variables[var_name] = new_value
                print(f"✓ Updated {var_name}")
            else:
                print("Invalid selection.")
        except (ValueError, IndexError):
            print("Invalid input. Please enter a number.")


def parse_cli_variables(variable_args: list) -> dict:
    """
    Parse CLI variable arguments in the format 'name=value'.
    
    Args:
        variable_args: List of strings in format 'name=value'
        
    Returns:
        Dictionary of parsed variables
        
    Raises:
        InteractiveVariableError: If parsing fails
    """
    if not variable_args:
        return {}
    
    variables = {}
    
    for var_arg in variable_args:
        if '=' not in var_arg:
            raise InteractiveVariableError(
                f"Invalid variable format: '{var_arg}'. Expected format: name=value"
            )
        
        name, value = var_arg.split('=', 1)  # Split only on first =
        name = name.strip()
        value = value.strip()
        
        if not name:
            raise InteractiveVariableError(f"Variable name cannot be empty in: '{var_arg}'")
        
        variables[name] = value
        logger.debug(f"Parsed CLI variable: {name} = {value}")
    
    return variables


def validate_external_variable_config(var_name: str, var_config: Any) -> None:
    """
    Validate external variable configuration.
    
    Args:
        var_name: Variable name
        var_config: Variable configuration to validate
        
    Raises:
        InteractiveVariableError: If configuration is invalid
    """
    if not isinstance(var_config, dict):
        raise InteractiveVariableError(
            f"External variable '{var_name}' must be a dictionary"
        )
    
    # Required field
    if 'description' not in var_config:
        raise InteractiveVariableError(
            f"External variable '{var_name}' must have a 'description' field"
        )
    
    if not isinstance(var_config['description'], str):
        raise InteractiveVariableError(
            f"Description for variable '{var_name}' must be a string"
        )
    
    # Optional fields validation
    if 'example' in var_config and not isinstance(var_config['example'], str):
        raise InteractiveVariableError(
            f"Example for variable '{var_name}' must be a string"
        )
    
    if 'validation' in var_config:
        if not isinstance(var_config['validation'], str):
            raise InteractiveVariableError(
                f"Validation for variable '{var_name}' must be a regex string"
            )
        # Test if it's a valid regex
        try:
            re.compile(var_config['validation'])
        except re.error as e:
            raise InteractiveVariableError(
                f"Invalid regex pattern for variable '{var_name}': {e}"
            )
    
    if 'choices' in var_config:
        if not isinstance(var_config['choices'], list):
            raise InteractiveVariableError(
                f"Choices for variable '{var_name}' must be a list"
            )
        if 'validation' in var_config:
            raise InteractiveVariableError(
                f"Variable '{var_name}' cannot have both 'choices' and 'validation'"
            )
    
    if 'default_value' in var_config and not isinstance(var_config['default_value'], str):
        raise InteractiveVariableError(
            f"Default value for variable '{var_name}' must be a string"
        )
    
    if 'allow_empty' in var_config and not isinstance(var_config['allow_empty'], bool):
        raise InteractiveVariableError(
            f"Allow empty for variable '{var_name}' must be a boolean"
        )


# End of file #
