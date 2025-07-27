"""
Recipe configuration loader for Excel automation recipes.

This module handles loading and basic validation of YAML/JSON recipe files,
including support for required external variables.
"""

import json
import yaml
import logging

from pathlib import Path

from excel_recipe_processor.core.interactive_variables import validate_external_variable_config


logger = logging.getLogger(__name__)

step_desc = 'step_description'
proc_type = 'processor_type'

class RecipeValidationError(Exception):
    """Raised when a recipe file has invalid structure or content."""
    pass


class RecipeLoader:
    """
    Loads and validates Excel processing recipe files.
    
    Supports both YAML and JSON formats. Provides basic validation
    of recipe structure and step definitions, including external variables.
    """
    
    def __init__(self):
        """Initialize the recipe loader."""
        self.recipe_data = None
        self.recipe_path = None
    
    def load_file(self, recipe_path) -> dict:
        """
        Load a recipe file from disk.
        
        Args:
            recipe_path: Path to the recipe file (.yaml, .yml, or .json)
            
        Returns:
            Loaded and validated recipe data
            
        Raises:
            FileNotFoundError: If the recipe file doesn't exist
            RecipeValidationError: If the recipe format is invalid
        """
        # Guard clause: ensure we have a valid path
        if not recipe_path:
            raise RecipeValidationError("Recipe path cannot be empty")
        
        self.recipe_path = Path(recipe_path)
        
        if not self.recipe_path.exists():
            raise FileNotFoundError(f"Recipe file not found: {self.recipe_path}")
        
        logger.info(f"Loading recipe from: {self.recipe_path}")
        
        # Load based on file extension
        try:
            with open(self.recipe_path, 'r', encoding='utf-8') as f:
                if self.recipe_path.suffix.lower() in ['.yaml', '.yml']:
                    self.recipe_data = yaml.safe_load(f)
                elif self.recipe_path.suffix.lower() == '.json':
                    self.recipe_data = json.load(f)
                else:
                    raise RecipeValidationError(
                        f"Unsupported file format: {self.recipe_path.suffix}. "
                        "Use .yaml, .yml, or .json"
                    )
        except yaml.YAMLError as e:
            raise RecipeValidationError(f"YAML parsing error: {e}")
        except json.JSONDecodeError as e:
            raise RecipeValidationError(f"JSON parsing error: {e}")
        except Exception as e:
            raise RecipeValidationError(f"Error reading recipe file: {e}")
        
        # Guard clause: ensure we loaded something
        if self.recipe_data is None:
            raise RecipeValidationError("Recipe file appears to be empty")
        
        # Validate the loaded data
        self._validate_recipe()
        
        logger.info(f"Successfully loaded recipe with {len(self.get_steps())} steps")
        return self.recipe_data
    
    def load_string(self, recipe_content: str, format_type: str = 'yaml') -> dict:
        """
        Load a recipe from a string.
        
        Args:
            recipe_content: Recipe content as string
            format_type: Either 'yaml' or 'json'
            
        Returns:
            Loaded and validated recipe data
            
        Raises:
            RecipeValidationError: If the recipe format is invalid
        """
        # Guard clauses
        if not isinstance(recipe_content, str):
            raise RecipeValidationError("Recipe content must be a string")
        if not recipe_content.strip():
            raise RecipeValidationError("Recipe content cannot be empty")
        if not isinstance(format_type, str):
            raise RecipeValidationError("Format type must be a string")
        
        logger.info(f"Loading recipe from {format_type} string")
        
        try:
            if format_type.lower() == 'yaml':
                self.recipe_data = yaml.safe_load(recipe_content)
            elif format_type.lower() == 'json':
                self.recipe_data = json.loads(recipe_content)
            else:
                raise RecipeValidationError(f"Unsupported format: {format_type}")
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise RecipeValidationError(f"Error parsing {format_type}: {e}")
        
        # Guard clause: ensure we loaded something
        if self.recipe_data is None:
            raise RecipeValidationError("Recipe content appears to be empty")
        
        self._validate_recipe()
        
        logger.info(f"Successfully loaded recipe with {len(self.get_steps())} steps")
        return self.recipe_data
    
    def _validate_recipe(self) -> None:
        """
        Validate the loaded recipe structure.
        
        Raises:
            RecipeValidationError: If the recipe structure is invalid
        """
        # Guard clause: recipe_data must be a dictionary
        if not isinstance(self.recipe_data, dict):
            raise RecipeValidationError("Recipe must be a dictionary (YAML object or JSON object)")
        
        # Guard clause: recipe must have a 'recipe' key with steps
        if 'recipe' not in self.recipe_data:
            raise RecipeValidationError("Recipe must contain a 'recipe' section with processing steps")
        
        if not isinstance(self.recipe_data['recipe'], list):
            raise RecipeValidationError("Recipe 'recipe' section must be a list of steps")
        
        if len(self.recipe_data['recipe']) == 0:
            raise RecipeValidationError("Recipe must contain at least one processing step")
        
        # Validate each step
        for i, step in enumerate(self.recipe_data['recipe'], 1):
            self._validate_step(step, i)
        
        # Validate settings if present
        if 'settings' in self.recipe_data:
            self._validate_settings(self.recipe_data['settings'])
        
        logger.debug("Recipe structure validation completed successfully")
    
    def _validate_step(self, step: dict, step_number: int) -> None:
        """
        Validate a single recipe step.
        
        Args:
            step: Step dictionary to validate
            step_number: Step number for error messages
        """
        # Guard clause: step must be a dictionary
        if not isinstance(step, dict):
            raise RecipeValidationError(f"Step {step_number} must be a dictionary")
        
        # Guard clause: step must have processor_type
        if proc_type not in step:
            raise RecipeValidationError(
                f"Step {step_number} missing required field '{proc_type}'"
            )
        
        if not isinstance(step[proc_type], str):
            raise RecipeValidationError(
                f"Step {step_number} field '{proc_type}' must be a string"
            )
        
        # step_description is optional but if present must be a string
        if step_desc in step and not isinstance(step[step_desc], str):
            raise RecipeValidationError(
                f"Step {step_number} field '{step_desc}' must be a string"
            )
        
        logger.debug(f"Step {step_number} validation passed")
    
    def _validate_settings(self, settings: dict) -> None:
        """
        Validate recipe settings including external variables.
        
        Args:
            settings: Settings dictionary to validate
        """
        # Guard clause: settings must be a dictionary
        if not isinstance(settings, dict):
            raise RecipeValidationError("Settings must be a dictionary")
        
        # Validate known settings with guard clauses
        if 'output_filename' in settings:
            if not isinstance(settings['output_filename'], str):
                raise RecipeValidationError("Setting 'output_filename' must be a string")
        
        if 'create_backup' in settings:
            if not isinstance(settings['create_backup'], bool):
                raise RecipeValidationError("Setting 'create_backup' must be boolean")
        
        if 'variables' in settings:
            if not isinstance(settings['variables'], dict):
                raise RecipeValidationError("Setting 'variables' must be a dictionary")
        
        # Validate external variables if present
        if 'required_external_vars' in settings:
            self._validate_external_variables(settings['required_external_vars'])
        
        logger.debug("Settings validated successfully")
    
    def _validate_external_variables(self, external_vars: dict) -> None:
        """
        Validate required external variables configuration.
        
        Args:
            external_vars: External variables dictionary to validate
        """
        if not isinstance(external_vars, dict):
            raise RecipeValidationError("required_external_vars must be a dictionary")
        
        for var_name, var_config in external_vars.items():
            if not isinstance(var_name, str):
                raise RecipeValidationError("External variable names must be strings")
            
            # Use the validation function from interactive_variables
            try:
                validate_external_variable_config(var_name, var_config)
            except Exception as e:
                raise RecipeValidationError(f"External variable validation failed: {e}")
        
        logger.debug(f"Validated {len(external_vars)} external variables")
    
    def get_steps(self) -> list:
        """
        Get the list of processing steps.
        
        Returns:
            List of step dictionaries
            
        Raises:
            RuntimeError: If no recipe is loaded
        """
        # Guard clause: recipe must be loaded
        if self.recipe_data is None:
            raise RuntimeError("No recipe loaded. Call load_file() or load_string() first.")
        
        steps = self.recipe_data['recipe']
        # Guard clause: ensure steps is a list
        if not isinstance(steps, list):
            raise RuntimeError("Recipe steps data is corrupted")
        
        return steps
    
    def get_settings(self) -> dict:
        """
        Get recipe settings.
        
        Returns:
            Settings dictionary (empty if no settings defined)
        """
        # Guard clause: recipe must be loaded  
        if self.recipe_data is None:
            raise RuntimeError("No recipe loaded. Call load_file() or load_string() first.")
        
        settings = self.recipe_data.get('settings', {})
        # Guard clause: ensure settings is a dict
        if not isinstance(settings, dict):
            return {}
        
        return settings
    
    def get_required_external_vars(self) -> dict:
        """
        Get required external variables configuration.
        
        Returns:
            Dictionary of required external variables (empty if none defined)
        """
        settings = self.get_settings()
        external_vars = settings.get('required_external_vars', {})
        
        # Guard clause: ensure external_vars is a dict
        if not isinstance(external_vars, dict):
            return {}
        
        return external_vars
    
    def get_step_by_name(self, step_desc_arg: str):
        """
        Find a step by its name.
        
        Args:
            step_name: Name of the step to find
            
        Returns:
            Step dictionary if found, None otherwise
        """
        # Guard clause: step_name must be a string
        if not isinstance(step_desc_arg, str):
            raise ValueError("Step name must be a string")
        
        for step in self.get_steps():
            # Guard clause: each step should be a dict
            if isinstance(step, dict) and step.get(step_desc) == step_desc_arg:
                return step
        return None
    
    def get_steps_by_type(self, step_type: str) -> list:
        """
        Get all steps of a specific type.
        
        Args:
            step_type: Processor type to filter by
            
        Returns:
            List of steps with matching processor type
        """
        # Guard clause: step_type must be a string
        if not isinstance(step_type, str):
            raise ValueError("Step type must be a string")
        
        matching_steps = []
        for step in self.get_steps():
            # Guard clause: each step should be a dict
            if isinstance(step, dict) and step.get(proc_type) == step_type:
                matching_steps.append(step)
        
        return matching_steps
    
    def summary(self) -> str:
        """
        Get a summary of the loaded recipe.
        
        Returns:
            Human-readable summary string
        """
        # Guard clause: recipe must be loaded
        if self.recipe_data is None:
            return "No recipe loaded"
        
        steps = self.get_steps()
        settings = self.get_settings()
        external_vars = self.get_required_external_vars()
        
        summary_parts = [f"{len(steps)} processing steps"]
        
        if settings:
            summary_parts.append(f"{len(settings)} settings")
        
        if external_vars:
            summary_parts.append(f"{len(external_vars)} external variables")
        
        return ", ".join(summary_parts)
