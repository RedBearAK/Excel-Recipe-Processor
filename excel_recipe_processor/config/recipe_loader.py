"""
Recipe configuration loader for Excel automation recipes.

This module handles loading and basic validation of YAML/JSON recipe files.
"""

import json
import logging
from pathlib import Path
from typing import Any, Optional

import yaml

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
    of recipe structure and step definitions.
    """
    
    def __init__(self):
        """Initialize the recipe loader."""
        self.recipe_data: Optional[dict[str, Any]] = None
        self.recipe_path: Optional[Path] = None
    
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
            RecipeValidationError: If validation fails
        """
        if not isinstance(self.recipe_data, dict):
            raise RecipeValidationError("Recipe must be a dictionary/object at root level")
        
        # Check for required 'recipe' key
        if 'recipe' not in self.recipe_data:
            raise RecipeValidationError("Recipe must contain a 'recipe' key with list of steps")
        
        steps = self.recipe_data['recipe']
        if not isinstance(steps, list):
            raise RecipeValidationError("Recipe 'recipe' value must be a list of steps")
        
        if len(steps) == 0:
            raise RecipeValidationError("Recipe must contain at least one step")
        
        # Validate each step
        for i, step in enumerate(steps):
            self._validate_step(step, i)
        
        # Validate settings if present
        if 'settings' in self.recipe_data:
            self._validate_settings(self.recipe_data['settings'])
    
    def _validate_step(self, step, step_index: int) -> None:
        """
        Validate a single recipe step.
        
        Args:
            step: Step dictionary to validate
            step_index: Zero-based index of the step for error reporting
        """
        # Guard clause: step must be a dictionary
        if not isinstance(step, dict):
            raise RecipeValidationError(f"Step {step_index + 1} must be a dictionary")
        
        # Required fields
        if 'processor_type' not in step:
            raise RecipeValidationError(f"Step {step_index + 1} missing required {proc_type} field")
        
        step_type = step[proc_type]
        # Guard clause: type must be a non-empty string
        if not isinstance(step_type, str) or not step_type.strip():
            raise RecipeValidationError(f"Step {step_index + 1} {proc_type} must be a non-empty string")
        
        # Optional but recommended fields
        if step_desc in step:
            # Guard clause: name must be a string if present
            if not isinstance(step[step_desc], str):
                raise RecipeValidationError(f"Step {step_index + 1} {step_desc} must be a string")
        
        logger.debug(f"Step {step_index + 1} validated: {step_type}")
    
    def _validate_settings(self, settings) -> None:
        """
        Validate recipe settings section.
        
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
        
        logger.debug("Settings validated successfully")
    
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
    
    def get_step_by_name(self, step_desc_arg: str) -> Optional[dict]:
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
            step_type: Type of steps to find
            
        Returns:
            List of matching step dictionaries
        """
        # Guard clause: step_type must be a string
        if not isinstance(step_type, str):
            raise ValueError("Step type must be a string")
        
        matching_steps = []
        for step in self.get_steps():
            # Guard clause: each step should be a dict with type
            if isinstance(step, dict) and step.get(proc_type) == step_type:
                matching_steps.append(step)
        
        return matching_steps
    
    def summary(self) -> str:
        """
        Get a human-readable summary of the loaded recipe.
        
        Returns:
            Summary string
        """
        # Guard clause: recipe must be loaded
        if self.recipe_data is None:
            return "No recipe loaded"
        
        steps = self.get_steps()
        settings = self.get_settings()
        
        # Guard clause: ensure steps is actually a list
        if not isinstance(steps, list):
            return "Recipe data is corrupted"
        
        summary_lines = [
            f"Recipe Summary:",
            f"  Steps: {len(steps)}",
        ]
        
        # Step type counts
        step_types = {}
        for step in steps:
            # Guard clause: ensure each step is a dict with type
            if isinstance(step, dict) and proc_type in step:
                step_type = step[proc_type]
                if isinstance(step_type, str):
                    step_types[step_type] = step_types.get(step_type, 0) + 1
        
        if step_types:
            summary_lines.append("  Step types:")
            for step_type, count in sorted(step_types.items()):
                summary_lines.append(f"    {step_type}: {count}")
        
        # Guard clause: ensure settings is a dict
        if isinstance(settings, dict) and settings:
            summary_lines.append(f"  Settings: {len(settings)} configured")
        
        if self.recipe_path:
            summary_lines.append(f"  Source: {self.recipe_path}")
        
        return "\n".join(summary_lines)
