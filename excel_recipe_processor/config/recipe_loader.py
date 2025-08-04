"""
Recipe configuration loader for Excel automation recipes.

This module handles loading and validation of YAML/JSON recipe files,
with friendly error reporting and structure validation.
"""

import json
import yaml
import logging
import re

from pathlib import Path
from collections import OrderedDict

from excel_recipe_processor.core.interactive_variables import validate_external_variable_config


logger = logging.getLogger(__name__)

step_desc = 'step_description'
proc_type = 'processor_type'

class RecipeValidationError(Exception):
    """Raised when a recipe file has invalid structure or content."""
    pass


class OrderedYAMLLoader(yaml.SafeLoader):
    """Custom YAML loader that preserves section order for validation."""
    pass


def construct_mapping(loader: yaml.SafeLoader, node):
    """Preserve order of sections in YAML."""
    loader.flatten_mapping(node)
    return OrderedDict(loader.construct_pairs(node))


OrderedYAMLLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping)


class RecipeLoader:
    """
    Loads and validates Excel processing recipe files.
    
    Supports both YAML and JSON formats with friendly error reporting
    and structure validation.
    """
    
    def __init__(self):
        """Initialize the recipe loader."""
        self.recipe_data = None
        self.recipe_path = None
        self._original_section_order = []
    
    def load_recipe_file(self, recipe_path) -> dict:  # ✅ RENAMED from load_file
        """
        Load a recipe file from disk with validation.
        
        Args:
            recipe_path: Path to the recipe file (.yaml, .yml, or .json)
            
        Returns:
            Loaded and validated recipe data
            
        Raises:
            RecipeValidationError: If the recipe format is invalid or has errors
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
                    # Use custom loader to preserve section order
                    self.recipe_data = yaml.load(f, Loader=OrderedYAMLLoader)
                    # Extract original section order
                    if isinstance(self.recipe_data, OrderedDict):
                        self._original_section_order = list(self.recipe_data.keys())
                elif self.recipe_path.suffix.lower() == '.json':
                    self.recipe_data = json.load(f)
                else:
                    raise RecipeValidationError(
                        f"Unsupported file format: {self.recipe_path.suffix}. "
                        f"Supported formats: .yaml, .yml, .json"
                    )
                    
        except yaml.YAMLError as e:
            raise RecipeValidationError(f"YAML syntax error in {self.recipe_path}: {e}")
        except json.JSONDecodeError as e:
            raise RecipeValidationError(f"JSON syntax error in {self.recipe_path}: {e}")
        except Exception as e:
            raise RecipeValidationError(f"Error reading recipe file: {e}")
        
        # Guard clause: ensure we got valid data
        if not self.recipe_data:
            raise RecipeValidationError("Recipe file is empty or contains no data")
        
        # Validate recipe structure
        validation_result = self.validate_recipe_structure()
        if not validation_result['valid']:
            # Convert to exception for now, but with better message
            error_msg = f"Recipe structure validation failed:\n"
            for error in validation_result['errors']:
                error_msg += f"  • {error}\n"
            raise RecipeValidationError(error_msg.strip())
        
        # Log warnings (non-fatal issues)
        for warning in validation_result.get('warnings', []):
            logger.warning(f"⚠️  {warning}")
        
        # Validate external variables if present
        self._validate_external_variables()
        
        logger.info(f"Successfully loaded recipe: {self.summary()}")
        return self.recipe_data

    # # Optional: Keep old method for backward compatibility (with deprecation warning)
    # def load_file(self, recipe_path) -> dict:
    #     """
    #     DEPRECATED: Use load_recipe_file() instead.
        
    #     This method is kept for backward compatibility but will be removed
    #     in a future version.
    #     """
    #     import warnings
    #     warnings.warn(
    #         "load_file() is deprecated. Use load_recipe_file() instead.",
    #         DeprecationWarning,
    #         stacklevel=2
    #     )
    #     return self.load_recipe_file(recipe_path)

    def validate_recipe_structure(self) -> dict:
        """
        Validate the overall structure of the recipe.
        
        Returns:
            Dictionary with validation results: {'valid': bool, 'errors': list, 'warnings': list}
        """
        errors = []
        warnings = []
        
        # Guard clause: ensure recipe data exists
        if not self.recipe_data:
            errors.append("No recipe data loaded")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        
        # Check for required settings section
        if 'settings' not in self.recipe_data:
            errors.append("Missing required 'settings' section")
            errors.append("💡 Add minimal settings section:")
            errors.append("settings:")
            errors.append("  description: 'Brief description of what this recipe does'")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        
        # Check section order (settings should come first)
        if self._original_section_order:
            if 'recipe' in self._original_section_order and 'settings' in self._original_section_order:
                recipe_index = self._original_section_order.index('recipe')
                settings_index = self._original_section_order.index('settings')
                
                if recipe_index < settings_index:
                    warnings.append("💡 Consider placing 'settings' section before 'recipe' section for better discoverability")
        
        # Validate settings section content
        settings_validation = self._validate_settings_section()
        errors.extend(settings_validation['errors'])
        warnings.extend(settings_validation['warnings'])
        
        # Check for required recipe section
        if 'recipe' not in self.recipe_data:
            errors.append("Missing required 'recipe' section with processing steps")
            return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings}
        
        # Validate recipe section
        recipe_validation = self._validate_recipe_section()
        errors.extend(recipe_validation['errors'])
        warnings.extend(recipe_validation['warnings'])
        
        return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings}
    
    def _validate_settings_section(self) -> dict:
        """Validate the settings section content."""
        errors = []
        warnings = []
        
        # Guard clause to light up the ".get()" method by verifying dict or OrderDict typing
        if isinstance(self.recipe_data, dict) or isinstance(self.recipe_data, OrderedDict):
            settings = self.recipe_data.get('settings', {})
        
        # Require at least a description (errors cause recipe to be invalid)
        if 'description' not in settings:
            errors.append("Missing required 'description' in settings section")
            errors.append("💡 Add description to document recipe purpose:")
            errors.append("settings:")
            errors.append("  description: 'Brief description of what this recipe does'")
        
        # Check for deprecated settings
        if 'output_filename' in settings:
            warnings.append("⚠️  'output_filename' is deprecated - use 'export_file' processor steps instead")
            warnings.append("💡 Replace with: processor_type: export_file, output_file: '...'")
        
        # Validate external variables if present
        if 'required_external_vars' in settings:
            ext_vars = settings['required_external_vars']
            if not isinstance(ext_vars, dict):
                errors.append("'required_external_vars' must be a dictionary")
            else:
                for var_name, var_config in ext_vars.items():
                    try:
                        validate_external_variable_config(var_name, var_config)
                    except Exception as e:
                        errors.append(f"Invalid external variable '{var_name}': {e}")
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_recipe_section(self) -> dict:
        """Validate the recipe section content."""
        errors = []
        warnings = []
        
        recipe_steps = self.recipe_data.get('recipe', [])
        
        # Guard clause: ensure steps is a list
        if not isinstance(recipe_steps, list):
            errors.append("'recipe' section must be a list of processing steps")
            return {'errors': errors, 'warnings': warnings}
        
        if len(recipe_steps) == 0:
            errors.append("Recipe must contain at least one processing step")
            return {'errors': errors, 'warnings': warnings}
        
        # Validate each step
        for step_index, step in enumerate(recipe_steps, 1):
            step_validation = self._validate_recipe_step(step, step_index)
            errors.extend(step_validation['errors'])
            warnings.extend(step_validation['warnings'])
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_recipe_step(self, step: dict, step_index: int) -> dict:
        """Validate a single recipe step."""
        errors = []
        warnings = []
        
        # Guard clause: ensure step is a dict
        if not isinstance(step, dict):
            errors.append(f"Step {step_index}: must be a dictionary")
            return {'errors': errors, 'warnings': warnings}
        
        # Check for required processor_type
        if proc_type not in step:
            errors.append(f"Step {step_index}: missing required field 'processor_type'")
            return {'errors': errors, 'warnings': warnings}
        
        processor_type = step[proc_type]
        step_name = step.get(step_desc, f'Step {step_index}')
        
        # Check for stage requirements based on processor type
        stage_validation = self._validate_step_stage_requirements(step, step_name, processor_type)
        errors.extend(stage_validation['errors'])
        warnings.extend(stage_validation['warnings'])
        
        return {'errors': errors, 'warnings': warnings}
    
    def _validate_step_stage_requirements(self, step: dict, step_name: str, processor_type: str) -> dict:
        """Validate stage requirements for a step."""
        errors = []
        warnings = []
        
        # Import processors need 'save_to_stage'
        if processor_type == 'import_file':
            if 'save_to_stage' not in step:
                errors.append(f"Step '{step_name}': missing required field 'save_to_stage'")
                errors.append("💡 All import_file steps must specify where to save imported data")
                errors.append("Example: save_to_stage: 'raw_data'")
        
        # Export processors need 'source_stage'
        elif processor_type == 'export_file':
            if 'source_stage' not in step:
                errors.append(f"Step '{step_name}': missing required field 'source_stage'")
                errors.append("💡 All export_file steps must specify which stage to export from")
                errors.append("Example: source_stage: 'processed_data'")
        
        # Processing steps need both 'save_to_stage' and 'source_stage' (except those in list)
        elif processor_type not in ['debug_breakpoint', 'create_stage', 'format_excel']:
            if 'source_stage' not in step:
                errors.append(f"Step '{step_name}': missing required field 'source_stage'")
                errors.append("💡 Processing steps must specify which stage to read data from")
                errors.append("Example: source_stage: 'raw_data'")
            
            if 'save_to_stage' not in step:
                errors.append(f"Step '{step_name}': missing required field 'save_to_stage'")
                errors.append("💡 Processing steps must specify where to save results")
                errors.append("Example: save_to_stage: 'processed_data'")
        
        return {'errors': errors, 'warnings': warnings}
    
    def load_string(self, recipe_string: str, format_type: str = 'yaml') -> dict:
        """
        Load a recipe from a string.
        
        Args:
            recipe_string: Recipe content as string
            format_type: Format type ('yaml' or 'json')
            
        Returns:
            Loaded recipe data
        """
        try:
            if format_type.lower() == 'yaml':
                self.recipe_data = yaml.safe_load(recipe_string)
            elif format_type.lower() == 'json':
                self.recipe_data = json.loads(recipe_string)
            else:
                raise RecipeValidationError(f"Unsupported format type: {format_type}")
                
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise RecipeValidationError(f"Parse error in {format_type}: {e}")
        
        # Validate structure
        validation_result = self.validate_recipe_structure()
        if not validation_result['valid']:
            error_msg = f"Recipe structure validation failed:\n"
            for error in validation_result['errors']:
                error_msg += f"  • {error}\n"
            raise RecipeValidationError(error_msg.strip())
        
        return self.recipe_data
    
    def _validate_external_variables(self) -> None:
        """Validate external variables configuration."""
        settings = self.recipe_data.get('settings', {})
        external_vars = settings.get('required_external_vars', {})
        
        for var_name, var_config in external_vars.items():
            try:
                validate_external_variable_config(var_name, var_config)
            except Exception as e:
                logger.warning(f"External variable '{var_name}' configuration issue: {e}")
    
    def get_steps(self) -> list:
        """
        Get recipe processing steps.
        
        Returns:
            List of step dictionaries
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
            step_desc_arg: Name of the step to find
            
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
    
    def get_settings_examples(self) -> dict:
        """Get complete usage examples for recipe settings section."""
        from excel_recipe_processor.utils.processor_examples_loader import load_settings_examples
        return load_settings_examples()
