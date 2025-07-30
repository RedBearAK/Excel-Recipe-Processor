"""
Recipe-based pipeline orchestrator for data processing recipes.

Handles loading recipes, validating stages, and executing processing steps
in a pure stage-based architecture.
"""

import pandas as pd
import logging

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import (
    registry, StepProcessorError, ImportBaseProcessor, ExportBaseProcessor
)
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution

logger = logging.getLogger(__name__)


class RecipePipelineError(Exception):
    """Raised when recipe pipeline execution fails."""
    pass


class RecipePipeline:
    """Pure stage-based recipe orchestrator."""
    
    def __init__(self):
        self.recipe_loader = RecipeLoader()
        self.recipe_data = None
        self.variable_substitution = None
        self.steps_executed = 0
    
    def load_recipe(self, recipe_path) -> dict:
        """Load and validate recipe with stages."""
        try:
            self.recipe_data = self.recipe_loader.load_recipe(recipe_path)
        except Exception as e:
            raise RecipePipelineError(f"Failed to load recipe: {e}")
        
        # Initialize variable substitution
        self._initialize_variable_substitution()
        
        # Declare and validate stages
        StageManager.declare_recipe_stages(self.recipe_data)
        
        stage_errors = StageManager.validate_recipe_stages(self.recipe_data)
        if stage_errors:
            error_msg = "Recipe validation failed:\n" + "\n".join(f"  - {error}" for error in stage_errors)
            raise RecipePipelineError(error_msg)
        
        logger.info(f"Loaded recipe with {len(self.recipe_data.get('recipe', []))} steps")
        return self.recipe_data
    
    def execute_recipe(self) -> dict:
        """Execute recipe steps and return completion report."""
        if not self.recipe_data:
            raise RecipePipelineError("No recipe loaded. Call load_recipe() first.")
        
        recipe_steps = self.recipe_data.get('recipe', [])
        if not recipe_steps:
            raise RecipePipelineError("Recipe contains no steps")
        
        logger.info(f"Executing {len(recipe_steps)} recipe steps")
        
        for step_index, step_config in enumerate(recipe_steps):
            step_desc = step_config.get('step_description', f'Step {step_index + 1}')
            processor_type = step_config.get('processor_type')
            
            logger.info(f"Step {step_index + 1}: {step_desc}")
            
            try:
                processor = registry.create_processor(step_config)
                
                # Execute based on processor type
                if isinstance(processor, ImportBaseProcessor):
                    processor.execute_import()
                elif isinstance(processor, ExportBaseProcessor):
                    processor.execute_export()
                else:
                    processor.execute_stage_to_stage()
                
                self.steps_executed += 1
                
            except Exception as e:
                logger.error(f"Step {step_index + 1} failed: {e}")
                raise RecipePipelineError(f"Step {step_index + 1} failed: {e}")
        
        # Return completion report
        return StageManager.get_recipe_completion_report()
    
    def run_complete_recipe(self, recipe_path) -> dict:
        """Load recipe and execute - returns completion report."""
        self.load_recipe(recipe_path)
        return self.execute_recipe()
    
    def _initialize_variable_substitution(self) -> None:
        """Initialize variable substitution from recipe."""
        custom_variables = self.recipe_data.get('settings', {}).get('variables', {})
        self.variable_substitution = VariableSubstitution(
            base_variables=custom_variables,
            recipe_path=Path('.')  # Default path
        )
