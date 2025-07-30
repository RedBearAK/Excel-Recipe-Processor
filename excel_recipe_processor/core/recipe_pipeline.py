"""
Recipe-based pipeline orchestrator for data processing recipes.

Handles loading recipes, validating stages, executing processing steps,
and managing external variables with interactive prompting support.
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
from excel_recipe_processor.core.interactive_variables import (
    InteractiveVariablePrompt, InteractiveVariableError
)

logger = logging.getLogger(__name__)


class RecipePipelineError(Exception):
    """Raised when recipe pipeline execution fails."""
    pass


class RecipePipeline:
    """Pure stage-based recipe orchestrator with variable support and interactive prompting."""
    
    def __init__(self):
        self.recipe_loader = RecipeLoader()
        self.recipe_data = None
        self.variable_substitution = None
        self.steps_executed = 0
        
        # Track pipeline state
        self._recipe_path = None
        self._custom_variables = {}
        self._external_variables = {}
        self._completion_report = None
    
    def load_recipe(self, recipe_path) -> dict:
        """Load and validate recipe with stages and variables."""
        try:
            self._recipe_path = Path(recipe_path)
            self.recipe_data = self.recipe_loader.load_recipe(recipe_path)
        except Exception as e:
            raise RecipePipelineError(f"Failed to load recipe: {e}")
        
        # Extract custom variables from recipe settings
        settings = self.recipe_data.get('settings', {})
        self._custom_variables = settings.get('variables', {})
        
        # Initialize variable substitution
        self._initialize_variable_substitution()
        
        # Declare and validate stages
        StageManager.declare_recipe_stages(self.recipe_data)
        
        stage_errors = StageManager.validate_recipe_stages(self.recipe_data)
        if stage_errors:
            error_msg = "Recipe validation failed:\n" + "\n".join(f"  - {error}" for error in stage_errors)
            raise RecipePipelineError(error_msg)
        
        logger.info(f"Loaded recipe with {len(self.recipe_data.get('recipe', []))} steps")
        
        # Log variable information
        if self._custom_variables:
            logger.info(f"Found {len(self._custom_variables)} custom variables in recipe")
        
        return self.recipe_data
    
    def collect_external_variables(self, cli_variables: dict = None) -> dict:
        """
        Collect required external variables through CLI overrides and interactive prompting.
        
        Args:
            cli_variables: Variables provided via CLI --var arguments
            
        Returns:
            Dictionary of collected external variables
            
        Raises:
            RecipePipelineError: If variable collection fails
        """
        if not self.recipe_data:
            raise RecipePipelineError("No recipe loaded. Call load_recipe() first.")
        
        cli_variables = cli_variables or {}
        
        # Get required external variables from recipe
        required_external_vars = self.recipe_loader.get_required_external_vars()
        
        if not required_external_vars:
            # No external variables required
            if cli_variables:
                logger.warning("CLI variables provided but recipe doesn't require external variables")
                # Use CLI variables anyway for flexibility
                return cli_variables
            return {}
        
        try:
            # Collect variables interactively
            prompt = InteractiveVariablePrompt(self.variable_substitution)
            external_variables = prompt.collect_variables(required_external_vars, cli_variables)
            
            logger.info(f"Collected {len(external_variables)} external variables")
            return external_variables
            
        except InteractiveVariableError as e:
            raise RecipePipelineError(f"Failed to collect external variables: {e}")
    
    def execute_recipe(self) -> dict:
        """Execute recipe steps and return completion report."""
        if not self.recipe_data:
            raise RecipePipelineError("No recipe loaded. Call load_recipe() first.")
        
        recipe_steps = self.recipe_data.get('recipe', [])
        if not recipe_steps:
            raise RecipePipelineError("Recipe contains no steps")
        
        logger.info(f"Executing {len(recipe_steps)} recipe steps")
        
        # Reset execution state
        self.steps_executed = 0
        
        for step_index, step_config in enumerate(recipe_steps):
            step_desc = step_config.get('step_description', f'Step {step_index + 1}')
            processor_type = step_config.get('processor_type')
            
            logger.info(f"Step {step_index + 1}: {step_desc}")
            
            try:
                # Create processor with variable injection
                processor = self._create_processor(step_config)
                
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
        
        # Generate completion report
        self._completion_report = self._generate_completion_report()
        return self._completion_report
    
    def run_complete_recipe(self, recipe_path, cli_variables: dict = None) -> dict:
        """Load recipe, collect variables, and execute - returns completion report."""
        # Load recipe
        self.load_recipe(recipe_path)
        
        # Collect external variables
        external_variables = self.collect_external_variables(cli_variables)
        
        # Add external variables to pipeline
        for name, value in external_variables.items():
            self.add_external_variable(name, value)
        
        # Execute recipe
        return self.execute_recipe()
    
    def add_external_variable(self, name: str, value: str) -> None:
        """Add an external variable (e.g., from CLI or interactive prompt)."""
        if not isinstance(name, str) or not name.strip():
            raise RecipePipelineError("Variable name must be a non-empty string")
        
        self._external_variables[name] = str(value)
        
        # Also add to variable substitution system
        if self.variable_substitution:
            self.variable_substitution.add_custom_variable(name, value)
        
        logger.debug(f"Added external variable: {name} = {value}")
    
    def get_available_variables(self) -> dict:
        """Get dictionary of all available variables."""
        if self.variable_substitution:
            return self.variable_substitution.get_available_variables()
        else:
            # Fallback: combine variables manually
            all_variables = {}
            all_variables.update(self._custom_variables)  # Recipe variables
            all_variables.update(self._external_variables)  # External variables
            return all_variables
    
    def substitute_template(self, template: str) -> str:
        """Apply variable substitution to a template string."""
        if self.variable_substitution:
            return self.variable_substitution.substitute(template)
        else:
            return template
    
    def get_completion_report(self) -> dict:
        """Get the last completion report, or current state if no execution completed."""
        if self._completion_report:
            return self._completion_report
        else:
            return self._generate_completion_report()
    
    def _initialize_variable_substitution(self) -> None:
        """Initialize variable substitution from recipe."""
        # Combine all variable sources
        all_variables = {}
        all_variables.update(self._custom_variables)  # Recipe variables
        all_variables.update(self._external_variables)  # External variables
        
        self.variable_substitution = VariableSubstitution(
            input_path=None,  # No single input file in stage architecture
            recipe_path=self._recipe_path,
            custom_variables=all_variables
        )
    
    def _create_processor(self, step_config: dict):
        """Create processor with variable injection."""
        try:
            processor = registry.create_processor(step_config)
            
            # Inject variables for dynamic configurations
            self._inject_variables_into_processor(processor)
            
            return processor
            
        except Exception as e:
            step_type = step_config.get('processor_type', 'unknown')
            raise RecipePipelineError(f"Failed to create processor '{step_type}': {e}")
    
    def _inject_variables_into_processor(self, processor) -> None:
        """Inject variables into processor for dynamic configurations."""
        
        # Set variables on processor for use in dynamic configurations
        processor._variables = self.get_available_variables()
        
        # Set variable substitution object for processors that need it
        processor.variable_substitution = self.variable_substitution
        
        logger.debug(f"Injected variables into processor {processor.__class__.__name__}")
    
    def _generate_completion_report(self) -> dict:
        """Generate completion report with execution statistics."""
        try:
            # Get stage manager report
            stage_report = StageManager.get_recipe_completion_report()
            
            # Enhance with pipeline-specific information
            completion_report = {
                'steps_executed': self.steps_executed,
                'recipe_path': str(self._recipe_path) if self._recipe_path else None,
                'variables_used': {
                    'custom_variables': len(self._custom_variables),
                    'external_variables': len(self._external_variables),
                    'total_variables': len(self.get_available_variables())
                },
                'stages_created': stage_report.get('stages_created', []),
                'final_stage_count': stage_report.get('final_stage_count', 0),
                'execution_successful': True
            }
            
            return completion_report
            
        except Exception as e:
            logger.warning(f"Failed to generate complete completion report: {e}")
            return {
                'steps_executed': self.steps_executed,
                'execution_successful': True,
                'report_generation_error': str(e)
            }
