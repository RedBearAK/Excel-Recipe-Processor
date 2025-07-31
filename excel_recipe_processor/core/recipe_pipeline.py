"""
Recipe-based pipeline orchestrator for data processing recipes.

Handles loading recipes, validating stages, executing processing steps,
and managing external variables with friendly error reporting.
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
    """Pure stage-based recipe orchestrator with variable support and friendly error reporting."""
    
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
        """Load and validate recipe with friendly error reporting and helpful suggestions."""
        try:
            self._recipe_path = Path(recipe_path)
            
            # Load recipe with structure validation (this may raise RecipeValidationError)
            try:
                self.recipe_data = self.recipe_loader.load_file(recipe_path)
            except RecipeValidationError as e:
                # Convert validation errors to friendly pipeline errors
                logger.error(f"❌ Recipe validation failed: {recipe_path}")
                logger.error(str(e))
                raise RecipePipelineError(f"Recipe validation failed: {e}")
            
        except FileNotFoundError:
            logger.error(f"❌ Recipe file not found: {recipe_path}")
            raise RecipePipelineError(f"Recipe file not found: {recipe_path}")
        except Exception as e:
            logger.error(f"❌ Failed to load recipe: {e}")
            raise RecipePipelineError(f"Failed to load recipe: {e}")
        
        # Extract custom variables from recipe settings
        settings = self.recipe_data.get('settings', {})
        self._custom_variables = settings.get('variables', {})
        
        # Initialize variable substitution
        self._initialize_variable_substitution()
        
        # Declare stages (this is always safe - just sets up the declarations)
        StageManager.declare_recipe_stages(self.recipe_data)
        
        # Validate stages and show helpful warnings/suggestions
        stage_validation = StageManager.validate_recipe_stages(self.recipe_data)
        
        # Log warnings about undeclared stages
        for warning in stage_validation['warnings']:
            logger.warning(f"⚠️  {warning}")
        
        # Show protection advice if there are undeclared stages
        for issue in stage_validation['protection_issues']:
            logger.info(f"💡 {issue}")
        
        # Show stage declaration suggestions if helpful
        if stage_validation['has_undeclared'] and stage_validation['suggested_declarations']:
            logger.info("💡 To enable stage protection and auto-completion, add these declarations:")
            for line in stage_validation['suggested_declarations'].split('\n'):
                if line.strip():
                    logger.info(f"   {line}")
        
        logger.info(f"✓ Recipe loaded successfully: {len(self.recipe_data.get('recipe', []))} steps")
        
        # Log variable information
        if self._custom_variables:
            logger.info(f"📋 Found {len(self._custom_variables)} custom variables in recipe")
        
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
                logger.warning("⚠️ CLI variables provided but recipe doesn't require external variables")
                # Use CLI variables anyway for flexibility
                return cli_variables
            return {}
        
        try:
            # Collect variables interactively
            prompt = InteractiveVariablePrompt(self.variable_substitution)
            external_variables = prompt.collect_variables(required_external_vars, cli_variables)
            
            logger.info(f"✓ Collected {len(external_variables)} external variables")
            return external_variables
            
        except InteractiveVariableError as e:
            logger.error(f"❌ Failed to collect external variables: {e}")
            raise RecipePipelineError(f"Failed to collect external variables: {e}")
    
    def execute_recipe(self) -> dict:
        """Execute recipe steps with friendly error reporting."""
        if not self.recipe_data:
            raise RecipePipelineError("No recipe loaded. Call load_recipe() first.")
        
        recipe_steps = self.recipe_data.get('recipe', [])
        if not recipe_steps:
            raise RecipePipelineError("Recipe contains no steps")
        
        logger.info(f"🚀 Executing {len(recipe_steps)} recipe steps")
        
        # Reset execution state
        self.steps_executed = 0
        
        for step_index, step_config in enumerate(recipe_steps):
            step_desc = step_config.get('step_description', f'Step {step_index + 1}')
            processor_type = step_config.get('processor_type')
            
            logger.info(f"📍 Step {step_index + 1}: {step_desc}")
            
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
                logger.info(f"✅ Step {step_index + 1} completed successfully")
                
            except StageError as e:
                # Stage-related errors with helpful context
                logger.error(f"❌ Step {step_index + 1} failed - Stage error: {e}")
                raise RecipePipelineError(f"Step {step_index + 1} failed: {e}")
            except StepProcessorError as e:
                # Processor configuration errors
                logger.error(f"❌ Step {step_index + 1} failed - Configuration error: {e}")
                raise RecipePipelineError(f"Step {step_index + 1} failed: {e}")
            except Exception as e:
                # Unexpected errors
                logger.error(f"❌ Step {step_index + 1} failed - Unexpected error: {e}")
                raise RecipePipelineError(f"Step {step_index + 1} failed: {e}")
        
        # Generate completion report
        self._completion_report = self._generate_completion_report()
        logger.info(f"🎉 Recipe execution completed successfully: {self.steps_executed} steps")
        
        return self._completion_report
    
    def run_complete_recipe(self, recipe_path, cli_variables: dict = None) -> dict:
        """Load recipe, collect variables, and execute with comprehensive error handling."""
        try:
            # Load recipe
            logger.info(f"📖 Loading recipe: {recipe_path}")
            self.load_recipe(recipe_path)
            
            # Collect external variables
            logger.info("🔧 Processing external variables...")
            external_variables = self.collect_external_variables(cli_variables)
            
            # Add external variables to pipeline
            for name, value in external_variables.items():
                self.add_external_variable(name, value)
            
            # Execute recipe
            logger.info("⚡ Starting recipe execution...")
            return self.execute_recipe()
            
        except RecipePipelineError:
            # Re-raise pipeline errors as-is (they're already friendly)
            raise
        except Exception as e:
            # Wrap unexpected errors in friendly pipeline error
            logger.error(f"❌ Unexpected error during recipe execution: {e}")
            raise RecipePipelineError(f"Unexpected error during recipe execution: {e}")
    
    def add_external_variable(self, name: str, value: str) -> None:
        """Add an external variable (e.g., from CLI or interactive prompt)."""
        if not isinstance(name, str) or not name.strip():
            raise RecipePipelineError("Variable name must be a non-empty string")
        
        self._external_variables[name] = str(value)
        
        # Also add to variable substitution system
        if self.variable_substitution:
            self.variable_substitution.add_custom_variable(name, value)
        
        logger.debug(f"📝 Added external variable: {name} = {value}")
    
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
        """Create processor with variable substitution applied BEFORE processor creation."""
        try:
            # CRITICAL FIX: Apply variable substitution to step config BEFORE creating processor
            substituted_config = self._apply_variable_substitution_to_config(step_config)
            
            # Create processor with substituted configuration
            processor = registry.create_processor(substituted_config)
            
            # Still inject variables for any dynamic configurations the processor might need
            self._inject_variables_into_processor(processor)
            
            return processor
            
        except KeyError as e:
            # Unknown processor type
            step_type = step_config.get('processor_type', 'unknown')
            available_types = registry.get_registered_types()
            
            error_msg = f"Unknown processor type: '{step_type}'"
            if available_types:
                error_msg += f"\n💡 Available types: {', '.join(sorted(available_types))}"
            
            raise RecipePipelineError(error_msg)
        except Exception as e:
            step_type = step_config.get('processor_type', 'unknown')
            raise RecipePipelineError(f"Failed to create processor '{step_type}': {e}")
    
    def _apply_variable_substitution_to_config(self, config: dict) -> dict:
        """
        Apply variable substitution to all string values in a configuration dictionary.
        
        This processes the step configuration before the processor is created,
        ensuring that variables like {min_sales} are substituted to actual values.
        """
        if not self.variable_substitution:
            return config.copy()
        
        # Create a deep copy to avoid modifying original
        import copy
        substituted_config = copy.deepcopy(config)
        
        # Recursively apply substitution to all string values
        self._substitute_config_values(substituted_config)
        
        return substituted_config
    
    def _substitute_config_values(self, obj):
        """
        Recursively substitute variables in configuration values.
        
        Handles nested dictionaries, lists, and string values.
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, str) and '{' in value:
                    # Apply variable substitution to string values containing variables
                    try:
                        original_value = value
                        obj[key] = self.variable_substitution.substitute(value)
                        if obj[key] != original_value:
                            logger.debug(f"🔄 Config substitution: {key}: '{original_value}' → '{obj[key]}'")
                    except Exception as e:
                        logger.warning(f"⚠️  Variable substitution failed for config '{key}={value}': {e}")
                elif isinstance(value, (dict, list)):
                    # Recursively process nested structures
                    self._substitute_config_values(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, str) and '{' in item:
                    try:
                        original_item = item
                        obj[i] = self.variable_substitution.substitute(item)
                        if obj[i] != original_item:
                            logger.debug(f"🔄 Config substitution: [{i}]: '{original_item}' → '{obj[i]}'")
                    except Exception as e:
                        logger.warning(f"⚠️  Variable substitution failed for config item '{item}': {e}")
                elif isinstance(item, (dict, list)):
                    self._substitute_config_values(item)
    
    def _inject_variables_into_processor(self, processor) -> None:
        """Inject variables into processor for dynamic configurations."""
        
        # Set variables on processor for use in dynamic configurations
        processor._variables = self.get_available_variables()
        
        # Set variable substitution object for processors that need it
        processor.variable_substitution = self.variable_substitution
        
        logger.debug(f"🔧 Injected variables into processor {processor.__class__.__name__}")
    
    def _generate_completion_report(self) -> dict:
        """Generate completion report with execution statistics."""
        try:
            # Get stage manager report
            stage_report = StageManager.get_recipe_completion_report()
            
            # Enhance with pipeline-specific information
            completion_report = {
                'execution_successful': True,
                'steps_executed': self.steps_executed,
                'recipe_path': str(self._recipe_path) if self._recipe_path else None,
                'variables_used': {
                    'custom_variables': len(self._custom_variables),
                    'external_variables': len(self._external_variables),
                    'total_variables': len(self.get_available_variables())
                },
                'stages_created': stage_report.get('stages_created', []),
                'stages_declared': stage_report.get('stages_declared', 0),
                'undeclared_stages_created': stage_report.get('undeclared_stages_created', []),
                'final_stage_count': stage_report.get('stages_created', 0),
                'total_memory_mb': stage_report.get('total_memory_mb', 0)
            }
            
            return completion_report
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to generate complete completion report: {e}")
            return {
                'execution_successful': True,
                'steps_executed': self.steps_executed,
                'report_generation_error': str(e)
            }
