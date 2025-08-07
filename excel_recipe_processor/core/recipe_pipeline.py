"""
Enhanced recipe_pipeline.py with improved step logging and configurable error handling.

Key changes:
1. Added blank lines and "START STEP" markers for better log readability
2. Added on_error handling that can be configured globally or per-step
3. Maintains all existing functionality while adding new features
"""

import logging

from enum import Enum
from pathlib import Path
from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import (
    BaseStepProcessor,
    ExportBaseProcessor,
    FileOpsBaseProcessor,
    ImportBaseProcessor,
    registry,
    StepProcessorError,
)
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.core.interactive_variables import (
    InteractiveVariablePrompt, InteractiveVariableError
)

logger = logging.getLogger(__name__)


class ErrorAction(Enum):
    """Defines possible actions when an error occurs during step execution."""
    HALT = "halt"                    # Stop processing immediately (default)
    CONTINUE = "continue"            # Log error but continue to next step
    LOG_AND_CONTINUE = "log_and_continue"  # Detailed logging then continue
    SKIP_REMAINING = "skip_remaining"      # Skip all remaining steps but don't raise


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
        self._global_on_error = ErrorAction.HALT  # Default error behavior
        
        # Track pipeline state
        self._recipe_path = None
        self._custom_variables = {}
        self._external_variables = {}
        self._completion_report = None
    
    def load_recipe(self, recipe_path) -> dict:
        """Load and validate recipe with friendly error reporting and helpful suggestions."""
        try:
            recipe_path = Path(recipe_path)
            self._recipe_path = recipe_path
            
            # Load recipe data
            self.recipe_data = self.recipe_loader.load_recipe_file(recipe_path)
            
            # Declare stages for execution (not just validation)
            StageManager.declare_recipe_stages(self.recipe_data)
            
            # Extract global error handling setting
            settings = self.recipe_data.get('settings', {})
            global_on_error = settings.get('on_error', 'halt')
            self._global_on_error = self._parse_error_action(global_on_error, "global settings")
            
            # Initialize variable substitution
            self._initialize_variable_substitution()
            
            logger.info(f"✓ Recipe loaded successfully: '{recipe_path}'")
            if self._global_on_error != ErrorAction.HALT:
                logger.info(f"⚙️ Global error handling: {self._global_on_error.value}")
            
            return self.recipe_data
            
        except RecipeValidationError as e:
            logger.error(f"❌ Recipe validation failed: {e}")
            raise RecipePipelineError(f"Recipe validation failed: {e}")
        except FileNotFoundError:
            logger.error(f"❌ Recipe file not found: {recipe_path}")
            raise RecipePipelineError(f"Recipe file not found: {recipe_path}")
        except Exception as e:
            logger.error(f"❌ Failed to load recipe: {e}")
            raise RecipePipelineError(f"Failed to load recipe: {e}")
    
    def _parse_error_action(self, action_str: str, context: str) -> ErrorAction:
        """Parse error action string into ErrorAction enum."""
        if not isinstance(action_str, str):
            logger.warning(f"⚠️ Invalid on_error value in {context}: {action_str}. Using 'halt'")
            return ErrorAction.HALT
        
        try:
            return ErrorAction(action_str.lower())
        except ValueError:
            valid_actions = [action.value for action in ErrorAction]
            logger.warning(f"⚠️ Unknown on_error action '{action_str}' in {context}. "
                            f"Valid options: {valid_actions}. Using 'halt'")
            return ErrorAction.HALT
    
    def _log_step_separator(self, step_index: int, step_desc: str) -> None:
        """Log a clean separator before each step for better readability."""
        # Add blank line before step (except for first step)
        if step_index >= 0:
            # logger.info("")  # Blank line
            print() # real blank line!
        
        # # Add START STEP marker
        # separator = f" -- START STEP '{step_desc}' -- "
        # logger.info(separator)
    
    def _handle_step_error(self, step_index: int, step_desc: str, error: Exception, 
                            step_on_error: ErrorAction) -> bool:
        """
        Handle step execution error according to configured error action.
        
        Args:
            step_index: Zero-based step index
            step_desc: Step description
            error: The exception that occurred
            step_on_error: Error action for this step
            
        Returns:
            True if processing should continue, False if it should stop
        """
        step_num = step_index + 1
        
        if step_on_error == ErrorAction.HALT:
            logger.error(f"❌ Step {step_num} failed - Halting execution: {error}")
            raise RecipePipelineError(f"Step {step_num} failed: {error}")
        
        elif step_on_error == ErrorAction.CONTINUE:
            logger.error(f"⚠️ Step {step_num} failed - Continuing execution: {error}")
            return True
            
        elif step_on_error == ErrorAction.LOG_AND_CONTINUE:
            logger.error(f"⚠️ Step {step_num} failed - Detailed logging enabled:")
            logger.error(f"  Step: {step_desc}")
            logger.error(f"  Error type: {type(error).__name__}")
            logger.error(f"  Error message: {error}")
            logger.error(f"  Continuing to next step...")
            return True
            
        elif step_on_error == ErrorAction.SKIP_REMAINING:
            logger.error(f"⚠️ Step {step_num} failed - Skipping remaining steps: {error}")
            return False
        
        else:
            # Fallback to halt for unknown actions
            logger.error(f"❌ Step {step_num} failed - Unknown error action, halting: {error}")
            raise RecipePipelineError(f"Step {step_num} failed: {error}")
    
    def execute_recipe(self) -> dict:
        """Execute recipe steps with enhanced logging and configurable error handling."""
        if not self.recipe_data:
            raise RecipePipelineError("No recipe loaded. Call load_recipe() first.")
        
        recipe_steps = self.recipe_data.get('recipe', [])
        if not recipe_steps:
            raise RecipePipelineError("Recipe contains no steps")
        
        recipe_steps_cnt = len(recipe_steps)
        logger.info(f"🚀 Executing {recipe_steps_cnt} recipe steps")
        
        # Reset execution state
        self.steps_executed = 0
        skipped_steps = 0
        
        for step_index, step_config in enumerate(recipe_steps):
            step_desc = step_config.get('step_description', f'Step {step_index + 1}')
            # processor_type = step_config.get('processor_type')
            
            # Determine error handling for this step
            step_on_error_str = step_config.get('on_error', self._global_on_error.value)
            step_on_error = self._parse_error_action(step_on_error_str, f"step {step_index + 1}")
            
            # Log enhanced step separator
            self._log_step_separator(step_index, step_desc)
            
            # Log step start with error handling info if non-default
            if step_on_error != ErrorAction.HALT:
                logger.info(f"📍 Step {step_index + 1}/{recipe_steps_cnt}: '{step_desc}' [on_error: {step_on_error.value}]")
            else:
                logger.info(f"📍 Step {step_index + 1}/{recipe_steps_cnt}: '{step_desc}'")
            
            try:
                # Create processor with variable injection
                processor = self._create_processor(step_config)
                
                
                # Execute based on processor type
                if isinstance(processor, ImportBaseProcessor):
                    processor.execute_import()
                elif isinstance(processor, ExportBaseProcessor):
                    processor.execute_export()
                elif isinstance(processor, FileOpsBaseProcessor):
                    processor.execute()
                else:
                    # This looks lost/generic to syntax highlighter because we can't check for 
                    # the base processor. It would match any processor, even ones that should 
                    # use a different execute method. 
                    # DO NOT USE isinstance(processor, BaseStepProcessor) to fix this!!!!!!
                    processor.execute_stage_to_stage()
                
                self.steps_executed += 1
                logger.info(f"✅ Step {step_index + 1} completed successfully")
                
            except (StageError, StepProcessorError, Exception) as e:
                # Handle error according to configured action
                should_continue = self._handle_step_error(step_index, step_desc, e, step_on_error)
                
                if not should_continue:
                    # Count remaining steps as skipped
                    skipped_steps = len(recipe_steps) - (step_index + 1)
                    break
        
        # Log final blank line for clean separation
        logger.info("")
        
        # Generate completion report
        self._completion_report = self._generate_completion_report()
        
        # Enhanced completion logging
        if skipped_steps > 0:
            logger.info(f"🎯 Recipe execution completed: {self.steps_executed} steps executed, "
                        f"{skipped_steps} steps skipped")
        else:
            logger.info(f"🎉 Recipe execution completed successfully: {self.steps_executed} steps")
        
        return self._completion_report

    def collect_external_variables(self, cli_variables: dict = None) -> dict:
        """
        Collect external variables from CLI arguments and interactive prompts.
        
        Enhanced to resolve CLI variables that contain template variables.
        """
        if not self.recipe_data:
            raise RecipePipelineError("No recipe loaded. Call load_recipe() first.")
        
        cli_variables = cli_variables or {}
        
        # Get required external variables from recipe
        required_external_vars = self.recipe_loader.get_required_external_vars()
        
        if not required_external_vars:
            # No external variables required by recipe
            if cli_variables:
                logger.warning("⚠️ CLI variables provided but recipe doesn't require external variables")
                # ENHANCEMENT: Still resolve CLI variables for flexibility
                resolved_cli_vars = {}
                for name, value in cli_variables.items():
                    if isinstance(value, str) and self.variable_substitution and '{' in value:
                        try:
                            resolved_value = self.variable_substitution.substitute(value)
                            resolved_cli_vars[name] = resolved_value
                            logger.debug(f"📝 Resolved CLI variable '{name}': '{value}' → '{resolved_value}'")
                        except Exception as e:
                            logger.warning(f"⚠️ Could not resolve CLI variable '{name}': {e}")
                            resolved_cli_vars[name] = value
                    else:
                        resolved_cli_vars[name] = value
                return resolved_cli_vars
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

    def run_complete_recipe(self, recipe_path, cli_variables: dict = None) -> dict:
        """Load recipe, collect variables, and execute with comprehensive error handling."""
        try:
            print()     # blank line to separate from parsing log line (if present) or command line
            # Load recipe
            logger.info(f"📖 Loading recipe: '{recipe_path}'")
            self.load_recipe(recipe_path)
            
            # Collect external variables
            print()     # blank line to separate from recipe loading logging
            logger.info("🔧 Processing external variables...")
            external_variables = self.collect_external_variables(cli_variables)
            
            # Add external variables to pipeline (now with resolution)
            for name, value in external_variables.items():
                self.add_external_variable(name, value)
            
            # Log resolved variables for transparency
            if external_variables:
                logger.info(f"✓ Resolved {len(external_variables)} external variables:")
                for name, value in external_variables.items():
                    logger.info(f"  {name} = {value}")
            
            # Final validation that all custom variables are fully resolved
            self._validate_all_variables_resolved()
            
            # Execute recipe
            print()     # blank line to separate from earlier meta-info (here we go!)
            logger.info("⚡ Starting recipe execution...")
            return self.execute_recipe()
            
        except RecipePipelineError:
            # Re-raise pipeline errors as-is (they're already friendly)
            raise
        except Exception as e:
            # Wrap unexpected errors in friendly pipeline error
            logger.error(f"❌ Unexpected error during recipe execution: {e}")
            raise RecipePipelineError(f"Unexpected error during recipe execution: {e}")
    
    def _validate_all_variables_resolved(self):
        """Final validation that all custom variables are fully resolved."""
        if not self.variable_substitution:
            return
        
        available_vars = self.variable_substitution.get_available_variables()
        unresolved_vars = []
        
        for name, value in available_vars.items():
            if isinstance(value, str) and '{' in value and '}' in value:
                # This variable still contains unresolved references
                unresolved_vars.append(f"{name} = '{value}'")
        
        if unresolved_vars:
            raise RecipePipelineError(
                f"Unresolved variables detected before recipe execution:\n" + 
                "\n".join(f"  - {var}" for var in unresolved_vars)
            )

    def add_external_variable(self, name: str, value: Any) -> None:
        """Add an external variable (e.g., from CLI or interactive prompt) with immediate resolution."""
        if not isinstance(name, str) or not name.strip():
            raise RecipePipelineError("Variable name must be a non-empty string")
        
        # ENHANCEMENT: Resolve variables in CLI values immediately
        if isinstance(value, str) and self.variable_substitution and '{' in value:
            try:
                # Resolve template variables in the CLI variable value
                resolved_value = self.variable_substitution.substitute(value)
                logger.debug(f"📝 Resolved CLI variable '{name}': '{value}' → '{resolved_value}'")
            except Exception as e:
                # If resolution fails, use original value and let validation catch it later
                logger.warning(f"⚠️ Could not resolve CLI variable '{name}': {e}")
                resolved_value = value
        else:
            resolved_value = value
        
        # Store resolved value
        self._external_variables[name] = resolved_value
        
        # Also add to variable substitution system
        if self.variable_substitution:
            self.variable_substitution.add_custom_variable(name, resolved_value)
        
        value_repr = repr(resolved_value) if not isinstance(resolved_value, str) else resolved_value
        logger.debug(f"📝 Added external variable: {name} = {value_repr} (type: {type(resolved_value).__name__})")

        # Re-resolve any custom variables that might reference this external variable
        self._re_resolve_custom_variables()

    def _re_resolve_custom_variables(self):
        """Re-resolve custom variables that might contain variable references."""
        settings = self.recipe_data.get('settings', {})
        custom_variables = settings.get('variables', {})
        
        for name, template_value in custom_variables.items():
            # Only try to resolve if it's a string with variable references
            if isinstance(template_value, str) and '{' in template_value:
                try:
                    resolved_value = self.variable_substitution.substitute(template_value)
                    self.variable_substitution.add_custom_variable(name, resolved_value)
                except Exception as e:
                    logger.warning(f"Failed to re-resolve variable '{name}': {e}")
    
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
        if not self.recipe_data:
            return
            
        # Create variable system
        self.variable_substitution = VariableSubstitution()
        
        # Add recipe-defined variables (preserving original types)
        settings = self.recipe_data.get('settings', {})
        custom_variables = settings.get('variables', {})
        
        for name, value in custom_variables.items():
            self.add_custom_variable(name, value)
    
    def add_custom_variable(self, name: str, value: Any) -> None:
        """Add a custom variable defined in the recipe (any type)."""
        if not isinstance(name, str) or not name.strip():
            raise RecipePipelineError("Variable name must be a non-empty string")
        
        # Store original value without conversion
        self._custom_variables[name] = value
        
        # Also add to variable substitution system
        if self.variable_substitution:
            self.variable_substitution.add_custom_variable(name, value)
        
        # Log with type information  
        value_repr = repr(value) if not isinstance(value, str) else value
        logger.debug(f"📝 Added custom variable: {name} = {value_repr} (type: {type(value).__name__})")
    
    def _create_processor(self, step_config: dict):
        """Create processor instance with variable injection."""
        processor_type = step_config.get('processor_type')
        
        if processor_type not in registry._processors:
            available_types = list(registry._processors.keys())
            raise StepProcessorError(f"Unknown processor type: {processor_type}. Available: {available_types}")
        
        # APPLY RECURSIVE VARIABLE SUBSTITUTION TO STEP CONFIG BEFORE CREATING PROCESSOR
        processed_step_config = self._substitute_variables_in_config(step_config)
        
        # Create processor instance with substituted config
        processor_class = registry._processors[processor_type]
        processor = processor_class(processed_step_config)
        
        # Set variables on processor for use in dynamic configurations
        processor._variables = self.get_available_variables()
        
        # Set variable substitution object for processors that need it
        processor.variable_substitution = self.variable_substitution
        
        logger.debug(f"🔧 Applied variable substitution and injected into processor {processor.__class__.__name__}")
        return processor

    def _substitute_variables_in_config(self, config: Any) -> Any:
        """
        Recursively substitute variables in a configuration structure.
        Handles nested dictionaries, lists, and string values.
        Now supports both string and structure replacement.
        """
        if not self.variable_substitution:
            return config
        
        try:
            return self.variable_substitution.substitute_structure(config)
        except Exception as e:
            # If substitution fails, log warning and return original
            logger.warning(f"Variable substitution failed for config: {e}")
            return config

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
                'global_error_handling': self._global_on_error.value,
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
                'global_error_handling': self._global_on_error.value,
                'report_generation_error': str(e)
            }