"""
Enhanced recipe_pipeline.py with improved step logging and configurable error handling.

Key changes:
1. Added blank lines and "START STEP" markers for better log readability
2. Added on_error handling that can be configured globally or per-step
3. Maintains all existing functionality while adding new features
"""

import logging
import time
import pandas as pd
from excel_recipe_processor.core.log_format import clock, now_stamp

from enum import Enum
from pathlib import Path
from typing import Any

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.verification_ledger import VerificationLedger
from excel_recipe_processor.core.base_processor import (
    BaseStepProcessor,
    ExportBaseProcessor,
    FileOpsBaseProcessor,
    ImportBaseProcessor,
    registry,
    StepProcessorError,
)
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
from excel_recipe_processor.core.stage_inspection import (
    dump_stage_to_file, StageInspectionError
)
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.core.interactive_variables import (
    InteractiveVariablePrompt, InteractiveVariableError
)

logger = logging.getLogger(__name__)


def count_step_elements(value) -> int:
    """
    Ballpark "elements" in one step's config, parsed from the YAML alone.

    The rule: every LIST ITEM the recipe author enumerated counts as one
    element, at every nesting depth; dict keys and scalar option values
    count as zero. A filters: list of five conditions is 5; a formula
    definition is 1 regardless of how many options it carries; a
    format_excel step counts its sheet entries plus each entry's nested
    column_formats rules.

    Known ballpark limits, accepted on purpose: a "{list_str:variable}" string
    counts as 1 though it may expand to many items, and a step with no
    list content at all reports 0 here (the caller floors it to 1). The
    number is a property of what the author WROTE - stable across data
    sizes, which is what makes it comparable between runs. A per-processor
    semantic count would need every processor touched; this needs none.
    """
    if isinstance(value, list):
        return len(value) + sum(count_step_elements(item) for item in value)
    if isinstance(value, dict):
        return sum(count_step_elements(inner) for inner in value.values())
    return 0


def count_recipe_elements(recipe_data: dict) -> int:
    """Sum of per-step elements across the recipe, each step counting at least 1."""
    steps = recipe_data.get('recipe', []) if isinstance(recipe_data, dict) else []
    return sum(max(count_step_elements(step), 1) for step in steps)


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
        # Development-time inspection, driven entirely from the command line so
        # that examining a recipe never means editing it.
        self._dump_requests = {}        # stage name -> row spec (or None)
        # stage name -> save count already dumped. A stage saved again after
        # its dump gets dumped again: a re-used stage that only dumped once
        # would be untroubleshootable from the outside.
        self._dumped_versions = {}
        self._dump_output_dir = '.'
        self._stop_after_stage = None
        self._validate_only = False

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
            from excel_recipe_processor.core.main import mirror_print
            mirror_print()  # per-step separator; mirrored to the log file
        
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
            WorkbookSession.discard_all()
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
        self._run_started_at = time.perf_counter()
        WorkbookSession.reset()
        VerificationLedger.reset()
        WorkbookSession.set_deferred(True)

        # OPT-IN: route every session save through the dynamic-array
        # declaration so injected/seeded formulas open without the
        # implicit-intersection @. See core/dynamic_array_metadata.py.
        declare_dynamic = self.recipe_data.get('settings', {}).get(
            'declare_dynamic_formulas', False
        )
        WorkbookSession.set_declare_dynamic(bool(declare_dynamic))
        if declare_dynamic:
            logger.info("🧬 Dynamic-array declaration enabled for all session saves")
        logger.info(f"🚀 Executing {recipe_steps_cnt} recipe steps")
        
        # Reset execution state
        self.steps_executed = 0
        skipped_steps = 0

        # TIMESTAMP DOCTRINE: the true processing extents get their own
        # dedicated lines, stamp-first like the step clocks; durations
        # live elsewhere
        from excel_recipe_processor.core.log_format import now_stamp
        logger.info(f"🕐 [{now_stamp()}] Recipe processing started")

        for step_index, step_config in enumerate(recipe_steps):
            step_desc = step_config.get('step_description', f'Step {step_index + 1}')
            # processor_type = step_config.get('processor_type')
            
            # Determine error handling for this step
            step_on_error_str = step_config.get('on_error', self._global_on_error.value)
            step_on_error = self._parse_error_action(step_on_error_str, f"step {step_index + 1}")
            
            # Log enhanced step separator
            self._log_step_separator(step_index, step_desc)
            
            # Log step start with error handling info if non-default
            _step_clock = time.perf_counter()
            if step_on_error != ErrorAction.HALT:
                logger.info(f"📍 [{clock()}] Step {step_index + 1}/{recipe_steps_cnt}: '{step_desc}' [on_error: {step_on_error.value}]")
            else:
                logger.info(f"📍 [{clock()}] Step {step_index + 1}/{recipe_steps_cnt}: '{step_desc}'")
            
            try:
                # Create processor with variable injection
                processor = self._create_processor(step_config)

                # The stage contract window (2026-09-04): inside it the
                # processor may read only the stages its schema declares
                # it reads and write only those it declares it writes.
                # Closed in the finally below so dumps and peeks run outside.
                StageManager.begin_step(step_index, step_desc)

                # Execute based on processor type
                if isinstance(processor, ImportBaseProcessor):
                    processor.execute_import()
                elif isinstance(processor, ExportBaseProcessor):
                    processor.execute_export()
                elif isinstance(processor, FileOpsBaseProcessor):
                    processor.execute()
                elif not processor.requires_source_stage:
                    # Processors that invent a stage rather than transform one -
                    # create_stage builds from inline recipe data. They declare
                    # this themselves rather than the pipeline carrying a list.
                    # Checked by attribute, NOT by isinstance, for the reason in
                    # the comment below.
                    # create_stage still takes a pass-through DataFrame it
                    # ignores, a leftover from before stages. An empty frame
                    # satisfies that without inventing data.
                    processor.execute(pd.DataFrame())
                else:
                    # This looks lost/generic to syntax highlighter because we can't check for 
                    # the base processor. It would match any processor, even ones that should 
                    # use a different execute method. 
                    # DO NOT USE isinstance(processor, BaseStepProcessor) to fix this!!!!!!
                    processor.execute_stage_to_stage()
                
                StageManager.end_step()
                self.steps_executed += 1
                logger.info(f"✅ Step {step_index + 1} completed successfully ({time.perf_counter() - _step_clock:.3f}s)")
                StageManager.auto_free_after_step(step_index)

                self._dump_requested_stages()

                if self._should_stop_after(step_config):
                    logger.info(
                        f"🛑 Stopping after '{self._stop_after_stage}' as requested "
                        f"(--stop-after). {self.steps_executed} of "
                        f"{recipe_steps_cnt} steps ran."
                    )
                    break
                
            except (StageError, StepProcessorError, Exception) as e:
                StageManager.end_step()
                # Handle error according to configured action
                should_continue = self._handle_step_error(step_index, step_desc, e, step_on_error)
                
                if not should_continue:
                    # Count remaining steps as skipped
                    skipped_steps = len(recipe_steps) - (step_index + 1)
                    break
        
        from excel_recipe_processor.core.main import mirror_print
        mirror_print()  # separator; mirrored so the file keeps the spacing
        
        # Generate completion report
        self._completion_report = self._generate_completion_report()
        self._completion_report['stage_memory'] = StageManager.get_memory_stats()
        
        # All steps succeeded: write every session workbook exactly once
        WorkbookSession.flush_all()

        # Enhanced completion logging
        if skipped_steps > 0:
            logger.info(f"🎯 Recipe execution completed: {self.steps_executed} steps executed, "
                        f"{skipped_steps} steps skipped")
        else:
            logger.info(f"🎉 Recipe execution completed successfully: {self.steps_executed} steps")
            VerificationLedger.log_summary()
            logger.info(
                f"🧮 Recipe elements: {count_recipe_elements(self.recipe_data)} "
                f"parsed from the YAML across {len(self.recipe_data.get('recipe', []))} "
                f"step(s) (ballpark: enumerated list items per step, min 1)"
            )
        
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

    def configure_inspection(self, dump_requests: dict = None,
                             stop_after_stage: str = None,
                             validate_only: bool = False,
                             dump_output_dir: str = '.') -> None:
        """
        Set up development-time stage inspection.

        Args:
            dump_requests:    Stage name -> row spec (or None for all rows)
            stop_after_stage: Halt once this stage has been written
            validate_only:    Stop after the validation phase (--validate)
            dump_output_dir:  Where dumped CSVs go
        """
        self._dump_requests = dump_requests or {}
        self._stop_after_stage = stop_after_stage
        self._validate_only = validate_only
        self._dump_output_dir = dump_output_dir or '.'

    def _dump_requested_stages(self) -> None:
        """Write out any requested stage that now exists."""
        if not self._dump_requests:
            return

        from excel_recipe_processor.core.stage_manager import StageManager

        for stage_name, spec in self._dump_requests.items():
            if not StageManager.stage_exists(stage_name):
                continue

            save_count = StageManager.get_stage_save_count(stage_name)
            if save_count <= self._dumped_versions.get(stage_name, 0):
                continue

            try:
                dump_stage_to_file(
                    stage_name, StageManager.load_stage(stage_name),
                    spec, self._dump_output_dir, save_number=save_count
                )
                self._dumped_versions[stage_name] = save_count
            except StageInspectionError as error:
                raise StepProcessorError(f"--dump-stage {stage_name}: {error}")

    def _should_stop_after(self, step_config: dict) -> bool:
        """Report whether this step wrote the stage named by --stop-after."""
        if not self._stop_after_stage:
            return False

        written = step_config.get('save_to_stage')
        return written == self._stop_after_stage

    def run_complete_recipe(self, recipe_path, cli_variables: dict = None) -> dict:
        """Load recipe, collect variables, and execute with comprehensive error handling."""
        try:
            from excel_recipe_processor.core.main import mirror_print
            mirror_print()  # separator; buffered for the file's head
            # Load recipe
            logger.info(f"📖 Loading recipe: '{recipe_path}'")
            self.load_recipe(recipe_path)
            
            # Collect external variables
            mirror_print()  # separator; buffered for the file's head
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

            # VALIDATION PHASE (2026-09-03): every step against its
            # processor's declared schema, plus the stage graph, on the
            # variable-resolved configs, before any step touches data.
            # --validate stops here; a real run continues only if clean.
            mirror_print()
            logger.info("\U0001f50d Validating recipe steps and stage graph...")
            from excel_recipe_processor.core.recipe_validation import validate_recipe
            report = validate_recipe(
                self.recipe_data, registry, self._substitute_variables_in_config)
            report.log()
            if not report.ok:
                raise RecipePipelineError(
                    f"Recipe validation failed with {len(report.errors)} error(s); "
                    f"nothing was run"
                )

            # Auto-free plan from the same schema-derived reads validation
            # just checked, on the same resolved configs (2026-09-04).
            StageManager.plan_auto_free(
                self.recipe_data, registry, self._substitute_variables_in_config)
            if self._validate_only:
                logger.info("\U0001f6d1 --validate: stopping before execution")
                return {
                    'validate_only': True,
                    'steps': len(self.recipe_data.get('recipe', [])),
                    'warnings': len(report.warnings),
                }

            # The log-file decision (2026-09-04): every run writes a log.
            # A recipe log_file template attaches HERE because paths like
            # {output_dir}/{output_basename}_log.txt need the external
            # variables just resolved above; no directive means the
            # platform default location (core/default_log_path.py); false
            # opts out. The buffered startup lines become the file's head.
            # --log-file on the CLI captured those live and outranks all.
            settings = self.recipe_data.get('settings', {}) \
                if isinstance(self.recipe_data, dict) else {}
            from excel_recipe_processor.core.default_log_path import resolve_log_file_setting
            substitute = (self.substitute_template if self.variable_substitution
                          else (lambda text: text))
            try:
                resolved_log_path = resolve_log_file_setting(
                    settings.get('log_file'), self._recipe_path, substitute)
            except ValueError as error:
                raise RecipePipelineError(str(error))
            if resolved_log_path is not None:
                from excel_recipe_processor.core.main import attach_log_file
                attach_log_file(str(resolved_log_path), source='recipe')
            else:
                # Opted out, and any CLI attach already consumed the
                # buffer - drop what remains
                from excel_recipe_processor.core.main import discard_early_log_buffer
                discard_early_log_buffer()

            # Execute recipe
            from excel_recipe_processor.core.main import mirror_print
            mirror_print()  # separator; mirrored so the file keeps the spacing
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
            # A name supplied on the command line must not be overwritten by the
            # recipe's own template for it. Without this guard, re-resolution
            # runs after every external variable is added and quietly restores
            # the recipe value, so --set donor_file appears to be accepted and
            # then has no effect.
            if name in self._external_variables:
                logger.debug(
                    f"📝 Keeping externally supplied '{name}', not re-resolving "
                    f"the recipe's own value"
                )
                continue

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
            
        # Create variable system.
        #
        # Passing the recipe path is what supplies {recipe_dir} and
        # {recipe_parent_dir}, which let a recipe reference its own siblings
        # regardless of the directory the command was run from. Without it those
        # variables never resolve and the run fails at load with "Unresolved
        # variables detected" - correct, but only after the user wonders why.
        self.variable_substitution = VariableSubstitution(recipe_path=self._recipe_path)
        
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
            # Fail LOUD (2026-08-17). The old warn-and-continue returned
            # the UNSUBSTITUTED config, so the step then failed on a
            # misleading shape complaint (e.g. verify_columns seeing the
            # literal '{list:...}' string instead of a list) while the
            # real, guided error - a retired reference, an unconvertible
            # member, a typo'd template - scrolled past as a warning.
            raise StepProcessorError(
                f"Variable substitution failed for step configuration: {e}"
            ) from e

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
                'elapsed_seconds': (
                    time.perf_counter() - self._run_started_at
                    if getattr(self, '_run_started_at', None) else None
                ),
                'stages_created': stage_report.get('stages_created', []),
                'stages_freed': stage_report.get('stages_freed', []),
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