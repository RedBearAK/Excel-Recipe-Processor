"""
Pipeline orchestrator for Excel automation recipes.

Coordinates loading recipes, reading files, executing steps, and saving results
with enhanced FileReader/FileWriter integration and comprehensive variable substitution.
"""

import pandas as pd
import logging

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.file_reader import FileReader, FileReaderError
from excel_recipe_processor.core.file_writer import FileWriter, FileWriterError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.processors.base_processor import registry, StepProcessorError
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError


logger = logging.getLogger(__name__)

step_desc = 'step_description'
proc_type = 'processor_type'


class PipelineError(Exception):
    """Raised when pipeline execution fails."""
    pass


class ExcelPipeline:
    """
    Orchestrates the execution of Excel data processing recipes.
    
    Handles the complete workflow from loading recipes and input files
    to executing processing steps and saving results with enhanced:
    - FileReader/FileWriter integration for consistent file handling
    - Comprehensive variable substitution across all operations
    - StageManager coordination for intermediate data storage
    - Processor variable injection for dynamic configurations
    """
    
    def __init__(self):
        """Initialize the pipeline orchestrator."""
        self.recipe_loader              = RecipeLoader()
        
        self.recipe_data                = None
        self.input_data                 = None
        self.current_data               = None
        self.steps_executed             = 0
        self.variable_substitution      = None
        
        # Track pipeline state for enhanced integration
        self._input_path                = None
        self._recipe_path               = None
        self._custom_variables          = {}

    def load_recipe(self, recipe_path) -> dict:
        """
        Load a recipe file with variable substitution support.
        
        Args:
            recipe_path: Path to the recipe file
            
        Returns:
            Loaded recipe data
            
        Raises:
            PipelineError: If recipe loading fails
        """
        try:
            self._recipe_path = Path(recipe_path)
            self.recipe_data = self.recipe_loader.load_file(recipe_path)
            
            # Extract custom variables from recipe settings
            settings = self.recipe_loader.get_settings()
            self._custom_variables = settings.get('variables', {})
            
            logger.info(f"Loaded recipe: {self.recipe_loader.summary()}")
            
            # Log variable information
            if self._custom_variables:
                logger.info(f"Found {len(self._custom_variables)} custom variables in recipe")
            
            return self.recipe_data
        except (RecipeValidationError, FileNotFoundError) as e:
            raise PipelineError(f"Failed to load recipe: {e}")
    
    def load_input_file(self, input_path, sheet_name=0) -> pd.DataFrame:
        """
        Load input file using FileReader with variable substitution.
        
        Args:
            input_path: Path to input file (supports variable substitution)
            sheet_name: Sheet to read (name or index)
            
        Returns:
            Loaded DataFrame
            
        Raises:
            PipelineError: If file loading fails
        """
        try:
            # Store original input path for variable context
            self._input_path = Path(input_path)
            
            # Initialize or update variable substitution
            self._initialize_variable_substitution()
            
            # Apply variable substitution to input path
            substituted_input_path = self.variable_substitution.substitute(str(input_path))
            if substituted_input_path != str(input_path):
                logger.info(f"Substituted variables in input filename: {input_path} → {substituted_input_path}")
            
            # Use FileReader for enhanced file handling
            self.input_data = FileReader.read_file(
                substituted_input_path,
                variables=self._get_current_variables(),
                sheet=sheet_name
            )
            
            self.current_data = self.input_data.copy()
            
            logger.info(f"Loaded input file: {substituted_input_path} ({len(self.input_data)} rows, {len(self.input_data.columns)} columns)")
            return self.input_data
            
        except FileReaderError as e:
            raise PipelineError(f"Failed to load input file: {e}")
    
    def execute_recipe(self) -> pd.DataFrame:
        """
        Execute all recipe steps with enhanced processor integration and detailed stage management.
        
        Returns:
            Final processed DataFrame
            
        Raises:
            PipelineError: If recipe execution fails
        """
        if self.recipe_data is None:
            raise PipelineError("No recipe loaded. Call load_recipe() first.")
        
        if self.current_data is None:
            raise PipelineError("No input data loaded. Call load_input_file() first.")
        
        # Initialize StageManager for the pipeline run
        max_stages = self.recipe_loader.get_settings().get('max_stages', 10)
        StageManager.initialize_stages(max_stages)
        
        steps = self.recipe_loader.get_steps()
        total_steps = len(steps)
        
        logger.info(f"Starting recipe execution: {total_steps} steps")
        
        self.steps_executed = 0
        
        try:
            # Ensure variable substitution is initialized
            self._initialize_variable_substitution()
            
            for i, step_config in enumerate(steps):
                step_number = i + 1
                step_name = step_config.get(step_desc, f'Step {step_number}')
                step_type = step_config.get(proc_type, 'unknown')
                
                try:
                    logger.info(f"Executing step {step_number}/{total_steps}: {step_name} ({step_type})")
                    
                    # Create processor with enhanced integration
                    processor = self._create_processor(step_config)
                    
                    # Inject variables into processor for dynamic configurations
                    self._inject_variables_into_processor(processor)
                    
                    # Execute step
                    self.current_data = processor.execute(self.current_data)
                    
                    # Guard clause: ensure we still have a DataFrame
                    if not isinstance(self.current_data, pd.DataFrame):
                        raise PipelineError(f"Step {step_number} did not return a DataFrame")
                    
                    self.steps_executed += 1
                    
                    logger.info(f"Completed step {step_number}: {len(self.current_data)} rows remaining")
                    
                except Exception as e:
                    logger.error(f"Failed at step {step_number} ({step_name}): {e}")
                    raise PipelineError(f"Step {step_number} failed: {e}")
            
            # Check for unused stages
            unused_stages = StageManager.get_unused_stages()
            if unused_stages:
                stage_list = StageManager.list_stages()
                for stage_name in unused_stages:
                    stage_info = stage_list.get(stage_name, {})
                    created_step = stage_info.get('step_name', 'unknown')
                    logger.warning(
                        f"Recipe completed with unused stage: '{stage_name}' "
                        f"(created in '{created_step}', never used)"
                    )
            
            # Log stage summary
            stage_summary = StageManager.get_stage_summary()
            if stage_summary['total_stages'] > 0:
                logger.info(
                    f"Stage summary: {stage_summary['total_stages']} stages created, "
                    f"{stage_summary['unused_stages']} unused, "
                    f"~{stage_summary['total_memory_mb']:.1f}MB used"
                )
            
            logger.info(f"Recipe execution complete: {self.steps_executed}/{total_steps} steps executed")
            return self.current_data
            
        except Exception as e:
            if isinstance(e, PipelineError):
                raise
            else:
                raise PipelineError(f"Recipe execution failed: {e}")
        
        finally:
            # Cleanup stages unless configured to keep them
            cleanup_stages = self.recipe_loader.get_settings().get('cleanup_stages', True)
            if cleanup_stages:
                StageManager.cleanup_stages()
                logger.debug("Cleaned up pipeline stages")
    
    def save_result(self, output_path, sheet_name='ProcessedData') -> None:
        """
        Save result using FileWriter with variable substitution.
        
        Args:
            output_path: Path to output file (supports variable substitution)
            sheet_name: Sheet name for output
            
        Raises:
            PipelineError: If saving fails
        """
        if self.current_data is None:
            raise PipelineError("No data to save. Execute recipe first.")
        
        try:
            # Ensure variable substitution is initialized
            self._initialize_variable_substitution()
            
            # Apply variable substitution to output path
            substituted_output_path = self.variable_substitution.substitute(str(output_path))
            if substituted_output_path != str(output_path):
                logger.info(f"Substituted variables in output filename: {output_path} → {substituted_output_path}")
            
            # Apply variable substitution to sheet name
            substituted_sheet_name = self.variable_substitution.substitute(str(sheet_name))
            if substituted_sheet_name != str(sheet_name):
                logger.info(f"Substituted variables in sheet name: {sheet_name} → {substituted_sheet_name}")
            
            # Use FileWriter for enhanced file handling
            FileWriter.write_file(
                self.current_data,
                substituted_output_path,
                variables=self._get_current_variables(),
                sheet_name=substituted_sheet_name,
                create_backup=self.recipe_loader.get_settings().get('create_backup', False)
            )
            
            logger.info(f"Saved result to: {substituted_output_path} ({len(self.current_data)} rows)")
            
        except FileWriterError as e:
            raise PipelineError(f"Failed to save result: {e}")
    
    def run_complete_pipeline(self, recipe_path, input_path, output_path, 
                            input_sheet=0, output_sheet='ProcessedData'):
        """
        Run complete pipeline with enhanced variable substitution and file handling.
        
        Args:
            recipe_path: Path to recipe file
            input_path: Path to input file (supports variable substitution)
            output_path: Path to output file (supports variable substitution)
            input_sheet: Sheet to read from input file
            output_sheet: Sheet name for output
            
        Returns:
            Final processed DataFrame
            
        Raises:
            PipelineError: If pipeline execution fails
        """
        logger.info("Starting complete pipeline execution")
        
        try:
            # Load recipe first to get variables
            self.load_recipe(recipe_path)
            
            # Load input file with variable support
            self.load_input_file(input_path, sheet_name=input_sheet)
            
            # Execute recipe steps
            result = self.execute_recipe()
            
            # Save result with variable support
            self.save_result(output_path, sheet_name=output_sheet)
            
            logger.info("Pipeline execution completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
    
    def _initialize_variable_substitution(self) -> None:
        """Initialize or update variable substitution with current context."""
        
        self.variable_substitution = VariableSubstitution(
            input_path=self._input_path,
            recipe_path=self._recipe_path,
            custom_variables=self._custom_variables
        )
    
    def _get_current_variables(self) -> dict:
        """Get current variable dictionary for file operations."""
        
        if self.variable_substitution:
            return self.variable_substitution.get_available_variables()
        else:
            return self._custom_variables
    
    def _inject_variables_into_processor(self, processor) -> None:
        """Inject variables into processor for dynamic configurations."""
        
        # Set variables on processor for use in dynamic configurations
        processor._variables = self._get_current_variables()
        
        # Also set variable substitution object if processor expects it
        if hasattr(processor, '_variable_substitution'):
            processor._variable_substitution = self.variable_substitution
    
    def _create_processor(self, step_config: dict):
        """
        Create a processor instance for a step configuration.
        
        Args:
            step_config: Step configuration dictionary
            
        Returns:
            Initialized processor instance
            
        Raises:
            PipelineError: If processor creation fails
        """
        # Guard clause
        if not isinstance(step_config, dict):
            raise PipelineError("Step configuration must be a dictionary")
        
        if proc_type not in step_config:
            raise PipelineError(f"Step configuration missing {proc_type} field")
        
        try:
            return registry.create_processor(step_config)
        except StepProcessorError as e:
            raise PipelineError(f"Failed to create processor: {e}")
    
    def get_pipeline_summary(self) -> dict:
        """
        Get comprehensive pipeline summary including stage and variable information.
        
        Returns:
            Dictionary with pipeline summary information
        """
        summary = {
            'recipe_loaded': self.recipe_data is not None,
            'input_loaded': self.input_data is not None,
            'steps_executed': self.steps_executed,
        }
        
        if self.recipe_data:
            summary['total_steps'] = len(self.recipe_loader.get_steps())
            summary['recipe_settings'] = self.recipe_loader.get_settings()
        
        if self.input_data is not None:
            summary['input_rows'] = len(self.input_data)
            summary['input_columns'] = len(self.input_data.columns)
        
        if self.current_data is not None:
            summary['current_rows'] = len(self.current_data)
            summary['current_columns'] = len(self.current_data.columns)
        
        # Add stage information
        summary['stages'] = StageManager.get_stage_summary()
        
        # Enhanced: Add detailed information for refactored version
        summary['recipe_info'] = {}
        summary['variable_info'] = {}
        summary['stage_info'] = {}
        
        # Recipe information
        if self.recipe_data:
            summary['recipe_info'] = {
                'total_steps': len(self.recipe_data.get('recipe', [])),
                'settings': self.recipe_loader.get_settings(),
                'recipe_path': str(self._recipe_path) if self._recipe_path else None
            }
        
        # Variable information  
        if self.variable_substitution:
            summary['variable_info'] = {
                'available_variables': self.variable_substitution.get_available_variables(),
                'custom_variables_count': len(self._custom_variables),
                'input_path': str(self._input_path) if self._input_path else None
            }
        
        # Stage information
        try:
            stage_list = StageManager.list_stages()
            summary['stage_info'] = {
                'active_stages': len(stage_list),
                'stage_names': list(stage_list.keys()),
                'max_stages': StageManager._max_stages
            }
        except:
            summary['stage_info'] = {'active_stages': 0, 'stage_names': [], 'max_stages': 0}
        
        return summary
    
    def create_backup(self, file_path) -> Path:
        """
        Create a backup of a file before processing.
        
        Args:
            file_path: Path to file to backup
            
        Returns:
            Path to backup file
            
        Raises:
            PipelineError: If backup creation fails
        """
        try:
            # Use FileWriter's backup capability instead of excel_writer directly
            backup_path = Path(str(file_path) + '.backup')
            
            # Copy file to backup location
            import shutil
            shutil.copy2(file_path, backup_path)
            
            logger.info(f"Created backup: {backup_path}")
            return backup_path
            
        except Exception as e:
            raise PipelineError(f"Failed to create backup: {e}")
    
    def get_available_variables(self) -> dict:
        """
        Get all available variables for the current pipeline context.
        
        Returns:
            Dictionary of available variables
        """
        if self.variable_substitution:
            return self.variable_substitution.get_available_variables()
        else:
            return self._custom_variables.copy()
    
    def add_custom_variable(self, name: str, value: str) -> None:
        """
        Add a custom variable to the pipeline context.
        
        Args:
            name: Variable name
            value: Variable value
        """
        self._custom_variables[name] = value
        
        # Update variable substitution if initialized
        if self.variable_substitution:
            self.variable_substitution.add_custom_variable(name, value)
        
        logger.debug(f"Added custom variable: {name} = {value}")
    
    def validate_variable_templates(self, templates: list) -> dict:
        """
        Validate variable templates and return validation results.
        
        Args:
            templates: List of template strings to validate
            
        Returns:
            Dictionary with validation results
        """
        if not self.variable_substitution:
            self._initialize_variable_substitution()
        
        results = {}
        for template in templates:
            unknown_vars = self.variable_substitution.validate_template(template)
            results[template] = {
                'valid': len(unknown_vars) == 0,
                'unknown_variables': unknown_vars
            }
        
        return results
    
    def substitute_template(self, template: str) -> str:
        """
        Substitute variables in a template string.
        
        Args:
            template: Template string with variable placeholders
            
        Returns:
            String with variables substituted
        """
        if not self.variable_substitution:
            self._initialize_variable_substitution()
        
        return self.variable_substitution.substitute(template)
    
    def cleanup_pipeline(self) -> None:
        """Clean up pipeline resources and stages."""
        
        try:
            StageManager.cleanup_stages()
            logger.debug("Cleaned up pipeline stages")
        except:
            pass  # Stages may not be initialized
        
        # Reset pipeline state
        self.current_data = None
        self.steps_executed = 0
        
        logger.debug("Pipeline cleanup completed")
    
    def __str__(self) -> str:
        """String representation of the pipeline."""
        recipe_name = self._recipe_path.name if self._recipe_path else 'No recipe'
        input_name = self._input_path.name if self._input_path else 'No input'
        return f"ExcelPipeline(recipe='{recipe_name}', input='{input_name}', steps_executed={self.steps_executed})"
    
    def __repr__(self) -> str:
        """Developer representation of the pipeline."""
        return (f"ExcelPipeline(recipe_loaded={self.recipe_data is not None}, "
                f"input_loaded={self.input_data is not None}, "
                f"steps_executed={self.steps_executed})")


def get_system_capabilities() -> dict:
    """
    Get comprehensive system capabilities including all processors and their features.
    Uses self-reported minimal configs from processors for accurate capability discovery.
    
    Returns:
        Dictionary with system overview and detailed processor capabilities
    """
    try:
        processor_types = registry.get_registered_types()
        
        capabilities = {
            'system_info': {
                'description': 'Excel Recipe Processor - Automated Excel data processing system',
                'total_processors': len(processor_types),
                'processor_types': sorted(processor_types)
            },
            'processors': {}
        }
        
        # Get capabilities for each registered processor using self-reported minimal config
        for processor_type in processor_types:
            try:
                # Get the processor class from registry
                processor_class = registry._processors.get(processor_type)
                
                if processor_class is None:
                    capabilities['processors'][processor_type] = {
                        'error': f'Could not get processor class for {processor_type}'
                    }
                    continue
                
                # Get minimal configuration from the processor class (if it has the method)
                if hasattr(processor_class, 'get_minimal_config'):
                    minimal_config = processor_class.get_minimal_config()
                    
                    # Add the processor_type field that's required by BaseStepProcessor
                    minimal_config['processor_type'] = processor_type
                    minimal_config['step_description'] = f'Capabilities check for {processor_type}'

                    # Create processor instance using minimal config
                    processor = registry.create_processor(minimal_config)
                    
                    # Get capabilities if available
                    if hasattr(processor, 'get_capabilities'):
                        capabilities['processors'][processor_type] = processor.get_capabilities()
                    else:
                        # Basic info for processors without get_capabilities method
                        capabilities['processors'][processor_type] = {
                            'description': f'{processor_type.replace("_", " ").title()} processor',
                            'status': 'available',
                            'capabilities_method': False
                        }
                else:
                    capabilities['processors'][processor_type] = {
                        'error': f'Processor class {processor_class.__name__} missing get_minimal_config() method. '
                                f'Add this method to enable self-discovery.'
                    }
                    
            except Exception as e:
                # Handle processors that can't be instantiated
                capabilities['processors'][processor_type] = {
                    'error': f'Could not load capabilities: {str(e)}',
                    'status': 'error'
                }
        
        return capabilities
        
    except Exception as e:
        # Fallback if entire capability discovery fails
        return {
            'system_info': {
                'description': 'Excel Recipe Processor',
                'total_processors': 0,
                'processor_types': [],
                'error': f'Capability discovery failed: {str(e)}'
            },
            'processors': {}
        }


# Import processor classes
from excel_recipe_processor.processors.add_calculated_column_processor  import AddCalculatedColumnProcessor
from excel_recipe_processor.processors.add_subtotals_processor          import AddSubtotalsProcessor
from excel_recipe_processor.processors.aggregate_data_processor         import AggregateDataProcessor
from excel_recipe_processor.processors.clean_data_processor             import CleanDataProcessor
from excel_recipe_processor.processors.create_stage_processor           import CreateStageProcessor
from excel_recipe_processor.processors.debug_breakpoint_processor       import DebugBreakpointProcessor
from excel_recipe_processor.processors.fill_data_processor              import FillDataProcessor
from excel_recipe_processor.processors.filter_data_processor            import FilterDataProcessor
from excel_recipe_processor.processors.group_data_processor             import GroupDataProcessor
from excel_recipe_processor.processors.load_stage_processor             import LoadStageProcessor
from excel_recipe_processor.processors.lookup_data_processor            import LookupDataProcessor
from excel_recipe_processor.processors.merge_data_processor             import MergeDataProcessor
from excel_recipe_processor.processors.pivot_table_processor            import PivotTableProcessor
from excel_recipe_processor.processors.rename_columns_processor         import RenameColumnsProcessor
from excel_recipe_processor.processors.save_stage_processor             import SaveStageProcessor
from excel_recipe_processor.processors.sort_data_processor              import SortDataProcessor
from excel_recipe_processor.processors.split_column_processor           import SplitColumnProcessor


def register_standard_processors():
    """Register all standard processors with the registry."""
    
    # Register existing processors
    registry.register('add_calculated_column',          AddCalculatedColumnProcessor        )
    registry.register('add_subtotals',                  AddSubtotalsProcessor               )
    registry.register('aggregate_data',                 AggregateDataProcessor              )
    registry.register('clean_data',                     CleanDataProcessor                  )
    registry.register('create_stage',                   CreateStageProcessor                )
    registry.register('debug_breakpoint',               DebugBreakpointProcessor            )
    registry.register('fill_data',                      FillDataProcessor                   )
    registry.register('filter_data',                    FilterDataProcessor                 )
    registry.register('group_data',                     GroupDataProcessor                  )
    registry.register('load_stage',                     LoadStageProcessor                  )
    registry.register('lookup_data',                    LookupDataProcessor                 )
    registry.register('merge_data',                     MergeDataProcessor                  )
    registry.register('pivot_table',                    PivotTableProcessor                 )
    registry.register('rename_columns',                 RenameColumnsProcessor              )
    registry.register('save_stage',                     SaveStageProcessor                  )
    registry.register('sort_data',                      SortDataProcessor                   )
    registry.register('split_column',                   SplitColumnProcessor                )
    
    logger.debug("Registered standard processors")


# Auto-register processors when module is imported
register_standard_processors()
