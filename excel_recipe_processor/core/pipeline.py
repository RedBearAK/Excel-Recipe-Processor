"""
Pipeline orchestrator for Excel automation recipes.

Coordinates loading recipes, reading Excel files, executing steps, and saving results.
"""

import pandas as pd
import logging

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.config.recipe_loader import RecipeLoader, RecipeValidationError
from excel_recipe_processor.readers.excel_reader import ExcelReader, ExcelReaderError
from excel_recipe_processor.writers.excel_writer import ExcelWriter, ExcelWriterError
from excel_recipe_processor.processors.base_processor import registry, StepProcessorError


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
    to executing processing steps and saving results.
    """
    
    def __init__(self):
        """Initialize the pipeline orchestrator."""
        self.recipe_loader              = RecipeLoader()
        self.excel_reader               = ExcelReader()
        self.excel_writer               = ExcelWriter()
        
        self.recipe_data                = None
        self.input_data                 = None
        self.current_data               = None
        self.steps_executed             = 0
        self.variable_substitution      = None

    def load_recipe(self, recipe_path) -> dict:
        """
        Load a recipe file.
        
        Args:
            recipe_path: Path to the recipe file
            
        Returns:
            Loaded recipe data
            
        Raises:
            PipelineError: If recipe loading fails
        """
        try:
            self.recipe_data = self.recipe_loader.load_file(recipe_path)
            logger.info(f"Loaded recipe: {self.recipe_loader.summary()}")
            return self.recipe_data
        except (RecipeValidationError, FileNotFoundError) as e:
            raise PipelineError(f"Failed to load recipe: {e}")
    
    def load_input_file(self, input_path, sheet_name=0) -> pd.DataFrame:
        """
        Load the input Excel file.
        
        Args:
            input_path: Path to input Excel file
            sheet_name: Sheet to read (default: first sheet)
            
        Returns:
            Loaded DataFrame
            
        Raises:
            PipelineError: If input loading fails
        """
        try:
            self.input_data = self.excel_reader.read_file(input_path, sheet_name=sheet_name)
            self.current_data = self.input_data.copy()
            
            logger.info(f"Loaded input file: {len(self.input_data)} rows, {len(self.input_data.columns)} columns")
            return self.input_data
        except ExcelReaderError as e:
            raise PipelineError(f"Failed to load input file: {e}")
    
    def execute_recipe(self) -> pd.DataFrame:
        """Execute recipe with stage management."""
        # Guard clauses
        if self.recipe_data is None:
            raise PipelineError("No recipe loaded. Call load_recipe() first.")
        
        if self.current_data is None:
            raise PipelineError("No input data loaded. Call load_input_file() first.")
        
        steps: list[dict] = self.recipe_loader.get_steps()  # ✅ Great type annotation!
        total_steps = len(steps)
        
        logger.info(f"Starting recipe execution: {total_steps} steps")
        
        # ✅ Initialize stages at recipe start
        StageManager.initialize_stages(max_stages=10)
        
        self.steps_executed = 0
        
        try:
            for i, step_config in enumerate(steps):
                step_number = i + 1
                step_name = step_config.get('step_description', f'Step {step_number}')
                step_type = step_config.get('processor_type', 'unknown')
                
                try:
                    logger.info(f"Executing step {step_number}/{total_steps}: {step_name} ({step_type})")
                    
                    # Create processor for this step
                    processor = self._create_processor(step_config)
                    
                    # Execute the step
                    self.current_data = processor.execute(self.current_data)
                    
                    # Guard clause: ensure we still have a DataFrame
                    if not isinstance(self.current_data, pd.DataFrame):
                        raise PipelineError(f"Step {step_number} did not return a DataFrame")
                    
                    self.steps_executed += 1
                    
                    logger.info(f"Completed step {step_number}: {len(self.current_data)} rows remaining")
                    
                except Exception as e:
                    logger.error(f"Failed at step {step_number} ({step_name}): {e}")
                    raise PipelineError(f"Step {step_number} failed: {e}")
            
            # ✅ Check for unused stages using module functions
            unused_stages = StageManager.get_unused_stages()
            if unused_stages:
                stage_list = StageManager.list_stages()
                for stage_name in unused_stages:
                    stage_info: dict = stage_list.get(stage_name, {})
                    created_step = stage_info.get('step_name', 'unknown')
                    logger.warning(
                        f"Recipe completed with unused stage: '{stage_name}' "
                        f"(created in '{created_step}', never used)"
                    )
            
            # ✅ Log stage summary using module functions
            stage_summary = StageManager.get_stage_summary()
            if stage_summary['total_stages'] > 0:
                logger.info(
                    f"Stage summary: {stage_summary['total_stages']} stages created, "
                    f"{stage_summary['unused_stages']} unused, "
                    f"~{stage_summary['total_memory_mb']:.1f}MB used"
                )
            
            logger.info(f"Recipe execution complete: {self.steps_executed}/{total_steps} steps executed")
            
            return self.current_data
        
        finally:
            # ✅ Always clean up stages using module function
            StageManager.cleanup_stages()
    
    def save_result(self, output_path, sheet_name='ProcessedData'):
        """Save result with variable substitution."""
        
        # Apply variable substitution if available
        if self.variable_substitution and isinstance(output_path, str):
            output_path = self.variable_substitution.substitute(output_path)
        
        # Guard clause
        if self.current_data is None:
            raise PipelineError("No processed data to save. Execute recipe first.")
        
        try:
            self.excel_writer.write_file(self.current_data, output_path, sheet_name=sheet_name)
            logger.info(f"Saved result to: {output_path}")
        except ExcelWriterError as e:
            raise PipelineError(f"Failed to save result: {e}")
    
    def run_complete_pipeline(self, recipe_path, input_path, output_path, 
                            input_sheet=0, output_sheet='ProcessedData'):
        """Run complete pipeline with variable substitution."""
        
        logger.info("Starting complete pipeline execution")
        
        try:
            # Load recipe and check for variables
            self.load_recipe(recipe_path)
            
            # Get custom variables from recipe settings
            custom_variables = self.recipe_loader.get_settings().get('variables', {})
            
            # Initialize variable substitution
            self.variable_substitution = VariableSubstitution(
                input_path=input_path,
                recipe_path=recipe_path,
                custom_variables=custom_variables
            )
            
            # Substitute variables in output path
            if isinstance(output_path, str):
                original_output = output_path
                output_path = self.variable_substitution.substitute(output_path)
                if output_path != original_output:
                    logger.info(f"Substituted variables in output filename: {original_output} → {output_path}")
            
            # Continue with normal pipeline execution
            self.load_input_file(input_path, sheet_name=input_sheet)
            result = self.execute_recipe()
            self.save_result(output_path, sheet_name=output_sheet)
            
            logger.info("Pipeline execution completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
    
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
        """Get pipeline summary including stage information.""" 
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
        
        # ✅ Add stage information using module functions
        summary['stages'] = StageManager.get_stage_summary()
        
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
            return self.excel_writer.create_backup(file_path)
        except ExcelWriterError as e:
            raise PipelineError(f"Failed to create backup: {e}")


def get_system_capabilities() -> dict:
    """Get capabilities of the entire Excel automation system using self-reported minimal configs."""
    capabilities = {
        'system_info': {
            'description': 'Excel Recipe Processor - Automated Excel data processing system',
            'total_processors': len(registry.get_registered_types()),
            'processor_types': registry.get_registered_types()
        },
        'processors': {}
    }
    
    # Get capabilities for each registered processor using their self-reported minimal config
    for processor_type in registry.get_registered_types():
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
            else:
                raise StepProcessorError(
                    f"Processor class {processor_class.__name__} missing get_minimal_config() method. "
                    f"Add this method to enable self-discovery."
                )

            # Add required fields
            minimal_config[proc_type] = processor_type
            minimal_config[step_desc] = f'Capability check for {processor_type}'
            
            # Create processor instance
            processor = registry.create_processor(minimal_config)
            
            # Get capabilities
            if hasattr(processor, 'get_capabilities'):
                capabilities['processors'][processor_type] = processor.get_capabilities()
            else:
                capabilities['processors'][processor_type] = {
                    'description': f'{processor_type} processor (capabilities method not implemented)'
                }
                
        except Exception as e:
            capabilities['processors'][processor_type] = {
                'error': f'Could not get capabilities: {e}'
            }
    
    return capabilities


def check_recipe_capabilities(recipe_data: dict) -> dict:
    """Check if all processors in a recipe are available and get their capabilities."""
    
    capabilities_report = {
        'recipe_valid': True,
        'total_steps': 0,
        'step_analysis': [],
        'missing_processors': [],
        'available_features': []
    }
    
    if 'recipe' not in recipe_data:
        capabilities_report['recipe_valid'] = False
        capabilities_report['error'] = 'No recipe section found'
        return capabilities_report
    
    steps = recipe_data['recipe']
    capabilities_report['total_steps'] = len(steps)
    
    available_types = registry.get_registered_types()
    
    for i, step in enumerate(steps):
        step_analysis = {
            'step_number': i + 1,
            'step_name': step.get(step_desc, f'Step {i + 1}'),
            'step_type': step.get(proc_type, 'unknown'),
            'available': False,
            'capabilities': None
        }
        
        step_type = step.get(proc_type)
        if step_type in available_types:
            step_analysis['available'] = True
            try:
                processor = registry.create_processor(step)
                if hasattr(processor, 'get_capabilities'):
                    step_analysis['capabilities'] = processor.get_capabilities()
            except:
                pass  # Skip capability extraction if step config is incomplete
        else:
            capabilities_report['missing_processors'].append(step_type)
            capabilities_report['recipe_valid'] = False
        
        capabilities_report['step_analysis'].append(step_analysis)
    
    return capabilities_report


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
from excel_recipe_processor.processors.lookup_data_processor            import LookupDataProcessor
from excel_recipe_processor.processors.merge_data_processor             import MergeDataProcessor
from excel_recipe_processor.processors.pivot_table_processor            import PivotTableProcessor
from excel_recipe_processor.processors.rename_columns_processor         import RenameColumnsProcessor
from excel_recipe_processor.processors.sort_data_processor              import SortDataProcessor
from excel_recipe_processor.processors.split_column_processor           import SplitColumnProcessor


def register_standard_processors():
    """Register all standard step processors with the global registry."""

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
    registry.register('lookup_data',                    LookupDataProcessor                 )
    registry.register('merge_data',                     MergeDataProcessor                  )
    registry.register('pivot_table',                    PivotTableProcessor                 )
    registry.register('rename_columns',                 RenameColumnsProcessor              )
    registry.register('sort_data',                      SortDataProcessor                   )
    registry.register('split_column',                   SplitColumnProcessor                )
    
    logger.debug("Registered standard processors")


# Auto-register processors when module is imported
register_standard_processors()
