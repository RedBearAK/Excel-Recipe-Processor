"""
Pipeline orchestrator for Excel automation recipes.

Coordinates loading recipes, reading Excel files, executing steps, and saving results.
"""

import re
import pandas as pd
import logging

from pathlib import Path
from datetime import datetime

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


class FilenameVariableSubstitution:
    """Handles variable substitution in filenames for dynamic naming."""
    
    def __init__(self, input_path=None, recipe_path=None, custom_variables=None):
        """
        Initialize variable substitution system.
        
        Args:
            input_path: Path to input file (for input-based variables)
            recipe_path: Path to recipe file (for recipe-based variables)
            custom_variables: Dictionary of custom variables
        """
        self.input_path = Path(input_path) if input_path else None
        self.recipe_path = Path(recipe_path) if recipe_path else None
        self.custom_variables = custom_variables or {}
        self.now = datetime.now()
    
    def substitute_variables(self, filename_template: str) -> str:
        """
        Substitute variables in filename template.
        
        Args:
            filename_template: Template string with {variable} placeholders
            
        Returns:
            Filename with variables substituted
        """
        if not isinstance(filename_template, str):
            return filename_template
        
        # Build variable dictionary
        variables = self._build_variable_dict()
        
        try:
            # Simple variable substitution: {variable}
            result = filename_template.format(**variables)
            
            # Advanced formatted substitution: {date:MMDD}
            result = self._substitute_formatted_variables(result)
            
            return result
            
        except KeyError as e:
            # If variable substitution fails, provide helpful error
            available_vars = list(variables.keys())
            raise ValueError(
                f"Unknown variable {e} in filename template '{filename_template}'. "
                f"Available variables: {available_vars}"
            )
    
    def _build_variable_dict(self) -> dict:
        """Build dictionary of available variables."""
        variables = {}
        
        # Date and time variables
        variables.update({
            'year':             self.now.strftime('%Y'),
            'month':            self.now.strftime('%m'),
            'day':              self.now.strftime('%d'),
            'hour':             self.now.strftime('%H'),
            'minute':           self.now.strftime('%M'),
            'second':           self.now.strftime('%S'),
            'date':             self.now.strftime('%Y%m%d'),
            'time':             self.now.strftime('%H%M%S'),
            'timestamp':        self.now.strftime('%Y%m%d_%H%M%S'),
            'MMDD':             self.now.strftime('%m%d'),
            'YYYY':             self.now.strftime('%Y'),
            'YY':               self.now.strftime('%y'),
            'MM':               self.now.strftime('%m'),
            'DD':               self.now.strftime('%d'),
            'HH':               self.now.strftime('%H'),
            'HHMM':             self.now.strftime('%H%M')
        })
        
        # Input file variables
        if self.input_path:
            variables.update({
                'input_filename':   self.input_path.name,
                'input_basename':   self.input_path.stem,
                'input_extension':  self.input_path.suffix
            })
        
        # Recipe file variables
        if self.recipe_path:
            variables.update({
                'recipe_filename':  self.recipe_path.name,
                'recipe_basename':  self.recipe_path.stem
            })
        
        # Custom variables override built-ins
        variables.update(self.custom_variables)
        
        return variables
    
    def _substitute_formatted_variables(self, filename: str) -> str:
        """Handle formatted variables like {date:MMDD}."""
        
        # Pattern to match {variable:format}
        pattern = r'\{(\w+):([^}]+)\}'
        
        def replace_formatted(match):
            var_name = match.group(1)
            format_spec = match.group(2)
            
            if var_name == 'date':
                return self._format_date(format_spec)
            elif var_name == 'time':
                return self._format_time(format_spec)
            else:
                # For other variables, just return the variable value
                variables = self._build_variable_dict()
                return variables.get(var_name, match.group(0))
        
        return re.sub(pattern, replace_formatted, filename)
    
    def _format_date(self, format_spec: str) -> str:
        """Format date according to specification."""
        format_map = {
            'YYYY': '%Y',
            'YY': '%y', 
            'MM': '%m',
            'DD': '%d',
            'MMDD': '%m%d',
            'YYYYMMDD': '%Y%m%d',
            'MonthDay': '%b%d',
            'Month': '%b',
            'MonthName': '%B'
        }
        
        # Try direct mapping first
        if format_spec in format_map:
            return self.now.strftime(format_map[format_spec])
        
        # Try as direct strftime format
        try:
            return self.now.strftime(format_spec)
        except ValueError:
            return format_spec  # Return original if format invalid
    
    def _format_time(self, format_spec: str) -> str:
        """Format time according to specification."""
        format_map = {
            'HH': '%H',
            'MM': '%M',
            'SS': '%S',
            'HHMM': '%H%M',
            'HHMMSS': '%H%M%S'
        }
        
        if format_spec in format_map:
            return self.now.strftime(format_map[format_spec])
        
        try:
            return self.now.strftime(format_spec)
        except ValueError:
            return format_spec


class ExcelPipeline:
    """
    Orchestrates the execution of Excel processing recipes.
    
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
        """
        Execute all steps in the loaded recipe.
        
        Returns:
            Final processed DataFrame
            
        Raises:
            PipelineError: If recipe execution fails
        """
        # Guard clauses
        if self.recipe_data is None:
            raise PipelineError("No recipe loaded. Call load_recipe() first.")
        
        if self.current_data is None:
            raise PipelineError("No input data loaded. Call load_input_file() first.")
        
        steps = self.recipe_loader.get_steps()
        total_steps = len(steps)
        
        logger.info(f"Starting recipe execution: {total_steps} steps")
        
        self.steps_executed = 0
        
        for i, step_config in enumerate(steps):
            step_number = i + 1
            step_name = step_config.get(step_desc, f'Step {step_number}')
            step_type = step_config.get(proc_type, 'unknown')
            
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
        
        logger.info(f"Recipe execution complete: {self.steps_executed}/{total_steps} steps executed")
        return self.current_data
    
    def save_result(self, output_path, sheet_name='ProcessedData'):
        """Enhanced save_result with variable substitution."""
        
        # Apply variable substitution if available
        if self.variable_substitution and isinstance(output_path, str):
            output_path = self.variable_substitution.substitute_variables(output_path)
        
        # Call original save logic
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
        """Enhanced run_complete_pipeline with variable substitution."""
        
        logger.info("Starting complete pipeline execution")
        
        try:
            # Load recipe and check for variables
            self.load_recipe(recipe_path)
            
            # Get custom variables from recipe settings
            custom_variables = self.recipe_loader.get_settings().get('variables', {})
            
            # Initialize variable substitution
            self.variable_substitution = FilenameVariableSubstitution(
                input_path=input_path,
                recipe_path=recipe_path,
                custom_variables=custom_variables
            )
            
            # Substitute variables in output path
            if isinstance(output_path, str):
                original_output = output_path
                output_path = self.variable_substitution.substitute_variables(output_path)
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
        """
        Get a summary of the current pipeline state.
        
        Returns:
            Dictionary with pipeline information
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


# def get_system_capabilities() -> dict:
#     """Get capabilities of the entire Excel automation system."""
#     from excel_recipe_processor.processors.base_processor import registry
    
#     capabilities = {
#         'system_info': {
#             'description': 'Excel Recipe Processor - Automated Excel data processing system',
#             'total_processors': len(registry.get_registered_types()),
#             'processor_types': registry.get_registered_types()
#         },
#         'processors': {}
#     }
    
#     # Get capabilities for each registered processor
#     for processor_type in registry.get_registered_types():
#         try:
#             # Create a dummy instance to get capabilities
#             dummy_config = {
#                 proc_type: processor_type,
#                 step_desc: f'Capability check for {processor_type}'
#             }
            
#             # Add minimal required fields for each processor type
#             if processor_type == 'add_calculated_column':
#                 dummy_config.update({'new_column': 'test', 'calculation': {}})
#             elif processor_type == 'clean_data':
#                 dummy_config.update({'rules': []})
#             elif processor_type == 'filter_data':
#                 dummy_config.update({'filters': []})
#             elif processor_type == 'group_data':
#                 dummy_config.update({'source_column': 'test', 'groups': {}})
#             elif processor_type == 'lookup_data':
#                 dummy_config.update({
#                     'lookup_source': {}, 'lookup_key': 'test',
#                     'source_key': 'test', 'lookup_columns': ['test']
#                 })
#             elif processor_type == 'rename_columns':
#                 dummy_config.update({'rename_type': 'mapping', 'mapping': {}})
#             elif processor_type == 'sort_data':
#                 dummy_config.update({'columns': ['test']})
#             # pivot_table needs no additional fields
            
#             processor = registry.create_processor(dummy_config)
            
#             if hasattr(processor, 'get_capabilities'):
#                 capabilities['processors'][processor_type] = processor.get_capabilities()
#             else:
#                 capabilities['processors'][processor_type] = {
#                     'description': f'{processor_type} processor (capabilities method not implemented)'
#                 }
                
#         except Exception as e:
#             capabilities['processors'][processor_type] = {
#                 'error': f'Could not get capabilities: {e}'
#             }
    
#     return capabilities


# SIMPLE FIX: Just replace the get_system_capabilities() function in pipeline.py

def get_system_capabilities() -> dict:
    """Get capabilities of the entire Excel automation system using self-reported minimal configs."""
    from excel_recipe_processor.processors.base_processor import registry
    
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
    
    from excel_recipe_processor.processors.base_processor import registry
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


def get_available_variables():
    """Get list of all available variables for documentation."""
    dummy_substitution = FilenameVariableSubstitution()
    variables = dummy_substitution._build_variable_dict()
    
    return {
        'date_time_variables': [
            'year', 'month', 'day', 'hour', 'minute', 'second',
            'date', 'time', 'timestamp', 'MMDD', 'YYYY', 'YY', 'MM', 'DD', 'HH', 'HHMM'
        ],
        'formatted_variables': [
            '{date:MMDD}', '{date:YYYYMMDD}', '{date:MonthDay}', 
            '{time:HHMM}', '{time:HHMMSS}'
        ],
        'file_variables': [
            'input_filename', 'input_basename', 'input_extension',
            'recipe_filename', 'recipe_basename'
        ],
        'custom_variables': 'Any variables defined in settings.variables'
    }


# Import each new processor object when created, and register in function below
from excel_recipe_processor.processors.add_calculated_column_processor  import AddCalculatedColumnProcessor
from excel_recipe_processor.processors.aggregate_data_processor         import AggregateDataProcessor
from excel_recipe_processor.processors.clean_data_processor             import CleanDataProcessor
from excel_recipe_processor.processors.debug_breakpoint_processor       import DebugBreakpointProcessor
from excel_recipe_processor.processors.filter_data_processor            import FilterDataProcessor
from excel_recipe_processor.processors.group_data_processor             import GroupDataProcessor
from excel_recipe_processor.processors.lookup_data_processor            import LookupDataProcessor
from excel_recipe_processor.processors.pivot_table_processor            import PivotTableProcessor
from excel_recipe_processor.processors.rename_columns_processor         import RenameColumnsProcessor
from excel_recipe_processor.processors.sort_data_processor              import SortDataProcessor
from excel_recipe_processor.processors.split_column_processor           import SplitColumnProcessor


def register_standard_processors():
    """Register all standard step processors with the global registry."""

    # Register the processors we've built
    registry.register('add_calculated_column',          AddCalculatedColumnProcessor        )
    registry.register('aggregate_data',                 AggregateDataProcessor              )
    registry.register('clean_data',                     CleanDataProcessor                  )
    registry.register('debug_breakpoint',               DebugBreakpointProcessor            )
    registry.register('filter_data',                    FilterDataProcessor                 )
    registry.register('group_data',                     GroupDataProcessor                  )
    registry.register('lookup_data',                    LookupDataProcessor                 )
    registry.register('pivot_table',                    PivotTableProcessor                 )
    registry.register('rename_columns',                 RenameColumnsProcessor              )
    registry.register('sort_data',                      SortDataProcessor                   )
    registry.register('split_column',                   SplitColumnProcessor                )
    
    logger.debug("Registered standard processors")


# Auto-register processors when module is imported
register_standard_processors()
