"""
Pipeline orchestrator for Excel automation recipes.

Coordinates loading recipes, reading Excel files, executing steps, and saving results.
"""

import pandas as pd
import logging

from pathlib import Path

import excel_recipe_processor as erp

from excel_recipe_processor.config.loader import RecipeLoader, RecipeValidationError
from excel_recipe_processor.readers.excel_reader import ExcelReader, ExcelReaderError
from excel_recipe_processor.writers.excel_writer import ExcelWriter, ExcelWriterError
from excel_recipe_processor.processors.base_processor import registry, StepProcessorError


logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Raised when pipeline execution fails."""
    pass


class ExcelPipeline:
    """
    Orchestrates the execution of Excel processing recipes.
    
    Handles the complete workflow from loading recipes and input files
    to executing processing steps and saving results.
    """
    
    def __init__(self):
        """Initialize the pipeline orchestrator."""
        self.recipe_loader = RecipeLoader()
        self.excel_reader = ExcelReader()
        self.excel_writer = ExcelWriter()
        
        self.recipe_data = None
        self.input_data = None
        self.current_data = None
        self.steps_executed = 0
    
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
            step_name = step_config.get('name', f'Step {step_number}')
            step_type = step_config.get('type', 'unknown')
            
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
    
    def save_result(self, output_path, sheet_name='ProcessedData') -> None:
        """
        Save the processed data to an Excel file.
        
        Args:
            output_path: Path for output file
            sheet_name: Name of the sheet to create
            
        Raises:
            PipelineError: If saving fails
        """
        # Guard clause
        if self.current_data is None:
            raise PipelineError("No processed data to save. Execute recipe first.")
        
        try:
            self.excel_writer.write_file(self.current_data, output_path, sheet_name=sheet_name)
            logger.info(f"Saved result to: {output_path}")
        except ExcelWriterError as e:
            raise PipelineError(f"Failed to save result: {e}")
    
    def run_complete_pipeline(self, recipe_path, input_path, output_path, 
                             input_sheet=0, output_sheet='ProcessedData') -> pd.DataFrame:
        """
        Run the complete pipeline from start to finish.
        
        Args:
            recipe_path: Path to recipe file
            input_path: Path to input Excel file  
            output_path: Path for output Excel file
            input_sheet: Sheet to read from input (default: first sheet)
            output_sheet: Sheet name for output
            
        Returns:
            Final processed DataFrame
            
        Raises:
            PipelineError: If any step fails
        """
        logger.info("Starting complete pipeline execution")
        
        try:
            # Load recipe
            self.load_recipe(recipe_path)
            
            # Load input data
            self.load_input_file(input_path, sheet_name=input_sheet)
            
            # Execute all steps
            result = self.execute_recipe()
            
            # Save result
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
        
        if 'type' not in step_config:
            raise PipelineError("Step configuration missing 'type' field")
        
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


def get_system_capabilities():
    capabilities = {'processors': {}}
    for processor_type in registry.get_registered_types():
        try:
            # Create processor and call its capabilities method
            processor = registry.create_processor({'type': processor_type})
            if hasattr(processor, 'get_capabilities'):
                capabilities['processors'][processor_type] = processor.get_capabilities()
        except:
            pass  # Skip processors that can't be created
    return capabilities


def register_standard_processors():
    """Register all standard step processors with the global registry."""
    from excel_recipe_processor.processors.add_calculated_column_processor import (
        AddCalculatedColumnProcessor
    )
    from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor
    from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor
    from excel_recipe_processor.processors.group_data_processor import GroupDataProcessor
    from excel_recipe_processor.processors.pivot_table_processor import PivotTableProcessor
    from excel_recipe_processor.processors.rename_columns_processor import RenameColumnsProcessor
    from excel_recipe_processor.processors.sort_data_processor import SortDataProcessor
    
    # Register the processors we've built
    registry.register('add_calculated_column',          AddCalculatedColumnProcessor)
    registry.register('clean_data',                     CleanDataProcessor)
    registry.register('filter_data',                    FilterDataProcessor)
    registry.register('group_data',                     GroupDataProcessor)
    registry.register('pivot_table',                    PivotTableProcessor)
    registry.register('rename_columns',                 RenameColumnsProcessor)
    registry.register('sort_data',                      SortDataProcessor)
    
    logger.debug("Registered standard processors")


# Auto-register processors when module is imported
register_standard_processors()
