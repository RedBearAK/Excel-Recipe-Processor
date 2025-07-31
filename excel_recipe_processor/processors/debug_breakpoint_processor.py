"""
Debug breakpoint processor for Excel automation recipes.

Provides a simple way to stop recipe execution for testing and troubleshooting.
"""

import pandas as pd
import logging

from pathlib import Path
from datetime import datetime

from excel_recipe_processor.core.base_processor import ExportBaseProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class DebugBreakpointProcessor(ExportBaseProcessor):  # ← CHANGE: inherit from ExportBaseProcessor
    """
    Processor that exports data for debugging and stops recipe execution.
    
    Acts like "exit 0" in shell scripts - provides a clean way to halt
    processing at any point for testing and troubleshooting.
    """
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'source_stage': 'test_stage',  # ← CHANGE: now requires source_stage
            'message': 'Debug checkpoint'
        }
    
    def save_data(self, data: pd.DataFrame) -> None:  # ← CHANGE: override save_data instead of execute
        """
        Export debug data and stop recipe execution.
        
        Args:
            data: DataFrame loaded from source_stage
            
        Raises:
            StepProcessorError: Always raises to stop execution
        """
        # Validate data (defensive programming)
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Debug breakpoint step '{self.step_name}' requires a pandas DataFrame")
        
        if data.empty:
            raise StepProcessorError(f"Debug breakpoint step '{self.step_name}' received empty DataFrame")
        
        # Get configuration options
        output_path = self.get_config_value('output_path', './debug_outputs/')
        filename_prefix = self.get_config_value('filename_prefix', 'debug_breakpoint')
        include_timestamp = self.get_config_value('include_timestamp', True)
        message = self.get_config_value('message', 'Debug breakpoint reached')
        
        # Create output directory if it doesn't exist
        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        if include_timestamp:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename_prefix}_{timestamp}.xlsx"
        else:
            filename = f"{filename_prefix}.xlsx"
        
        output_file = output_dir / filename
        
        try:
            # Save current data state
            data.to_excel(output_file, index=False)
            
            # Log breakpoint information
            logger.info(f"Debug breakpoint reached: {message}")
            logger.info(f"Current data saved to: {output_file}")
            logger.info(f"Data shape: {data.shape[0]} rows, {data.shape[1]} columns")
            
            # Print summary to console
            print(f"\n{'='*60}")
            print(f"🔍 DEBUG BREAKPOINT REACHED")
            print(f"{'='*60}")
            print(f"Message: {message}")
            print(f"Data saved: {output_file}")
            print(f"Data shape: {data.shape[0]} rows, {data.shape[1]} columns")
            print(f"Columns: {list(data.columns)}")
            print(f"{'='*60}")
            
            # Show sample data if requested
            show_sample = self.get_config_value('show_sample', True)
            sample_rows = self.get_config_value('sample_rows', 5)
            
            if show_sample and len(data) > 0:
                print(f"\nFirst {min(sample_rows, len(data))} rows:")
                print(data.head(sample_rows).to_string())
                print()
            
        except Exception as e:
            raise StepProcessorError(f"Error saving debug data to {output_file}: {e}")
        
        # Stop execution by raising an exception
        raise StepProcessorError(
            f"Recipe execution stopped at debug breakpoint: '{self.step_name}'. "
            f"Data saved to {output_file}"
        )
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Export data for debugging and stop recipe execution',
            'features': [
                'automatic_timestamping', 'custom_output_paths', 'data_preview',
                'execution_stopping', 'troubleshooting_support', 'stage_based_loading'
            ],
            'options': [
                'source_stage', 'output_path', 'filename_prefix', 'include_timestamp', 
                'message', 'show_sample', 'sample_rows'
            ],
            'examples': {
                'simple': "Stop execution and save stage data with timestamp",
                'custom': "Save to specific location with custom message",
                'testing': "Check intermediate results during recipe development"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the debug_breakpoint processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('debug_breakpoint')
