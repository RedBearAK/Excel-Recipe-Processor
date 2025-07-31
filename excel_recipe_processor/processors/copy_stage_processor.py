
import pandas as pd
import logging

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class CopyStageProcessor(BaseStepProcessor):
    """Processor for saving current data as a named stage."""
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {'stage_name': 'test_stage'}
    
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Save stage with proper error handling."""
        self.log_step_start()
        
        # Validate inputs
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Save stage step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_data_not_empty(data)
        self.validate_required_fields(['stage_name'])
        
        # Get configuration
        stage_name = self.get_config_value('stage_name')
        overwrite = self.get_config_value('overwrite', False)
        description = self.get_config_value('description', '')
        
        try:
            # ✅ Call module function with proper error handling
            StageManager.save_stage(
                stage_name=stage_name,
                data=data,
                overwrite=overwrite,
                description=description,
                step_name=self.step_name
            )
            
            result_info = f"saved stage '{stage_name}' ({len(data)} rows, {len(data.columns)} columns)"
            self.log_step_complete(result_info)
            
            return data  # Pass through unchanged
            
        except StageError as e:
            # ✅ Convert StageError to StepProcessorError for consistency
            raise StepProcessorError(f"Error saving stage in step '{self.step_name}': {e}")
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Save current DataFrame as a named stage for later use',
            'stage_features': [
                'data_preservation', 'overwrite_protection', 'metadata_tracking',
                'usage_monitoring', 'memory_tracking', 'stage_limits'
            ],
            'safety_features': [
                'overwrite_validation', 'stage_name_validation', 'data_copying'
            ],
            'examples': {
                'backup': "Save original data before processing",
                'checkpoint': "Save intermediate results for later analysis",
                'branching': "Save data before trying different processing paths"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the copy_stage processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('copy_stage')
