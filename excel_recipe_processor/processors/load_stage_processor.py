
import pandas as pd
import logging

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import BaseStepProcessor, StepProcessorError


logger = logging.getLogger(__name__)


class LoadStageProcessor(BaseStepProcessor):
    """Processor for loading a previously saved stage."""
    
    @classmethod
    def get_minimal_config(cls) -> dict:
        return {'stage_name': 'test_stage', 'confirm_replace': True}
    
    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Load stage with proper error handling."""
        self.log_step_start()
        
        # Validate inputs
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Load stage step '{self.step_name}' requires a pandas DataFrame")
        
        self.validate_required_fields(['stage_name', 'confirm_replace'])
        
        # Get configuration
        stage_name = self.get_config_value('stage_name')
        confirm_replace = self.get_config_value('confirm_replace')
        
        if not confirm_replace:
            raise StepProcessorError(
                "'confirm_replace' must be set to true to acknowledge that current data will be replaced"
            )
        
        try:
            # ✅ Call module function with proper error handling
            stage_data = StageManager.load_stage(stage_name=stage_name)
            
            # Log the replacement
            logger.warning(
                f"Replacing current data ({len(data)} rows) "
                f"with stage '{stage_name}' ({len(stage_data)} rows)"
            )
            
            result_info = f"loaded stage '{stage_name}' ({len(stage_data)} rows, {len(stage_data.columns)} columns)"
            self.log_step_complete(result_info)
            
            return stage_data
            
        except StageError as e:
            # ✅ Convert StageError to StepProcessorError for consistency
            raise StepProcessorError(f"Error loading stage in step '{self.step_name}': {e}")
    
    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Load previously saved stage data, replacing current DataFrame',
            'stage_features': [
                'stage_retrieval', 'usage_tracking', 'data_replacement',
                'confirm_replace_safety', 'stage_validation'
            ],
            'safety_features': [
                'confirm_replace_required', 'stage_existence_validation', 'data_copying'
            ],
            'examples': {
                'restore_backup': "Return to original data after processing",
                'branch_switching': "Switch to different processing branch",
                'comparison': "Load reference data for comparison"
            }
        }
    
    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the load_stage processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('load_stage')
