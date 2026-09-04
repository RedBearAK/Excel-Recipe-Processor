"""
Copy a stage's data to another named stage.

excel_recipe_processor/processors/copy_stage_processor.py

Standard data-flow keys: source_stage in, save_to_stage out - the
2026-08-13 standardization retired this processor's old 'stage_name'
destination key, which had a worse problem than inconsistent naming:
without save_to_stage set, the pipeline's execute_stage_to_stage() still
called save_output_data() after the internal save and crashed the step.
The processor now rides the normal save path, overriding it only to honor
its own 'description' and 'overwrite' options.
"""

import logging
import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.base_processor import StepProcessorError, TransformBaseProcessor
from excel_recipe_processor.core.config_schema import Key, Schema, name_list


logger = logging.getLogger(__name__)


class CopyStageProcessor(TransformBaseProcessor):
    """Copy source_stage to save_to_stage, with description/overwrite control."""

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-03); see core/config_schema.py."""
        return Schema([
            Key('description', 'str', default=''),
            Key('overwrite', 'bool', default=False),
        ])

    @classmethod
    def get_minimal_config(cls) -> dict:
        return {
            'source_stage': 'stg_original',
            'save_to_stage': 'stg_copy',
        }

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Pass the data through unchanged; the save path does the copy."""
        if self.step_config.get('stage_name'):
            raise StepProcessorError(
                f"Copy stage step '{self.step_name}': the destination key is "
                f"'save_to_stage' (2026-08-13 standardization; 'stage_name' is "
                f"for declarations and rule references, never step-level flow)"
            )

        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(
                f"Copy stage step '{self.step_name}' requires a pandas DataFrame"
            )
        self.validate_data_not_empty(data)

        return data

    def save_output_data(self, data) -> None:
        """The standard save, honoring this processor's description/overwrite."""
        description = self.get_config_value('description', '')
        overwrite = self.get_config_value('overwrite', False)

        try:
            StageManager.save_stage(
                stage_name=self.save_to_stage,
                data=data,
                overwrite=overwrite,
                description=description or f"Copy of '{self.source_stage}'",
                step_name=self.step_name,
            )
        except StageError as error:
            raise StepProcessorError(
                f"Error saving stage in step '{self.step_name}': {error}"
            )

    def get_capabilities(self) -> dict:
        """Get processor capabilities information."""
        return {
            'description': 'Copy a stage to another named stage',
            'keys': 'source_stage in, save_to_stage out - the standard data-flow '
                    'pair; plus optional description and overwrite',
            'stage_features': [
                'data_preservation', 'overwrite_protection', 'metadata_tracking',
                'usage_monitoring', 'memory_tracking', 'stage_limits'
            ],
            'examples': {
                'backup': "Copy original data before processing",
                'checkpoint': "Copy intermediate results for later analysis",
                'branching': "Copy data before trying different processing paths"
            }
        }

    def get_usage_examples(self) -> dict:
        """Get complete usage examples for the copy_stage processor."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('copy_stage')

# End of file #
