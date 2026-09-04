"""
Verify a stage's VALUES against per-rule expectations, mid-pipeline.

excel_recipe_processor/processors/verify_stage_data_processor.py

A Transform CHECK: reads source_stage, writes nothing. Each rule is a
filter_data condition every row must satisfy ("SHIP REF not_empty",
"Booking unique"), warning (default) or halting per rule. Split from the
former verify_data on 2026-09-03; its file-sheet twin is
verify_sheet_data. Verify formula INPUTS here, in stages - written
formula cells have no cached values for the sheet twin to read.
"""

import logging

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError, TransformBaseProcessor
from excel_recipe_processor.core.config_schema import Key, Schema
from excel_recipe_processor.processors._helpers.verify_data_rules import check_rules_config, run_rules


logger = logging.getLogger(__name__)


class VerifyStageDataProcessor(TransformBaseProcessor):
    """Value-level verification of an in-memory stage."""

    writes_stage = False
    requires_save_to_stage = False
    check_summary = ''

    @classmethod
    def config_schema(cls) -> Schema:
        """Declared keys (2026-09-03)."""
        rule = Schema([
            Key('column', 'str', required=True),
            Key('condition', 'str', required=True, description='Any filter_data condition'),
            Key('value', 'any'),
            Key('case_sensitive', 'bool', default=False),
            Key('stage_name', 'stage_in', description='For in_stage / not_in_stage conditions'),
            Key('stage_column', 'str'), Key('stage_key_column', 'str'), Key('stage_value_column', 'str'),
            Key('key_column', 'str'), Key('comparison_operator', 'str'),
            Key('severity', 'str', default='warn', choices=['warn', 'halt']),
            Key('description', 'str', description='Replaces the generated expectation line'),
        ])
        return Schema([Key('rules', 'list_of_mappings', required=True, schema=rule)])

    def __init__(self, step_config: dict):
        super().__init__(step_config)
        # Rule shape is checked at construction so a direct caller (tests,
        # tooling) fails as early as a recipe does at load
        if self.get_config_value('stage', None):
            raise StepProcessorError(
                f"Step '{self.step_name}': use 'source_stage' (the stage this step reads), not 'stage'"
            )
        check_rules_config(self.get_config_value('rules', []), self.step_name)

    @classmethod
    def get_minimal_config(cls) -> dict:
        return {'source_stage': 'stg_to_verify',
                'rules': [{'column': 'Test', 'condition': 'not_empty'}]}

    def execute(self, data):
        self.log_step_start()
        if not isinstance(data, pd.DataFrame):
            raise StepProcessorError(f"Step '{self.step_name}' requires a pandas DataFrame")
        rules = self.get_config_value('rules')
        check_rules_config(rules, self.step_name)
        self.check_summary = run_rules(data, rules, f"stage '{self.source_stage}'", self.step_name)
        logger.info(f"   {self.check_summary}")
        return data

    def get_capabilities(self) -> dict:
        return {
            'description': 'Check a stage row values against rules; warn or halt per rule',
            'vocabulary': 'the full filter_data condition set, borrowed live so the two cannot drift',
            'family': 'transform check: reads source_stage, writes nothing',
        }


# End of file #
