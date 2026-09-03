"""
Verify a workbook sheet's VALUES against per-rule expectations.

excel_recipe_processor/processors/verify_sheet_data_processor.py

FileOps: reads target_file / sheet_name (live from the workbook session
when held, else from disk) and runs the same rules as verify_stage_data.
CAVEAT: formula cells in a file this framework wrote have no cached
values, so a rule aimed at an injected formula column sees blanks -
verify formula INPUTS in stages with verify_stage_data. Split from the
former verify_data on 2026-09-03.
"""

import logging

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.core.config_schema import Key, Schema
from excel_recipe_processor.processors._helpers.verify_data_rules import (
    check_rules_config, load_sheet_frame, run_rules,
)


logger = logging.getLogger(__name__)


class VerifySheetDataProcessor(FileOpsBaseProcessor):
    """Value-level verification of a written sheet."""

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
        return Schema([
            Key('target_file', 'str', required=True),
            Key('sheet_name', 'any', required=True, description='Tab name, number, or ?sheet_NNN? token'),
            Key('rules', 'list_of_mappings', required=True, schema=rule),
        ])

    @classmethod
    def get_minimal_config(cls) -> dict:
        return {'target_file': 'output.xlsx', 'sheet_name': '?sheet_001?',
                'rules': [{'column': 'Test', 'condition': 'not_empty'}]}

    def _validate_file_operation_config(self):
        check_rules_config(self.get_config_value('rules', []), self.step_name)

    def _resolve_path(self, filename: str) -> str:
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(filename)
        return filename

    def perform_file_operation(self):
        target_file = self._resolve_path(self.get_config_value('target_file'))
        frame, label = load_sheet_frame(target_file, self.get_config_value('sheet_name'), self.step_name)
        return run_rules(frame, self.get_config_value('rules'), label, self.step_name)

    def get_capabilities(self) -> dict:
        return {
            'description': 'Check a written sheet\'s row values against expectations - warn (default) or halt per rule',
            'formula_caveat': 'written formula cells have no cached values; verify formula inputs in stages',
            'family': 'file_ops: target_file and sheet_name',
        }


# End of file #
