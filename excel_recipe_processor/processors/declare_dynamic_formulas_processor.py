"""
Declare dynamic-array formulas in an on-disk workbook.

excel_recipe_processor/processors/declare_dynamic_formulas_processor.py

Applies the dynamic-array declaration (cm="1" cell markers, the XLDAPR
xl/metadata.xml part, and its package registrations) to a finished xlsx, so
formulas containing dynamic-era functions open in Excel without the
implicit-intersection @.

The at-save form of the same pass is enabled recipe-wide with
`declare_dynamic_formulas: true` in settings; this processor exists for
files already on disk - a donor workbook carrying inherited {CSE} braces,
or an output flushed before a recipe wants the declaration applied. Shared
guts live in core/dynamic_array_metadata.py, which also documents why the
default function vocabulary makes the pass safe by construction.
"""

import logging

from pathlib import Path

from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.dynamic_array_metadata import (
    DynamicArrayMetadataError,
    declare_dynamic_formulas_in_zip,
)


logger = logging.getLogger(__name__)


class DeclareDynamicFormulasProcessor(FileOpsBaseProcessor):
    """Apply the dynamic-array declaration to an xlsx already on disk."""

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'input_file': 'output.xlsx',
        }

    def _validate_file_operation_config(self):
        """Require the input file; everything else has defaults."""
        if not self.get_config_value('input_file'):
            raise StepProcessorError(
                f"Declare dynamic formulas step '{self.step_name}' requires 'input_file'"
            )

        extra_functions = self.get_config_value('extra_functions', [])
        if extra_functions and not isinstance(extra_functions, list):
            raise StepProcessorError(
                f"Declare dynamic formulas step '{self.step_name}': 'extra_functions' "
                f"must be a list of function names, got {type(extra_functions).__name__}"
            )

    def perform_file_operation(self):
        """Run the declaration pass on the resolved file path."""
        input_file = self.get_config_value('input_file')
        output_file = self.get_config_value('output_file', None)
        extra_functions = self.get_config_value('extra_functions', [])

        resolved_input = self._resolve_path(input_file)
        resolved_output = self._resolve_path(output_file) if output_file else resolved_input

        input_path = Path(resolved_input)
        if not input_path.is_file():
            raise StepProcessorError(
                f"Declare dynamic formulas step '{self.step_name}': "
                f"input file not found: {resolved_input}"
            )
        if input_path.suffix.lower() not in ('.xlsx', '.xlsm'):
            raise StepProcessorError(
                f"Declare dynamic formulas step '{self.step_name}': input must be "
                f".xlsx or .xlsm, got: {input_path.suffix}"
            )

        if WorkbookSession.is_open(resolved_input):
            # The disk bytes are stale while the session holds the workbook;
            # rewriting them here would be overwritten (or fight) at flush.
            raise StepProcessorError(
                f"Declare dynamic formulas step '{self.step_name}': the session "
                f"currently holds '{input_path.name}' in memory. Place a "
                f"flush_workbooks step before this one, or use the recipe-wide "
                f"'declare_dynamic_formulas: true' setting instead."
            )

        try:
            report = declare_dynamic_formulas_in_zip(
                resolved_input, resolved_output, extra_functions
            )
        except DynamicArrayMetadataError as error:
            raise StepProcessorError(
                f"Declare dynamic formulas step '{self.step_name}': {error}"
            )

        return (
            f"declared {report['cells_marked']} formula cell(s) dynamic, "
            f"completed {report['cells_completed']} legacy array cell(s), "
            f"skipped {report['cells_already_declared']} already declared "
            f"in {Path(resolved_output).name}"
        )

    def _resolve_path(self, filename: str) -> str:
        """Apply recipe variable substitution to a configured path."""
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(filename)
        return filename

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Declare dynamic-era formulas in a finished xlsx so Excel '
                           'shows them without the implicit-intersection @',
            'when_to_use': 'repairing a file already on disk (an inherited donor, a '
                           'flushed output); for recipe outputs prefer the settings '
                           'key declare_dynamic_formulas: true',
            'safety': 'only formulas containing post-dynamic-array functions are '
                      'marked by default, so legacy implicit-intersection semantics '
                      'cannot be changed; extend with extra_functions only for files '
                      'whose formulas are known recipe-authored',
        }

# End of file #
