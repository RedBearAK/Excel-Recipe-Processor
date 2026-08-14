"""
Value-level data verification with per-rule warn/halt severity.

excel_recipe_processor/processors/verify_data_processor.py

The sibling of verify_columns, one level down: that processor checks a
stage's SHAPE; this one checks its VALUES. Each rule states an expectation
every row must satisfy - "SHIP REF not_empty", "Booking unique", "Carrier
in_list [...]" - in the same condition vocabulary filter_data speaks
(borrowed live from filter_data itself, so the two can never drift).
Violations either WARN (default: log the count and a sample, keep going)
or HALT per rule, and every outcome lands in the run-end verification
summary line.

Verifies a STAGE (mid-pipeline, values as the pipeline sees them) or a
FILE's sheet. The file mode carries one loud caveat: formula cells in a
file this framework wrote have no cached values - openpyxl computes
nothing - so a rule aimed at an injected formula column sees blanks, not
results. Verify formula INPUTS in stages; verify written VALUE columns in
files.
"""

import logging
import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager, StageError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.verification_ledger import VerificationLedger
from excel_recipe_processor.core.base_processor import FileOpsBaseProcessor, StepProcessorError
from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor
from excel_recipe_processor.processors._helpers.sheet_addressing import resolve_sheet_ref


logger = logging.getLogger(__name__)

VALID_SEVERITIES = ('warn', 'halt')


class VerifyDataProcessor(FileOpsBaseProcessor):
    """Check row values against expectations; warn or halt per rule."""

    @classmethod
    def get_minimal_config(cls) -> dict:
        """Smallest configuration that constructs and validates."""
        return {
            'source_stage': 'stg_to_verify',
            'rules': [
                {'column': 'Key', 'condition': 'not_empty'},
            ],
        }

    def _validate_file_operation_config(self):
        """Exactly one source; a non-empty rules list; sane severities."""
        source_stage = self.get_config_value('source_stage', None)
        target_file = self.get_config_value('target_file', None)

        if self.get_config_value('stage', None):
            # The bare 'stage' key belongs to no family: data-flow steps say
            # source_stage, rule-level references say stage_name. Refusing
            # here (this processor is new, nothing deployed uses 'stage')
            # keeps one step from speaking two dialects.
            raise StepProcessorError(
                f"Verify data step '{self.step_name}': use 'source_stage' "
                f"(the stage this step reads), not 'stage'"
            )

        if bool(source_stage) == bool(target_file):
            raise StepProcessorError(
                f"Verify data step '{self.step_name}' needs exactly one source: "
                f"'source_stage' or 'target_file' (with 'sheet')"
            )
        if self.get_config_value('sheet', None):
            raise StepProcessorError(
                f"Verify data step '{self.step_name}': 'sheet' was replaced by "
                f"'sheet_name' (2026-08-14 sheet-addressing doctrine)"
            )
        if target_file and not self.get_config_value('sheet_name', None):
            raise StepProcessorError(
                f"Verify data step '{self.step_name}': file mode needs 'sheet_name'"
            )

        rules = self.get_config_value('rules', [])
        if not isinstance(rules, list) or not rules:
            raise StepProcessorError(
                f"Verify data step '{self.step_name}' requires a non-empty 'rules' list"
            )

        for rule_index, rule in enumerate(rules, start=1):
            if not isinstance(rule, dict):
                raise StepProcessorError(
                    f"Rule {rule_index} in step '{self.step_name}' must be a mapping"
                )
            for required in ('column', 'condition'):
                if not rule.get(required):
                    raise StepProcessorError(
                        f"Rule {rule_index} in step '{self.step_name}' needs '{required}'"
                    )
            if 'stage' in str(rule.get('condition', '')):
                wrong_keys = [key for key in ('stage', 'source_stage') if key in rule]
                if wrong_keys and 'stage_name' not in rule:
                    raise StepProcessorError(
                        f"Rule {rule_index} in step '{self.step_name}': stage "
                        f"conditions reference their lookup stage with "
                        f"'stage_name' (filter_data's rule grammar), not "
                        f"{wrong_keys} - see the in_stage example"
                    )
            severity = rule.get('severity', 'warn')
            if severity not in VALID_SEVERITIES:
                raise StepProcessorError(
                    f"Rule {rule_index} in step '{self.step_name}': severity must "
                    f"be one of {list(VALID_SEVERITIES)}, got '{severity}'"
                )

    def perform_file_operation(self):
        """Load the frame from its source, run every rule, record outcomes."""
        frame, source_label = self._load_frame()
        rules = self.get_config_value('rules')

        # A shim instance lends us filter_data's condition evaluation - the
        # whole 30+ condition vocabulary, guaranteed in sync forever because
        # it IS filter_data's implementation. _apply_filter returns the rows
        # SATISFYING a condition with their original index intact, so the
        # violations are exactly the index difference.
        condition_engine = FilterDataProcessor(
            FilterDataProcessor.get_minimal_config()
            | {'processor_type': 'filter_data',
               'step_description': 'verify_data condition shim'}
        )

        passed_count = 0
        warned_count = 0

        for rule_index, rule in enumerate(rules, start=1):
            engine_rule = {key: value for key, value in rule.items()
                           if key not in ('severity', 'description')}
            try:
                satisfied = condition_engine._apply_filter(frame, engine_rule, rule_index - 1)
            except StepProcessorError as error:
                raise StepProcessorError(
                    f"Verify data step '{self.step_name}', rule {rule_index}: {error}"
                )

            violation_index = frame.index.difference(satisfied.index)
            expectation = self._describe_rule(rule)

            if len(violation_index) == 0:
                VerificationLedger.record_pass()
                passed_count += 1
                logger.debug(f"✓ {expectation}: all {len(frame)} row(s) satisfy it")
                continue

            sample = self._sample_violations(frame, violation_index, rule)
            severity = rule.get('severity', 'warn')

            if severity == 'halt':
                VerificationLedger.record_halt()
                raise StepProcessorError(
                    f"Verify data step '{self.step_name}': {expectation} FAILED - "
                    f"{len(violation_index)} of {len(frame)} row(s) violate it "
                    f"({source_label}). Sample: {sample}"
                )

            VerificationLedger.record_warn()
            warned_count += 1
            logger.warning(
                f"⚠️ {expectation}: {len(violation_index)} of {len(frame)} "
                f"row(s) violate it ({source_label}). Sample: {sample}"
            )

        return (
            f"verified {len(rules)} rule(s) on {source_label}: "
            f"{passed_count} passed, {warned_count} warned"
        )

    def _load_frame(self) -> tuple:
        """The frame to verify, plus a label naming where it came from."""
        source_stage = self.get_config_value('source_stage', None)

        if source_stage:
            try:
                return (StageManager.load_stage(source_stage),
                        f"stage '{source_stage}'")
            except StageError as error:
                raise StepProcessorError(
                    f"Verify data step '{self.step_name}': error loading "
                    f"stage '{source_stage}': {error}"
                )

        target_file = self._resolve_path(self.get_config_value('target_file'))
        sheet_name = self.get_config_value('sheet_name')
        label = f"file '{Path(target_file).name}' sheet '{sheet_name}'"

        if WorkbookSession.is_open(target_file):
            # The disk copy is stale while the session holds the workbook;
            # verify what WILL be written, straight from the live object.
            workbook = WorkbookSession.get_workbook(target_file)
            try:
                sheet_name = resolve_sheet_ref(
                    sheet_name, workbook.sheetnames,
                    f"Verify data step '{self.step_name}'"
                )
            except ValueError as error:
                raise StepProcessorError(str(error))
            worksheet = workbook[sheet_name]
            row_iter = worksheet.iter_rows(values_only=True)
            try:
                headers = [str(cell) if cell is not None else '' for cell in next(row_iter)]
            except StopIteration:
                raise StepProcessorError(
                    f"Verify data step '{self.step_name}': sheet '{sheet_name}' is empty"
                )
            frame = pd.DataFrame(list(row_iter), columns=headers)
            return frame, label + " (session, pre-save)"

        file_path = Path(target_file)
        if not file_path.is_file():
            raise StepProcessorError(
                f"Verify data step '{self.step_name}': file not found: {target_file}"
            )
        try:
            with pd.ExcelFile(target_file) as workbook_file:
                sheet_name = resolve_sheet_ref(
                    sheet_name, workbook_file.sheet_names,
                    f"Verify data step '{self.step_name}'"
                )
            frame = pd.read_excel(target_file, sheet_name=sheet_name)
        except Exception as error:
            raise StepProcessorError(
                f"Verify data step '{self.step_name}': could not read "
                f"{target_file} [{sheet_name}]: {error}"
            )
        return frame, label

    @staticmethod
    def _describe_rule(rule) -> str:
        """The human line a pass/warn/halt message leads with."""
        described = rule.get('description')
        if described:
            return str(described)
        value = rule.get('value')
        value_part = '' if value is None else f" {value!r}"
        return f"'{rule['column']}' {rule['condition']}{value_part}"

    @staticmethod
    def _sample_violations(frame, violation_index, rule) -> str:
        """A few offending values (and row numbers) for the message."""
        column = rule['column']
        head = violation_index[:5]
        pairs = []
        for index_value in head:
            cell = frame.at[index_value, column]
            shown = '<blank>' if (pd.isna(cell) or str(cell).strip() == '') else repr(cell)
            pairs.append(f"row {index_value + 2}: {shown}")
        suffix = ', ...' if len(violation_index) > 5 else ''
        return '[' + '; '.join(pairs) + suffix + ']'

    def _resolve_path(self, filename: str) -> str:
        """Apply recipe variable substitution to a configured path."""
        if hasattr(self, 'variable_substitution') and self.variable_substitution:
            return self.variable_substitution.substitute(filename)
        return filename

    def get_usage_examples(self) -> dict:
        """Get usage examples from the external YAML file."""
        from excel_recipe_processor.utils.processor_examples_loader import load_processor_examples
        return load_processor_examples('verify_data')

    def get_capabilities(self) -> dict:
        """
        Get processor capabilities information.

        Returns:
            Dictionary with processor capabilities
        """
        return {
            'description': 'Check row values against expectations - warn (default) '
                           'or halt per rule',
            'vocabulary': 'the full filter_data condition set, borrowed live from '
                          'filter_data itself so the two can never drift: '
                          'not_empty, equals, in_list, in_stage (referential '
                          'checks against a lookup stage), and the rest',
            'severity': 'per rule: warn (default) logs count + sample and '
                        'continues; halt raises naming the rule and sample',
            'sources': 'source_stage (mid-pipeline values) or a file sheet; a '
                       'session-held file is read live, pre-save',
            'formula_caveat': 'formula cells in files this framework wrote have '
                              'no cached values, so file-mode rules on injected '
                              'formula columns see blanks - verify formula INPUTS '
                              'in stages instead',
            'run_summary': 'every rule outcome lands in the run-end '
                           'verification summary line',
        }

# End of file #
