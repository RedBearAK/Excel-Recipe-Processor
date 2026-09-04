"""
Shared engine for the two value-verification processors.

excel_recipe_processor/processors/_helpers/verify_data_rules.py

verify_stage_data (a Transform CHECK: values as the pipeline sees them)
and verify_sheet_data (FileOps: values written to a sheet) both run the
same rule vocabulary - filter_data's condition set, borrowed live so the
three can never drift - with per-rule warn / halt severity recorded in
the run-end verification ledger. Split from the single verify_data
processor on 2026-09-03 so each mode sits in its family; this module is
the one place the rule semantics live.
"""

import logging

import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.workbook_session import WorkbookSession
from excel_recipe_processor.core.verification_ledger import VerificationLedger
from excel_recipe_processor.processors._helpers.sheet_addressing import resolve_sheet_ref


logger = logging.getLogger(__name__)

VALID_SEVERITIES = ('warn', 'halt')


def check_rules_config(rules, step_name: str) -> None:
    """A non-empty rules list with sane severities; the schema covers the rest."""
    if not isinstance(rules, list) or not rules:
        raise StepProcessorError(f"Step '{step_name}' requires a non-empty 'rules' list")
    for rule_index, rule in enumerate(rules, start=1):
        if not isinstance(rule, dict):
            raise StepProcessorError(f"Rule {rule_index} in step '{step_name}' must be a mapping")
        for required in ('column', 'condition'):
            if not rule.get(required):
                raise StepProcessorError(f"Rule {rule_index} in step '{step_name}' needs '{required}'")
        if 'stage' in str(rule.get('condition', '')):
            wrong_keys = [key for key in ('stage', 'source_stage') if key in rule]
            if wrong_keys and 'stage_name' not in rule:
                raise StepProcessorError(
                    f"Rule {rule_index} in step '{step_name}': stage conditions reference "
                    f"their lookup stage with 'stage_name' (filter_data's rule grammar), "
                    f"not {wrong_keys}"
                )
        severity = rule.get('severity', 'warn')
        if severity not in VALID_SEVERITIES:
            raise StepProcessorError(
                f"Rule {rule_index} in step '{step_name}': severity must be one of "
                f"{list(VALID_SEVERITIES)}, got '{severity}'"
            )


def describe_rule(rule) -> str:
    """The human line a pass/warn/halt message leads with."""
    described = rule.get('description')
    if described:
        return str(described)
    value = rule.get('value')
    value_part = '' if value is None else f" {value!r}"
    return f"'{rule['column']}' {rule['condition']}{value_part}"


def sample_violations(frame, violation_index, rule) -> str:
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


def run_rules(frame: pd.DataFrame, rules: list, source_label: str, step_name: str) -> str:
    """Run every rule against the frame; warn or halt per rule; return the summary."""
    from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor
    # A shim instance lends us filter_data's condition evaluation - the
    # whole condition vocabulary, guaranteed in sync forever because it IS
    # filter_data's implementation. _apply_filter returns the rows
    # SATISFYING a condition with their original index intact, so the
    # violations are exactly the index difference.
    condition_engine = FilterDataProcessor(
        FilterDataProcessor.get_minimal_config()
        | {'processor_type': 'filter_data', 'step_description': 'verify condition shim'}
    )
    passed_count = 0
    warned_count = 0
    for rule_index, rule in enumerate(rules, start=1):
        engine_rule = {key: value for key, value in rule.items()
                       if key not in ('severity', 'description')}
        try:
            satisfied = condition_engine._apply_filter(frame, engine_rule, rule_index - 1)
        except StepProcessorError as error:
            raise StepProcessorError(f"Step '{step_name}', rule {rule_index}: {error}")
        violation_index = frame.index.difference(satisfied.index)
        expectation = describe_rule(rule)
        if len(violation_index) == 0:
            VerificationLedger.record_pass()
            passed_count += 1
            logger.debug(f"\u2713 {expectation}: all {len(frame)} row(s) satisfy it")
            continue
        sample = sample_violations(frame, violation_index, rule)
        severity = rule.get('severity', 'warn')
        if severity == 'halt':
            VerificationLedger.record_halt()
            raise StepProcessorError(
                f"Step '{step_name}': {expectation} FAILED - {len(violation_index)} of "
                f"{len(frame)} row(s) violate it ({source_label}). Sample: {sample}"
            )
        VerificationLedger.record_warn()
        warned_count += 1
        logger.warning(
            f"\u26a0\ufe0f {expectation}: {len(violation_index)} of {len(frame)} "
            f"row(s) violate it ({source_label}). Sample: {sample}"
        )
    return (f"verified {len(rules)} rule(s) on {source_label}: "
            f"{passed_count} passed, {warned_count} warned")


def load_sheet_frame(target_file: str, sheet_name, step_name: str) -> tuple:
    """The frame of a workbook sheet - live from the session if held, else from disk."""
    label = f"file '{Path(target_file).name}' sheet '{sheet_name}'"
    if WorkbookSession.is_open(target_file):
        # The disk copy is stale while the session holds the workbook;
        # verify what WILL be written, straight from the live object.
        workbook = WorkbookSession.get_workbook(target_file)
        try:
            sheet_name = resolve_sheet_ref(sheet_name, workbook.sheetnames, f"Step '{step_name}'")
        except ValueError as error:
            raise StepProcessorError(str(error))
        worksheet = workbook[sheet_name]
        row_iter = worksheet.iter_rows(values_only=True)
        try:
            headers = [str(cell) if cell is not None else '' for cell in next(row_iter)]
        except StopIteration:
            raise StepProcessorError(f"Step '{step_name}': sheet '{sheet_name}' is empty")
        frame = pd.DataFrame(list(row_iter), columns=headers)
        return frame, label + " (session, pre-save)"
    file_path = Path(target_file)
    if not file_path.is_file():
        raise StepProcessorError(f"Step '{step_name}': file not found: {target_file}")
    try:
        with pd.ExcelFile(target_file) as workbook_file:
            sheet_name = resolve_sheet_ref(sheet_name, workbook_file.sheet_names, f"Step '{step_name}'")
        frame = pd.read_excel(target_file, sheet_name=sheet_name)
    except Exception as error:
        raise StepProcessorError(
            f"Step '{step_name}': could not read {target_file} [{sheet_name}]: {error}")
    return frame, label


# End of file #
