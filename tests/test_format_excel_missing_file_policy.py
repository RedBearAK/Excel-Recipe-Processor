"""
format_excel missing_file_policy: absence is tolerated, nothing else is.

tests/test_format_excel_missing_file_policy.py

Born 2026-11-02 with the presence-is-the-signal side reports of the VMS
recipe (future dates, missing tracking, van seq corrections): export_file
writes them under empty_stage_policy: skip, so on a clean run the file
does not exist - and a format_excel step after the export would halt on
"Target file not found". missing_file_policy: skip turns that one case
into a quiet success. It is the format-side mirror of the export policy
and deliberately NOT on_error: continue - a bad config on a file that
DOES exist must still halt, and this drill asserts both halves.

Also here, from the same session: an apply_templates name the step does
not declare was a WARNING (an unstyled sheet, the only trace in the log)
and --validate passed it. It is now refused at construction AND reported
by the validation phase through the config_errors() hook, so the typo
fails before any run.

Run: PYTHONPATH=. python3 tests/test_format_excel_missing_file_policy.py
"""

import os
import sys
import tempfile

import openpyxl
import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.format_excel_processor import FormatExcelProcessor


def make_workbook(path: str) -> None:
    frame = pd.DataFrame({'Booking': ['28179', '28147'], 'Carrier': ['Matson', 'Matson']})
    frame.to_excel(path, sheet_name='Report', index=False)


def step(target: str, policy=None, template_names=None, zoom=None) -> dict:
    entry = {'sheet_names': ['?sheet_001?'], 'header_bold': True, 'auto_fit_columns': True}
    if template_names:
        entry['apply_templates'] = template_names
    if zoom is not None:
        entry['zoom_percent'] = zoom
    config = {
        'step_description': 'drill format',
        'processor_type': 'format_excel',
        'target_file': target,
        'templates': [{'template_name': 'tpl_drill', 'header_bold': True}],
        'formatting': [entry],
    }
    if policy is not None:
        config['missing_file_policy'] = policy
    return config


def test_default_policy_halts_on_a_missing_file() -> bool:
    print('\nDefault policy: a missing target file halts the step...')
    missing = os.path.join(tempfile.mkdtemp(), 'absent.xlsx')
    try:
        FormatExcelProcessor(step(missing)).execute()
    except StepProcessorError as error:
        if 'not found' in str(error):
            print(f'  ✓ halted: {error}')
            return True
        print(f'  ✗ halted for the wrong reason: {error}')
        return False
    print('  ✗ did not halt')
    return False


def test_skip_policy_passes_a_missing_file_quietly() -> bool:
    print('\nmissing_file_policy: skip - a missing target file is a quiet success...')
    missing = os.path.join(tempfile.mkdtemp(), 'absent.xlsx')
    try:
        FormatExcelProcessor(step(missing, policy='skip')).execute()
    except StepProcessorError as error:
        print(f'  ✗ halted: {error}')
        return False
    if os.path.exists(missing):
        print('  ✗ a file appeared where none should')
        return False
    print('  ✓ step completed, no file created')
    return True


def test_skip_policy_still_formats_a_present_file() -> bool:
    print('\nmissing_file_policy: skip - a PRESENT file is formatted as usual...')
    path = os.path.join(tempfile.mkdtemp(), 'present.xlsx')
    make_workbook(path)
    try:
        FormatExcelProcessor(step(path, policy='skip', template_names=['tpl_drill'])).execute()
    except StepProcessorError as error:
        print(f'  ✗ halted: {error}')
        return False
    sheet = openpyxl.load_workbook(path)['Report']
    if not sheet['A1'].font.bold:
        print('  ✗ header was not bolded')
        return False
    print('  ✓ header bold applied through the template')
    return True


def test_skip_policy_does_not_swallow_other_failures() -> bool:
    print('\nmissing_file_policy: skip - a bad config on a present file still halts...')
    path = os.path.join(tempfile.mkdtemp(), 'present.xlsx')
    make_workbook(path)
    try:
        FormatExcelProcessor(step(path, policy='skip', zoom=999)).execute()
    except StepProcessorError as error:
        print(f'  ✓ halted: {str(error)[:90]}')
        return True
    print('  ✗ the bad config was swallowed - this is the on_error: continue failure mode')
    return False


def test_unknown_template_name_is_refused_at_construction() -> bool:
    # Until 2026-11-02 this was a warning: a typo in apply_templates ran the
    # step and produced an unstyled sheet with the only trace in the log.
    print('\nA template name not declared in the step is refused at construction...')
    path = os.path.join(tempfile.mkdtemp(), 'present.xlsx')
    make_workbook(path)
    try:
        FormatExcelProcessor(step(path, policy='skip', template_names=['tpl_drlil']))
    except StepProcessorError as error:
        if 'tpl_drlil' in str(error) and 'tpl_drill' in str(error):
            print(f'  ✓ refused, naming the typo and the declared names: {str(error)[:100]}')
            return True
        print(f'  ✗ refused without the guidance: {error}')
        return False
    print('  ✗ the unknown template was accepted')
    return False


def test_unknown_template_name_is_reported_by_the_validation_phase() -> bool:
    # The same rule through the --validate path: validate_recipe calls each
    # processor's config_errors() hook, so the typo fails BEFORE any run,
    # not minutes into one. No file is needed - nothing here touches disk.
    print('\n--validate (validate_recipe) reports the undeclared template as a step error...')
    from excel_recipe_processor.core.base_processor import registry
    from excel_recipe_processor.core.pipeline import register_standard_processors
    from excel_recipe_processor.core.recipe_validation import validate_recipe
    register_standard_processors()
    recipe = {'settings': {'stages': []},
              'recipe': [step('never_opened.xlsx', policy='skip', template_names=['tpl_drlil'])]}
    report = validate_recipe(recipe, registry, lambda config: config)
    hits = [message for message in report.errors if 'tpl_drlil' in message and 'tpl_drill' in message]
    if hits:
        print(f'  ✓ validation error: {hits[0][:110]}')
        return True
    print(f'  ✗ validation passed or reported something else: {report.errors}')
    return False


def test_invalid_policy_value_is_refused() -> bool:
    print('\nAn unknown missing_file_policy value is refused with a guided error...')
    missing = os.path.join(tempfile.mkdtemp(), 'absent.xlsx')
    try:
        FormatExcelProcessor(step(missing, policy='ignore')).execute()
    except StepProcessorError as error:
        if 'missing_file_policy' in str(error):
            print(f'  ✓ refused: {str(error)[:90]}')
            return True
        print(f'  ✗ refused for the wrong reason: {error}')
        return False
    print('  ✗ accepted an unknown policy value')
    return False


def main() -> bool:
    print('=== format_excel missing_file_policy ===')
    tests = [
        test_default_policy_halts_on_a_missing_file,
        test_skip_policy_passes_a_missing_file_quietly,
        test_skip_policy_still_formats_a_present_file,
        test_skip_policy_does_not_swallow_other_failures,
        test_unknown_template_name_is_refused_at_construction,
        test_unknown_template_name_is_reported_by_the_validation_phase,
        test_invalid_policy_value_is_refused,
    ]
    passed = sum(1 for test in tests if test())
    print(f'\n=== Results: {passed}/{len(tests)} tests passed ===')
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)


# End of file #
