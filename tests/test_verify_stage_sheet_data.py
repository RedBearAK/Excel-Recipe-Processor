"""
Tests for the verify_data processor and its run-end ledger.

tests/test_verify_stage_sheet_data.py

Runnable with pytest, but written to run standalone and report a score.
"""

import pandas as pd
import numpy as np

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.verification_ledger import VerificationLedger
from excel_recipe_processor.processors.verify_sheet_data_processor import VerifySheetDataProcessor
from excel_recipe_processor.processors.verify_stage_data_processor import VerifyStageDataProcessor


def stage_main_data():
    """Four rows with deliberate defects, plus a small carrier lookup stage."""
    StageManager.initialize_stages(max_stages=10)
    StageManager.save_stage('stg_main', pd.DataFrame({
        'SHIP REF': ['S1', '', 'S3', np.nan],
        'Booking':  ['B1', 'B2', 'B1', 'B4'],
        'Carrier':  ['CMA', 'Matson', 'CMA', 'Weird'],
    }), description='test')
    StageManager.save_stage('stg_known_carriers',
                            pd.DataFrame({'Carrier': ['CMA', 'Matson']}),
                            description='test')


def run_rules(rules, source=None):
    config = {'step_description': 'verify test', 'rules': rules}
    config.update(source or {'source_stage': 'stg_main'})
    if 'target_file' in config:
        config['processor_type'] = 'verify_sheet_data'
        return VerifySheetDataProcessor(config).perform_file_operation()
    config['processor_type'] = 'verify_stage_data'
    processor = VerifyStageDataProcessor(config)
    processor.execute_stage_to_stage()
    return processor.check_summary


def test_warn_default_counts_and_samples():
    """A failing rule warns with the count and offending sample, run continues."""
    print("\nTesting warn default with counts and samples...")

    stage_main_data()
    VerificationLedger.reset()

    message = run_rules([
        {'column': 'SHIP REF', 'condition': 'not_empty'},   # 2 violations
        {'column': 'Booking', 'condition': 'not_empty'},    # passes
    ])

    if '1 passed, 1 warned' in message:
        print("  ✓ Step completed with the pass/warn tally in its result")
        return True

    print(f"  ✗ Result message: {message}")
    return False


def test_halt_severity_raises_naming_the_rule():
    """severity: halt raises with rule description, count, and sample."""
    print("\nTesting halt severity...")

    stage_main_data()
    VerificationLedger.reset()

    try:
        run_rules([{'column': 'SHIP REF', 'condition': 'not_empty',
                    'severity': 'halt',
                    'description': 'SHIP REF must be fully resolved'}])
        print("  ✗ Halt rule did not raise")
        return False
    except StepProcessorError as error:
        message = str(error)
        if ('SHIP REF must be fully resolved' in message
                and '2 of 4' in message and '<blank>' in message):
            print("  ✓ Raised, naming the rule, the count, and blank samples")
            return True
        print(f"  ✗ Raised, but unhelpfully: {message[:140]}")
        return False


def test_in_stage_referential_check():
    """The CMA-style check: values must exist in a lookup stage."""
    print("\nTesting in_stage referential verification...")

    stage_main_data()
    VerificationLedger.reset()

    records = []
    import logging
    handler = logging.Handler()
    handler.emit = lambda record: records.append(record.getMessage())
    vd_logger = logging.getLogger('excel_recipe_processor.processors._helpers.verify_data_rules')
    vd_logger.addHandler(handler)
    try:
        run_rules([{'column': 'Carrier', 'condition': 'in_stage',
                    'stage_name': 'stg_known_carriers', 'stage_column': 'Carrier'}])
    finally:
        vd_logger.removeHandler(handler)

    hits = [m for m in records if "'Weird'" in m and '1 of 4' in m]
    if hits:
        print("  ✓ Unknown carrier caught by the lookup-stage check, named in the warning")
        return True

    print(f"  ✗ Warnings: {records}")
    return False


def test_ledger_accumulates_across_steps():
    """The run-end summary reflects every rule from every step."""
    print("\nTesting the verification ledger...")

    stage_main_data()
    VerificationLedger.reset()

    run_rules([{'column': 'Booking', 'condition': 'not_empty'}])          # pass
    run_rules([{'column': 'SHIP REF', 'condition': 'not_empty'}])         # warn
    try:
        run_rules([{'column': 'SHIP REF', 'condition': 'not_empty',
                    'severity': 'halt'}])
    except StepProcessorError:
        pass                                                              # halt

    counts = (VerificationLedger._passed, VerificationLedger._warned,
              VerificationLedger._halted)
    if counts == (1, 1, 1):
        print("  ✓ Ledger holds 1 passed, 1 warned, 1 halted")
        return True

    print(f"  ✗ Ledger counts: {counts}")
    return False


def test_file_mode_and_validation():
    """File mode reads a sheet; config errors are loud and specific."""
    print("\nTesting file mode and validation failures...")

    passed = True

    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as work_dir:
        file_path = str(Path(work_dir) / 'check.xlsx')
        # The second column anchors the row: a single-column sheet whose
        # last cell is blank reads back one row short (pandas drops the
        # trailing all-empty row), and the "violation" would never exist.
        pd.DataFrame({'K': ['a', ''], 'V': [1, 2]}).to_excel(
            file_path, index=False, sheet_name='Data')

        from excel_recipe_processor.core.workbook_session import WorkbookSession
        WorkbookSession.reset()
        VerificationLedger.reset()

        message = run_rules(
            [{'column': 'K', 'condition': 'not_empty'}],
            source={'target_file': file_path, 'sheet_name': 'Data'},
        )
        if '1 warned' in message:
            print("  ✓ File mode found the blank cell")
        else:
            print(f"  ✗ File mode result: {message}")
            passed = False

    cases = [
        ('bad severity', {'source_stage': 's'}, 'severity must be'),
        ('legacy bare stage key', {'stage': 's'}, "'source_stage'"),
        ('stage key inside a stage rule',
         {'source_stage': 's', '_rule_extra': {'condition': 'in_stage', 'stage': 'lk'}},
         'stage_name'),
    ]
    for label, source, expected in cases:
        rule_extra = source.pop('_rule_extra', {})
        rules = [{'column': 'K', 'condition': 'not_empty',
                  **({'severity': 'sorta'} if label == 'bad severity' else {}),
                  **rule_extra}]
        try:
            VerifyStageDataProcessor({'processor_type': 'verify_stage_data',
                                 'step_description': 'v', 'rules': rules, **source})
            print(f"  ✗ {label}: accepted silently")
            passed = False
        except StepProcessorError as error:
            if expected.lower() in str(error).lower():
                print(f"  ✓ {label}: raised, message names the problem")
            else:
                print(f"  ✗ {label}: unhelpful message: {error}")
                passed = False

    return passed


def main():
    tests = [
        test_warn_default_counts_and_samples,
        test_halt_severity_raises_naming_the_rule,
        test_in_stage_referential_check,
        test_ledger_accumulates_across_steps,
        test_file_mode_and_validation,
    ]

    passed = 0

    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"  ✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")

    if passed == len(tests):
        print("✅ All verify_data tests passed!")
        return 1

    print("❌ Some verify_data tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
