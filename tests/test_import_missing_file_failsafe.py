"""
import_file on_missing_file fail-safe: empty stage with declared columns.

tests/test_import_missing_file_failsafe.py

The 2026-08-17 fail-safe lets a recipe import a file that legitimately may
not exist yet - a lookup produced by a sibling recipe that has not run, or
a first-run diff baseline - without dying. 'error' keeps the historical
loud failure as the default; 'create_empty' stands up an empty stage
carrying the DECLARED columns so downstream keyed steps stay valid.
Declaring the columns is mandatory for 'create_empty' and forbidden
otherwise, both enforced with guided errors.

Runnable with pytest, but written to run standalone and report a score.
"""

import sys
import tempfile

import pandas as pd

from pathlib import Path

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.import_file_processor import ImportFileProcessor


def reset_stages():
    """Clear stage state between tests."""
    StageManager.initialize_stages(max_stages=10)


def declare(stage_name):
    """Declare a stage so the import may save to it."""
    StageManager.declare_recipe_stages({
        'settings': {'stages': [
            {'stage_name': stage_name, 'description': 'test stage'}
        ]}
    })


def test_missing_file_creates_empty_stage():
    """Absent file with create_empty yields an empty frame, declared columns."""
    print("Testing create_empty on an absent file...")

    reset_stages()
    declare('stg_absent')
    processor = ImportFileProcessor({
        'processor_type': 'import_file',
        'input_file': '/nonexistent/never_written.xlsx',
        'on_missing_file': 'create_empty',
        'create_empty_columns': ['Order ID', 'Customer', 'Notes'],
        'save_to_stage': 'stg_absent',
    })
    processor.execute()
    frame = StageManager.load_stage('stg_absent')

    passed = True
    if list(frame.columns) == ['Order ID', 'Customer', 'Notes']:
        print(f"  ✓ declared columns present: {list(frame.columns)}")
    else:
        print(f"  ✗ columns wrong: {list(frame.columns)}")
        passed = False
    if len(frame) == 0:
        print("  ✓ stage is empty")
    else:
        print(f"  ✗ expected 0 rows, got {len(frame)}")
        passed = False
    return passed


def test_present_file_imports_normally():
    """create_empty must not disturb the normal path when the file exists."""
    print("\nTesting create_empty with the file present...")

    reset_stages()
    declare('stg_present')
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / 'real.xlsx'
        pd.DataFrame({'Order ID': ['A1'], 'Customer': ['Acme']}).to_excel(
            path, index=False)

        processor = ImportFileProcessor({
            'processor_type': 'import_file',
            'input_file': str(path),
            'on_missing_file': 'create_empty',
            'create_empty_columns': ['Order ID', 'Customer'],
            'save_to_stage': 'stg_present',
        })
        processor.execute()
        frame = StageManager.load_stage('stg_present')

    if len(frame) == 1 and frame.iloc[0]['Customer'] == 'Acme':
        print("  ✓ real file imported normally")
        return True
    print(f"  ✗ unexpected content: {frame}")
    return False


def test_default_policy_still_fails_loud():
    """Without the opt-in, an absent file still raises like it always has."""
    print("\nTesting default policy on an absent file...")

    reset_stages()
    declare('stg_loud')
    processor = ImportFileProcessor({
        'processor_type': 'import_file',
        'input_file': '/nonexistent/never_written.xlsx',
        'save_to_stage': 'stg_loud',
    })
    try:
        processor.execute()
    except Exception:
        print("  ✓ absent file raised under the default 'error' policy")
        return True
    print("  ✗ absent file was silently tolerated")
    return False


def test_config_guardrails():
    """create_empty without columns, and columns without create_empty, both refuse."""
    print("\nTesting configuration guardrails...")

    passed = True

    reset_stages()
    declare('stg_guard')
    try:
        ImportFileProcessor({
            'processor_type': 'import_file',
            'input_file': '/nonexistent/x.xlsx',
            'on_missing_file': 'create_empty',
            'save_to_stage': 'stg_guard',
        }).execute()
        print("  ✗ create_empty without create_empty_columns was accepted")
        passed = False
    except StepProcessorError as error:
        if 'create_empty_columns' in str(error):
            print("  ✓ missing create_empty_columns refused with guidance")
        else:
            print(f"  ✗ guidance lacks the key name: {error}")
            passed = False

    reset_stages()
    declare('stg_guard')
    try:
        ImportFileProcessor({
            'processor_type': 'import_file',
            'input_file': '/nonexistent/x.xlsx',
            'create_empty_columns': ['Order ID'],
            'save_to_stage': 'stg_guard',
        }).execute()
        print("  ✗ stray create_empty_columns under 'error' policy accepted")
        passed = False
    except StepProcessorError as error:
        if 'on_missing_file' in str(error) or "unknown key 'create_empty_columns'" in str(error):
            print("  ✓ stray create_empty_columns refused with guidance")
        else:
            print(f"  ✗ guidance lacks the policy key: {error}")
            passed = False

    return passed


def main():
    """Run every test and report a final score."""
    print("=== import_file on_missing_file fail-safe tests ===")

    tests = [
        test_missing_file_creates_empty_stage,
        test_present_file_imports_normally,
        test_default_policy_still_fails_loud,
        test_config_guardrails,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"✗ {test_func.__name__} crashed: {error}")

    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
