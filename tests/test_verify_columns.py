"""
Tests for the verify_columns processor.

tests/test_verify_columns.py

Runnable with pytest, but written to run standalone and report a score.
"""

import pandas as pd

import excel_recipe_processor.core.pipeline  # registers processors

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.verify_columns_processor import VerifyColumnsProcessor


def stage_with(columns):
    StageManager.initialize_stages(max_stages=5)
    StageManager.declare_recipe_stages({'settings': {'stages': [
        {'stage_name': 'stg_check', 'description': 'x', 'protected': False}]}})
    StageManager.save_stage('stg_check', pd.DataFrame(columns=columns), overwrite=True)


def build(expected, **overrides):
    config = {'processor_type': 'verify_columns', 'stage': 'stg_check',
              'expected_columns': expected}
    config.update(overrides)
    return VerifyColumnsProcessor(config)


def test_exact_match_verifies():
    """All expected present, nothing extra."""
    print("\nTesting an exact match...")

    stage_with(['A', 'B', 'C'])
    result = build(['A', 'B', 'C']).perform_file_operation()

    if 'verified' in result:
        print(f"  ✓ {result!r}")
        return True
    print(f"  ✗ {result!r}")
    return False


def test_reorder_is_not_a_failure():
    """Order is deliberately ignored."""
    print("\nTesting reordered columns pass...")

    stage_with(['C', 'A', 'B'])
    result = build(['A', 'B', 'C']).perform_file_operation()

    if 'verified' in result:
        print("  ✓ Reorder verified clean")
        return True
    print(f"  ✗ {result!r}")
    return False


def test_missing_expected_halts_naming_it():
    """Default on_missing_expected is error, and the message names the column."""
    print("\nTesting a missing expected column...")

    stage_with(['A', 'B'])

    try:
        build(['A', 'B', 'C']).perform_file_operation()
        print("  ✗ Missing column accepted")
        return False
    except Exception as error:
        if "MISSING" in str(error) and "'C'" in str(error):
            print("  ✓ Halted, naming C")
            return True
        print(f"  ✗ Wrong error: {error}")
        return False


def test_new_column_warns_and_proceeds():
    """Default on_unexpected is warn; the result reports the drift."""
    print("\nTesting a new unexpected column...")

    stage_with(['A', 'B', 'Surprise'])
    result = build(['A', 'B']).perform_file_operation()

    if '1 new' in result:
        print(f"  ✓ Proceeded with drift report: {result!r}")
        return True
    print(f"  ✗ {result!r}")
    return False


def test_knobs_invert_the_defaults():
    """error/warn are swappable per direction."""
    print("\nTesting inverted knobs...")

    passed = True

    stage_with(['A', 'B', 'Surprise'])
    try:
        build(['A', 'B'], on_unexpected='error').perform_file_operation()
        print("  ✗ on_unexpected: error did not halt")
        passed = False
    except Exception as error:
        if 'Surprise' in str(error):
            print("  ✓ on_unexpected: error halts, naming the column")
        else:
            print(f"  ✗ Wrong error: {error}")
            passed = False

    stage_with(['A'])
    result = build(['A', 'Ghost'], on_missing_expected='warn').perform_file_operation()
    if '1 missing' in result:
        print("  ✓ on_missing_expected: warn proceeds with the drift report")
    else:
        print(f"  ✗ {result!r}")
        passed = False

    return passed


def test_expected_from_stage_compares_two_stages():
    """The expectation can be another stage's columns; messages name both."""
    print("\nTesting expected_from_stage...")

    StageManager.initialize_stages(max_stages=5)
    StageManager.declare_recipe_stages({'settings': {'stages': [
        {'stage_name': 'stg_check', 'description': 'x', 'protected': False},
        {'stage_name': 'stg_other', 'description': 'x', 'protected': False}]}})
    StageManager.save_stage('stg_check', pd.DataFrame(columns=['X', 'OnlyA']), overwrite=True)
    StageManager.save_stage('stg_other', pd.DataFrame(columns=['X', 'OnlyB']), overwrite=True)

    result = build(None, expected_columns=None, expected_from_stage='stg_other',
                   on_missing_expected='warn', on_unexpected='warn').perform_file_operation()

    if '1 missing' in result and '1 new' in result:
        print(f"  ✓ Both directions reported: {result!r}")
        return True
    print(f"  ✗ {result!r}")
    return False


def test_exactly_one_expectation_source():
    """Neither or both expectation sources fail at construction."""
    print("\nTesting expectation-source exclusivity...")

    passed = True

    try:
        VerifyColumnsProcessor({'processor_type': 'verify_columns', 'stage': 'stg_check'})
        print("  ✗ Neither source accepted")
        passed = False
    except Exception:
        print("  ✓ Neither source rejected")

    try:
        VerifyColumnsProcessor({'processor_type': 'verify_columns', 'stage': 'stg_check',
                                'expected_columns': ['A'], 'expected_from_stage': 'stg_other'})
        print("  ✗ Both sources accepted")
        passed = False
    except Exception:
        print("  ✓ Both sources rejected")

    return passed


def main():
    """Run every test and report a final score."""
    print("=== verify_columns tests ===")

    tests = [
        test_exact_match_verifies,
        test_reorder_is_not_a_failure,
        test_missing_expected_halts_naming_it,
        test_new_column_warns_and_proceeds,
        test_knobs_invert_the_defaults,
        test_expected_from_stage_compares_two_stages,
        test_exactly_one_expectation_source,
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
        print("✅ All verify_columns tests passed!")
        return 1

    print("❌ Some verify_columns tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
