"""
Tests for the slice_data transpose slice type.

tests/test_slice_data_transpose.py

Runnable with pytest, but written to run standalone and report a score.
"""

import pandas as pd
import numpy as np

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.slice_data_processor import SliceDataProcessor
from excel_recipe_processor.core.base_processor import StepProcessorError


def run_transpose(frame, **extra):
    """Stage the frame, run a transpose slice on it, return the result."""
    StageManager.initialize_stages(max_stages=5)
    StageManager.save_stage('stg_transpose_in', frame, description='test input')

    config = {
        'processor_type': 'slice_data',
        'step_description': 'transpose test',
        'source_stage': 'stg_transpose_in',
        'save_to_stage': 'stg_transpose_out',
        'slice_type': 'transpose',
    }
    config.update(extra)
    return SliceDataProcessor(config).execute(frame)


def test_headers_aware_transpose():
    """Label column becomes the header row; old headers become the first column."""
    print("\nTesting headers-aware transpose...")

    passed = True

    frame = pd.DataFrame({
        'Metric':  ['Revenue', 'Cost'],
        'Jan':     [100, 60],
        'Feb':     [110, 65],
        'Mar':     [120, 70],
    })

    result = run_transpose(frame, old_headers_column_name='Month')

    if list(result.columns) == ['Month', 'Revenue', 'Cost']:
        print("  ✓ New headers are the Metric values, after the Month column")
    else:
        print(f"  ✗ Columns came out as: {list(result.columns)}")
        passed = False

    if list(result['Month']) == ['Jan', 'Feb', 'Mar']:
        print("  ✓ Old headers preserved as the Month column, in order")
    else:
        print(f"  ✗ Month column: {list(result['Month'])}")
        passed = False

    if list(result['Revenue']) == [100, 110, 120] and list(result['Cost']) == [60, 65, 70]:
        print("  ✓ Values landed under their correct new headers")
    else:
        print(f"  ✗ Values misplaced: {result.to_dict('list')}")
        passed = False

    return passed


def test_numeric_labels_become_string_headers():
    """Non-string label values stringify, since column names must be strings."""
    print("\nTesting numeric labels become string headers...")

    frame = pd.DataFrame({'Year': [2024, 2025], 'Total': [10, 20]})
    result = run_transpose(frame)

    if list(result.columns) == ['Field', '2024', '2025']:
        print("  ✓ Integer years became '2024'/'2025' headers, default 'Field' first")
        return True

    print(f"  ✗ Columns came out as: {list(result.columns)}")
    return False


def test_corruption_cases_fail_loud():
    """Duplicate labels, blank labels, and name collisions all raise clearly."""
    print("\nTesting corruption cases fail loud...")

    passed = True

    cases = [
        ('duplicate labels',
         pd.DataFrame({'K': ['a', 'a'], 'V': [1, 2]}), {}, 'duplicate'),
        ('blank label',
         pd.DataFrame({'K': ['a', '  '], 'V': [1, 2]}), {}, 'blank'),
        ('NaN label',
         pd.DataFrame({'K': ['a', np.nan], 'V': [1, 2]}), {}, 'blank'),
        ('old-headers name collision',
         pd.DataFrame({'K': ['Field', 'b'], 'V': [1, 2]}), {}, 'collides'),
        ('single column',
         pd.DataFrame({'K': ['a']}), {}, 'at least'),
        ('missing header_column',
         pd.DataFrame({'K': ['a'], 'V': [1]}), {'header_column': 'Nope'}, 'not found'),
    ]

    for label, frame, extra, expected_word in cases:
        try:
            run_transpose(frame, **extra)
            print(f"  ✗ {label}: accepted silently")
            passed = False
        except StepProcessorError as error:
            if expected_word in str(error).lower():
                print(f"  ✓ {label}: raised, message names the problem")
            else:
                print(f"  ✗ {label}: raised, but message unhelpful: {error}")
                passed = False

    return passed


def test_double_transpose_round_trips():
    """Transposing twice returns the original table, values and layout."""
    print("\nTesting double transpose round-trips...")

    frame = pd.DataFrame({
        'Region': ['West', 'East'],
        'Q1':     [5, 7],
        'Q2':     [6, 8],
    })

    once = run_transpose(frame, old_headers_column_name='Quarter')
    twice = run_transpose(once, header_column='Quarter',
                          old_headers_column_name='Region')

    same_layout = list(twice.columns) == list(frame.columns)
    same_values = all(
        [str(v) for v in twice[c]] == [str(v) for v in frame[c]] for c in frame.columns
    ) if same_layout else False

    if same_layout and same_values:
        print("  ✓ Round trip restored columns and values (as strings - dtype")
        print("    normalization through a transpose is expected)")
        return True

    print(f"  ✗ Round trip differs:\n{twice}")
    return False


def main():
    tests = [
        test_headers_aware_transpose,
        test_numeric_labels_become_string_headers,
        test_corruption_cases_fail_loud,
        test_double_transpose_round_trips,
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
        print("✅ All slice_data transpose tests passed!")
        return 1

    print("❌ Some slice_data transpose tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
