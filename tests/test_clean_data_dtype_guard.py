"""
Tests for dtype preservation in clean_data text actions.

tests/test_clean_data_dtype_guard.py

Runnable with pytest, but written to run standalone and report a score.

The text actions in clean_data route through .astype(str). Applied to a numeric
or datetime column that silently produces text, and the damage only becomes
visible in the Excel output, where a Price of 123.45 is written as the string
"123.45" and no longer takes a number format. These tests pin the guard that
skips typed columns.
"""

import numpy as np
import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor


def build_mixed_frame():
    """Build a frame with the dtypes a real VMS download carries."""
    return pd.DataFrame({
        'Product ID': [10001, 10002, 10003],
        'Price': [123.45, 0.0, 99.99],
        'Ship Date': pd.to_datetime(['2026-08-01', '2026-08-02', '2026-08-03']),
        'Customer': ['  Acme  ', 'Ocean Co ', ' Silver Bay '],
        'Notes': [' KT GALLEY, ', 'plain', 'ok  '],
    })


def run_clean(df, action, columns):
    """Run one cleaning action and return the result."""
    StageManager.initialize_stages()
    StageManager.save_stage('src', df, 'test input')
    return CleanDataProcessor({
        'processor_type': 'clean_data',
        'source_stage': 'src',
        'save_to_stage': 'out',
        'rules': [{'columns': columns, 'action': action}]
    }).execute(df)


def test_numeric_dtype_survives():
    """A blanket strip must not turn numeric columns into text."""
    print("\nTesting numeric dtype preservation...")

    df = build_mixed_frame()
    result = run_clean(df, 'strip_whitespace', list(df.columns))

    passed = True

    for column in ['Product ID', 'Price']:
        if str(result[column].dtype) == str(df[column].dtype):
            print(f"  ✓ {column} kept dtype {result[column].dtype}")
        else:
            print(f"  ✗ {column} became {result[column].dtype}")
            passed = False

    return passed


def test_datetime_dtype_survives():
    """A blanket strip must not turn datetime columns into text."""
    print("\nTesting datetime dtype preservation...")

    df = build_mixed_frame()
    result = run_clean(df, 'strip_whitespace', list(df.columns))

    if str(result['Ship Date'].dtype).startswith('datetime'):
        print(f"  ✓ Ship Date kept dtype {result['Ship Date'].dtype}")
        return True

    print(f"  ✗ Ship Date became {result['Ship Date'].dtype}")
    return False


def test_text_columns_still_cleaned():
    """The guard must not stop text columns being cleaned."""
    print("\nTesting text columns are still cleaned...")

    df = build_mixed_frame()
    result = run_clean(df, 'strip_whitespace', list(df.columns))

    passed = True

    if list(result['Customer']) == ['Acme', 'Ocean Co', 'Silver Bay']:
        print("  ✓ Customer stripped")
    else:
        print(f"  ✗ Customer is {list(result['Customer'])}")
        passed = False

    if list(result['Notes']) == ['KT GALLEY,', 'plain', 'ok']:
        print("  ✓ Notes stripped")
    else:
        print(f"  ✗ Notes is {list(result['Notes'])}")
        passed = False

    return passed


def test_nulls_survive():
    """Nulls must not become the literal string 'nan'."""
    print("\nTesting null preservation...")

    df = pd.DataFrame({
        'Customer Ref #': ['PO 1', np.nan, '  PO 2  '],
        'Price': [1.0, np.nan, 3.0],
    })
    result = run_clean(df, 'strip_whitespace', list(df.columns))

    text_nulls = result['Customer Ref #'].isna().sum()
    numeric_nulls = result['Price'].isna().sum()
    literal_nan = (result['Customer Ref #'].astype(str) == 'nan').sum()

    if text_nulls == 1 and numeric_nulls == 1 and literal_nan == 0:
        print("  ✓ Nulls preserved in both text and numeric columns")
        return True

    print(f"  ✗ text_nulls={text_nulls} numeric_nulls={numeric_nulls} literal='nan'={literal_nan}")
    return False


def test_case_actions_also_guarded():
    """uppercase and friends carry the same coercion risk."""
    print("\nTesting case actions are guarded too...")

    df = build_mixed_frame()
    passed = True

    for action in ['uppercase', 'lowercase', 'title_case']:
        result = run_clean(df, action, list(df.columns))
        if str(result['Price'].dtype) == str(df['Price'].dtype):
            print(f"  ✓ {action:12} left Price as {result['Price'].dtype}")
        else:
            print(f"  ✗ {action:12} made Price {result['Price'].dtype}")
            passed = False

    return passed


def test_explicit_numeric_target_is_skipped_not_errored():
    """Naming a numeric column explicitly should skip, not raise."""
    print("\nTesting explicit numeric target is skipped quietly...")

    df = build_mixed_frame()

    try:
        result = run_clean(df, 'strip_whitespace', ['Price'])
    except Exception as error:
        print(f"  ✗ Raised instead of skipping: {error}")
        return False

    if str(result['Price'].dtype) == str(df['Price'].dtype):
        print("  ✓ Skipped without raising, dtype intact")
        return True

    print(f"  ✗ Price became {result['Price'].dtype}")
    return False


def test_nulls_never_become_literal_nan():
    """
    Nulls must survive every text action, on any pandas version.

    Calling .astype(str) on a whole column turns nulls into the literal string
    "nan". Under pandas 3 the string dtype hides this; under pandas 2 it does
    not, and the "nan" text reaches the Excel output as real content. This test
    asserts on the values rather than the dtype, so it catches the problem on
    either version.
    """
    print("\nTesting nulls survive every text action...")

    passed = True

    for action in ['strip_whitespace', 'uppercase', 'lowercase', 'title_case',
                   'normalize_whitespace', 'remove_invisible_chars']:
        df = pd.DataFrame({
            'Customer Ref #': pd.Series(['  PO 1  ', np.nan, 'PO 2', np.nan], dtype=object),
            'Notes': pd.Series([np.nan, np.nan, np.nan, np.nan], dtype=object),
        })
        result = run_clean(df, action, ['Customer Ref #', 'Notes'])

        literal = [
            v for v in result['Customer Ref #'].tolist()
            if isinstance(v, str) and v.strip().lower() in ('nan', 'none', '<na>')
        ]
        still_null = result['Customer Ref #'].isna().sum()
        notes_null = result['Notes'].isna().sum()

        if literal or still_null != 2 or notes_null != 4:
            print(f"  ✗ {action:22} nulls={still_null}/2 notes_nulls={notes_null}/4 literal={literal}")
            passed = False
        else:
            print(f"  ✓ {action:22} nulls preserved in partly and fully empty columns")

    return passed


def test_all_empty_object_column():
    """A column that is entirely blank must stay entirely blank."""
    print("\nTesting a fully empty text column...")

    df = pd.DataFrame({
        'Temp Logger': pd.Series([np.nan] * 5, dtype=object),
        'Customer': ['a', 'b', 'c', 'd', 'e'],
    })
    result = run_clean(df, 'strip_whitespace', ['Temp Logger', 'Customer'])

    non_null = result['Temp Logger'].notna().sum()

    if non_null == 0:
        print("  ✓ Fully empty column still fully empty")
        return True

    print(f"  ✗ {non_null} cells gained content: {result['Temp Logger'].tolist()[:3]}")
    return False


def main():
    """Run every test and report a final score."""
    print("=== clean_data dtype guard tests ===")

    tests = [
        test_numeric_dtype_survives,
        test_datetime_dtype_survives,
        test_text_columns_still_cleaned,
        test_nulls_survive,
        test_case_actions_also_guarded,
        test_explicit_numeric_target_is_skipped_not_errored,
        test_nulls_never_become_literal_nan,
        test_all_empty_object_column,
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
        print("✅ All clean_data dtype guard tests passed!")
        return 1

    print("❌ Some clean_data dtype guard tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
