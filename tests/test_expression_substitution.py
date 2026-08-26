"""
Tests for expression column-name substitution in add_calculated_column.

tests/test_expression_substitution.py

The 2026-08-25 engine repairs, each born in production: names ending in
non-word characters (Van Seq #) must substitute, and column names inside
string literals ('Paid or No Price' beside live Paid and Price columns)
must NOT. Longest-first one-pass behavior must survive the repair.
Runnable directly or with pytest.
"""

import pandas as pd

from excel_recipe_processor.processors.add_calculated_column_processor import (
    AddCalculatedColumnProcessor,
)


def build_processor():
    config = AddCalculatedColumnProcessor.get_minimal_config()
    config['processor_type'] = 'add_calculated_column'
    return AddCalculatedColumnProcessor(config)


def test_hash_ending_name():
    """A column name ending in # substitutes instead of commenting out."""
    print("\nTesting hash-ending column name...")
    frame = pd.DataFrame({'Van Seq #': ['2603D101A', '2610K055']})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Real',
        {'formula': 'Van Seq #.astype(str).str.replace(r"[A-Za-z]+$", "", regex=True)'})
    if result['Real'].tolist() != ['2603D101', '2610K055']:
        print(f"✗ got {result['Real'].tolist()}")
        return False
    print("✓ hash-ending name substituted and evaluated")
    return True


def test_literal_containing_column_names():
    """Column names inside quoted literals stay untouched."""
    print("\nTesting literals containing live column names...")
    frame = pd.DataFrame({'Paid': [None, 'Y'], 'Price': [1.0, None]})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Tag',
        {'formula': "Price.where(Price > 0, 'Paid or No Price')"})
    got = result['Tag'].tolist()
    if got[0] != 1.0 or got[1] != 'Paid or No Price':
        print(f"✗ got {got}")
        return False
    print("✓ literal preserved verbatim while code substituted")
    return True


def test_double_quoted_literal():
    """Double-quoted literals are masked the same as single-quoted."""
    print("\nTesting double-quoted literal masking...")
    frame = pd.DataFrame({'Carrier': ['AML', 'X']})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Note',
        {'formula': 'Carrier.where(Carrier == "AML", "no Carrier match")'})
    if result['Note'].tolist() != ['AML', 'no Carrier match']:
        print(f"✗ got {result['Note'].tolist()}")
        return False
    print("✓ double-quoted literal preserved")
    return True


def test_longest_first_overlap():
    """Species inside Major Species still resolves longest-first."""
    print("\nTesting longest-first overlapping names...")
    frame = pd.DataFrame({'Species': ['a', 'b'], 'Major Species': ['SQUID', 'COD']})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Both',
        {'formula': "Major Species.str.lower() + Species"})
    if result['Both'].tolist() != ['squida', 'codb']:
        print(f"✗ got {result['Both'].tolist()}")
        return False
    print("✓ overlap resolved longest-first")
    return True


def test_name_adjacent_punctuation():
    """A name followed directly by an operator still matches."""
    print("\nTesting punctuation-adjacent matching...")
    frame = pd.DataFrame({'Units': [2, 3], 'Price': [5.0, 7.0]})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Total', {'formula': 'Units*Price'})
    if result['Total'].tolist() != [10.0, 21.0]:
        print(f"✗ got {result['Total'].tolist()}")
        return False
    print("✓ operator-adjacent names matched")
    return True


def test_partial_identifier_not_matched():
    """A column name embedded in a longer identifier is left alone."""
    print("\nTesting identifier-boundary protection...")
    frame = pd.DataFrame({'Price': [3.0]})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Out', {'formula': 'pd.Series([9.0]).rename("Priceless") * Price'})
    if result['Out'].tolist() != [27.0]:
        print(f"✗ got {result['Out'].tolist()}")
        return False
    print("✓ Priceless untouched while Price substituted")
    return True


def main():
    tests = [
        test_hash_ending_name,
        test_literal_containing_column_names,
        test_double_quoted_literal,
        test_longest_first_overlap,
        test_name_adjacent_punctuation,
        test_partial_identifier_not_matched,
    ]
    passed = sum(1 for test in tests if test())
    print("\n" + "=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    raise SystemExit(0 if main() else 1)

# End of file #
