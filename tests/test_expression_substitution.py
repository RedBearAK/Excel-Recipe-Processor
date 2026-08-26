"""
Tests for expression column-name substitution in add_calculated_column.

tests/test_expression_substitution.py

The 2026-08-26 grammar: column names enter formulas ONLY as backticked
tokens, parsed by a typed tokenizer - never recognized in free text.
Hostile names (python identifiers, hash-enders, letter-shaped) are
planted to prove the collision class is unrepresentable. Runnable
directly or with pytest.
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
        {'formula': '`Van Seq #`.astype(str).str.replace(r"[A-Za-z]+$", "", regex=True)'})
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
        {'formula': "`Price`.where(`Price` > 0, 'Paid or No Price')"})
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
        {'formula': '`Carrier`.where(`Carrier` == "AML", "no Carrier match")'})
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
        {'formula': "`Major Species`.str.lower() + `Species`"})
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
        frame.copy(), 'Total', {'formula': '`Units`*`Price`'})
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
        frame.copy(), 'Out', {'formula': 'pd.Series([9.0]).rename("Priceless") * `Price`'})
    if result['Out'].tolist() != [27.0]:
        print(f"✗ got {result['Out'].tolist()}")
        return False
    print("✓ Priceless untouched while Price substituted")
    return True


def test_python_identifier_named_column():
    """A column named sum cannot corrupt method calls."""
    print("\nTesting a column named like a python identifier...")
    frame = pd.DataFrame({'sum': [5.0, 7.0], 'Price': [2.0, 3.0]})
    processor = build_processor()
    result = processor._apply_expression_calculation(
        frame.copy(), 'Total',
        {'formula': 'pd.Series([`Price`.sum(), `sum`.max()], index=`Price`.index)'})
    if result['Total'].tolist() != [5.0, 7.0]:
        print(f"✗ got {result['Total'].tolist()}")
        return False
    print("✓ .sum() call untouched; `sum` column addressed explicitly")
    return True


def test_bare_name_is_a_hard_error():
    """A bare column name loose in code is refused with guidance."""
    print("\nTesting the bare-name guard...")
    frame = pd.DataFrame({'Price': [2.0]})
    processor = build_processor()
    try:
        processor._apply_expression_calculation(
            frame.copy(), 'Out', {'formula': 'Price * 2'})
    except Exception as error:
        if 'backticked' in str(error):
            print("✓ bare name refused with backtick guidance")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ bare name was accepted")
    return False


def test_unknown_token_guided():
    """An unknown column token names near matches."""
    print("\nTesting unknown-token guidance...")
    frame = pd.DataFrame({'Ship Date': [1]})
    processor = build_processor()
    try:
        processor._apply_expression_calculation(
            frame.copy(), 'Out', {'formula': '`Ship Dtae` * 2'})
    except Exception as error:
        if 'names no column' in str(error):
            print("✓ unknown token refused with guidance")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ unknown token accepted")
    return False


def test_split_token_guided_forward():
    """`Carrier` Echo for column Carrier Echo names the real fix."""
    print("\nTesting split-token guidance (token first)...")
    frame = pd.DataFrame({'Carrier Echo': ['AML']})
    processor = build_processor()
    try:
        processor._apply_expression_calculation(
            frame.copy(), 'Out', {'formula': '`Carrier` Echo.notna()'})
    except Exception as error:
        if 'Split column token' in str(error) and '`Carrier Echo`' in str(error):
            print("✓ split token diagnosed with the whole-name fix")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ split token accepted")
    return False


def test_split_token_guided_backward():
    """Van `Number` for column Van Number names the real fix."""
    print("\nTesting split-token guidance (token last)...")
    frame = pd.DataFrame({'Van Number': ['X']})
    processor = build_processor()
    try:
        processor._apply_expression_calculation(
            frame.copy(), 'Out', {'formula': 'Van `Number`.str.upper()'})
    except Exception as error:
        if 'Split column token' in str(error) and '`Van Number`' in str(error):
            print("✓ reversed split diagnosed")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ reversed split accepted")
    return False


def main():
    tests = [
        test_hash_ending_name,
        test_literal_containing_column_names,
        test_double_quoted_literal,
        test_longest_first_overlap,
        test_name_adjacent_punctuation,
        test_partial_identifier_not_matched,
        test_python_identifier_named_column,
        test_bare_name_is_a_hard_error,
        test_unknown_token_guided,
        test_split_token_guided_forward,
        test_split_token_guided_backward,
    ]
    passed = sum(1 for test in tests if test())
    print("\n" + "=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    raise SystemExit(0 if main() else 1)

# End of file #
