"""
Tests for expression column references in add_calculated_column.

tests/test_expression_substitution.py

The 2026-08-26 grammar: a column reference is ALWAYS {col:Column Name}
- the house convention shared with conditional_format and
inject_formulas. Bare identifiers in code are always python; there is
no recognition, no collision concept, and no disambiguation burden. A
column named sum is {col:sum} exactly as Price is {col:Price}.
Unmigrated bare references surface as eval NameErrors carrying {col:}
guidance. Runnable directly or with pytest.
"""

import pandas as pd

from excel_recipe_processor.processors.add_calculated_column_processor import (
    AddCalculatedColumnProcessor,
)


def build_processor():
    config = AddCalculatedColumnProcessor.get_minimal_config()
    config['processor_type'] = 'add_calculated_column'
    return AddCalculatedColumnProcessor(config)


def run(frame, formula):
    processor = build_processor()
    return processor._apply_expression_calculation(
        frame.copy(), 'Out', {'pandas_formula': formula})


def test_hash_ending_name():
    """{col:Van Seq #} carries the hash safely."""
    print("\nTesting a hash-ending column name...")
    frame = pd.DataFrame({'Van Seq #': ['2603D101A', '2610K055']})
    result = run(frame,
                 '{col:Van Seq #}.astype(str).str.replace(r"[A-Za-z]+$", "", regex=True)')
    if result['Out'].tolist() != ['2603D101', '2610K055']:
        print(f"✗ got {result['Out'].tolist()}")
        return False
    print("✓ hash-ending reference evaluated")
    return True


def test_literal_containing_placeholder_text():
    """Placeholder-shaped text inside a literal is never touched."""
    print("\nTesting literal protection...")
    frame = pd.DataFrame({'Price': [1.0, None]})
    result = run(frame,
                 "{col:Price}.where({col:Price} > 0, 'Paid or No Price')")
    got = result['Out'].tolist()
    if got[0] != 1.0 or got[1] != 'Paid or No Price':
        print(f"✗ got {got}")
        return False
    print("✓ literal preserved verbatim")
    return True


def test_sum_column_beside_python_sum():
    """THE requirement: a column named sum, zero special handling."""
    print("\nTesting a column named sum with python sum() alongside...")
    frame = pd.DataFrame({'sum': [5.0, 7.0], 'Price': [2.0, 3.0]})
    result = run(frame,
                 'pd.Series([{col:Price}.sum() + sum([0]), {col:sum}.max()],'
                 ' index={col:Price}.index)')
    if result['Out'].tolist() != [5.0, 7.0]:
        print(f"✗ got {result['Out'].tolist()}")
        return False
    print("✓ {col:sum}, .sum(), and python sum() coexist untouched")
    return True


def test_multiword_and_overlapping_names():
    """Species and Major Species address independently."""
    print("\nTesting overlapping multi-word names...")
    frame = pd.DataFrame({'Species': ['a', 'b'],
                          'Major Species': ['SQUID', 'COD']})
    result = run(frame, '{col:Major Species}.str.lower() + {col:Species}')
    if result['Out'].tolist() != ['squida', 'codb']:
        print(f"✗ got {result['Out'].tolist()}")
        return False
    print("✓ overlap is a non-concept under delimiters")
    return True


def test_unmigrated_bare_reference_guided():
    """A bare column reference fails with {col:} guidance."""
    print("\nTesting NameError guidance...")
    frame = pd.DataFrame({'Price': [2.0]})
    try:
        run(frame, 'Price * 2')
    except Exception as error:
        if '{col:Price}' in str(error):
            print("✓ bare reference guided to {col:Price}")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ bare reference accepted")
    return False


def test_multiword_bare_reference_guided():
    """Bare Major Species guides to its full {col:} form."""
    print("\nTesting multi-word NameError guidance...")
    frame = pd.DataFrame({'Major Species': ['SQUID']})
    try:
        run(frame, 'Major Species.str.lower()')
    except Exception as error:
        if '{col:Major Species}' in str(error):
            print("✓ first-word NameError mapped to the full column")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ bare multi-word accepted")
    return False


def test_unknown_reference_guided():
    """{col:Ship Dtae} names near matches."""
    print("\nTesting unknown-reference guidance...")
    frame = pd.DataFrame({'Ship Date': [1]})
    try:
        run(frame, '{col:Ship Dtae} * 2')
    except Exception as error:
        if 'names no column' in str(error):
            print("✓ unknown reference refused with guidance")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ unknown reference accepted")
    return False


def test_unterminated_reference():
    """{col:Price with no closing brace is a parse error."""
    print("\nTesting unterminated reference...")
    frame = pd.DataFrame({'Price': [1]})
    try:
        run(frame, '{col:Price * 2')
    except Exception as error:
        if 'Unterminated column reference' in str(error):
            print("✓ unterminated reference refused")
            return True
        print(f"✗ wrong error: {error}")
        return False
    print("✗ unterminated reference accepted")
    return False


def main():
    tests = [
        test_hash_ending_name,
        test_literal_containing_placeholder_text,
        test_sum_column_beside_python_sum,
        test_multiword_and_overlapping_names,
        test_unmigrated_bare_reference_guided,
        test_multiword_bare_reference_guided,
        test_unknown_reference_guided,
        test_unterminated_reference,
    ]
    passed = sum(1 for test in tests if test())
    print("\n" + "=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    raise SystemExit(0 if main() else 1)

# End of file #
