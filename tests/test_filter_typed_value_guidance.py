"""
Typed-variable guidance for filter_data ordering comparisons.

tests/test_filter_typed_value_guidance.py

Substituted recipe variables arrive as STRINGS unless typed at the
reference site. Before 2026-08-17, an untyped '{min_sales}' feeding a
greater_than filter died inside pandas with "Invalid comparison between
dtype=int64 and str" - true, baffling, and silent about the fix. These
tests pin the two halves of the remedy:

1. The GUARD: a string value on an ordering condition fails first, in
   ERP vocabulary, naming the filter/column/value and teaching the
   typed reference syntax ({int:name}, {float:name}, {list:name}).
2. The FEATURE: typed references substitute the actual typed value, so
   the same recipe works by changing '{min_sales}' to '{int:min_sales}'
   - even when the variable itself arrives as a CLI string.

Equality conditions stay untouched: '120' == 120 being False is
meaningful, and string equality against string columns is the dominant
filter case.

Runnable standalone or under pytest; the exit code carries the verdict.
"""

import sys

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.core.variable_substitution import VariableSubstitution
from excel_recipe_processor.processors.filter_data_processor import FilterDataProcessor


def make_sales_frame():
    """Small numeric frame for threshold filters."""
    return pd.DataFrame({'Sales': [100, 150, 200], 'Name': ['a', 'b', 'c']})


def run_filter(filters, frame=None):
    """Execute filter_data with the given filters list."""
    step_config = {
        'processor_type': 'filter_data',
        'step_description': 'typed guidance probe',
        'filters': filters,
    }
    data = make_sales_frame() if frame is None else frame
    return FilterDataProcessor(step_config).execute(data)


def test_string_value_on_ordering_gets_guided_error():
    """A string threshold fails loud, in ERP words, teaching {int:...}."""
    print("Testing the guided error for string ordering values...")

    passed = True
    for condition in ('greater_than', 'less_than', 'greater_equal', 'less_equal'):
        try:
            run_filter([{'column': 'Sales', 'condition': condition, 'value': '120'}])
            print(f"  ✗ {condition}: string value was accepted")
            passed = False
            continue
        except StepProcessorError as error:
            message = str(error)
            mentions = ('{int:' in message and '{float:' in message
                        and "'Sales'" in message and "'120'" in message)
            if mentions:
                print(f"  ✓ {condition}: guided error names column, value, and typed syntax")
            else:
                print(f"  ✗ {condition}: error lacks guidance: {message[:120]}")
                passed = False
    return passed


def test_string_list_members_get_guided_error():
    """String members in a min/max list fail with the lexical-order warning."""
    print("\nTesting the guided error for string list members...")

    try:
        run_filter([{'column': 'Sales', 'condition': 'greater_than_min_in_list',
                     'value': ['9', '100']}])
        print("  ✗ string list members were accepted")
        return False
    except StepProcessorError as error:
        message = str(error)
        if '{list:' in message and 'lexically' in message:
            print("  ✓ guided error teaches {list:...} and names the lexical trap")
            return True
        print(f"  ✗ error lacks guidance: {message[:120]}")
        return False


def test_typed_reference_fixes_the_same_recipe():
    """Swapping '{min_sales}' for '{int:min_sales}' makes the filter work."""
    print("\nTesting the typed reference end to end...")

    # The variable arrives as a STRING - the CLI/interactive case - and
    # the typed reference still substitutes a real int.
    substitution = VariableSubstitution(custom_variables={'min_sales': '120'})
    filters = substitution.substitute_structure(
        [{'column': 'Sales', 'condition': 'greater_than',
          'value': '{int:min_sales}'}]
    )

    substituted_value = filters[0]['value']
    if substituted_value != 120 or isinstance(substituted_value, str):
        print(f"  ✗ typed substitution yielded {substituted_value!r}")
        return False
    print("  ✓ {int:min_sales} substituted the actual int 120")

    result = run_filter(filters)
    if result['Sales'].tolist() == [150, 200]:
        print("  ✓ filter executed correctly on the typed value")
        return True
    print(f"  ✗ wrong rows: {result['Sales'].tolist()}")
    return False


def test_equality_with_strings_stays_untouched():
    """Equality keeps its existing semantics: no guard fires."""
    print("\nTesting that equality conditions are not guarded...")

    # equals defaults to case-INSENSITIVE, which string-normalizes both
    # sides, so '150' matches the numeric 150 by design - the guard must
    # not interfere with that documented behavior.
    result = run_filter([{'column': 'Sales', 'condition': 'equals', 'value': '150'}])
    if result['Sales'].tolist() == [150]:
        print("  ✓ default equals kept its string-normalized match, unguarded")
    else:
        print(f"  ✗ default equals changed: {result['Sales'].tolist()}")
        return False

    # case_sensitive equals compares raw: '150' == int 150 is False
    result = run_filter([{'column': 'Sales', 'condition': 'equals',
                          'value': '150', 'case_sensitive': True}])
    if len(result) == 0:
        print("  ✓ case_sensitive equals kept exact semantics, unguarded")
    else:
        print(f"  ✗ case_sensitive equals changed: {result['Sales'].tolist()}")
        return False

    # And ordinary string equality against a string column still works
    result = run_filter([{'column': 'Name', 'condition': 'equals', 'value': 'b'}])
    if result['Name'].tolist() == ['b']:
        print("  ✓ ordinary string equality unaffected")
        return True
    print(f"  ✗ string equality broken: {result['Name'].tolist()}")
    return False


def main():
    """Run every test and report a final score."""
    print("=== filter_data typed-value guidance tests ===")

    tests = [
        test_string_value_on_ordering_gets_guided_error,
        test_string_list_members_get_guided_error,
        test_typed_reference_fixes_the_same_recipe,
        test_equality_with_strings_stays_untouched,
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
