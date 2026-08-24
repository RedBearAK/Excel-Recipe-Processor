"""
lookup_data substring mode: lookup key text found INSIDE the main column.

tests/test_lookup_substring_mode.py

match_mode 'lookup_value_within_main_text' (2026-08-23) is the data-driven
form of the XLOOKUP(TRUE, ISNUMBER(SEARCH(range, cell)), range) worksheet
pattern: lookup rows are tried in stage order, the first whose key string
appears (case-insensitive) inside the main column's text wins, unmatched
rows keep NA for default_values to fill. Built for the contract-Notes
due-date-indicator decode.

Runnable with pytest, but written to run standalone and report a score.
"""

import sys

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.lookup_data_processor import LookupDataProcessor


def run_lookup(main: pd.DataFrame, lookup: pd.DataFrame, **extra):
    """Run a substring-mode lookup of Notes against Indicator."""
    StageManager.initialize_stages(max_stages=10)
    StageManager.declare_recipe_stages({
        'settings': {'stages': [
            {'stage_name': 'stg_decode', 'description': 'lookup table'},
        ]}
    })
    StageManager.save_stage('stg_decode', lookup, 'lookup table')
    config = {
        'processor_type': 'lookup_data',
        'lookup_stage': 'stg_decode',
        'match_col_in_main_data': 'Notes',
        'match_col_in_lookup_data': 'Indicator',
        'lookup_columns': ['Indicator', 'Relevant Date'],
        'match_mode': 'lookup_value_within_main_text',
    }
    config.update(extra)
    return LookupDataProcessor(config).execute(main)


DECODE = pd.DataFrame({
    'Indicator': ['Email copies of proof / draft documents',
                  'Email of Invoice',
                  'SWB Release'],
    'Relevant Date': ['Date Docs Sent', 'Invoice Date', 'Release Date'],
})


def test_first_match_in_stage_order_wins():
    """Row order of the lookup stage decides ties, like the XLOOKUP."""
    print("Testing stage-order precedence and basic matching...")

    main = pd.DataFrame({'Notes': [
        'DDI=Email of Invoice, halperns PO# 123',
        'terms: SWB Release then balance',
        'Email copies of proof / draft documents AND SWB Release',
    ]})
    result = run_lookup(main, DECODE)

    expect = ['Email of Invoice', 'SWB Release',
              'Email copies of proof / draft documents']
    got = list(result['Indicator'])
    if got == expect:
        print(f"  ✓ matches resolved in stage order: {got}")
        return True
    print(f"  ✗ expected {expect}, got {got}")
    return False


def test_case_insensitive_and_regex_safe():
    """Lower-case notes match, and '/' '+' in keys are literal text."""
    print("\nTesting case handling and regex escaping...")

    main = pd.DataFrame({'Notes': [
        'ddi: email of invoice per seller',
        'email copies of proof / draft documents attached',
    ]})
    result = run_lookup(main, DECODE)

    expect = ['Email of Invoice', 'Email copies of proof / draft documents']
    got = list(result['Indicator'])
    if got == expect:
        print("  ✓ case-insensitive; slash-bearing key matched literally")
        return True
    print(f"  ✗ expected {expect}, got {got}")
    return False


def test_unmatched_gets_defaults():
    """No hit -> NA -> default_values fills; other columns untouched."""
    print("\nTesting defaults on unmatched rows...")

    main = pd.DataFrame({'Notes': ['ptb 8/15', None]})
    result = run_lookup(main, DECODE, default_values={'Indicator': 'Email of Invoice'})

    passed = True
    if list(result['Indicator']) == ['Email of Invoice', 'Email of Invoice']:
        print("  ✓ unmatched and blank Notes both defaulted")
    else:
        print(f"  ✗ Indicator column: {list(result['Indicator'])}")
        passed = False
    if result['Relevant Date'].isna().all():
        print("  ✓ column without a default stayed NA")
    else:
        print(f"  ✗ Relevant Date column: {list(result['Relevant Date'])}")
        passed = False
    return passed


def test_guardrails():
    """Bad mode, stray join_type, and column collisions all refuse loudly."""
    print("\nTesting guardrails...")

    main = pd.DataFrame({'Notes': ['x']})
    passed = True

    try:
        run_lookup(main, DECODE, match_mode='fuzzy_vibes')
        print("  ✗ invalid match_mode accepted")
        passed = False
    except StepProcessorError as error:
        if 'lookup_value_within_main_text' in str(error):
            print("  ✓ invalid match_mode refused, modes named")
        else:
            print(f"  ✗ guidance incomplete: {error}")
            passed = False

    try:
        run_lookup(main, DECODE, join_type='left')
        print("  ✗ join_type accepted in substring mode")
        passed = False
    except StepProcessorError as error:
        if 'join_type' in str(error):
            print("  ✓ stray join_type refused")
        else:
            print(f"  ✗ wrong guidance: {error}")
            passed = False

    collide = pd.DataFrame({'Notes': ['x'], 'Indicator': ['already here']})
    try:
        run_lookup(collide, DECODE)
        print("  ✗ column collision silently overwritten")
        passed = False
    except StepProcessorError as error:
        if 'Indicator' in str(error):
            print("  ✓ column collision refused, column named")
        else:
            print(f"  ✗ wrong guidance: {error}")
            passed = False

    return passed


def main():
    """Run every test and report a final score."""
    print("=== lookup_data substring mode tests ===")

    tests = [
        test_first_match_in_stage_order_wins,
        test_case_insensitive_and_regex_safe,
        test_unmatched_gets_defaults,
        test_guardrails,
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
