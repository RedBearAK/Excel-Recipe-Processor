"""
fill_data coalesce pair: blanks_from_column and overwrite_from_column.

tests/test_fill_from_column.py

The 2026-08-23 pair takes a 'source_column' and coalesces in one of two
directions: blanks_from_column keeps the target and fills only its
blanks (coalesce(target, source)); overwrite_from_column lets a
non-blank source WIN and the target only covers the source's gaps
(coalesce(source, target)) - the override-beats-derived rule that
consumes DDI Manual Override. Blank means NA, '' or whitespace-only,
matching the diff processor's blank-equivalence doctrine.

Runnable with pytest, but written to run standalone and report a score.
"""

import sys

import pandas as pd

from excel_recipe_processor.core.base_processor import StepProcessorError
from excel_recipe_processor.processors.fill_data_processor import FillDataProcessor


def run_fill(frame: pd.DataFrame, method: str, **extra) -> pd.DataFrame:
    """Run one fill over the 'Target' column."""
    config = {
        'processor_type': 'fill_data',
        'columns': ['Target'],
        'fill_method': method,
        'source_column': 'Override',
        'save_to_stage': 'stg_out',
    }
    config.update(extra)
    return FillDataProcessor(config).execute(frame)


def build_frame() -> pd.DataFrame:
    """Every blank/non-blank combination of target and source."""
    return pd.DataFrame({
        'Target':   ['derived', 'derived', None, '',   '   ', None],
        'Override': [None,      'manual',  'manual', 'manual', 'manual', None],
    })


def test_overwrite_from_column():
    """Non-blank source wins everywhere; target covers source gaps."""
    print("Testing overwrite_from_column (override beats derived)...")

    result = run_fill(build_frame(), 'overwrite_from_column')
    expect = ['derived', 'manual', 'manual', 'manual', 'manual', None]
    got = [v if isinstance(v, str) else None for v in result['Target']]
    if got == expect:
        print(f"  ✓ coalesce(source, target): {got}")
        return True
    print(f"  ✗ expected {expect}, got {got}")
    return False


def test_blanks_from_column():
    """Target keeps its values; only its blanks take the source."""
    print("\nTesting blanks_from_column (target wins)...")

    result = run_fill(build_frame(), 'blanks_from_column')
    expect = ['derived', 'derived', 'manual', 'manual', 'manual', None]
    got = [v if isinstance(v, str) else None for v in result['Target']]
    if got == expect:
        print(f"  ✓ coalesce(target, source): {got}")
        return True
    print(f"  ✗ expected {expect}, got {got}")
    return False


def test_source_column_untouched():
    """The source column itself is never modified by either direction."""
    print("\nTesting source column preservation...")

    frame = build_frame()
    before = list(frame['Override'])
    result = run_fill(frame, 'overwrite_from_column')
    after = [v if isinstance(v, str) else None for v in result['Override']]
    expect = [v if isinstance(v, str) else None for v in before]
    if after == expect:
        print("  ✓ source column unchanged")
        return True
    print(f"  ✗ source mutated: {after}")
    return False


def test_guardrails():
    """Missing and unknown source_column both refuse with guidance."""
    print("\nTesting guardrails...")

    passed = True
    try:
        FillDataProcessor({
            'processor_type': 'fill_data', 'columns': ['Target'],
            'fill_method': 'overwrite_from_column', 'save_to_stage': 's',
        }).execute(build_frame())
        print("  ✗ missing source_column accepted")
        passed = False
    except StepProcessorError as error:
        if 'source_column' in str(error):
            print("  ✓ missing source_column refused, key named")
        else:
            print(f"  ✗ wrong guidance: {error}")
            passed = False

    try:
        run_fill(build_frame(), 'blanks_from_column', source_column='Nope')
        print("  ✗ unknown source_column accepted")
        passed = False
    except StepProcessorError as error:
        if 'Nope' in str(error) and 'Available' in str(error):
            print("  ✓ unknown source_column refused, columns listed")
        else:
            print(f"  ✗ wrong guidance: {error}")
            passed = False

    return passed


def main():
    """Run every test and report a final score."""
    print("=== fill_data coalesce pair tests ===")

    tests = [
        test_overwrite_from_column,
        test_blanks_from_column,
        test_source_column_untouched,
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
