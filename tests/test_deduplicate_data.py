"""
Tests for the deduplicate_data processor.

tests/test_deduplicate_data.py

Runnable with pytest, but written to run standalone and report a score.
Assertions are on values, not just row counts.
"""

import os
import tempfile

from pathlib import Path

import pandas as pd

import excel_recipe_processor.core.pipeline  # registers processors
import excel_recipe_processor.core.stage_manager as stage_manager_module

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.deduplicate_data_processor import DeduplicateDataProcessor


def build(**overrides):
    config = {'processor_type': 'deduplicate_data', 'key_columns': ['Order ID'],
              'save_to_stage': 'stg_test_dedupe'}
    config.update(overrides)
    return DeduplicateDataProcessor(config)


def sample_frame():
    """A1 conflicts on Terms; A2 is pure repetition; A3 is unique."""
    return pd.DataFrame({
        'Order ID': ['A1', 'A1', 'A2', 'A2', 'A3'],
        'Terms':    ['Bill To', 'Cash', 'Net 30', 'Net 30', 'Wire'],
        'Dest':     ['X', 'X', 'Y', 'Y', 'Z'],
    })


def test_collapse_keeps_first_values():
    """One row per key, first occurrence's VALUES survive."""
    print("\nTesting collapse keeps first values...")

    result = build().execute(sample_frame())

    passed = True

    if list(result['Order ID']) == ['A1', 'A2', 'A3']:
        print("  ✓ One row per key, input order preserved")
    else:
        print(f"  ✗ Keys: {list(result['Order ID'])}")
        passed = False

    if result.loc[result['Order ID'] == 'A1', 'Terms'].iloc[0] == 'Bill To':
        print("  ✓ A1 kept 'Bill To' (first)")
    else:
        print(f"  ✗ A1 kept {result.loc[result['Order ID'] == 'A1', 'Terms'].iloc[0]!r}")
        passed = False

    return passed


def test_keep_last_flips_the_winner():
    """keep: last makes A1 keep 'Cash'."""
    print("\nTesting keep: last...")

    result = build(keep='last').execute(sample_frame())

    if result.loc[result['Order ID'] == 'A1', 'Terms'].iloc[0] == 'Cash':
        print("  ✓ A1 kept 'Cash' (last)")
        return True

    print(f"  ✗ A1 kept {result.loc[result['Order ID'] == 'A1', 'Terms'].iloc[0]!r}")
    return False


def test_all_columns_survive_unenumerated():
    """Columns nobody mentioned in the config come through intact."""
    print("\nTesting unenumerated columns survive...")

    frame = sample_frame()
    frame['Surprise Column'] = ['s1', 's2', 's3', 's4', 's5']

    result = build().execute(frame)

    if 'Surprise Column' in result.columns and \
            result.loc[result['Order ID'] == 'A3', 'Surprise Column'].iloc[0] == 's5':
        print("  ✓ Unmentioned column present with correct value")
        return True

    print(f"  ✗ Columns: {list(result.columns)}")
    return False


def test_conflicts_annotated_and_repetition_ignored():
    """Only A1 is conflicted; both its rows appear, annotated."""
    print("\nTesting conflict stage contents...")

    StageManager.initialize_stages(max_stages=10)
    build(save_conflicts_to_stage='stg_test_conflicts').execute(sample_frame())
    conflicts = StageManager.load_stage('stg_test_conflicts')

    passed = True

    if set(conflicts['Order ID']) == {'A1'} and len(conflicts) == 2:
        print("  ✓ Only A1's two rows in the conflict stage (A2 repetition excluded)")
    else:
        print(f"  ✗ Conflict rows: {conflicts[['Order ID']].values.ravel().tolist()}")
        passed = False

    if list(conflicts['Dedupe Status']) == ['kept', 'discarded']:
        print("  ✓ Dedupe Status marks kept then discarded")
    else:
        print(f"  ✗ Status: {list(conflicts.get('Dedupe Status', []))}")
        passed = False

    if list(conflicts['Conflicting Columns'].unique()) == ['Terms']:
        print("  ✓ Conflicting Columns names exactly 'Terms'")
    else:
        print(f"  ✗ Conflicting Columns: {list(conflicts['Conflicting Columns'].unique())}")
        passed = False

    return passed


def test_report_file_only_written_when_dirty():
    """A clean collapse leaves no file; a dirty one writes it."""
    print("\nTesting report file emission...")

    with tempfile.TemporaryDirectory() as temp_dir:
        report = Path(temp_dir) / 'conflicts.xlsx'

        clean = pd.DataFrame({'Order ID': ['B1', 'B1'], 'Terms': ['Net 30', 'Net 30']})
        build(conflicts_file=str(report)).execute(clean)

        passed = True

        if not report.exists():
            print("  ✓ Clean collapse: no file written")
        else:
            print("  ✗ File written for a clean collapse")
            passed = False

        build(conflicts_file=str(report)).execute(sample_frame())

        if report.exists():
            written = pd.read_excel(report)
            if set(written['Order ID']) == {'A1'}:
                print("  ✓ Dirty collapse: file written with A1's rows")
            else:
                print(f"  ✗ File contains: {list(written['Order ID'])}")
                passed = False
        else:
            print("  ✗ No file for a dirty collapse")
            passed = False

        return passed


def test_keep_none_drops_every_repeated_key():
    """keep: none leaves only rows whose key appeared exactly once."""
    print("\nTesting keep: none...")

    result = build(keep='none').execute(sample_frame())

    passed = True

    if list(result['Order ID']) == ['A3']:
        print("  ✓ A1 and A2 dropped entirely, A3 alone survives")
    else:
        print(f"  ✗ Keys: {list(result['Order ID'])}")
        passed = False

    # The symmetric-difference use: key on EVERY column, so exact twins
    # cancel and only unmatched rows remain, with their source tag intact.
    left = pd.DataFrame({'Van': ['V1', 'V1', 'V2'], 'Product': [10, 11, 20], 'Src': 'left'})
    right = pd.DataFrame({'Van': ['V1', 'V1', 'V2'], 'Product': [10, 12, 20], 'Src': 'right'})
    stacked = pd.concat([left, right], ignore_index=True)

    diff = build(key_columns=['Van', 'Product'], keep='none').execute(stacked)
    expected = [('V1', 11, 'left'), ('V1', 12, 'right')]
    actual = list(diff[['Van', 'Product', 'Src']].itertuples(index=False, name=None))

    if actual == expected:
        print("  ✓ Symmetric difference: one unmatched row per side, source preserved")
    else:
        print(f"  ✗ Symmetric difference gave {actual}")
        passed = False

    return passed


def test_empty_input_and_bad_config():
    """Empty in means empty out; bad keys and modes fail loudly."""
    print("\nTesting empty input and validation...")

    passed = True

    empty = build().execute(sample_frame().head(0))
    if len(empty) == 0 and list(empty.columns) == ['Order ID', 'Terms', 'Dest']:
        print("  ✓ Empty in, empty out with columns intact")
    else:
        print(f"  ✗ Empty gave {empty.shape}")
        passed = False

    try:
        build(key_columns=['No Such Column']).execute(sample_frame())
        print("  ✗ Missing key column accepted")
        passed = False
    except Exception as error:
        if 'not found' in str(error):
            print("  ✓ Missing key column named in the error")
        else:
            print(f"  ✗ Wrong error: {error}")
            passed = False

    try:
        build(keep='middle')
        print("  ✗ keep: middle accepted")
        passed = False
    except Exception:
        print("  ✓ Invalid keep rejected at construction")

    return passed


def main():
    """Run every test and report a final score."""
    print("=== deduplicate_data tests ===")

    tests = [
        test_collapse_keeps_first_values,
        test_keep_last_flips_the_winner,
        test_keep_none_drops_every_repeated_key,
        test_all_columns_survive_unenumerated,
        test_conflicts_annotated_and_repetition_ignored,
        test_report_file_only_written_when_dirty,
        test_empty_input_and_bad_config,
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
        print("✅ All deduplicate_data tests passed!")
        return 1

    print("❌ Some deduplicate_data tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
