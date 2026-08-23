"""
diff_data blank equivalence: '', whitespace-only, and NaN are one absence.

tests/test_diff_blank_equivalence.py

Excel cannot represent the difference between an empty string and an
empty cell: '' written to a cell reads back as NaN. A diff that
distinguishes them therefore flags a phantom CHANGED row on every run
whose baseline round-tripped through a file - found live on a Notes
cell containing a single space. The 2026-08-17 rule: None/NaN, '', and
whitespace-only strings are the SAME absent value; a real value against
any of them is still a change.

Runnable with pytest, but written to run standalone and report a score.
"""

import sys

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.diff_data_processor import DiffDataProcessor


def run_diff(current: pd.DataFrame, reference: pd.DataFrame) -> pd.DataFrame:
    """Diff two frames on 'Key' and return the full result."""
    StageManager.initialize_stages(max_stages=10)
    StageManager.declare_recipe_stages({
        'settings': {'stages': [
            {'stage_name': 'stg_reference', 'description': 'baseline'},
            {'stage_name': 'stg_result', 'description': 'diff output'},
        ]}
    })
    StageManager.save_stage('stg_reference', reference, 'baseline')
    processor = DiffDataProcessor({
        'processor_type': 'diff_data',
        'reference_stage': 'stg_reference',
        'key_columns': ['Key'],
        'save_to_stage': 'stg_result',
    })
    return processor.execute(current)


def test_blank_variants_are_equal():
    """NaN vs '' vs whitespace-only never produces CHANGED."""
    print("Testing blank variants across the diff...")

    current = pd.DataFrame({
        'Key': ['A', 'B', 'C'],
        'Notes': ['', '   ', None],
    })
    reference = pd.DataFrame({
        'Key': ['A', 'B', 'C'],
        'Notes': [None, '', ' '],
    })
    result = run_diff(current, reference).set_index('Key')

    passed = True
    for key in ('A', 'B', 'C'):
        status = result.loc[key, 'Row_Status']
        if status == 'UNCHANGED':
            print(f"  ✓ {key}: blank variants judged UNCHANGED")
        else:
            print(f"  ✗ {key}: expected UNCHANGED, got {status}")
            passed = False
    return passed


def test_real_change_still_detected():
    """A real value against a blank, and value-vs-value, still register."""
    print("\nTesting genuine changes still fire...")

    current = pd.DataFrame({
        'Key': ['A', 'B'],
        'Notes': ['now filled in', 'NET 30'],
    })
    reference = pd.DataFrame({
        'Key': ['A', 'B'],
        'Notes': [None, 'NET 14'],
    })
    result = run_diff(current, reference).set_index('Key')

    passed = True
    for key, why in (('A', 'blank -> value'), ('B', 'value -> value')):
        status = result.loc[key, 'Row_Status']
        if status == 'CHANGED':
            print(f"  ✓ {key}: {why} judged CHANGED")
        else:
            print(f"  ✗ {key}: {why} expected CHANGED, got {status}")
            passed = False
    return passed


def test_file_round_trip_is_idempotent():
    """A frame diffed against its own Excel round-trip shows no differences."""
    print("\nTesting Excel round-trip idempotence...")

    import tempfile
    from pathlib import Path

    frame = pd.DataFrame({
        'Key': ['A', 'B', 'C'],
        'Notes': [' ', '', 'real note'],
        'Terms': ['NET 14', None, 'NET 30'],
    })
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / 'roundtrip.xlsx'
        frame.to_excel(path, index=False)
        reloaded = pd.read_excel(path)

    result = run_diff(frame, reloaded)
    statuses = set(result['Row_Status'])
    if statuses == {'UNCHANGED'}:
        print("  ✓ round-tripped baseline diffs clean")
        return True
    print(f"  ✗ phantom statuses: {sorted(statuses)}")
    return False


def main():
    """Run every test and report a final score."""
    print("=== diff_data blank equivalence tests ===")

    tests = [
        test_blank_variants_are_equal,
        test_real_change_still_detected,
        test_file_round_trip_is_idempotent,
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
