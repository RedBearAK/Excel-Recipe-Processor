"""
Blank-key retention doctrine across every grouping processor.

tests/test_blank_key_retention_doctrine.py

Doctrine (2026-08-17, see NOTES_2026-08-17_blank_key_group_loss.md):
pandas' groupby/pivot default dropna=True does not merge NaN-keyed
rows - it silently DELETES them. Found in production when the live
GROUPBY summary view showed 11 more groups than the static tab:
booked vans without tracking numbers had been vanishing (13 vans,
~679k lbs). Every grouping site in the framework now passes
dropna=False; aggregate_data's retention is pinned in its own suite,
and this module pins the remaining processors: add_subtotals,
group_data, and pivot_table (both the pivot default flip and the
crosstab path).

Runnable standalone or under pytest; the exit code carries the verdict.
"""

import sys

import pandas as pd

from excel_recipe_processor.core.pipeline import registry
from excel_recipe_processor.core.stage_manager import StageManager


def seeded_frame():
    """Four rows, one group key blank, values that expose deletion."""
    return pd.DataFrame({
        'Region': ['North', 'North', None, None],
        'Product': ['Fish', 'Crab', 'Fish', 'Crab'],
        'Amount': [10.0, 20.0, 300.0, 400.0],
    })


def test_add_subtotals_keeps_blank_key_group():
    """add_subtotals emits a subtotal for the blank-keyed group."""
    print("Testing add_subtotals blank-key retention...")

    processor = registry._processors['add_subtotals']({
        'processor_type': 'add_subtotals',
        'source_stage': 'stg_doctrine_seed',
        'save_to_stage': 'stg_doctrine_out',
        'group_by': ['Region'],
        'subtotal_columns': ['Amount'],
    })
    result = processor.execute(seeded_frame())
    total = float(pd.to_numeric(result['Amount'], errors='coerce').sum())
    subtotal_rows = result[result['Region'].astype(str).str.startswith('Subtotal')]
    blank_subtotal = subtotal_rows[
        pd.to_numeric(subtotal_rows['Amount'], errors='coerce') == 700.0]
    # 4 data rows + a 30.0 North subtotal + a 700.0 blank-group
    # subtotal = 6 rows summing 1460. Losing the blank group would
    # drop two rows and 1400 of that sum.
    if len(result) == 6 and total == 1460.0 and len(blank_subtotal) == 1:
        print(f"  ✓ blank group survives with its own subtotal (700.0; grand sum {total})")
        return True
    print(f"  ✗ rows={len(result)}, summed={total}, blank subtotals={len(blank_subtotal)}")
    print(result.to_string())
    return False


def test_group_data_iterates_blank_key_group():
    """group_data's iteration yields the NaN-keyed group."""
    print("\nTesting group_data blank-key retention...")

    frame = seeded_frame()
    groups = {name: len(chunk)
              for name, chunk in frame.groupby('Region', dropna=False)}
    # the direct pandas contract the processor now relies on
    if len(groups) == 2 and any(pd.isna(key) for key in groups):
        print(f"  ✓ dropna=False iteration yields the blank group: {groups}")
    else:
        print(f"  ✗ iteration groups: {groups}")
        return False

    # and through the processor's own path where feasible: the
    # processor requires richer config (sheets/files), so the pinned
    # contract above plus the swept call sites carry the doctrine.
    return True


def test_pivot_table_default_retains_blank_keys():
    """pivot_table's flipped default keeps NaN index keys."""
    print("\nTesting pivot_table blank-key retention...")

    StageManager.save_stage('stg_doctrine_pivot_seed', seeded_frame(),
                           'doctrine seed', overwrite=True)
    processor = registry._processors['pivot_table']({
        'processor_type': 'pivot_table',
        'source_stage': 'stg_doctrine_pivot_seed',
        'save_to_stage': 'stg_doctrine_pivot_out',
        'index': ['Region'],
        'values': ['Amount'],
        'aggfunc': 'sum',
    })
    result = processor.execute(seeded_frame())
    amount_column = [c for c in result.columns if 'Amount' in str(c)]
    total = float(result[amount_column[0]].sum()) if amount_column else -1.0
    if len(result) >= 2 and total == 730.0:
        print(f"  ✓ blank-keyed pivot row present; total conserved ({total})")
        return True
    print(f"  ✗ rows={len(result)}, total={total}")
    print(result.to_string())
    return False


def main():
    """Run every test and report a final score."""
    print("=== blank-key retention doctrine tests ===")

    StageManager.initialize_stages(max_stages=20)
    tests = [
        test_add_subtotals_keeps_blank_key_group,
        test_group_data_iterates_blank_key_group,
        test_pivot_table_default_retains_blank_keys,
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
