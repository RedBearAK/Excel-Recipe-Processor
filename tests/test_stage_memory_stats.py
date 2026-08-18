"""
Tests for the run-level stage-memory accounting.

tests/test_stage_memory_stats.py

All stage traffic funnels through save_stage/delete_stage, so a
running concurrent total with a high-water mark gives the exact peak
at the estimation level. The interesting cases are the ones a naive
sum gets wrong: mid-run freeing keeps peak below total-allocated, and
an OVERWRITE of an existing stage releases the old frame before
counting the new one. Runnable directly or with pytest; direct runs
are the authoritative score.
"""

import sys

import pandas as pd

from excel_recipe_processor.core.stage_manager import StageManager


def frame_of_mb(target_mb: float) -> pd.DataFrame:
    """A DataFrame whose deep memory footprint is roughly target_mb."""
    cells = int(target_mb * 1024 * 1024 / 8)
    return pd.DataFrame({'v': range(cells)})


def test_peak_totals_and_overwrite():
    """Peak tracks the concurrent maximum; overwrite releases-then-adds."""
    print("\nTesting peak, totals, and overwrite accounting...")

    StageManager.initialize_stages(max_stages=30)
    baseline = StageManager.get_memory_stats()
    if any(baseline.values()):
        print(f"✗ counters not zeroed by initialize: {baseline}")
        return False
    print("✓ initialize zeroes the counters")

    StageManager.save_stage('stg_t_a', frame_of_mb(8), 'test')
    StageManager.save_stage('stg_t_b', frame_of_mb(4), 'test')
    at_two = StageManager.get_memory_stats()
    StageManager.delete_stage('stg_t_a')
    StageManager.save_stage('stg_t_c', frame_of_mb(2), 'test')
    after = StageManager.get_memory_stats()

    checks = [
        ('peak was the two-stage moment',
         10 < at_two['peak_concurrent_mb'] < 14
         and after['peak_concurrent_mb'] == at_two['peak_concurrent_mb']),
        ('current dropped below peak after the free',
         after['still_held_mb'] < after['peak_concurrent_mb']),
        ('allocated total keeps growing',
         after['total_allocated_mb'] > at_two['total_allocated_mb']),
        ('freed total recorded the delete',
         7 < after['total_freed_mb'] < 9),
    ]
    for name, ok in checks:
        if not ok:
            print(f"✗ {name}: {after}")
            return False
        print(f"✓ {name}")

    # Overwrite: stg_t_b re-saved larger must release the old 4 first
    before_overwrite = StageManager.get_memory_stats()
    StageManager.save_stage('stg_t_b', frame_of_mb(6), 'test', overwrite=True)
    overwritten = StageManager.get_memory_stats()
    grew = overwritten['still_held_mb'] - before_overwrite['still_held_mb']
    if not (1 < grew < 3):  # +6 new -4 released = ~+2, not +6
        print(f"✗ overwrite double-counted: current grew {grew:.1f} MB")
        return False
    if not (3 < overwritten['total_freed_mb'] - before_overwrite['total_freed_mb'] < 5):
        print("✗ overwrite did not record the released old frame")
        return False
    print("✓ overwrite releases the old frame before counting the new")

    StageManager.initialize_stages(max_stages=30)
    return True


def main():
    """Run all tests and report results."""
    print("Stage-memory accounting tests")
    print("=" * 50)

    tests = [
        test_peak_totals_and_overwrite,
    ]

    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                print(f"FAILED: {test.__name__}")
        except Exception as error:
            print(f"FAILED with exception: {test.__name__}: {error}")

    print("=" * 50)
    print(f"Final score: {passed}/{len(tests)} tests passed")
    return passed == len(tests)


if __name__ == '__main__':
    sys.exit(0 if main() else 1)

# End of file #
