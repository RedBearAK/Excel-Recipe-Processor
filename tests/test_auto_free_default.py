"""
Tests for the auto_free_stages default (2026-09-04).

tests/test_auto_free_default.py

Runnable with pytest, but written to run standalone and report a score.
A recipe that says nothing gets auto-free; false opts out; true still
works; and under the default a stage actually frees after its last
consuming load.
"""

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from excel_recipe_processor.core.stage_manager import StageManager


def _recipe(extra_settings: dict | None = None) -> dict:
    """Two stages; the first is consumed twice, the second once."""
    settings = {
        'stages': [
            {'stage_name': 'stg_free_test_a', 'description': 'a', 'protected': False},
            {'stage_name': 'stg_free_test_b', 'description': 'b', 'protected': False},
        ],
    }
    settings.update(extra_settings or {})
    return {
        'settings': settings,
        'recipe': [
            {'processor_type': 'filter_data', 'source_stage': 'stg_free_test_a',
             'save_to_stage': 'stg_free_test_b', 'filters': []},
            {'processor_type': 'export_file', 'source_stage': 'stg_free_test_a',
             'output_file': 'x.xlsx'},
            {'processor_type': 'export_file', 'source_stage': 'stg_free_test_b',
             'output_file': 'y.xlsx'},
        ],
    }


def _fresh(recipe: dict) -> None:
    StageManager.cleanup_stages()
    StageManager.initialize_stages(max_stages=20)
    StageManager.declare_recipe_stages(recipe)


def test_default_is_on():
    """No setting at all: auto-free is on and the consumer scan ran."""
    print("\nTesting the default...")

    _fresh(_recipe())

    if not StageManager._auto_free:
        print("  ✗ auto-free off with no setting")
        return False
    print("  ✓ auto-free on with no setting")

    expected = StageManager._expected_uses
    if expected.get('stg_free_test_a') == 2 and expected.get('stg_free_test_b') == 1:
        print(f"  ✓ consumer scan counted a=2, b=1")
        return True

    print(f"  ✗ consumer scan gave {expected}")
    return False


def test_false_opts_out_and_true_still_works():
    """auto_free_stages: false disables it; true is the same as the default."""
    print("\nTesting the opt-out and the explicit opt-in...")

    _fresh(_recipe({'auto_free_stages': False}))
    if StageManager._auto_free or StageManager._expected_uses:
        print("  ✗ false did not opt out")
        return False
    print("  ✓ false opts out, no consumer scan")

    _fresh(_recipe({'auto_free_stages': True}))
    if not StageManager._auto_free:
        print("  ✗ true did not enable")
        return False
    print("  ✓ true enables")
    return True


def test_stage_frees_after_last_consuming_step_by_default():
    """Under the default, a stage frees when its last consuming STEP completes.

    Steps 0 and 1 both consume stg_free_test_a; loads do not free anything
    (consuming-step counting, 2026-08-26), the pipeline's step-complete
    hook does.
    """
    print("\nTesting that a stage frees under the default...")

    _fresh(_recipe())
    frame = pd.DataFrame({'x': [1, 2, 3]})
    StageManager.save_stage('stg_free_test_a', frame, description='a')

    StageManager.load_stage('stg_free_test_a')
    StageManager.load_stage('stg_free_test_a')
    if not StageManager.stage_exists('stg_free_test_a'):
        print("  ✗ freed on load; freeing belongs to step completion")
        return False
    print("  ✓ two loads free nothing")

    StageManager.auto_free_after_step(0)
    if not StageManager.stage_exists('stg_free_test_a'):
        print("  ✗ freed after the FIRST of two consuming steps")
        return False
    print("  ✓ alive after the first consuming step")

    StageManager.auto_free_after_step(1)
    if StageManager.stage_exists('stg_free_test_a'):
        print("  ✗ still alive after its last consuming step")
        return False
    print("  ✓ freed after its last consuming step")
    return True


def main():
    """Run every test and report a final score."""
    print("=== auto_free_stages default tests ===")

    tests = [
        test_default_is_on,
        test_false_opts_out_and_true_still_works,
        test_stage_frees_after_last_consuming_step_by_default,
    ]

    passed = 0
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as error:
            print(f"  ✗ {test_func.__name__} crashed: {error}")

    StageManager.cleanup_stages()
    print(f"\n=== Results: {passed}/{len(tests)} tests passed ===")
    return 0 if passed == len(tests) else 1


if __name__ == '__main__':
    exit(main())


# End of file #
