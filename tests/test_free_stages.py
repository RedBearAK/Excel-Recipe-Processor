"""
Tests for the free_stages processor.

tests/test_free_stages.py

Runnable with pytest, but written to run standalone and report a score.
"""

import pandas as pd

import excel_recipe_processor.core.pipeline  # registers processors

from excel_recipe_processor.core.stage_manager import StageManager
from excel_recipe_processor.processors.free_stages_processor import FreeStagesProcessor


def fresh_stages(protected_names=()):
    StageManager.initialize_stages(max_stages=20)
    decls = [{'stage_name': n, 'description': 'x', 'protected': n in protected_names}
             for n in ('stg_a', 'stg_b', 'stg_keep')]
    StageManager.declare_recipe_stages({'settings': {'stages': decls}})
    for n in ('stg_a', 'stg_b', 'stg_keep'):
        StageManager.save_stage(n, pd.DataFrame({'x': range(1000)}), overwrite=True)


def build(stages, **overrides):
    config = {'processor_type': 'free_stages', 'stages': stages}
    config.update(overrides)
    return FreeStagesProcessor(config)


def test_deletion_frees_only_the_listed():
    """Listed stages vanish; others remain loadable with data intact."""
    print("\nTesting listed stages are freed, others intact...")

    fresh_stages()
    build(['stg_a']).execute()

    passed = True

    if not StageManager.stage_exists('stg_a'):
        print("  ✓ stg_a gone")
    else:
        print("  ✗ stg_a still present")
        passed = False

    survivor = StageManager.load_stage('stg_b')
    if len(survivor) == 1000 and survivor['x'].iloc[999] == 999:
        print("  ✓ stg_b intact with correct values")
    else:
        print(f"  ✗ stg_b: {survivor.shape}")
        passed = False

    return passed


def test_protected_stage_refuses():
    """A protected stage survives and the step halts, naming it."""
    print("\nTesting protected stages refuse deletion...")

    fresh_stages(protected_names=('stg_keep',))

    try:
        build(['stg_keep']).execute()
        print("  ✗ Deletion of a protected stage succeeded")
        return False
    except Exception as error:
        if 'protected' in str(error) and StageManager.stage_exists('stg_keep'):
            print("  ✓ Refused with 'protected' in the error; stage intact")
            return True
        print(f"  ✗ Wrong outcome: {error}")
        return False


def test_missing_stage_default_and_skip():
    """Absent stage halts by default, is tolerated with on_missing: skip."""
    print("\nTesting missing-stage handling...")

    fresh_stages()
    passed = True

    try:
        build(['stg_typo']).execute()
        print("  ✗ Absent stage accepted by default")
        passed = False
    except Exception as error:
        if 'not found' in str(error):
            print("  ✓ Default halts, naming the stage")
        else:
            print(f"  ✗ Wrong error: {error}")
            passed = False

    build(['stg_typo'], on_missing='skip').execute()
    print("  ✓ skip mode proceeds")

    return passed


def test_freed_memory_is_reported():
    """The operation result mentions the count and megabytes."""
    print("\nTesting the freed-memory report...")

    fresh_stages()
    result = build(['stg_a', 'stg_b']).perform_file_operation()

    if 'freed 2 stage(s)' in result and 'MB' in result:
        print(f"  ✓ Report: {result!r}")
        return True

    print(f"  ✗ Report: {result!r}")
    return False


def test_invalid_config_rejected():
    """Missing stages list and bad on_missing fail at construction."""
    print("\nTesting configuration validation...")

    passed = True

    try:
        FreeStagesProcessor({'processor_type': 'free_stages'})
        print("  ✗ Absent stages list accepted")
        passed = False
    except Exception:
        print("  ✓ Absent stages list rejected")

    try:
        build(['stg_a'], on_missing='maybe')
        print("  ✗ on_missing 'maybe' accepted")
        passed = False
    except Exception:
        print("  ✓ Bad on_missing rejected")

    return passed


def main():
    """Run every test and report a final score."""
    print("=== free_stages tests ===")

    tests = [
        test_deletion_frees_only_the_listed,
        test_protected_stage_refuses,
        test_missing_stage_default_and_skip,
        test_freed_memory_is_reported,
        test_invalid_config_rejected,
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
        print("✅ All free_stages tests passed!")
        return 1

    print("❌ Some free_stages tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
