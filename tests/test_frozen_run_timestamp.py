"""
Test: the run timestamp is frozen at recipe load, not per substitution.

tests/test_frozen_run_timestamp.py

The output workbook name and the recipe-triggered log file both build
on {hour}{minute}{second}. They resolve at DIFFERENT moments of the
run (the log at the post-external-variables seam, the export path tens
of steps later), and they must land the SAME stamp - one provenance
pair, one clock. VariableSubstitution guarantees this by capturing
datetime.now() ONCE at construction; this test exists so a future
refactor to per-call now() cannot silently split the pair. Runnable
directly or with pytest; direct runs are the authoritative score.
"""

import sys
import time

from excel_recipe_processor.core.variable_substitution import (
    VariableSubstitution,
)


def test_timestamp_frozen_across_time():
    """Substitutions seconds apart carry the identical stamp."""
    print("\nTesting the frozen run timestamp...")

    substitution = VariableSubstitution()
    template = "{source_stem}_proc_{hour}{minute}{second}" \
        .replace("{source_stem}", "x")  # keep only the builtins

    first = substitution.substitute(template)
    time.sleep(1.1)  # guarantees a wall-clock second boundary can pass
    second = substitution.substitute(template)

    if first != second:
        print(f"✗ stamp drifted across the run: {first} vs {second}")
        return False
    frozen = substitution.now.strftime('%H%M%S')
    if not first.endswith(frozen):
        print(f"✗ stamp does not match the frozen clock: {first} vs {frozen}")
        return False
    print(f"✓ identical stamp 1.1s apart ({first}) - the workbook/log pair")
    print("  cannot split")
    return True


def main():
    """Run all tests and report results."""
    print("Frozen run-timestamp tests")
    print("=" * 50)

    tests = [
        test_timestamp_frozen_across_time,
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
