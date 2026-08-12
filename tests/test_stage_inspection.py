"""
Tests for the recipe development inspection helpers.

tests/test_stage_inspection.py

Runnable with pytest, but written to run standalone and report a score.
Covers row specification parsing and slicing for --dump-stage.
"""

import pandas as pd
import tempfile

from pathlib import Path

from excel_recipe_processor.core.stage_inspection import (
    apply_row_spec,
    describe_spec,
    validate_spec,
    parse_dump_argument,
    dump_stage_to_file,
    StageInspectionError,
    ELLIPSIS_MARKER,
)


def build_frame(rows: int = 100):
    """Build a frame whose values encode their own row number."""
    return pd.DataFrame({
        'id': range(1, rows + 1),
        'label': [f'row_{i}' for i in range(1, rows + 1)],
    })


def test_argument_parsing():
    """A --dump-stage argument splits into a name and an optional spec."""
    print("\nTesting argument parsing...")

    cases = [
        ('stg_enriched', ('stg_enriched', None)),
        ('stg_enriched:20', ('stg_enriched', '20')),
        ('stg_enriched:-20', ('stg_enriched', '-20')),
        ('stg_enriched:100-150', ('stg_enriched', '100-150')),
        ('stg_enriched:20,-20', ('stg_enriched', '20,-20')),
        ('  stg_enriched : 20 ', ('stg_enriched', '20')),
    ]

    passed = True

    for argument, expected in cases:
        result = parse_dump_argument(argument)
        if result == expected:
            print(f"  ✓ {argument!r:26} -> {result}")
        else:
            print(f"  ✗ {argument!r:26} -> {result}, expected {expected}")
            passed = False

    return passed


def test_head_spec():
    """A bare number takes the first N rows."""
    print("\nTesting head spec...")

    result = apply_row_spec(build_frame(100), '20')

    if len(result) == 20 and result['id'].iloc[0] == 1 and result['id'].iloc[-1] == 20:
        print("  ✓ '20' gave rows 1 to 20")
        return True

    print(f"  ✗ Got {len(result)} rows: {result['id'].iloc[0]}..{result['id'].iloc[-1]}")
    return False


def test_tail_spec():
    """A leading minus takes the last N rows."""
    print("\nTesting tail spec...")

    result = apply_row_spec(build_frame(100), '-20')

    if len(result) == 20 and result['id'].iloc[0] == 81 and result['id'].iloc[-1] == 100:
        print("  ✓ '-20' gave rows 81 to 100")
        return True

    print(f"  ✗ Got {len(result)} rows: {result['id'].iloc[0]}..{result['id'].iloc[-1]}")
    return False


def test_range_spec():
    """A range is 1-based and inclusive, matching how rows are counted."""
    print("\nTesting range spec...")

    result = apply_row_spec(build_frame(100), '40-45')

    if len(result) == 6 and result['id'].iloc[0] == 40 and result['id'].iloc[-1] == 45:
        print("  ✓ '40-45' gave 6 rows, 40 through 45 inclusive")
        return True

    print(f"  ✗ Got {len(result)} rows: {result['id'].iloc[0]}..{result['id'].iloc[-1]}")
    return False


def test_both_ends_spec():
    """Both ends with a visible gap marker between them."""
    print("\nTesting both-ends spec...")

    result = apply_row_spec(build_frame(100), '10,-2')

    passed = True

    if len(result) == 13:
        print("  ✓ 10 + marker + 2 = 13 rows")
    else:
        print(f"  ✗ Got {len(result)} rows")
        passed = False

    if str(result['id'].iloc[10]) == ELLIPSIS_MARKER:
        print("  ✓ Gap marked so it cannot read as contiguous")
    else:
        print(f"  ✗ Row 11 is {result['id'].iloc[10]!r}")
        passed = False

    if str(result['id'].iloc[-1]) == '100':
        print("  ✓ Tail is the real last row")
    else:
        print(f"  ✗ Last row is {result['id'].iloc[-1]!r}")
        passed = False

    return passed


def test_both_ends_smaller_than_frame():
    """No marker when the two halves already cover everything."""
    print("\nTesting both-ends on a short frame...")

    result = apply_row_spec(build_frame(5), '10,-10')

    if len(result) == 5 and ELLIPSIS_MARKER not in result['id'].astype(str).values:
        print("  ✓ Returned all 5 rows with no marker")
        return True

    print(f"  ✗ Got {len(result)} rows")
    return False


def test_none_spec_returns_everything():
    """Omitting the spec dumps the whole stage."""
    print("\nTesting an omitted spec...")

    frame = build_frame(37)
    result = apply_row_spec(frame, None)

    if len(result) == 37:
        print("  ✓ All 37 rows returned")
        return True

    print(f"  ✗ Got {len(result)} rows")
    return False


def test_bad_specs_rejected_early():
    """
    A typo must be caught before the recipe runs.

    Without early validation this surfaced at whichever step first produced the
    stage, after everything upstream had already executed.
    """
    print("\nTesting bad specs are rejected...")

    passed = True

    for spec in ['banana', '90-40', '0-10', '20-', '--5', '1,2,3']:
        try:
            validate_spec(spec)
            print(f"  ✗ {spec!r} was accepted")
            passed = False
        except StageInspectionError:
            print(f"  ✓ {spec!r} rejected")

    return passed


def test_good_specs_accepted():
    """Every documented form passes validation."""
    print("\nTesting good specs are accepted...")

    passed = True

    for spec in ['20', '-20', '100-150', '20,-20', '1-1', None]:
        try:
            validate_spec(spec)
            print(f"  ✓ {spec!r} accepted")
        except StageInspectionError as error:
            print(f"  ✗ {spec!r} rejected: {error}")
            passed = False

    return passed


def test_descriptions_are_readable():
    """The log line should say what was asked for in words."""
    print("\nTesting spec descriptions...")

    cases = [
        (None, 'all rows'),
        ('20', 'first 20 rows'),
        ('-20', 'last 20 rows'),
        ('100-150', 'rows 100 to 150'),
        ('20,-20', 'first 20 and last 20 rows'),
    ]

    passed = True

    for spec, expected in cases:
        result = describe_spec(spec)
        if result == expected:
            print(f"  ✓ {str(spec):10} -> {result}")
        else:
            print(f"  ✗ {str(spec):10} -> {result!r}, expected {expected!r}")
            passed = False

    return passed


def test_dump_writes_csv():
    """The dump lands on disk with the requested rows."""
    print("\nTesting the dump writes a file...")

    with tempfile.TemporaryDirectory() as temp_dir:
        path = dump_stage_to_file('stg_example', build_frame(50), '5', temp_dir)

        written = Path(path)

        if not written.exists():
            print(f"  ✗ Nothing written to {path}")
            return False

        result = pd.read_csv(written)

        if written.name == 'stg_example.csv' and len(result) == 5:
            print(f"  ✓ {written.name} holds 5 rows")
            return True

        print(f"  ✗ {written.name} holds {len(result)} rows")
        return False


def main():
    """Run every test and report a final score."""
    print("=== stage inspection tests ===")

    tests = [
        test_argument_parsing,
        test_head_spec,
        test_tail_spec,
        test_range_spec,
        test_both_ends_spec,
        test_both_ends_smaller_than_frame,
        test_none_spec_returns_everything,
        test_bad_specs_rejected_early,
        test_good_specs_accepted,
        test_descriptions_are_readable,
        test_dump_writes_csv,
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
        print("✅ All stage inspection tests passed!")
        return 1

    print("❌ Some stage inspection tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
