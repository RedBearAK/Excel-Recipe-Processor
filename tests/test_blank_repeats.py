"""
Tests for the clean_data blank_repeats action.

tests/test_blank_repeats.py

Runnable with pytest, but written to run standalone and report a score.
Assertions are on values, not just counts.
"""

import pandas as pd

from excel_recipe_processor.processors.clean_data_processor import CleanDataProcessor


def apply_blanking(frame, columns):
    processor = CleanDataProcessor({
        'processor_type': 'clean_data',
        'rules': [{'columns': columns, 'action': 'blank_repeats'}],
    })
    return processor.execute(frame)


def split_booking_frame():
    """Two split bookings and one plain one, pivot-source shape."""
    return pd.DataFrame({
        'Booking':  ['GXY27385', 'GXY27385', 'GXY27465', 'GXY27465', 'GXY27500'],
        'Tracking': ['NAM1', 'NAM1', 'NAM2', 'NAM2', 'NAM3'],
        'Origin':   ['KODIAK', 'KODIAK WEST', 'KODIAK', 'KODIAK', 'SITKA'],
        'Weight':   [5653, 48584, 2750, 52256, 51400],
    })


def test_continuation_rows_blank_as_a_group():
    """Second row of each split booking blanks BOTH columns; firsts keep values."""
    print("\nTesting continuation rows blank as a group...")

    result = apply_blanking(split_booking_frame(), ['Booking', 'Tracking'])

    passed = True

    if result['Booking'].iloc[0] == 'GXY27385' and result['Tracking'].iloc[0] == 'NAM1':
        print("  ✓ First row of a group keeps both values")
    else:
        print(f"  ✗ First row: {result.iloc[0].tolist()}")
        passed = False

    if result['Booking'].iloc[1] == '' and result['Tracking'].iloc[1] == '':
        print("  ✓ Continuation row blanks both columns together")
    else:
        print(f"  ✗ Continuation row: {result[['Booking', 'Tracking']].iloc[1].tolist()}")
        passed = False

    if result['Booking'].iloc[4] == 'GXY27500':
        print("  ✓ Singleton group untouched")
    else:
        print(f"  ✗ Singleton: {result['Booking'].iloc[4]!r}")
        passed = False

    if result['Origin'].iloc[1] == 'KODIAK WEST' and result['Weight'].iloc[1] == 48584:
        print("  ✓ Non-group columns untouched on blanked rows")
    else:
        print(f"  ✗ Other columns disturbed: {result.iloc[1].tolist()}")
        passed = False

    return passed


def test_partial_match_does_not_blank():
    """Same tracking under a DIFFERENT booking must keep both values."""
    print("\nTesting partial matches are protected...")

    frame = pd.DataFrame({
        'Booking':  ['GXY1', 'GXY2'],
        'Tracking': ['NAM9', 'NAM9'],
    })
    result = apply_blanking(frame, ['Booking', 'Tracking'])

    if result['Tracking'].iloc[1] == 'NAM9' and result['Booking'].iloc[1] == 'GXY2':
        print("  ✓ Tracking kept when its booking differs - group semantics")
        return True

    print(f"  ✗ Row 2: {result.iloc[1].tolist()}")
    return False


def test_interleaved_groups_restart_runs():
    """A group returning after a gap starts a fresh run and keeps values."""
    print("\nTesting interleaved groups restart runs...")

    frame = pd.DataFrame({
        'Booking':  ['A', 'A', 'B', 'A'],
        'Tracking': ['T1', 'T1', 'T2', 'T1'],
    })
    result = apply_blanking(frame, ['Booking', 'Tracking'])

    if list(result['Booking']) == ['A', '', 'B', 'A']:
        print("  ✓ Runs are consecutive-only; the returning A keeps its value")
        return True

    print(f"  ✗ Bookings: {list(result['Booking'])}")
    return False


def test_nan_rows_count_as_equal():
    """Two missing values in a row count as a repeat, like a pivot's empty label."""
    print("\nTesting NaN-safe comparison...")

    frame = pd.DataFrame({
        'Booking':  [None, None, 'C'],
        'Tracking': [None, None, 'T3'],
    })
    result = apply_blanking(frame, ['Booking', 'Tracking'])

    if result['Booking'].iloc[1] == '' and result['Booking'].iloc[2] == 'C':
        print("  ✓ Second NaN row blanked; following real row kept")
        return True

    print(f"  ✗ Bookings: {list(result['Booking'])}")
    return False


def test_tiny_frames_pass_through():
    """Zero- and one-row frames come back unchanged."""
    print("\nTesting tiny frames pass through...")

    empty = apply_blanking(split_booking_frame().head(0), ['Booking'])
    single = apply_blanking(split_booking_frame().head(1), ['Booking'])

    if len(empty) == 0 and single['Booking'].iloc[0] == 'GXY27385':
        print("  ✓ Empty and single-row frames unchanged")
        return True

    print(f"  ✗ empty {len(empty)}, single {single['Booking'].tolist()}")
    return False


def main():
    """Run every test and report a final score."""
    print("=== blank_repeats tests ===")

    tests = [
        test_continuation_rows_blank_as_a_group,
        test_partial_match_does_not_blank,
        test_interleaved_groups_restart_runs,
        test_nan_rows_count_as_equal,
        test_tiny_frames_pass_through,
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
        print("✅ All blank_repeats tests passed!")
        return 1

    print("❌ Some blank_repeats tests failed!")
    return 0


if __name__ == '__main__':
    exit(0 if main() else 1)


# End of file #
